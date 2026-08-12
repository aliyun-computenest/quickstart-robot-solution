"""VLA GPU Pipeline — Argo Workflow + RayJob 的 GPU 阶段。

五个 Stage 在 Ray Data 上流式执行：
    帧提取 (CPU) -> MoGe-2 相机标定 (GPU) -> HaWoR + MegaSaM (GPU)
    -> 手部动作计算 (CPU) -> LeRobot v2.0 导出 (CPU)

与原始单机脚本的差异：
  * 视频列表从挂载的数据集目录扫描得到，不再硬编码；
  * GPU Actor 数量按集群实际 GPU 卡数自动放大，用于整机并行；
  * 输出目录、模型目录、并行度均可通过环境变量覆盖。

环境变量：
  VLA_DATASET_DIR   输入视频目录，默认 /data/dataset
  VLA_OUTPUT_DIR    输出目录，默认 /data/output/vla-gpu-pipeline
  VLA_MODEL_DIR     模型根目录，默认 /data/models
  VLA_MAX_VIDEOS    最多处理多少个视频，默认 0（不限制）
  VLA_GPU_ACTORS    GPU Actor 数量，默认取集群 GPU 卡数
  VLA_FRAME_NUM     每个视频抽帧数，默认 20（MegaSaM 至少需要 8 帧）
  VLA_SLIM_OUTPUT   置 1 时落盘前裁剪大字段，规模化时显著降低 I/O
"""
import glob
import os
import time

import ray
from ray.data import ActorPoolStrategy, DataContext

from data_juicer.utils.constant import Fields, MetaKeys
from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.ops.mapper import (
    VideoExtractFramesMapper,
    VideoCameraCalibrationMogeMapper,
    VideoHandReconstructionHaworMapper,
    VideoCameraPoseMegaSaMMapper,
    VideoHandActionComputeMapper,
    ExportToLeRobotMapper,
)

DATASET_DIR = os.environ.get("VLA_DATASET_DIR", "/data/dataset")
OUTPUT_DIR = os.environ.get("VLA_OUTPUT_DIR", "/data/output/vla-gpu-pipeline")
MODEL_ROOT = os.environ.get("VLA_MODEL_DIR", "/data/models")
MAX_VIDEOS = int(os.environ.get("VLA_MAX_VIDEOS", "0"))
FRAME_NUM = int(os.environ.get("VLA_FRAME_NUM", "20"))
SLIM_OUTPUT = os.environ.get("VLA_SLIM_OUTPUT", "0") == "1"

HAWOR_DIR = os.path.join(MODEL_ROOT, "hawor")
MANO_DIR = os.path.join(MODEL_ROOT, "mano")
VIDEO_KEY = "videos"

print("=== VLA GPU Pipeline (Argo + RayCluster) ===", flush=True)


@OPERATORS.register_module("video_hawor_megasam_combined_mapper")
class VideoHaWorMegaSaMCombinedMapper(Mapper):
    """把 HaWoR 与 MegaSaM 合并到同一个 GPU Actor，共享 GPU 上下文。

    MegaSaM（DROID-SLAM）失败时通过 try/except 保留 HaWoR 的手部重建结果，
    不让已完成的 GPU 计算白跑。
    """

    _accelerator = "cuda"

    def __init__(
        self,
        hawor_model_path="hawor.ckpt",
        hawor_config_path="model_config.yaml",
        hawor_detector_path="detector.pt",
        mano_right_path="MANO_RIGHT.pkl",
        mano_left_path="MANO_LEFT.pkl",
        camera_calibration_field=MetaKeys.camera_calibration_moge_tags,
        hawor_tag_field=MetaKeys.hand_reconstruction_hawor_tags,
        frame_field=MetaKeys.video_frames,
        hawor_thresh=0.2,
        megasam_tag_field=MetaKeys.video_camera_pose_tags,
        megasam_max_frames=1000,
        megasam_droid_buffer=1024,
        megasam_save_dir=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._hawor_kwargs = dict(
            hawor_model_path=hawor_model_path,
            hawor_config_path=hawor_config_path,
            hawor_detector_path=hawor_detector_path,
            mano_right_path=mano_right_path,
            mano_left_path=mano_left_path,
            camera_calibration_field=camera_calibration_field,
            tag_field_name=hawor_tag_field,
            frame_field=frame_field,
            thresh=hawor_thresh,
            batch_mode=True,
            skip_op_error=kwargs.get("skip_op_error", False),
        )
        self._megasam_kwargs = dict(
            tag_field_name=megasam_tag_field,
            camera_calibration_field=camera_calibration_field,
            frame_field=frame_field,
            max_frames=megasam_max_frames,
            droid_buffer=megasam_droid_buffer,
            save_dir=megasam_save_dir,
            batch_mode=True,
            skip_op_error=kwargs.get("skip_op_error", False),
        )
        self._hawor_op = None
        self._megasam_op = None

    def _ensure_ops(self):
        if self._hawor_op is None:
            self._hawor_op = VideoHandReconstructionHaworMapper(**self._hawor_kwargs)
        if self._megasam_op is None:
            self._megasam_op = VideoCameraPoseMegaSaMMapper(**self._megasam_kwargs)

    def process_single(self, sample=None, rank=None):
        from loguru import logger as _logger

        self._ensure_ops()
        t0 = time.time()
        sample = self._hawor_op.process_single(sample, rank=rank)
        t1 = time.time()
        _logger.info(f"HaWoR completed in {t1 - t0:.1f}s")
        try:
            sample = self._megasam_op.process_single(sample, rank=rank)
            _logger.info(f"MegaSaM completed in {time.time() - t1:.1f}s")
        except Exception as e:  # noqa: BLE001 - 保留 HaWoR 结果优先于中断整条流水线
            _logger.error(f"MegaSaM failed (HaWoR preserved): {e}")
            megasam_field = self._megasam_kwargs.get(
                "tag_field_name", MetaKeys.video_camera_pose_tags
            )
            if Fields.meta not in sample:
                sample[Fields.meta] = {}
            n = len(
                sample.get(
                    self._hawor_kwargs.get("frame_field", MetaKeys.video_frames), []
                )
            )
            sample[Fields.meta][megasam_field] = [
                {"cam_c2w": [], "cam_w2c": []} for _ in range(max(1, n))
            ]
        return sample


def discover_videos():
    patterns = ("**/*.mp4", "**/*.MP4", "**/*.mov")
    paths = []
    for p in patterns:
        paths.extend(glob.glob(os.path.join(DATASET_DIR, p), recursive=True))
    paths = sorted(set(paths))
    if MAX_VIDEOS > 0:
        paths = paths[:MAX_VIDEOS]
    return paths


def main():
    DataContext.get_current().enable_fallback_to_arrow_object_ext_type = True
    ray.init(address="auto")
    resources = ray.cluster_resources()
    print(f"Ray cluster resources: {resources}", flush=True)

    gpu_actors = int(os.environ.get("VLA_GPU_ACTORS", "0")) or max(
        1, int(resources.get("GPU", 1))
    )
    print(f"GPU actors: {gpu_actors}", flush=True)

    lerobot_output_dir = os.path.join(OUTPUT_DIR, "lerobot_dataset")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    videos = discover_videos()
    if not videos:
        raise SystemExit(
            f"No video found under {DATASET_DIR}. "
            "Upload the source videos to oss://<bucket>/dataset/ first."
        )
    print(f"Processing {len(videos)} videos with {gpu_actors} GPU actors", flush=True)

    samples = [{VIDEO_KEY: [p], "text": "", Fields.meta: {}} for p in videos]
    ds = ray.data.from_items(samples)
    s_time = time.time()

    # Stage 1: 帧提取（CPU）。frame_num 必须 >= 8，否则 MegaSaM 无法完成 Bundle Adjustment
    ds = ds.map_batches(
        VideoExtractFramesMapper,
        fn_constructor_kwargs=dict(
            frame_sampling_method="uniform",
            frame_num=FRAME_NUM,
            video_backend="ffmpeg",
            output_format="path",
            frame_dir=os.path.join(OUTPUT_DIR, "frames"),
            frame_field=MetaKeys.video_frames,
            legacy_split_by_text_token=False,
            batch_mode=True,
            video_key=VIDEO_KEY,
        ),
        batch_size=1,
        num_cpus=1,
        batch_format="pyarrow",
    )

    # Stage 2: 相机标定 MoGe-2（GPU）。depth map 是整条流水线最大的中间产物
    ds = ds.map_batches(
        VideoCameraCalibrationMogeMapper,
        fn_constructor_kwargs=dict(
            model_path="Ruicheng/moge-2-vitl",
            tag_field_name=MetaKeys.camera_calibration_moge_tags,
            frame_field=MetaKeys.video_frames,
            output_depth=True,
            output_points=False,
            output_mask=False,
            output_intrinsics=True,
            output_hfov=True,
            save_dir=os.path.join(OUTPUT_DIR, "moge_arrays"),
            batch_mode=True,
        ),
        batch_size=1,
        num_gpus=0.5,
        batch_format="pyarrow",
        compute=ActorPoolStrategy(min_size=1, max_size=gpu_actors),
    )

    # Stage 3: HaWoR + MegaSaM（GPU，合并 Actor）。MegaSaM 占整体耗时约 8 成
    ds = ds.map_batches(
        VideoHaWorMegaSaMCombinedMapper,
        fn_constructor_kwargs=dict(
            camera_calibration_field=MetaKeys.camera_calibration_moge_tags,
            hawor_tag_field=MetaKeys.hand_reconstruction_hawor_tags,
            megasam_tag_field=MetaKeys.video_camera_pose_tags,
            hawor_model_path=os.path.join(HAWOR_DIR, "hawor.ckpt"),
            hawor_config_path=os.path.join(HAWOR_DIR, "model_config.yaml"),
            hawor_detector_path=os.path.join(HAWOR_DIR, "detector.pt"),
            mano_right_path=os.path.join(MANO_DIR, "MANO_RIGHT.pkl"),
            mano_left_path=os.path.join(MANO_DIR, "MANO_LEFT.pkl"),
            frame_field=MetaKeys.video_frames,
            megasam_max_frames=1000,
            megasam_save_dir=os.path.join(OUTPUT_DIR, "megasam_arrays"),
            batch_mode=True,
            skip_op_error=False,
        ),
        batch_size=1,
        num_gpus=0.5,
        batch_format="pyarrow",
        compute=ActorPoolStrategy(min_size=1, max_size=gpu_actors),
    )

    # Stage 4: 手部动作计算（CPU）。依赖 Stage 2-3 的 Arrow 嵌套 struct，因此留在 GPU 阶段
    ds = ds.map_batches(
        VideoHandActionComputeMapper,
        fn_constructor_kwargs=dict(
            hand_reconstruction_field=MetaKeys.hand_reconstruction_hawor_tags,
            camera_pose_field=MetaKeys.video_camera_pose_tags,
            tag_field_name=MetaKeys.hand_action_tags,
            hand_type="both",
            batch_mode=True,
        ),
        batch_size=1,
        num_cpus=1,
        batch_format="pyarrow",
    )

    # Stage 5: 导出 LeRobot v2.0（CPU，中间产物）
    ds = ds.map_batches(
        ExportToLeRobotMapper,
        fn_constructor_kwargs=dict(
            output_dir=lerobot_output_dir,
            hand_action_field=MetaKeys.hand_action_tags,
            frame_field=MetaKeys.video_frames,
            video_key=VIDEO_KEY,
            fps=10,
            robot_type="egodex_hand",
            batch_mode=True,
        ),
        batch_size=1,
        num_cpus=1,
        batch_format="pyarrow",
    )

    if SLIM_OUTPUT:
        # 落盘前裁掉 CPU 阶段不需要的大字段（depth map / MANO 原始参数 / c2w 矩阵），
        # 规模化时可把落盘量从数十 MB/2 视频降到约 1 MB/2 视频
        columns = [c for c in ("video_frames", "hand_action_tags", "videos") if c in ds.columns()]
        print(f"Slim output enabled, keeping columns: {columns}", flush=True)
        ds = ds.select_columns(columns)

    ds.write_json(OUTPUT_DIR, force_ascii=False)
    ExportToLeRobotMapper.finalize_dataset(
        output_dir=lerobot_output_dir, fps=10, robot_type="egodex_hand"
    )

    total = time.time() - s_time
    print(f"GPU Pipeline COMPLETE: {total:.1f}s ({total / 60:.1f}min)", flush=True)
    print(f"LeRobot output: {lerobot_output_dir}", flush=True)


if __name__ == "__main__":
    main()
