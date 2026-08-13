"""VLA CPU 后处理 — Argo Workflow 的第二阶段（普通 Pod，不需要 GPU）。

读取 GPU 阶段落盘的 *.json，执行：
    轨迹平滑 (Savitzky-Golay) -> 原子动作分割 -> LeRobot v2.0 最终导出

环境变量：
  VLA_INPUT_DIR   GPU 阶段输出目录，默认 /data/output/vla-gpu-pipeline
  VLA_OUTPUT_DIR  本阶段输出目录，默认 /data/output/vla-cpu-postprocess
"""
import glob
import os
import time

import ray
from ray.data import DataContext

from data_juicer.utils.constant import MetaKeys
from data_juicer.ops.mapper import (
    VideoHandMotionSmoothMapper,
    VideoAtomicActionSegmentMapper,
    ExportToLeRobotMapper,
)

INPUT_DIR = os.environ.get("VLA_INPUT_DIR", "/data/output/vla-gpu-pipeline")
OUTPUT_DIR = os.environ.get("VLA_OUTPUT_DIR", "/data/output/vla-cpu-postprocess")


def main():
    print("=== VLA CPU Post-Processing (Argo Step 2) ===", flush=True)
    DataContext.get_current().enable_fallback_to_arrow_object_ext_type = True
    ray.init()

    lerobot_dir = os.path.join(OUTPUT_DIR, "lerobot_dataset_smoothed")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    s_time = time.time()

    # 只匹配顶层 JSON，避免递归读到 lerobot_dataset/meta/ 下的元数据文件
    json_files = sorted(glob.glob(os.path.join(INPUT_DIR, "*.json")))
    if not json_files:
        raise SystemExit(f"No *.json found under {INPUT_DIR}; did the GPU stage finish?")

    ds = ray.data.read_json(json_files)
    print(f"Read {ds.count()} records from GPU pipeline output", flush=True)

    ds = ds.map_batches(
        VideoHandMotionSmoothMapper,
        fn_constructor_kwargs=dict(
            hand_action_field=MetaKeys.hand_action_tags,
            savgol_window=11,
            savgol_polyorder=3,
            outlier_velocity_threshold=5.0,
            smooth_joints=True,
            batch_mode=True,
        ),
        batch_size=1,
        num_cpus=1,
        batch_format="pyarrow",
    )
    ds = ds.map_batches(
        VideoAtomicActionSegmentMapper,
        fn_constructor_kwargs=dict(
            hand_action_field=MetaKeys.hand_action_tags,
            segment_field="atomic_action_segments",
            min_segment_frames=8,
            max_segment_frames=300,
            hand_type="both",
            batch_mode=True,
        ),
        batch_size=1,
        num_cpus=1,
        batch_format="pyarrow",
    )
    ds = ds.map_batches(
        ExportToLeRobotMapper,
        fn_constructor_kwargs=dict(
            output_dir=lerobot_dir,
            hand_action_field=MetaKeys.hand_action_tags,
            segment_field="atomic_action_segments",
            frame_field=MetaKeys.video_frames,
            fps=10,
            robot_type="egodex_hand",
            batch_mode=True,
        ),
        batch_size=1,
        num_cpus=1,
        batch_format="pyarrow",
    )

    ds.write_json(OUTPUT_DIR, force_ascii=False)
    ExportToLeRobotMapper.finalize_dataset(
        output_dir=lerobot_dir, fps=10, robot_type="egodex_hand"
    )

    print(f"CPU Post-Processing COMPLETE: {time.time() - s_time:.1f}s", flush=True)
    print(f"LeRobot smoothed dataset: {lerobot_dir}", flush=True)


if __name__ == "__main__":
    main()
