# 机器人数据处理方案（VLA 数据流水线 + eRDMA 就绪）部署文档

> 计算巢服务：`service-525a4efce8e4433e9c8c`（beta 版本 `v19-argo-ack-workflow-addon`）。
>
> **本版已将 eRDMA 能力合并进本服务：一套 GPU 节点池，既跑 VLA 数据流水线，又是 eRDMA 就绪的训练集群。**
>
> 已完成真机验证：
> - **真 L20N（ecs.ebmgn9gc.64xlarge，上海）**：eRDMA 体检 **10 项全 PASS**（两张网卡 / PORT_ACTIVE / MTU 4096 / NUMA 全部通过）、驱动 580.126.09 实机确认、节点上报 `nvidia.com/gpu:8` + `aliyun/erdma:400`、Demo A/B 全链路 Succeeded、data-juicer 六算子输出真实、Savitzky-Golay 平滑实测降抖动 45%~76%。
> - **A10 替代规格全新部署回归**：集群 + terway v1.17.5 + ack-workflow addon + eRDMA 组件 + 合并 App 全部原生装成，Demo A/B 跑通（体检在 A10 上 PASS 6 / FAIL 4，项 2～5 需真 L20N，属预期）。

本方案一键交付面向具身智能（VLA）的数据生产环境，内置两个可直接提交的 Demo 与一份 eRDMA 体检：

- **Demo A**：GPU 环境自检（驱动、CUDA、Data-Juicer 算子、存储卷挂载）
- **Demo B**：两阶段 VLA 数据流水线（第一人称视频 → LeRobot v2.0 数据集），由 Argo Workflows 编排、KubeRay 承载 GPU 阶段
- **eRDMA 体检**：部署完即可用一条 `kubectl logs` 看每台节点的 10 项 PASS/FAIL 汇总

## 一、方案架构

```
ACK 托管 Pro 集群（Terway-ENIIP）
├─ 节点池 sys-pool（主+次可用区打散）：N × 4c16g
│    ├ ACK eRDMA Controller（ofed + allocateAllDevices）
│    ├ Terway 网卡白名单 Job（eni_tag_filter=creator:terway，幂等）
│    └ Argo Workflows（ACK ack-workflow addon）/ Fluid / KubeRay / CSI
│
└─ 节点池 gpu-pool（固定主可用区）：M × L20N 裸金属（默认 ecs.ebmgn9g.64xlarge）
     ├ 云市场 eRDMA 镜像（cmjj00066236）
     ├ UserData 三步：升 eRDMA 驱动至 1.5.9 → 关闭 PCIe ACS → 落地体检脚本
     ├ 节点标签 robot-solution.aliyun.com/node-role=gpu（VLA Demo 靠它调度）
     ├ 固定驱动 ack.aliyun.com/nvidia-driver-version=580.126.09
     ├ 污点 nvidia.com/gpu=true:NoSchedule（隔离算力）
     └ eRDMA 体检 DaemonSet（逐节点 PASS/FAIL，readinessProbe 门禁）

存储（全部由服务自动创建，无需您提前准备）：
  OSS Bucket（服务创建）  dataset/ → PVC robot-dataset（RWX）    源视频
                          models/  → PVC robot-models（RWX）     HaWoR / MANO 权重
  NAS（服务创建）                  → PVC robot-output（RWX）      中间结果与最终数据集
  vla-ops 运维控制台                三个卷全挂载 + /materials 物料脚本
  Fluid                             缓存 OSS 上的模型目录，削减模型冷加载耗时
```

> **eRDMA 生效依赖编排顺序**：先建 CPU 管控节点池 → 装 eRDMA Controller 与 Terway 白名单 → 再拉起 GPU 池。若先建 GPU 池后装组件，需把节点移出再重新加入才能生效。本模板已用 `DependsOn` 锁死这个顺序。

数据流：`OSS(视频+模型) → GPU 阶段(RayJob) → NAS(*.json + LeRobot 原始集) → CPU 阶段 → NAS(最终平滑数据集)`

## 二、部署后操作（3 步）

资源栈的**输出**里已给出每一步可直接复制的命令。先获取 kubeconfig：

```bash
aliyun cs GET /k8s/<ClusterId>/user_config | jq -r .config > ~/.kube/config-robot
export KUBECONFIG=~/.kube/config-robot
```

### 第 1 步：在运维控制台备料（一条命令）

服务已在 `robot-demo` 命名空间部署了一个常驻的 **vla-ops 运维控制台**，三个卷都已挂好。模型权重与样例视频都在计算巢公开制品库里，**无需您自备任何数据**：

```bash
kubectl exec -it deploy/vla-ops -n robot-demo -- bash /materials/prepare-data.sh
```

脚本会拉取并校验三份物料（HaWoR 3.1 GB、MANO 165 MB、样例视频 12 MB），已存在的会自动跳过，可反复重跑。常用参数：

| 参数 | 作用 |
|---|---|
| `--check-only` | 只校验不下载 |
| `--region <r>` | 指定制品库地域（默认从 ECS 元数据自动探测）|
| `--public` | 内网端点不通时改走公网 |

想用自己的视频替掉样例数据时，Bucket 名见输出 `OssBucketName`（形如 `robot-data-xxxxxxxx`）：

```bash
aliyun oss cp -r ./videos/ oss://<Bucket>/dataset/ --endpoint oss-<region>.aliyuncs.com
```

### 第 2 步：看 eRDMA 体检与 GPU 资源

服务在每台 GPU 节点上跑了体检 DaemonSet。**实例 Deployed 时报告一定已完整**（readinessProbe 盯 sentinel 文件，不会看到半成品）：

```bash
kubectl logs -n erdma-check -l app=erdma-env-check --tail=-1 --prefix
kubectl get nodes -l robot-solution.aliyun.com/node-role=gpu -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.allocatable}{"\n"}{end}'
```

体检报告 A 段是 10 项 PASS/FAIL 判定 + 一行 `SUMMARY PASS=.. FAIL=.. SKIP=..`（B 段官方 `env_check.py`、C 段原始信息供排查）。真 L20N 上应 **10 项全 PASS**，且节点同时上报 `"nvidia.com/gpu":"8"` 与 `"aliyun/erdma":"400"`。

- 体检项 **2～5**（两张网卡 / PORT_ACTIVE / MTU 4096 / NUMA）依赖 L20N 出厂自带的 eRDMA 网卡，**用非 L20N 规格（如 A10）时必然 FAIL，属预期**。
- 节点命令**无输出** = GPU 数为 0（无配额时的正常态），配额到位后节点池改回 1 即可。
- 没有 `aliyun/erdma` = eRDMA 组件未生效；节点有但无 `nvidia.com/gpu` = 驱动还在装，等几分钟。

### 第 3 步：提交 Demo

Demo 清单与流水线 Python 脚本都已随部署下发（ConfigMap `demo-manifests` 与 `vla-pipeline-script`），**无需再手动 git clone 或建 ConfigMap**，直接提交即可（见下节）。需调整脚本时，`kubectl -n robot-demo edit cm vla-pipeline-script` 即可。

## 三、运行 Demo

### Demo A：GPU 环境自检（约 1 分钟）

```bash
kubectl -n robot-demo get cm demo-manifests -o jsonpath='{.data.01-gpu-check\.yaml}' | kubectl apply -f -
kubectl -n robot-demo logs -f gpu-check
```

依次输出 `nvidia-smi`、torch CUDA 可用性与卡数、Data-Juicer 算子导入结果、挂载目录内容。**这一步同时验证 OSS 卷能否真的挂载**，建议在跑 Demo B 前先过这关。

### Demo B：两阶段 VLA 数据流水线

```bash
kubectl -n robot-demo get cm demo-manifests -o jsonpath='{.data.02-vla-argo-workflow\.yaml}' | kubectl create -f -
argo -n robot-demo watch @latest
```

流水线阶段：

```
Step 1（GPU，RayJob，任务结束自动销毁 RayCluster）
  帧提取(CPU) → MoGe-2 相机标定(GPU) → HaWoR+MegaSaM(GPU) → 手部动作计算(CPU) → LeRobot 导出(CPU)
Step 2（CPU，普通 Pod）
  Savitzky-Golay 轨迹平滑 → 原子动作分割 → LeRobot 最终导出
```

最终数据集结构（LeRobot v2.0）：

```
lerobot_dataset_smoothed/
  data/chunk-000/episode_000000.parquet      # state(8维) / action(7维) / gripper
  videos/chunk-000/observation.images.image/episode_000000.mp4
  meta/{info.json, episodes.jsonl, tasks.jsonl, modality.json}
```

脚本行为可用环境变量调节（在 Workflow 的 `env` 中追加）：

| 变量 | 默认 | 说明 |
|------|------|------|
| `VLA_MAX_VIDEOS` | 0（不限） | 首次验证建议设 2，快速跑通 |
| `VLA_FRAME_NUM` | 20 | 每视频抽帧数，**必须 ≥ 8**，否则 MegaSaM 无法完成 Bundle Adjustment |
| `VLA_GPU_ACTORS` | 集群 GPU 卡数 | GPU Actor 并行度 |
| `VLA_SLIM_OUTPUT` | 0 | 置 1 时落盘前裁掉 depth 等大字段，规模化时可把落盘量降低一个数量级 |

## 四、排查清单（运行阶段）

| 现象 | 原因 | 处理 |
|------|------|------|
| RayJob 一直 Pending，submitter Pod `ImagePullBackOff` | KubeRay 自动创建的 submitter Pod 不继承 head/worker 的 `imagePullSecrets` | 内置清单已通过 `submitterPodTemplate` 显式指定 |
| Workflow 显示 Succeeded 但 MegaSaM 输出为空 | `:latest` + `IfNotPresent`，部分节点残留旧镜像 | 用固定 tag；首次部署新镜像时临时改 `Always` |
| Argo DAG 一直 Running，GPU 阶段实际已成功 | KubeRay validation bug：设了 `ttlSecondsAfterFinished` 时 `jobDeploymentStatus` 卡在 `ValidationFailed` | 内置清单已移除该字段，只保留 `shutdownAfterJobFinishes: true` |
| GPU 阶段报 `Forbidden: pods is forbidden` | Argo 的 wait sidecar 需要 pods 的 get/patch | 部署已下发对应 Role/RoleBinding |
| MegaSaM 报 `no kernel image is available` | 镜像内 CUDA 扩展的算力架构不匹配 | 重编 `droid_backends`/`lietorch_backends`，`TORCH_CUDA_ARCH_LIST` 含目标架构（L20 为 8.9） |
| MegaSaM 运行但 `cam_c2w` 为空 | DROID-SLAM 至少需要 8 帧 | `VLA_FRAME_NUM` ≥ 8，推荐 20 |
| HaWoR 阶段超时或 `ConnectionError` | 模型权重未备齐 | 按[第 1 步](#第-1-步在运维控制台备料一条命令)在运维控制台跑一次 `prepare-data.sh` |
| MoGe-2 相机标定阶段报 `LocalEntryNotFoundError` / HuggingFace 下载失败 | MoGe-2 权重在**运行时从 HF（hf-mirror.com）在线拉取**，未预置；HF 偶发不可达时该阶段失败（实测碰到过一次） | 重试 Workflow 即可（实测重试即成功）；HF 长期不通时在有网环境预拉 MoGe-2 权重放入 models/ 卷 |
| Pod 卡 ContainerCreating，事件报 OSS 挂载失败 | AccessKey 无权限，或 Bucket 前缀不存在 | 检查 `oss-secret` 与 RAM 权限；`dataset/`、`models/` 前缀由服务预建，勿删 |
| GPU Pod 一直 Pending | GPU 节点被污点隔离，负载缺 toleration | 内置清单已带 `nvidia.com/gpu` toleration；自定义负载需补上 |
