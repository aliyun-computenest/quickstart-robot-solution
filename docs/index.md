# 机器人数据处理方案（VLA 数据流水线 + eRDMA 就绪）部署文档

> 计算巢服务：`service-525a4efce8e4433e9c8c`（beta 版本 `v13-drop-nvidia-driver-param`）。
>
> **本版已将 eRDMA 能力合并进本服务：一套 GPU 节点池，既跑 VLA 数据流水线，又是 eRDMA 就绪的训练集群。**
>
> 已在测试账号完成两路实跑验证：
> - **计算巢真实路径**（GPU 数=0，约 11 分钟）：Argo 1/1 + ack-fluid 1/1（部署物占位符正确解析）、Ray CRD 3 个、eRDMA Controller 2+2 Running、Terway 白名单 `{"creator":"terway"}` 生效、OSS 自动创建 RAM 用户/AK + 预检通过。
> - **GPU 节点侧**（A10 替代规格，GPU 数=1，约 16 分钟）：UserData 三步全部落地、GPU 上报、固定驱动标签与体检校验一致、体检报告首次即完整（PASS 6 / FAIL 4 / SKIP 0）、三个 VLA 卷 Bound、vla-ops 控制台 1/1。
>
> 两张 eRDMA 网卡的设备级校验（体检项 2～5：PORT_ACTIVE / MTU 4096 / NUMA / `aliyun/erdma` 上报）只能在真 L20N 上跑，待白名单到位后补。

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
│    └ Argo Workflows / Fluid / KubeRay / CSI
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

## 二、部署前置准备

存储资源（OSS Bucket、NAS）**由服务自动创建**，您不需要提前准备。部署前只需确认下面 3 项。

| # | 事项 | 说明 |
|---|------|------|
| 1 | **L20N 配额/白名单（最容易踩的坑）** | 默认 GPU 规格是 **L20N 裸金属 `ecs.ebmgn9g.64xlarge`**（只有它默认自带两张 eRDMA 网卡）。<br>⚠️ L20N 当前**全地域售罄且弹性配额默认为 0**，部署前必须先申请 L20N 配额/白名单，否则报 `InstanceTypeNoStock` 或 `QuotaExceed.ElasticQuota` 并整栈回滚。<br>⚠️ `DescribeAvailableResource` 显示 `WithStock` **不代表能创建**——它只查库存、不校验配额。<br>**未拿到配额时的用法**：把「GPU 节点数量」填 `0` 先部署控制面（集群 + eRDMA 组件 + 白名单 + 所有存储与工具都会就位，部署 100% 成功），拿到配额后在节点池把数量改成 1，体检 DaemonSet 会自动铺到新节点上。 |
| 2 | **可用区要有交集** | L20N 与管控节点规格的开服可用区**并不一致**，主可用区必须同时满足两者。NAS 可用区由服务自动选择，无需操心。详见下方[可用区实测表](#可用区实测表)。 |
| 3 | **镜像与模型权重已内置，无需自备** | VLA 流水线镜像用计算巢公开镜像 `compute-nest-registry.cn-hangzhou.cr.aliyuncs.com/public/vla-pipeline:torch2.7.0-cu128-20260729`（含 Data-Juicer + MoGe-2 + HaWoR + MegaSaM，固定 tag）。<br>HaWoR（3.1 GB）/ MANO（165 MB）权重与样例视频（12 MB）托管在计算巢公开制品库 `computenest-artifacts-<地域>/embodied-ai/`，**部署完成后在运维控制台跑一条命令即可备齐**（见第 1 步）。 |

### 可用区实测表

各地域实测结果（会随阿里云供给变化，部署前建议复核）：

| 地域 | 管控 `g8i` | 通用型 NAS |
|---|---|---|
| cn-hangzhou | b / i / j / k | f / g |
| cn-beijing | f / i / l | d / e / h / i / l |
| cn-shanghai | l / m / n / b / e | b / e / l |

L20N 裸金属（`ebmgn9g*`）的可用区随配额审批结果确定，申请白名单时一并跟阿里云确认；本服务开放地域为 **杭州 / 北京 / 上海**（eRDMA 云市场镜像只在这三地有镜像）。

**挑选方法**：主可用区需同时满足 L20N 与管控规格。NAS 可用区由服务自动选择（不再暴露参数），走 VPC 挂载点，与计算节点跨可用区也可用，仅增加少量延迟。

## 三、部署参数

服务共 **24 个参数**，其中通常只需要改 3 个：可用区（主/次）、节点登录密码、GPU 节点数量。

| 分组 | 参数 | 默认 | 说明 |
|---|---|---|---|
| 网络 | 主/次可用区 | — | **必填**。GPU 节点池只落主可用区；次可用区仅用于打散管控节点 |
| 网络 | VPC / 交换机 / 网段 | 新建 `/16` + `/20` | Terway-ENIIP 下节点与 Pod 共用交换机网段，建议 `/20` 及以上 |
| 管控节点池 | 规格 / 数量 | `ecs.g8i.xlarge` / 2 | 承载 Argo、KubeRay、Fluid、eRDMA Controller、CSI 等组件，不承载 GPU 计算 |
| 管控节点池 | 节点登录密码 | — | **必填** |
| GPU 节点池 | 规格 | `ecs.ebmgn9g.64xlarge` | ⚠️ 只有 L20N 裸金属（`ebmgn9g` / `ebmgn9gc` / `ebmgn9ge`）默认自带两张 eRDMA 网卡。**需先申请 L20N 配额/白名单** |
| GPU 节点池 | 数量 | 1 | **配额未到位时填 `0`**：控制面会完整就位且部署 100% 成功，拿到配额后改成 1 即可 |
| GPU 节点池 | eRDMA 云市场镜像 ID | 留空 | 留空自动用当前地域的 `cmjj00066236` 镜像。**部署账号必须先订阅该云市场镜像**，否则节点创建失败 |
| 存储 | OSS Bucket 选项 | `新建Bucket` | 新建时自动创建 Bucket；也可选「已有Bucket」复用自己的 |
| 存储 | OSS AK 选项 | `NewAK` | 默认自动创建 **权限仅限该 Bucket** 的 RAM 用户与 AccessKey，**无需您提供任何凭证**；选 `ExistAK` 才需自填 AK |
| 付费 | 付费类型 / 周期 | 按量付费 | 包年包月时才需填周期 |

已写死在模板里、不再作为参数暴露的项（避免填错就整栈失败）：

| 项 | 固定值 | 原因 |
|---|---|---|
| 节点镜像类型 | `AliyunLinux3ContainerOptimized` | ACK 1.34+ 用 containerd 2.x，要求 cgroup v2 镜像；普通 Alinux3 是 cgroup v1，会报 `does not support cgroup v2` |
| containerd 版本 | `2.1.9` | 必须是 ACK 当前为该 K8s 版本提供的版本 |
| GPU 节点系统盘 | 500 GiB | 流水线镜像大、本地缓存模型权重 |
| NVIDIA 驱动版本 | `580.126.09` | 官方 L20N + eRDMA 文档要求的版本；体检项 10 会校验实际装上的是否就是它 |
| eRDMA 驱动大包 | `erdma_installer-1.5.9` | ≥1.5.9 默认启用 MPCC 拥塞算法，且 NCCL 不再需要 `NCCL_GRAPH_FILE` |
| VLA 流水线镜像 | `public/vla-pipeline:torch2.7.0-cu128-20260729` | 计算巢公开镜像，固定 tag（`:latest` 会让部分节点残留旧镜像） |
| 共享输出卷 | 通用型 NAS | 服务自动创建 |
| 删除时一并删除存储 | `true` | 模板预建了 `dataset/`、`models/` 目录，Bucket 永远非空；若为 false 则实例永远删不掉。**删除实例前请自行备份需要保留的数据** |
| Demo 清单下发 | 始终下发 | 只下发 ConfigMap 与 RBAC，不自动提交任务 |

### 存储说明

存储不再有可选项，全部由服务自动创建、固定形态：

| 用途 | 后端 | PVC | 说明 |
|---|---|---|---|
| 源视频（只读输入） | OSS `dataset/` | `robot-dataset`（RWX） | ossfs 挂载 |
| 模型权重（只读输入） | OSS `models/` | `robot-models`（RWX） | ossfs 挂载；RWX 是为了让运维控制台能写入下载的权重 |
| 中间结果与最终数据集 | 通用型 NAS | `robot-output`（RWX） | 频繁写小文件，ossfs 不适合 |

⚠️ **NAS 文件系统数量有账号级配额（默认 20）**，实测遇到过 `exceeds the quota: creation(1) + usage(20) > quota(20)` 而直接建栈失败。部署前建议看一眼 NAS 控制台，清理残留的空 `cnfs-nas` 文件系统或提配额。

> 早期版本曾提供 CPFS 通用版 / CPFS 智算版 / 不使用共享存储 三个选项，现已移除：CPFS 通用版每地域只开服少数可用区且通常与 GPU 可用区不重叠，CPFS 智算版处于邀测无法编排创建，实际都不可用。

## 四、部署后操作（4 步）

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

### 第 1.5 步：看 eRDMA 体检报告（合并后新增）

服务在每台 GPU 节点上都跑了一个体检 DaemonSet。**实例状态变成 Deployed 时报告一定已完整**（容器配了 readinessProbe 盯 sentinel 文件，不会让你看到半成品）：

```bash
kubectl logs -n erdma-check -l app=erdma-env-check --tail=-1 --prefix
```

报告分三段：**A. 判定项**（10 项逐项 PASS/FAIL，末尾一行 `SUMMARY ... PASS=.. FAIL=.. SKIP=..`）、**B. 官方一键体检** `env_check.py -s egs_l20n`、**C. 原始信息**（ibv_devinfo / 拥塞控制 / GID / ACSCtl / 驱动版本）。

| # | 判定项 | 真 L20N 上的预期 |
|---|---|---|
| 1 | eRDMA 内核模块已加载 | PASS |
| 2 | 两张 eRDMA 设备存在 | PASS |
| 3 | 两张网卡均 PORT_ACTIVE | PASS |
| 4 | MTU 4096（巨型帧） | PASS |
| 5 | NUMA 分别绑定 0 与 1 | PASS（都为 0 说明辅助网卡挂载错） |
| 6 | PCIe ACS 已关闭（SrcValid-） | PASS |
| 7 | disable_pcie_acs 开机自启 | PASS |
| 8 | eadm 可用（驱动已升级到 1.5.9） | PASS |
| 9 | GPU 已被识别 | PASS |
| 10 | NVIDIA 驱动版本 == 固定的 580.126.09 | PASS |

> 注：项 2～5 依赖 L20N 出厂自带的两张 eRDMA 网卡。用非 L20N 规格（如 A10）部署时这 4 项必然 FAIL，属预期，不代表模板有问题。

还要确认节点上报了 eRDMA 资源：

```bash
kubectl get nodes -l robot-solution.aliyun.com/node-role=gpu -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.allocatable}{"\n"}{end}'
```

真 L20N 上应同时出现 `"nvidia.com/gpu":"8"` 与 `"aliyun/erdma":"400"`。没有 `aliyun/erdma` 说明 eRDMA 组件未生效。

### 第 2 步：确认 GPU 节点已就位

上一步的命令已经把节点资源打印出来了，这里只说两种异常：

- **没有任何输出** → GPU 节点数为 0（未申请到 L20N 配额时的正常状态）。到 ACK 节点池把数量改成 1 即可，体检 DaemonSet 会自动铺到新节点上。
- **有节点但没有 `nvidia.com/gpu`** → 节点刚 Ready、GPU 驱动仍在安装，等几分钟再看。

### 第 3 步：创建脚本 ConfigMap

```bash
git clone https://github.com/aliyun-computenest/quickstart-robot-solution.git
cd quickstart-robot-solution
kubectl -n robot-demo create cm vla-pipeline-script \
  --from-file=demo/vla_gpu_pipeline.py \
  --from-file=demo/vla_cpu_postprocess.py
```

### 第 4 步：提交 Demo

Demo 清单已随部署下发到 ConfigMap `demo-manifests`，见下一节。

## 五、运行 Demo

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

## 六、排查清单

以下均为实测出现过的问题。

### 部署阶段

| 现象 | 原因 | 处理 |
|------|------|------|
| 节点池报 `QuotaExceed.ElasticQuota ... limit 0` | GPU 实例族弹性配额为 0 | 配额中心提申请；或先把 GPU 节点数填 0 部署 |
| 节点池报 `InstanceTypeNoStock` | 该可用区该规格实际无货（与库存查询结果可能不一致） | 换可用区或换规格 |
| 节点池报 `does not support cgroup v2` | 节点镜像类型选了 cgroup v1 的 `AliyunLinux3` | 用 `AliyunLinux3ContainerOptimized` |
| 节点池创建失败、提示 runtime 版本 | `containerd` 版本过期 | 用 ACK 当前提供的版本（现为 2.1.9） |
| NAS 创建报 `InvaildZone.NotExist` | NAS 可用区不支持该 NAS 类型 | 本服务已不再暴露 NAS 可用区参数、由 NAS 自行选择，正常不会再遇到；若出现请提工单 |
| Helm 应用超时 30 分钟、namespace 已建但无工作负载 | Chart 从 GitHub 直链拉取，国内不可达 | 用计算巢 Chart 部署物（默认） |
| Argo 的 `crd-install` Pod `ImagePullBackOff` | `crds.full=true` 的钩子 Job 用 `registry.k8s.io/kubectl`（Google 域，国内 i/o timeout） | 模板已设 `crds.full: false`，改用精简 CRD 由 Helm 直接 apply |
| Fluid 报 `function "lookup" not defined` | 社区版 fluid ≥1.0.0 的 chart 用了 Helm `lookup`，渲染器不支持 | 模板已改用 **ack-fluid**（≤1.0.4 不含 lookup，镜像走 region 内网前缀）|
| `Properties.ChartUrl: "oci://compute-nest-chart-registry..." does not match pattern "^(http\|https)://"` | 计算巢 HelmChart 部署物占位符解析出的是 `oci://`，而 `ALIYUN::CS::ClusterHelmApplication.ChartUrl` 只接受 http/https | 模板已改用 `MODULE::ACS::ComputeNest::FluxOciHelmDeploy`（属性名是 `HelmChartUrl` / `ReleaseName`，并配 `DockerConfigJson` 占位符）|
| 多个资源同时报 `RAM policy Forbidden for action cs:DescribeUserPermission` | **用服务商账号去创建服务实例**，该 RAM 用户没有这个权限（连模块内部资源也报，与模板无关）| 改用**被授权的消费者账号**部署；切勿因此把 `RolePolicy` 改成 `None`（会引出下一行的 403）|
| KubeRay 组件报 `403 Forbidden` | ROS 以服务实例身份读 cluster-scoped 的 CRD 时权限不足 | 模板已设 `RolePolicy: EnsureAdminRoleAndBinding` |
| 改完模板重新 import，报错跟之前一模一样 | `import` 只更新 draft，beta 仍指旧模板 | import 后再执行一次 `aliyun computenestsupplier PreLaunchService --ServiceId <sid>` |
| 部署很久后才发现组件其实没装上 | 只要 API 调用返回就算成功 | 模板已给 Helm 应用与 addon 加 `WaitUntil`，把失败前移到部署阶段 |

### 运行阶段

| 现象 | 原因 | 处理 |
|------|------|------|
| RayJob 一直 Pending，submitter Pod `ImagePullBackOff` | KubeRay 自动创建的 submitter Pod 不继承 head/worker 的 `imagePullSecrets` | 内置清单已通过 `submitterPodTemplate` 显式指定 |
| Workflow 显示 Succeeded 但 MegaSaM 输出为空 | `:latest` + `IfNotPresent`，部分节点残留旧镜像 | 用固定 tag；首次部署新镜像时临时改 `Always` |
| Argo DAG 一直 Running，GPU 阶段实际已成功 | KubeRay validation bug：设了 `ttlSecondsAfterFinished` 时 `jobDeploymentStatus` 卡在 `ValidationFailed` | 内置清单已移除该字段，只保留 `shutdownAfterJobFinishes: true` |
| GPU 阶段报 `Forbidden: pods is forbidden` | Argo 的 wait sidecar 需要 pods 的 get/patch | 部署已下发对应 Role/RoleBinding |
| MegaSaM 报 `no kernel image is available` | 镜像内 CUDA 扩展的算力架构不匹配 | 重编 `droid_backends`/`lietorch_backends`，`TORCH_CUDA_ARCH_LIST` 含目标架构（L20 为 8.9） |
| MegaSaM 运行但 `cam_c2w` 为空 | DROID-SLAM 至少需要 8 帧 | `VLA_FRAME_NUM` ≥ 8，推荐 20 |
| HaWoR 阶段超时或 `ConnectionError` | 模型权重未备齐 | 按[第 1 步](#第-1-步在运维控制台备料一条命令)在运维控制台跑一次 `prepare-data.sh` |
| Pod 卡 ContainerCreating，事件报 OSS 挂载失败 | AccessKey 无权限，或 Bucket 前缀不存在 | 检查 `oss-secret` 与 RAM 权限；`dataset/`、`models/` 前缀由服务预建，勿删 |
| GPU Pod 一直 Pending | GPU 节点被污点隔离，负载缺 toleration | 内置清单已带 `nvidia.com/gpu` toleration；自定义负载需补上 |

## 七、成本与清理

- **GPU 节点是主要成本**。Demo B 的 RayCluster 配了 `shutdownAfterJobFinishes: true`，任务结束即销毁 Ray 集群，但**节点不会自动释放**；长期不用请把 GPU 节点池数量调到 0。
- 通用型 NAS 按实际用量计费。
- **删除服务实例的注意事项**：
  - 本服务把「删除实例时一并删除存储」**写死为 `true`**：删实例会连带删掉 OSS Bucket 与 NAS（因为模板预建了 `dataset/`、`models/` 目录，Bucket 永远非空，若不强删则实例永远删不掉）。**删除前请自行备份需要保留的数据集与产出。**
  - ACK 集群的 CSI 组件可能自动创建一个 `cnfs-nas` 文件系统，**删除集群时不会回收**，它会占住交换机导致删除失败，并持续占用 NAS 配额。若实例删除卡在交换机依赖，请到 NAS 控制台删除对应的空 `cnfs-nas` 文件系统后重试。
  - 云监控（CloudMonitor）等服务托管的弹性网卡也可能残留并阻塞交换机删除，这类 ENI 无法自行删除，需等其释放或提工单。

## 八、已知限制

1. **L20N 配额是硬门槛**：默认 GPU 规格 `ecs.ebmgn9g.64xlarge` 当前全地域售罄、弹性配额默认 0，必须先申请白名单。未拿到配额时把 GPU 节点数填 `0`，控制面可正常交付。
2. **eRDMA 的设备级能力只在 L20N 上成立**：体检项 2～5（两张网卡 / PORT_ACTIVE / MTU 4096 / NUMA 绑定）依赖 L20N 出厂自带的两张 eRDMA 网卡。换成其他 GPU 规格时这 4 项会 FAIL，其余 6 项仍会 PASS。
3. **地域限制为杭州 / 北京 / 上海**：eRDMA 云市场镜像目前只在这三个地域有对应镜像 ID（原先开放的香港已移除）。
4. **多机 NCCL over eRDMA 尚未实测**：需要 2 台 L20N 才能验证跨节点集合通信走 IB（在训练日志里搜 `Using network IB`）。训练镜像内还必须包含 RDMA 用户态库（`libibverbs`、`librdmacm`、`ibverbs-providers`），否则节点侧正常但容器内用不到 eRDMA。
5. **NAS 文件系统数量有账号级配额**（默认 20）。超限时报 `exceeds the quota`，需清理残留的空 `cnfs-nas` 文件系统或提配额。
6. **节点 Ready 早于 UserData 跑完**：eRDMA 驱动 DKMS 编译约需 15 分钟。服务已用 readinessProbe 把这个时差封住（实例 Deployed 时体检报告一定完整），但如果你绕过服务直接看节点，可能看到驱动仍在编译。
