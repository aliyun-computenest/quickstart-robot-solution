# 机器人数据处理方案（VLA 数据流水线 + eRDMA 就绪）

面向具身智能（VLA）数据生产 **与 eRDMA 训练** 的阿里云计算巢服务：一键交付 **ACK 托管 Pro 集群（Terway）+ 一套 L20N GPU 节点池 + OSS/NAS 分层存储 + Fluid 数据加速**，内置由 **Argo Workflows + KubeRay** 编排的两阶段数据流水线（第一人称视频 → LeRobot v2.0 数据集），并把 **eRDMA 就绪**（升驱动 1.5.9 / 关 PCIe ACS / Terway 网卡白名单 / 逐节点体检）做进同一套 GPU 池。

> 商家侧服务：`service-525a4efce8e4433e9c8c`（beta `v13-drop-nvidia-driver-param`，已授权测试账号 DeployableIncludeBeta）。
> 已在测试账号两路实跑验证：计算巢真实路径（Argo/Fluid 占位符解析、eRDMA 组件、Terway 白名单、OSS 自动授权+预检）+ GPU 节点侧（A10 替代规格：UserData 三步、固定驱动校验、体检报告首次即完整）。
> 两张 eRDMA 网卡的设备级校验待真 L20N 白名单到位后补。

查看服务实例部署在线文档，请访问 [服务实例部署文档](https://aliyun-computenest.github.io/quickstart-robot-solution)

## 仓库结构

```
.computenest/
  config.yaml                  计算巢服务定义（含 Helm Chart 部署物）
  ros_templates/template.yaml  ROS 编排模板（集群 / 双节点池 / 存储 / 组件 / Demo 清单）
  resources/
    icons/                     服务图标
    artifact_resources/helm_chart/
      ack-fluid-1.0.4.tgz      Fluid（ACK 发行版，当前使用）
      argo-workflows-1.0.23.tgz Argo Workflows
      fluid-1.0.8.tgz          社区版 Fluid（备查，未被引用）
demo/
  vla_gpu_pipeline.py          Demo B：GPU 阶段（帧提取 → MoGe-2 → HaWoR+MegaSaM → 动作计算 → LeRobot 导出）
  vla_cpu_postprocess.py       Demo B：CPU 阶段（轨迹平滑 → 原子动作分割 → 最终导出）
docs/index.md                  部署与运维文档
```

## 交付要点

| 能力 | 实现 |
|------|------|
| VPC / 双交换机 | 新建或使用已有，双可用区打散管控节点 |
| ACK 集群 | 裸 `ALIYUN::CS::ManagedKubernetesCluster`（托管 Pro）+ Terway-ENIIP，两个节点池均自建 |
| 节点池 | 镜像类型与 containerd 版本参数化（cgroup v2 要求）；GPU 池带驱动标签与污点隔离 |
| 存储 | **OSS Bucket 与 NAS 均由模板创建**（含 `dataset/` `models/` 前缀预建）；可选 CPFS 通用版 / 智算版；带存储删除保护开关 |
| 组件 | KubeRay Operator（ACK addon）、Argo Workflows 与 ack-Fluid（计算巢 OCI chart + Flux 部署） |
| 可观测性 | 关键组件均配 `WaitUntil`，把“装失败但报成功”前置到部署阶段 |
| Demo | A：GPU 环境与存储卷自检；B：两阶段 VLA 数据流水线 |

部署前请务必阅读文档的 **部署前置准备**：**GPU 弹性配额（可能为 0）**、可用区需有交集、镜像 tag 与 CUDA 架构、HaWoR/MANO 权重自备。

## 文档本地预览

本文档通过 [MkDocs](https://github.com/mkdocs/mkdocs) 生成，请参考[使用文档](https://www.mkdocs.org/getting-started/#installation)。

```shell
pip install mkdocs
pip install --upgrade mkdocs-aliyun-computenest
mkdocs serve
```

本地在浏览器打开 [http://localhost:8000/](http://localhost:8000/) 预览。
