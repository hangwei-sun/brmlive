# 媒体整改候选版（2026-10-10，未部署）

Gemini `gemini-3.8-flash-high` 经 Antigravity 实际参与两轮方案/源码评审；Codex复核并落实。本地代理可能向外部模型转发，只提供选定源码，不发送凭证或生产媒体。

## 六项范围与验收边界

| 项目 | 已实现的候选代码 | 尚需验证 |
|---|---|---|
| 共享调度 | ASR/新闻导出/素材worker共用本机SQLite准入，CPU预算6、编码2、ASR1；每新闻片段归还，取消/超时清理 | Linux不同UID同组、journal权限、子进程继承锁、混合压力和任务公平性 |
| NVDEC | 编码器可选`ENCODER_DECODE=nvdec`，默认cpu，保留准确seek、不做GOP复制 | 3060首帧、非关键帧seek、声画同步、耗时与显存；不能依据参数即认定精确 |
| 画质/大小 | `ENCODER_PROFILE=capped`候选p5/CQ23/4M/6M/12M；默认legacy不变 | 同源静态/运动/字幕素材主观评审、SSIM/VMAF、文件大小；不将CQ等同CRF |
| 缓存 | 源SHA256校验/hardlink复用、PCM转写缓存、整期机器分条版本缓存；用户编辑和导出权限不共享 | 多人同源、模型/策略版本切换、配额/磁盘满、源变更与失败回退 |
| PHP分流 | 内部FastCGI鉴权+固定origin Nginx Range代理模板，仍每次检查token/user/employee/department | 实际Nginx PHP路由、401/403/416、撤销、跨用户、拖拽下载、媒体JWT刷新 |
| 恢复/清理/监控 | 可选导出重启重跑，源完整且同参数才恢复；周期清理，排队/编码/ASR阶段指标，GPU/多磁盘/缺录信号及监控timer模板 | supervisor先停止旧子进程、异常重启、挂载掉线；接告警渠道和监控展示仍未完成 |

## 上线前配置，不自动执行

- 先将此目录所需Python模块安装在同一`/opt/brm-media-runtime`。创建本地共享状态目录`/var/lib/brm-media-runtime`，组`brm-media-runtime`，权限2770；四个服务用户加入共享组。不可放到NFS。
- `media-resource.conf.in`分别作为服务drop-in候选。SQLite主文件660；各服务UMask0007保证journal同组可写。先以各UID做并发读写实测，不在请求线程临时切换全局umask。素材worker原有ProtectSystem需要ReadWritePaths放行该状态目录。
- `MEDIA_RESOURCE_ROOT`未配置时原并发限制保留；配置错误应阻断新增任务，不静默忽略预算。严格FIFO保留，队头阻塞是公平/吞吐取舍，后续由负载数据决定是否回填。
- 内核锁仍被进程/FFmpeg占用时，绝不可按时间强行释放槽位。监控长时间占用后人工定位；systemd KillMode=control-group负责服务重启清除旧子进程。
- 编码恢复开关`ENCODER_RECOVER_ON_RESTART=1`默认关闭。只恢复已有完整source且设置相同的queued/running任务，重做生成片段，重置耗时；中断上传不可恢复。控制端任务重启后的自动关联重试仍需进一步完善。
- 源缓存需同时启用control-api的`NEWS_SOURCE_CACHE=1`和encoder的`ENCODER_SOURCE_CACHE=1`。新旧协议默认兼容；配额压力优先删除nlink=1未使用对象，不删除活任务引用。Cache无公开下载接口。
- ASR需`ASR_CACHE_ROOT`、明确`ASR_CACHE_REVISION`；版本必须覆盖权重、提示词、库行为，模型同名变更也必须升版。`MEDIA_RUNTIME_PATH`指向共用模块目录。转写缓存默认关闭，TTL24小时、64MiB上限；损坏结果按未命中处理。
- 整期分条缓存需`NEWS_ANALYSIS_CACHE_ROOT`和`NEWS_ANALYSIS_CACHE_REVISION`。键含完整源内容、模型/服务位置、时长和策略修订，不含凭据。TTL24小时，最多128份，0600；相同内容合并同进程计算。清理当前为写入后执行，长期无任务时需运维定时器补清。
- `monitor.py`只读观测，可输出受保护JSON；GPU不可采样/磁盘不可用不是0占用/0空间。GPU、控制节点按各自磁盘路径配置，缺录查询只读SQLite。监控timer模板须替换用户/Python路径并配置读权限后才启用，不输出节目、员工或转写正文。告警信号不等于通知渠道已接通。
- 网关模板位于FastAdmin项目`tools/media_worker/nginx-live-recording-gateway.conf.in`，必须审阅真实站点正则location顺序、auth_request模块、FastCGI参数、PHPsocket、固定媒体origin。不要上传密钥文件。鉴权响应头仅内部使用，公开gatewayAuth没有专用FastCGI参数时404；浏览器不可看到媒体Bearer，不能缓存鉴权结果。

## 实机编解码测试

`encoder-service/validate_encoder.py <已有批准样本> --start <非关键帧切点秒> --duration 60 --output <全新测试目录>`生成CPU/NVENC/NVDEC以及码率候选对照，记录耗时/大小/流时间。最初3帧比较的是原始软硬解码帧，不是有损编码文件。测试不修改原始录制，但会使用GPU；未经维护测试安排不在生产执行。

至少覆盖主持人首字、节目字幕、运动画面、GOP边界前后；逐帧核对成片首3帧和末帧，并人工检查声画同步。A/B重复3次、交换顺序、排除其他GPU任务后才给速度/画质结论。当前没有新NVDEC效率或同画质压缩率成绩。

## 评审取舍

未采纳模型建议中的强制租约回收、删除原ASR单槽、在线线程切换umask、凭`-ss 0`保证硬解精度及任意SSIM固定阈值。二轮评审的缓存配额淘汰、FastCGI PATH_INFO及恢复指标问题已复核修正；推断性“客户端请求头能变成upstream响应头”等未作为已证实漏洞。

须先完成本地人工验证，用户明确发布后再备份/哈希/最小部署。README、现有密码和其他工作区修改保持不动。

## 当前本地检查结果

- 编码器11项、ASR14项、runtime11项（Linux孤儿fd测试1项跳过）；素材worker13项（需真实FFmpeg/FFprobe的3项跳过）。实际通过45项Python测试，4项跳过。
- Go control-api测试通过；真实FFmpeg相关5项因本地PATH不可用而跳过，并非通过硬件验收。
- PHP7.4对LiveRecording、LiveApiClient、LiveRecordingGateway和route的语法检查及网关路径/owner白名单测试通过。
- 改动文件的`git diff --check`通过；没有commit/push、没有线上写入或启用功能开关。
- 发布门禁仍未满足：本地人工确认、Linux混合用户/孤儿锁测试、实际Nginx鉴权/Range测试、3060编解码/画质A/B、告警渠道与control-api任务恢复联动。
