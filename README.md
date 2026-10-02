# 边缘数据接入平台

Go 实现的轻量边缘接入层，不依赖消息中间件或外部数据库。当前内置两种虚拟设备：

- `temperature_v1`：JSON 温湿度读数。
- `vibration_v1`：单行 CSV 振动读数。

## 架构

- `internal/model`：设备身份、原始消息、统一观测等核心模型。
- `internal/adapter`：协议适配接口与注册表。
- `internal/device`：两种虚拟设备及其消息编码、协议解码。
- `internal/store`：本地 JSONL 日志与重启恢复。
- `internal/hub`：注册、补传位点、去重、确认与身份更换流程。
- `cmd/edge-ingest`：无网络演示入口。

## 关键语义

- 存储单元是一条原子日志记录，同时包含原始消息、统一观测和该次序列推进。
- 每条记录使用 SHA-256 校验；重启时最后一条未写完或损坏的尾部记录不会计入完成。
- 只有日志写入和 `fsync` 成功后，`Receive` 才返回确认。
- 原始观测时间 `ObservedAt` 与平台接收时间 `ReceivedAt` 分开保存，不按观测时间重排。
- 序列水位按“身份 ID + 启动 ID”维护；设备重启产生新启动序列时不会删除或覆盖旧序列，也不会误删新序列。
- `Resume` 返回下一序列和当前启动内缺失序列，用于断线后的补传。
- 去重键为“身份 ID + 观测 ID”。重复消息保留原始报文并标记 `duplicate_of`，不重复写入统一观测。
- 身份更换会停用旧身份；同一观测 ID 在新身份下是独立观测，旧身份数据继续保留。
- 每条原始报文携带当时的 `format`。适配器升级后只影响新报文，历史观测仍保留落盘时的格式版本和数值，不会被重新换算。
- 当前注册表无法解释的报文先以 `pending` 原始记录原子落盘；补充适配器后调用重处理，只新增观测确认记录，不重复生成已确认观测。
- 已接收位点和已确认位点分开维护。同启动、同序号但原始负载哈希不同会被拒绝；确认记录通过原始报文 ID 和 SHA-256 回指，失败或半条记录不能推进确认位点。
- 同序号不同内容的冲突不会静默丢弃：拒绝的同时原子写入 `conflict` 证据记录，保存已存报文的 SHA-256 引用与 incoming 原始报文，重启后仍可通过 `Hub.Conflicts` 查询，且水位线不受影响。
- 解释规则可纠正：`Hub.Reinterpret(rule, filter)` 用新规则重新解释已存原始报文，按 `reinterpretation` 记录追加新观测与比较结果，原观测值保留不覆盖。
- 重新处理可按设备（`IdentityID`）与接收时间段（`From`/`To`）筛选；同一投递已被某规则解释过时自动跳过，中断后重跑即可续跑，不会重复生成已发布结果。
- 比较输出对每条重解释记录分类：`value_changed`（数值变化）、`unit_changed`（仅单位变化，如 `temperature_c`→`temperature_f`）、`annotation_changed`（仅解释说明/属性变化）、`unchanged`。
- 未知格式补充适配器后，`ReprocessPending` 的 `Scope` 说明本次补解释覆盖的设备、启动会话、序号区间与接收时间范围。
- 更正走"分支 + 正式采用"两段式：`Hub.OpenCorrection(rule, filter, operator, reason)` 只登记更正分支（含操作者、理由与基准修订号），原始观测始终只保存一份；`Hub.AdoptBranch` 才生成正式 `adoption` 记录使分支生效，`Hub.PreviewCorrection` 可按设备与时间范围比较分支解释与当前有效值。
- 正式采用/撤回后报告下游影响：`Hub.RegisterDownstream` 登记的引用被分为 `downstream_valid`（仍有效）与 `downstream_needs_review`（引用了被取代的解释，需要重审）。
- 两名操作者更正重叠范围时不存在"最后保存覆盖"：分支记录基准修订号，若另一人此后对重叠投递采用了不兼容解释（如不同单位），采用返回 `ErrCorrectionConflict` 且不写入任何状态；可用 `AdoptOptions.Narrow` 收窄后只采用不冲突部分，分支保持 `partially_adopted`，剩余部分可 `AbandonBranch` 放弃——放弃的分支仍保留来源（操作者、规则）与理由，且每条投递始终只有一套解释，不会混入两套不兼容单位。
- `Hub.RetractAdoption(adoptionID, filter, ...)` 支持局部撤回：按设备/时间范围撤回正式采用的一部分，受影响投递回退到此前解释，撤回本身也是带理由的正式记录。
- 中断后接续：分支、采用、撤回与下游引用全部随日志恢复；重启后需 `RegisterRule` 重新登记规则适配器才能预览或采用。`Hub.CurrentValue(s)` 返回每条投递的当前有效值及其完整溯源——原消息引用（含负载 SHA-256）与产生该值的采用决定（空表示原始解释）。

## 扩展设备

新增设备类型时实现 `adapter.Adapter`：

```go
type Adapter interface {
	Type() string
	Decode(msg model.RawMessage) (model.Observation, error)
}
```

然后注册到 `adapter.NewRegistry(...)`。存储、补传、去重和确认语义不需要修改。

## 验证

```bash
go test ./...
go vet ./...
go run ./cmd/edge-ingest -data /tmp/edge-ingest-demo
```

测试覆盖断线缺口补传、重复观测、重启双序列、乱序观测的双时间戳、身份更换、格式升级、未知格式重处理与范围说明、规则纠正重解释（数值/单位/说明分类、按设备与时间段筛选、中断续跑不重复）、同序号冲突证据保留、尾部半条记录恢复、更正分支采用与下游有效/重审划分、重叠更正的冲突拒绝与收窄采用及放弃理由保留、局部撤回回退、未知格式补解释与中断重开后的采用接续和当前值溯源。
