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

测试覆盖断线缺口补传、重复观测、重启双序列、乱序观测的双时间戳、身份更换和尾部半条记录恢复。
