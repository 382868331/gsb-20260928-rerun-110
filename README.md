# 边缘数据接入平台

第一轮只使用 Go 标准库和本地持久化日志，不依赖消息中间件。两种内置虚拟设备为温度设备（`temperature/v1`）和振动设备（`vibration/v1`）。

## 架构

- `internal/device`：生成两种虚拟设备帧、重启 BootID 和每启动周期递增序号。
- `internal/adapter`：协议适配层。新设备只新增适配器并在注册表登记，不改变存储模型。
- `internal/store`：原始消息与统一观测在同一个 CRC append-only 事务中落盘；只有 prepare 且缺少 commit 的记录恢复时不会生效。
- `internal/link`：在线时存储成功才返回成功；断连期间缓存，恢复后按设备、BootID、序号补传。
- `internal/model`：稳定设备身份使用硬件 ID，逻辑身份可更换并保留历史。

去重键包含硬件 ID、BootID 和序号，同时保留消息 ID 幂等；统一观测 ID 防止同一观测被重复写入。乱序数据分别保留 `ObservedAt` 与 `ReceivedAt`，不会按观测时间重排或覆盖接收时间。

## 验证

```bash
go test ./...
go run ./cmd/edge-ingest
```
