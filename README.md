# 本地函数执行平台

只运行编译进二进制的白名单函数，不执行任何来历不明的代码。请求接收、
槽位调度、结果记录三段解耦，新增函数只需在 `DefaultRegistry()` 注册，
自动获得并发限制、超时、取消与结果持久化的统一生命周期。

## 运行

```sh
go run ./cmd/server -addr :8080 -slots 4 -queue 16 -timeout 30s -store data/records.jsonl
```

## API

- `POST /invoke` `{"function":"add","args":{"a":1,"b":2}}` → `202 {"id":...}`；队列满返回 `429`，未知函数返回 `404`
- `GET /result/{id}` → 完整记录（状态、结果/错误、时间戳）
- `POST /cancel/{id}` → 请求取消；已结束返回 `409`
- `GET /functions` → 白名单函数列表
- `GET /records` → 全部记录

## 统一生命周期

`queued → running → succeeded | failed | timeout | cancelled`

- **并发限制**：固定 worker 池（每槽位一个 goroutine），结构上不可能超发。
- **有界队列**：队列满立即拒绝并记录为 `cancelled(queue full)`，绝不无限开任务。
- **超时/取消**：每次调用带 `context.WithTimeout`；终端状态通过 CAS 写入，
  超时函数的迟到结果会因状态已非 `running` 被丢弃，绝不会翻盘成成功。
- **重启恢复**：所有状态迁移追加写入 JSONL 存储。重启加载时，已完成的
  记录原样保留；仍处于 `queued/running` 的记录标记为 `unknown`
  （执行结果不确定），与已完成请求明确区分。

## 内置函数

`echo`、`add`、`sleep{ms}`、`fib{n}`。函数签名为
`func(ctx context.Context, args json.RawMessage) (any, error)`，
必须响应 `ctx` 取消才能获得完整的超时/取消语义。

## 测试

```sh
go test ./...        # 并发上限、队列满拒绝、超时迟到结果、取消、快慢混合槽位回收、重启恢复
go test -race ./...
```
