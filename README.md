# Local Function Execution Platform

Go 标准库实现的本地函数执行平台。它只运行进程内注册表中的有限内置函数，不加载脚本、插件或任何外部提交的可执行代码。

## 运行

```bash
go run . -addr :8080 -slots 2 -queue 4 -data data/state.json
```

内置函数：

- `echo`：返回 `{"text": "..."}`。
- `add`：返回两个数字的 `sum`。
- `wait`：支持上下文取消的等待函数，最大 30 秒。

## API

- `GET /v1/functions`：列出内置函数。
- `POST /v1/invocations`：提交调用，请求形如 `{"function":"add","argument":{"a":1,"b":2},"timeout_ms":1000}`。
- `GET /v1/invocations/{id}`：读取持久化调用记录。
- `POST /v1/invocations/{id}/cancel`：取消排队中的调用，或请求取消运行中的可中断函数。

槽位由固定数量 worker 控制；另有有界等待队列。运行槽位和队列都占满时，接口返回 `503`，状态记录为 `rejected`，不会继续创建执行任务。

状态使用原子替换的 JSON 文件持久化。终态包括 `succeeded`、`failed`、`timed_out`、`canceled`、`rejected`。重启时上次处于 `running` 的请求无法确认外部观察结果，会明确标记为 `uncertain`；已完成记录保持不变，仍排队的记录会重新进入有界队列。

超时或取消后，函数晚到的结果只会被忽略，不能把终态覆盖成成功。新增函数应接收 `context.Context` 并及时响应取消，沿用同一套提交、排队、执行、终态记录生命周期。
