# Performance baseline

该基线用于发现版本间明显性能回退，不代表生产容量承诺。

## Environment

- Date: 2026-09-17
- OS: Windows
- Python: 3.11
- Network: `127.0.0.1` loopback
- Server: BYO-Redis v0.2.1, AOF disabled
- Workload: 2,000 requests, 20 concurrent connections, one request at a time per connection

## Results

| Command | Throughput | p50 | p95 | p99 |
|---------|------------|-----|-----|-----|
| PING | 20,857.97 req/s | 0.843 ms | 1.406 ms | 1.858 ms |
| SET | 18,950.79 req/s | 0.979 ms | 1.536 ms | 1.736 ms |
| GET | 21,610.29 req/s | 0.812 ms | 1.411 ms | 1.692 ms |

## Reproduce

先启动本地实例：

```bash
python -m byo_redis --port 6398 --dir ./benchmark-data --no-aof
```

再分别运行：

```bash
python -m tools.benchmark --port 6398 --requests 2000 --concurrency 20 --command ping
python -m tools.benchmark --port 6398 --requests 2000 --concurrency 20 --command set
python -m tools.benchmark --port 6398 --requests 2000 --concurrency 20 --command get
```

启用认证时，工具从 `BYO_REDIS_PASSWORD` 环境变量读取密码，不接受命令行明文密码。

## Interpretation

- 结果包含 Python 客户端、事件循环和本机 TCP 往返开销。
- AOF `always`、跨主机网络、大值、流水线和混合读写会产生不同结果。
- 正式容量规划需要在目标硬件、真实数据规模和持久化策略下进行长时间压测。
