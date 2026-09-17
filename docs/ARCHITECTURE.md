# BYO-Redis 架构设计（ARCHITECTURE）

| 字段 | 值 |
|------|----|
| 对应需求 | `docs/REQUIREMENTS.md` |
| 运行时 | Python 3.11+ / asyncio 单进程事件循环 |
| 目标 | 模块边界清晰、可测、可扩展到验收清单全部能力 |

---

## 1. 设计原则

1. **单线程事件循环优先**：所有命令在 asyncio 事件循环内串行进入存储层，避免粗粒度锁；阻塞磁盘 IO 用 `asyncio.to_thread` 或显式异步策略隔离。
2. **字节流边界与业务边界分离**：`protocol` 只负责编解码；`commands` 只负责语义；`storage` 只负责内存数据结构与过期。
3. **持久化与复制为旁路钩子**：写路径统一经过 `Server`/`CommandContext` 的 post-hook，保证 AOF 与复制传播看到同一成功写集合。
4. **失败可见**：持久化失败要记日志并反映到命令结果（如 `SAVE`）；复制断开要可观测（日志 + INFO）。
5. **配置驱动角色**：同一份代码通过配置成为 master 或 replica。

---

## 2. 逻辑架构

```
                    ┌─────────────────────────────────────────┐
                    │                 Client                  │
                    │         (redis-cli / 测试客户端)          │
                    └─────────────────┬───────────────────────┘
                                      │ TCP / RESP2
                    ┌─────────────────▼───────────────────────┐
                    │              server                     │
                    │  connection accept · read loop · write  │
                    └───────────┬─────────────────────────────┘
                                │ bytes ↔ RESP values
                    ┌───────────▼─────────────────────────────┐
                    │             protocol                    │
                    │     RespParser / RespEncoder            │
                    └───────────┬─────────────────────────────┘
                                │ argv: list[bytes]
                    ┌───────────▼─────────────────────────────┐
                    │             commands                    │
                    │   registry · handlers · validation      │
                    └───────────┬─────────────────────────────┘
                                │
              ┌─────────────────┼─────────────────┐
              ▼                 ▼                 ▼
        ┌──────────┐    ┌─────────────┐    ┌─────────────┐
        │ storage  │    │ persistence │    │ replication │
        │  KV/TTL  │    │  RDB / AOF  │    │ master/repl │
        └──────────┘    └─────────────┘    └─────────────┘
```

写命令成功路径（概念）：

1. Handler 调用 `storage` 变更
2. 若成功且为写命令 → `await aof.append(argv)`；`always` 在回复前完成 fsync，失败返回 `MISCONF`
3. 若角色为 master → `replication.propagate(argv)`
4. 编码 RESP 回复写回客户端

---

## 3. 目录结构（必须）

所有路径相对仓库根下的 `byo-redis/`：

```text
byo-redis/
├── pyproject.toml                 # 项目元数据与依赖
├── README.md                      # 使用说明（实现阶段骨架 / release 完善）
├── LICENSE                        # integration 阶段
├── .gitignore
├── byo_redis/                     # 安装包（import 名）
│   ├── __init__.py                # __version__
│   ├── __main__.py                # python -m byo_redis
│   ├── cli.py                     # 参数解析 / 入口
│   ├── config.py                  # 配置数据类与加载
│   ├── server.py                  # asyncio.start_server 与连接调度
│   ├── protocol/
│   │   ├── __init__.py
│   │   ├── parser.py              # 增量 RESP 解析器
│   │   └── encoder.py             # RESP 编码工具
│   ├── commands/
│   │   ├── __init__.py
│   │   ├── registry.py            # 命令名 → handler
│   │   ├── base.py                # Context / 错误类型
│   │   ├── connection_cmds.py     # PING ECHO
│   │   ├── string_cmds.py         # SET GET EXPIRE TTL
│   │   ├── list_cmds.py           # LPUSH RPOP
│   │   ├── hash_cmds.py           # HSET HGETALL
│   │   ├── server_cmds.py         # INFO SAVE CONFIG SELECT DBSIZE DEL...
│   │   └── repl_cmds.py           # REPLCONF PSYNC（master 侧）
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── store.py               # Database / 键空间
│   │   ├── types.py               # RedisType 枚举与值包装
│   │   └── expiry.py              # TTL 惰性/主动过期
│   ├── persistence/
│   │   ├── __init__.py
│   │   ├── rdb.py                 # RDB dump/load
│   │   └── aof.py                 # AOF append/replay/fsync
│   └── replication/
│       ├── __init__.py
│       ├── master.py              # 管理 replicas、FULLRESYNC、propagate
│       └── replica.py             # 连接 master、加载 RDB、应用命令流
├── data/                          # 运行时数据（gitignore）
├── tests/
│   ├── unit/
│   │   ├── test_resp_parser.py
│   │   ├── test_commands_string.py
│   │   ├── test_commands_list.py
│   │   ├── test_commands_hash.py
│   │   └── test_expiry.py
│   ├── integration/
│   │   ├── test_server_ping.py
│   │   ├── test_rdb_recovery.py
│   │   ├── test_aof_replay.py
│   │   └── test_replication.py
│   └── conftest.py
└── docs/
    ├── REQUIREMENTS.md
    ├── ARCHITECTURE.md
    ├── ACCEPTANCE.md
    └── ...
```

> 包名使用 `byo_redis`（下划线）以符合 Python 导入规范；仓库目录可为 `byo-redis`。

---

## 4. 模块职责与边界

### 4.1 `server`

**职责**

- 调用 `asyncio.start_server`
- 为每个连接创建 `ClientConnection`：读缓冲、解析循环、写回
- 持有共享的 `Store`、`AOF`、`RDB`、`ReplicationManager`、`Config`
- 优雅关闭：取消 tasks、flush AOF、关闭 socket

**不负责**

- RESP 细粒度类型解析实现细节（委托 protocol）
- 具体命令业务逻辑

**关键接口（示意，非强制签名）**

```text
class RedisServer:
    async def start() -> None
    async def stop() -> None
    async def handle_client(reader, writer) -> None
```

### 4.2 `protocol`

**职责**

- `RespParser.feed(data: bytes) -> list[Any]`：消费缓冲，产出零或多个完整消息
- `encode_*`：simple string / error / integer / bulk / array / null bulk
- 解析错误类型：`ProtocolError`（由 server 决定断连）

**不负责**

- 命令分发、鉴权、业务校验

**解析状态机要点**

- 维护 `buffer: bytearray` 与读指针
- 支持嵌套 array（命令为 array of bulk）
- 半包时返回已完成消息列表，保留残余缓冲

### 4.3 `commands`

**职责**

- 注册表：`Dict[bytes, Callable]`（键为大写命令名）
- 参数个数校验
- 调用 storage / persistence / replication
- 返回已是 RESP 可编码的 Python 值，或直接返回 encoded `bytes`

**CommandContext 建议字段**

```text
store: Store
config: Config
server: RedisServer   # 用于 SAVE、复制等
connection: ClientConnection  # 标记是否为 replica 连接
is_replica_client: bool       # 来自复制链路的写可放行
```

**错误**

- 业务错误编码为 RESP Error，不抛到连接层（除非断言级 bug）

### 4.4 `storage`

**职责**

- 键 → `(type, value, expire_at|None)`
- 类型：`STRING` / `LIST` / `HASH`
- `get`/`set`/`delete`、list/hash 操作、TTL API
- lazy expire on access；可选 `active_expire_cycle`

**并发约定**

- 仅在事件循环线程触碰 Store；后台线程生成 RDB 时必须对 Store 做**同步快照拷贝**（深拷贝或在 loop 内序列化后交给线程写盘）

**不负责**

- 文件 IO、网络、RESP

### 4.5 `persistence`

#### RDB

- `dump(store, path) -> None`
- `load(path) -> Store` 或 `load_into(store, path)`
- 原子写：`path.tmp` → `os.replace`
- 格式：见 REQUIREMENTS；若简化格式，魔数建议 `BYOR` + version，并在 README 声明

#### AOF

- `append(argv: list[bytes])`
- `replay(path) -> Iterator[list[bytes]]` 或直接应用到 store
- fsync 策略任务（everysec 用周期 `fsync`）
- 重放时走与在线命令相同的 handler（`is_loading=True`，跳过再次 AOF/复制）

### 4.6 `replication`

#### Master

- 维护 `replicas: set[ReplicaLink]`
- 处理 `PSYNC`：生成/复用 `replid`，`offset`，对 store 做 RDB dump，发送：
  1. `+FULLRESYNC <replid> <offset>\r\n`
  2. `$<rdb_len>\r\n` + rdb_bytes（**注意**：官方实现 RDB 后可不带尾部 CRLF；需与自研 replica 解析一致并文档化）
  3. 之后的写命令原文传播
- `propagate(argv)`：编码为 RESP array 写入各 replica socket

#### Replica

- 启动后 `asyncio.open_connection(master)`
- 发送 `PING` / `REPLCONF listening-port` / `REPLCONF capa eof`（最小集）/ `PSYNC ? -1`
- 解析 FULLRESYNC 与 RDB，载入本地 store
- 持续读取后续命令并应用到 store（`is_replica_client=True`）
- 用户连接上的写命令返回 `-READONLY`

---

## 5. 数据模型

```text
Store
  entries: dict[bytes, KeyEntry]

KeyEntry
  type: enum { STRING, LIST, HASH }
  value:
    STRING -> bytes
    LIST   -> deque[bytes]
  HASH   -> dict[bytes, bytes]
  expire_at_ms: int | None   # Unix ms；None 永不过期
```

键与字段均以 `bytes` 存储，保证二进制安全。

---

## 6. 关键时序

### 6.1 普通命令

```text
Client → TCP → Server.read → Parser.feed → argv
      → Registry.dispatch → Handler → Store
      → (write?) AOF.append + Master.propagate
      → Encoder → Client
```

### 6.2 启动加载

```text
main → load Config
    → create empty Store
    → if aof_enabled and aof exists: AOF.replay → handlers(loading)
      else if rdb exists: RDB.load
    → if role=replica: spawn ReplicaClient task
    → start RedisServer
```

### 6.3 全量同步

```text
Replica                         Master
   |-- PING ------------------->|
   |<-- PONG -------------------|
   |-- REPLCONF ... ----------->|
   |<-- OK ---------------------|
   |-- PSYNC ? -1 ------------->|
   |                     dump RDB (snapshot)
   |<-- FULLRESYNC id off ------|
   |<-- $len + RDB bytes -------|
   |  load RDB locally          |
   |<-- subsequent write cmds --|
```

---

## 7. 错误与韧性

| 场景 | 行为 |
|------|------|
| 协议半包 | 继续等待 |
| 协议非法 | 日志 + 关闭连接 |
| 命令参数错误 | RESP Error，连接保持 |
| AOF 磁盘满 | 返回 `MISCONF`，AOF 标记不健康，后续写入不再伪成功 |
| RDB 加载损坏 | 启动失败（非零退出）或跳过并警告（默认：**失败退出**更安全） |
| 复制断开 | Replica 指数退避重连；Master 移除死连接 |
| 处理中异常 | 捕获、日志、对客户端 `-ERR internal error`（避免堆栈泄漏） |

---

## 8. 测试架构

| 层级 | 手段 | 焦点 |
|------|------|------|
| 单元 | pytest | parser 粘包半包、编解码、过期、命令纯逻辑（可用假 Ctx） |
| 集成 | 启真实 server（临时端口） | redis-py 或裸 asyncio 客户端发 RESP |
| 持久化 | tmp_path | SAVE 后杀进程语义用 stop+new instance 模拟 |
| 复制 | 双实例临时端口 | SET 后从节点 GET 一致 |

禁止测试依赖本机已安装的官方 `redis-server`；**允许**依赖系统 `redis-cli`（若存在）做手工/可选检测，自动化以纯 Python 客户端为准。

---

## 9. 扩展点（非本版本实现）

- RESP3 / HELLO
- AOF rewrite
- PSYNC 部分重同步 backlog
- 多 DB / AUTH / TLS
- Cluster

模块边界应允许日后在不改动 protocol 核心的前提下增加 command 模块。

---

## 10. 实现约束清单（工程师检查）

- [ ] 仅使用标准库 + 测试所需第三方（推荐 `pytest`；可选 `redis` 客户端库做集成测）
- [ ] 入口：`python -m byo_redis --port 6379`
- [ ] 类型注解覆盖公共函数与数据类
- [ ] 无全局可变单例隐蔽状态（显式注入 Server/Context）
- [ ] `data/` 自动创建
- [ ] Windows / POSIX 路径与 `os.replace` 行为验证（本工作区为 Windows，需可跑）
