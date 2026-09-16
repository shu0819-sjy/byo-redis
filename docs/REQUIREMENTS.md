# BYO-Redis 产品需求规格（REQUIREMENTS）

| 字段 | 值 |
|------|----|
| 产品名 | BYO-Redis（Build-Your-Own-Redis） |
| 版本目标 | v0.1.0（可发布工业级子集） |
| 技术栈 | Python 3.11+ / `asyncio` / `asyncio.start_server` / 字节流 RESP 解析 |
| 协议 | RESP2（与 `redis-cli` 兼容） |
| 文档语言 | 中文为主，关键术语保留英文 |
| 状态 | Draft for implementation |

---

## 1. 产品目标

实现一个**可运行、可验证、可发布到 GitHub** 的 Redis 协议兼容子集服务器，覆盖：

1. RESP2 请求/响应解析（字节流增量解析）
2. String：`SET` / `GET` / `EXPIRE`
3. List：`LPUSH` / `RPOP`
4. Hash：`HSET` / `HGETALL`
5. RDB 快照持久化与启动加载（崩溃后可恢复）
6. AOF 追加日志与启动重放
7. 主从复制（Replica 最终与 Master 关键数据一致）

验收底线（团队目标原文）：

- `redis-cli` 可连接并执行上述命令
- 崩溃后 RDB 可恢复
- 主从数据一致

质量要求：模块边界清晰、类型注解、可测试、错误处理明确、文档齐全，达到工业级可维护与可发布水平（非玩具 Demo）。

---

## 2. 功能范围（In Scope）

### 2.1 网络与协议（W1 RESP）

| ID | 能力 | 说明 |
|----|------|------|
| F-NET-01 | TCP Server | 使用 `asyncio.start_server` 监听可配置 host/port（默认 `127.0.0.1:6379`） |
| F-NET-02 | 多连接并发 | 每个客户端连接独立 `asyncio` Task；连接间不互相阻塞 |
| F-NET-03 | RESP2 解析 | 从字节流增量解析 Array / Bulk String / Simple String / Error / Integer / Null Bulk |
| F-NET-04 | RESP2 编码 | 正确编码回复；命令请求按 Array of Bulk Strings 解析 |
| F-NET-05 | Pipeline | 同一连接上连续多条命令按序处理并按序回复（至少正确处理粘包/半包） |
| F-NET-06 | 连接生命周期 | 客户端断开时清理连接资源；畸形协议可关闭连接或返回错误后继续（见错误策略） |

### 2.2 辅助命令（连通性 / 运维基础）

| ID | 命令 | 语义摘要 |
|----|------|----------|
| F-CMD-PING | `PING [message]` | 无参返回 `+PONG`；有参返回对应 bulk/simple |
| F-CMD-ECHO | `ECHO message` | 原样返回 message（Bulk String） |
| F-CMD-COMMAND | `COMMAND`（最小） | 可返回空数组 `*0\r\n` 或已实现命令列表；至少不导致 `redis-cli` 握手失败 |
| F-CMD-INFO | `INFO [section]` | 返回含 `role:master|slave`、`redis_version` 等关键字段的 Bulk String（复制验收需要） |
| F-CMD-REPLCONF | `REPLCONF ...` | 主从握手所需子集（见复制） |
| F-CMD-PSYNC | `PSYNC ...` | 全量同步入口（见复制） |
| F-CMD-WAIT | （可选） | 本版本**不要求** |
| F-CMD-SELECT | `SELECT index` | 仅支持 DB `0`；其他返回错误或忽略策略见命令契约 |
| F-CMD-CONFIG | `CONFIG GET/SET`（最小） | 至少支持读取 `dir` / `dbfilename`（复制传 RDB 场景常用）；写可选 |
| F-CMD-SAVE / BGSAVE | 触发 RDB | 至少提供一种同步或异步快照触发方式（命令名可定为 `SAVE`；`BGSAVE` 可用 asyncio 任务模拟） |
| F-CMD-DBSIZE | （可选） | 推荐实现，便于验收 |
| F-CMD-KEYS / TYPE | （可选） | 不作为硬性验收，但调试有用 |
| F-CMD-DEL | （推荐） | 删除键；利于测试与 AOF 语义完整 |

> 说明：辅助命令以实现主验收为目标，不扩展成完整 Redis 管理面。

### 2.3 String（W2）

| ID | 命令 | 必须行为 |
|----|------|----------|
| F-STR-SET | `SET key value [EX seconds]` | 设置字符串；可选 `EX` 设置秒级过期；成功 `+OK` |
| F-STR-GET | `GET key` | 存在且未过期返回 Bulk String；不存在/已过期返回 Null Bulk `$-1\r\n` |
| F-STR-EXPIRE | `EXPIRE key seconds` | 对已存在键设置 TTL（秒）；成功 `:1`，键不存在 `:0` |
| F-STR-TTL | `TTL key`（推荐） | 无过期 `-1`；不存在 `-2`；否则剩余秒数 |

过期语义：

- 过期时间以服务器单调/墙上时钟为准（文档约定使用 `time.time()` 秒/毫秒精度）
- **惰性删除（lazy expire）必须**：读/写触及键时若已过期则视为不存在并删除
- **主动过期（active expire）推荐**：后台周期性抽样清理，避免只写不读导致内存泄漏
- `SET` 默认覆盖旧值并清除旧 TTL，除非使用 `EX` 同时设置新 TTL
- 类型错误：对非 string 键执行 `GET` 返回 `-WRONGTYPE ...`

### 2.4 List（W3）

| ID | 命令 | 必须行为 |
|----|------|----------|
| F-LIST-LPUSH | `LPUSH key element [element ...]` | 从左侧推入一个或多个元素；返回推入后列表长度（Integer） |
| F-LIST-RPOP | `RPOP key` | 从右侧弹出一个元素；空/不存在返回 Null Bulk |

约束：

- 键不存在时 `LPUSH` 创建 list；`RPOP` 对不存在键返回 null
- 对非 list 键操作返回 `-WRONGTYPE`
- 多元素 `LPUSH` 按参数从左到右依次推入（与 Redis 一致：最终最左侧是最后一个参数）

### 2.5 Hash（W4）

| ID | 命令 | 必须行为 |
|----|------|----------|
| F-HASH-HSET | `HSET key field value [field value ...]` | 设置一个或多个 field；返回**新增** field 数量（Integer） |
| F-HASH-HGETALL | `HGETALL key` | 返回 `[field, value, ...]` 扁平 Array；不存在返回空 Array `*0\r\n` |

约束：

- 对非 hash 键返回 `-WRONGTYPE`
- field 覆盖已存在值时不计入“新增”

### 2.6 RDB 快照

| ID | 能力 | 说明 |
|----|------|------|
| F-RDB-01 | 保存 | 将当前 DB0 的键值（含 TTL）写入 RDB 文件 |
| F-RDB-02 | 加载 | 进程启动时若 RDB 存在则加载；已过期键丢弃 |
| F-RDB-03 | 崩溃恢复 | 写入成功后的 RDB，在进程被杀/崩溃重启后可恢复数据 |
| F-RDB-04 | 格式策略 | **优先**：兼容 Redis RDB 子集（magic `REDIS` + version + 本产品用到的 string/list/hash 编码）。**允许**：若工期紧张，采用文档化的“BYO-RDB”简化二进制/JSON 快照，但必须自洽可恢复，并在 README 标明**非官方 Redis RDB 互通** |
| F-RDB-05 | 原子替换 | 写临时文件再 `os.replace`，避免半截文件 |
| F-RDB-06 | 触发 | 启动加载；`SAVE`/`BGSAVE`；可选定时；主从全量同步前生成 |

推荐默认路径：`./data/dump.rdb`（可通过配置修改 `dir` + `dbfilename`）。

### 2.7 AOF（W33）

| ID | 能力 | 说明 |
|----|------|------|
| F-AOF-01 | 追加 | 写命令成功后以 RESP Array 形式追加到 AOF |
| F-AOF-02 | 重放 | 启动时按配置加载：若 AOF 开启且文件存在，以 AOF 为准重放（或 RDB+AOF 混合策略见配置） |
| F-AOF-03 | fsync 策略 | 至少支持 `always` / `everysec` / `no` 之一；默认 `everysec` |
| F-AOF-04 | 记录范围 | 记录变更类命令：`SET`/`EXPIRE`/`LPUSH`/`RPOP`/`HSET`/`DEL`（若实现）等；不记录纯读命令 |
| F-AOF-05 | 损坏处理 | 尾部截断/半条命令：启动时跳过损坏尾部或报错退出（需在日志明确）；不得静默加载错误状态 |
| F-AOF-06 | Rewrite（可选） | v0.1 可不实现 AOF rewrite；若未实现，文档说明膨胀风险 |

推荐默认路径：`./data/appendonly.aof`。

**启动加载优先级（必须写死在配置契约中）**：

1. 若 `aof_enabled=true` 且 AOF 文件非空 → 重放 AOF（可忽略同目录旧 RDB，或先载 RDB 再仅重放 AOF 增量——v0.1 选择其一并文档化；**推荐：AOF-only 当 AOF 开启**）
2. 否则若 RDB 存在 → 加载 RDB
3. 否则空库启动

### 2.8 主从复制（W21）

| ID | 能力 | 说明 |
|----|------|------|
| F-REPL-01 | 角色 | 实例可配置为 `master` 或 `replica`（slave） |
| F-REPL-02 | 握手 | Replica 连接 Master 后完成最小握手：`PING` → `REPLCONF` → `PSYNC` |
| F-REPL-03 | 全量同步 | Master 对 `PSYNC ? -1`（或等价）执行 FULLRESYNC：发送 replication id + offset，再发送 RDB（Bulk String 空长度头后跟 RDB 字节流，兼容常见 redis 复制传输方式），随后持续传播写命令 |
| F-REPL-04 | 命令传播 | Master 在写命令成功后，将相同 RESP 命令转发给所有在线 Replica |
| F-REPL-05 | 一致性 | 在网络稳定、全量同步完成后，对验收键集合，Replica 与 Master 读结果一致 |
| F-REPL-06 | 只读 Replica | Replica 默认拒绝用户写命令（返回 `-READONLY ...`），但接受来自 Master 的复制流写入 |
| F-REPL-07 | INFO role | `INFO replication`（或 `INFO`）中可观察到 `role:master` / `role:slave` |

v0.1 **不要求**：部分重同步（psync2 backlog）、磁盘less、多级链式复制、故障自动升主、Sentinel/Cluster。

---

## 3. 非目标（Out of Scope / Non-Goals）

以下内容**明确不做**（本版本及本任务）：

1. 完整 Redis 命令全集（SET 的 NX/XX/GET、阻塞列表、Sorted Set、Stream、Pub/Sub、事务 MULTI/EXEC、Lua 等）
2. Redis Cluster / Sentinel / Cluster bus
3. RESP3 / `HELLO` 协议升级（可忽略或对 HELLO 返回简单错误）
4. ACL / TLS / 密码认证（`AUTH` 可不实现；若收到可返回未实现错误）
5. 多数据库（仅 DB0）
6. 内存淘汰策略（maxmemory / eviction）
7. 官方 Redis RDB 全版本互操作保证（若采用简化格式，必须声明）
8. 生产级性能压测达标承诺（吞吐/延迟 SLA）
9. 本任务内直接推送 GitHub（由 integration / 用户确认）
10. 实现代码不在本需求任务产出（architect 只出文档）

---

## 4. RESP 协议契约

参考：[Redis serialization protocol specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)

### 4.1 传输

- TCP 流式连接；分隔符 CRLF `\r\n`
- 客户端命令：`*<argc>\r\n` + 若干 `$<len>\r\n<data>\r\n`
- 命令名大小写不敏感；参数保持二进制安全（按字节处理，不强制 UTF-8）

### 4.2 服务器必须支持的类型

| 类型 | 前缀 | 用途 |
|------|------|------|
| Simple String | `+` | `OK` / `PONG` 等 |
| Error | `-` | `-ERR` / `-WRONGTYPE` / `-READONLY` 等 |
| Integer | `:` | 长度、0/1 标志等 |
| Bulk String | `$` | 值、INFO、ECHO |
| Null Bulk | `$-1\r\n` | 缺失值 |
| Array | `*` | 命令、HGETALL、空列表 |

### 4.3 解析器要求

- 状态机 / 缓冲增量解析，正确处理 TCP 粘包与半包
- 单条 bulk 默认上限建议可配置（如 512MB 或更保守的 16MB）；超限关闭连接
- 非法前缀、负数长度（除 null）、超长未收完超时：记录日志并关闭连接

### 4.4 错误回复约定

| 场景 | 回复示例 |
|------|----------|
| 未知命令 | `-ERR unknown command 'FOO'` |
| 参数个数错误 | `-ERR wrong number of arguments for 'get' command` |
| 类型错误 | `-WRONGTYPE Operation against a key holding the wrong kind of value` |
| Replica 写拒绝 | `-READONLY You can't write against a read only replica.` |
| 语法/选项不支持 | `-ERR syntax error` 或明确 `-ERR unsupported option` |

---

## 5. 命令语义契约（详表）

### 5.1 `PING`

- `PING` → `+PONG\r\n`
- `PING msg` → Bulk/Simple 返回 `msg`（推荐 Bulk）

### 5.2 `SET key value [EX seconds]`

- 成功：`+OK`
- `EX` 必须为正整数；否则 `-ERR`
- 覆盖同名键（任意原类型）为 string（与 Redis 一致：SET 可改变类型）

### 5.3 `GET key`

- string → Bulk
- 缺失/过期 → `$-1\r\n`
- 其他类型 → `-WRONGTYPE`

### 5.4 `EXPIRE key seconds`

- `seconds` 为整数；`<=0` 的处理：推荐立即过期删除并返回 `:1`（若键存在），与常见 Redis 行为对齐并在测试中固定
- 键不存在：`:0`

### 5.5 `LPUSH` / `RPOP`

- 见 2.4；`LPUSH` 至少 1 个 element

### 5.6 `HSET` / `HGETALL`

- `HSET` field/value 必须成对
- `HGETALL` 字段顺序不做强保证（测试按集合比较）

### 5.7 持久化相关命令

- `SAVE`：同步生成 RDB，成功 `+OK`，失败 `-ERR`
- `BGSAVE`（若实现）：立即 `+Background saving started`，后台完成后更新 `last_save`

### 5.8 复制相关

- Replica 启动参数：`--replicaof host port` 或配置 `replicaof`
- Master 收到复制连接后的行为见 ARCHITECTURE.md

---

## 6. 配置项

支持 CLI 参数与/或配置文件（推荐 `byo-redis.conf` 或环境变量 + CLI）。最小配置：

| 键 | 类型 | 默认 | 说明 |
|----|------|------|------|
| `host` | str | `127.0.0.1` | 绑定地址 |
| `port` | int | `6379` | 监听端口 |
| `dir` | path | `./data` | 数据目录 |
| `dbfilename` | str | `dump.rdb` | RDB 文件名 |
| `aof_enabled` | bool | `true` | 是否启用 AOF |
| `aof_filename` | str | `appendonly.aof` | AOF 文件名 |
| `aof_fsync` | enum | `everysec` | `always`/`everysec`/`no` |
| `role` | enum | `master` | `master`/`replica` |
| `replicaof` | host:port | 空 | Replica 指向的 Master |
| `log_level` | enum | `info` | debug/info/warning/error |
| `rdb_save_seconds` | int | `0` | `>0` 时周期性 SAVE；`0` 关闭 |
| `proto_max_bulk_len` | int | `16_777_216` | bulk 上限 |

配置校验失败时进程以非零退出码退出并打印可读错误。

---

## 7. 运维与发布清单（需求层）

实现与 integration 阶段必须覆盖：

1. **运行方式**：`python -m byo_redis` 或 `byo-redis` 控制台入口；README 给出 `redis-cli -p 6379 PING` 示例
2. **依赖声明**：`pyproject.toml`（推荐）或 `requirements.txt`；开发依赖含 `pytest`
3. **测试**：`pytest` 单元 + 集成；提供脚本或文档化手工验收步骤
4. **许可证**：MIT 或 Apache-2.0（integration 选定）
5. **`.gitignore`**：排除 `data/`、`__pycache__/`、`.venv/`、`.rdb`/`.aof` 运行产物
6. **文档**：根 README + `docs/` 本规格；不提交密钥与本地绝对路径
7. **日志**：启动监听地址、角色、加载持久化结果、复制状态变更

---

## 8. 分阶段实现映射（给 engineer）

| 阶段 | 代号 | 交付切片 | 建议顺序 |
|------|------|----------|----------|
| 1 | W1 | TCP + RESP 编解码 + PING/ECHO | 先通 redis-cli |
| 2 | W2 | 内存存储 + SET/GET/EXPIRE(+TTL) | 过期语义 |
| 3 | W3 | LPUSH/RPOP | 类型系统 WRONGTYPE |
| 4 | W4 | HSET/HGETALL | |
| 5 | RDB | SAVE/加载/崩溃恢复演示 | |
| 6 | W33 | AOF 追加与重放 | |
| 7 | W21 | 主从全量同步 + 命令传播 | INFO role |
| 8 | 打磨 | 配置、入口、测试、README 骨架 | 工业级 |

各阶段应保持可运行；禁止把未解析的半包状态机留在主干。

---

## 9. 质量属性

| 属性 | 要求 |
|------|------|
| 正确性 | 命令语义与本文契约一致；验收用例通过 |
| 并发 | 单进程 asyncio；存储访问需避免跨 task 数据竞争（可用单线程事件循环天然串行 + 明确临界区约定） |
| 可维护 | 模块边界见 ARCHITECTURE.md；公共 API 有类型注解 |
| 可测试 | 协议解析与命令处理可单测；持久化/复制有集成测或脚本 |
| 可观测 | 结构化或至少分级日志 |
| 安全（基础） | 默认绑定回环；不实现 AUTH 时 README 警告勿暴露公网 |

---

## 10. 成功定义（产品层）

当且仅当：

1. 本文件与 `ARCHITECTURE.md`、`ACCEPTANCE.md` 被实现侧遵循；
2. ACCEPTANCE.md 中的硬性验收项全部通过；
3. 仓库具备发布所需的最小工程化材料（由后续任务完成）；

则视为 BYO-Redis v0.1 需求目标达成。

---

## 11. 参考

- [RESP 协议](https://redis.io/docs/latest/develop/reference/protocol-spec/)
- [Redis Persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)
- [Redis Replication](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)
- [RDB File Format](https://rdb.fnordig.de/file_format.html)
