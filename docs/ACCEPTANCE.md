# BYO-Redis 验收标准（ACCEPTANCE）

| 字段 | 值 |
|------|----|
| 产品 | BYO-Redis v0.1 |
| 依据 | `REQUIREMENTS.md` / `ARCHITECTURE.md` |
| 用途 | 实现自检、QA 验证、Review 放行依据 |

本文中的 **Must** 为硬性验收；**Should** 为强烈建议；**May** 为可选加分项。

---

## 1. 环境与通用约定

### 1.1 运行环境

- Python **3.11+**
- OS：开发/验收主环境为 Windows；脚本应避免仅 POSIX 可用的假设（路径用 `pathlib`）
- 工作目录：仓库内 `byo-redis/`

### 1.2 启动约定

```text
# 主节点示例
python -m byo_redis --host 127.0.0.1 --port 6379 --dir ./data

# 从节点示例
python -m byo_redis --host 127.0.0.1 --port 6380 --dir ./data-replica --replicaof 127.0.0.1:6379
```

具体 CLI 旗标以实现为准，但必须在 README 中与本文一致地可复制。

### 1.3 客户端约定

验收优先顺序：

1. **自动化**：`pytest`（纯 Python RESP 客户端或 `redis` 库）
2. **手工/附加**：若主机有 `redis-cli`，执行同等命令作为旁证

团队目标中的「redis-cli 可连」含义：协议兼容到 `redis-cli` 能完成 PING 与核心命令；若 CI 环境无 `redis-cli`，以兼容客户端证明 + 文档中的 redis-cli 示例为准。

### 1.4 判定符号

| 结果 | 含义 |
|------|------|
| PASS | 观测与期望完全一致 |
| FAIL | 不一致或无法执行 |
| BLOCKED | 环境缺失导致无法测（需在报告说明；核心自动化项不得 BLOCKED） |

---

## 2. 硬性验收项（Must）

### A1. RESP 与连通性

| ID | 准则 | 期望 | 验证方法 |
|----|------|------|----------|
| A1.1 | TCP 监听可连接 | 客户端 connect 成功 | 启动后连指定 port |
| A1.2 | `PING` | 返回 `PONG` | redis-cli / 自动化 |
| A1.3 | 粘包 | 一次写入两条命令，收到两条正确回复 | 单元/集成 |
| A1.4 | 半包 | 分两次送出完整命令，最终正确回复 | 单元/集成 |
| A1.5 | 未知命令 | 返回 `-ERR unknown command ...` | 自动化 |

**通过标准**：A1.1–A1.5 全部 PASS。

### A2. String：SET / GET / EXPIRE

| ID | 准则 | 期望 |
|----|------|------|
| A2.1 | `SET k v` → `OK`；`GET k` → `v` | 精确匹配 |
| A2.2 | `GET` 缺失键 | Null（`None` / `$-1`） |
| A2.3 | `SET k v EX 1`；等待 >1s 后 `GET k` | Null |
| A2.4 | `SET k v` 后 `EXPIRE k 1`；等待后 `GET` | Null |
| A2.5 | `EXPIRE` 不存在键 | Integer `0` |
| A2.6 | 对 list/hash 键 `GET` | `-WRONGTYPE` |

**通过标准**：A2.1–A2.6 全部 PASS。

### A3. List：LPUSH / RPOP

| ID | 准则 | 期望 |
|----|------|------|
| A3.1 | `LPUSH k a` → `(integer) 1` | |
| A3.2 | `LPUSH k b c` 后顺序符合 Redis（左侧为最后推入） | 结合 RPOP 序列验证 |
| A3.3 | `RPOP` 空/缺失 | Null |
| A3.4 | 对 string 键 `LPUSH` | `-WRONGTYPE` |

**通过标准**：A3.1–A3.4 全部 PASS。

**顺序参考用例**：

```text
LPUSH mylist a
LPUSH mylist b
RPOP mylist  → a
RPOP mylist  → b
RPOP mylist  → null
```

（若实现多参数 `LPUSH mylist x y`，按 REQUIREMENTS：最终左侧为 `y`。）

### A4. Hash：HSET / HGETALL

| ID | 准则 | 期望 |
|----|------|------|
| A4.1 | `HSET k f1 v1` → `1` | 新增计 1 |
| A4.2 | 再次 `HSET k f1 v2` → `0` | 覆盖不计新增 |
| A4.3 | `HGETALL k` 含 `f1,v2` | 字段集合与值匹配（顺序不限） |
| A4.4 | `HGETALL` 缺失键 | 空数组 |
| A4.5 | 对 string 键 `HSET` | `-WRONGTYPE` |

**通过标准**：A4.1–A4.5 全部 PASS。

### A5. RDB 崩溃恢复

| ID | 准则 | 步骤 | 期望 |
|----|------|------|------|
| A5.1 | 快照生成 | SET 若干键（含 list/hash/TTL）→ `SAVE`/`BGSAVE` 完成 | RDB 文件存在且大小 > 0 |
| A5.2 | 模拟崩溃重启 | 停止进程（杀进程或优雅退出均可作为“崩溃后重启”的可重复近似；**至少一种强制杀进程**场景）→ 同配置重新启动 | 无异常退出 |
| A5.3 | 数据恢复 | `GET`/`HGETALL`/`RPOP` 或等价读 | 与保存前一致（已过期键可不存在） |
| A5.4 | 原子性 | 人为制造半截临时文件不应破坏上一份完好 RDB（若可测） | 启动仍可用旧文件或明确失败 | Should |

**通过标准**：A5.1–A5.3 全部 PASS。A5.4 为 Should。

### A6. AOF

| ID | 准则 | 步骤 | 期望 |
|----|------|------|------|
| A6.1 | 追加发生 | 开启 AOF，执行写命令 | AOF 文件增长且含 RESP 命令痕迹 |
| A6.2 | 重启重放 | 写数据后重启（不依赖 RDB 或按配置 AOF 优先） | 数据恢复一致 |
| A6.3 | 读命令不污染 | 大量 `GET` 后 AOF 无明显只读命令追加 | PASS |

**通过标准**：A6.1–A6.2 全部 PASS。A6.3 Should。

### A7. 主从复制一致

| ID | 准则 | 步骤 | 期望 |
|----|------|------|------|
| A7.1 | 角色可见 | Master/Replica `INFO` | `role:master` / `role:slave`（或 `replica` 但需文档说明；推荐 Redis 兼容 `slave`） |
| A7.2 | 全量同步 | Replica 启动并完成同步（日志或 INFO 可观察） | 无持续重连失败 |
| A7.3 | 数据一致 | Master：`SET`/`LPUSH`/`HSET` 后，在 Replica 读取 | 值一致 |
| A7.4 | 同步后增量 | 同步完成后再在 Master 写入新键 | Replica 最终可见（允许短暂延迟；验收等待 ≤ 2s） |
| A7.5 | Replica 只读 | 对 Replica 直连 `SET` | `-READONLY` 或等价错误 |

**通过标准**：A7.1–A7.5 全部 PASS。

### A8. 工程化与可发布底线

| ID | 准则 | 期望 |
|----|------|------|
| A8.1 | `python -m compileall byo-redis`（或包路径） | 退出码 0 |
| A8.2 | `cd byo-redis && python -m pytest -q` | 退出码 0；核心 Must 用例覆盖 A1–A7 |
| A8.3 | 模块目录存在 | 符合 ARCHITECTURE 的 `server/protocol/commands/storage/persistence/replication` 边界 |
| A8.4 | README 骨架 | 含安装/启动/redis-cli 示例/测试命令 |
| A8.5 | 无密钥与个人机器路径硬编码 | 抽查 PASS |

**通过标准**：A8.1–A8.4 全部 PASS。

---

## 3. Should / May 项

| ID | 级别 | 准则 |
|----|------|------|
| S1 | Should | `TTL` 命令行为正确 |
| S2 | Should | 主动过期任务（不仅 lazy） |
| S3 | Should | `DEL`、`DBSIZE` |
| S4 | Should | 日志包含复制状态切换 |
| S5 | Should | 配置文件或完备 CLI help |
| M1 | May | `BGSAVE` 不阻塞事件循环 |
| M2 | May | AOF rewrite |
| M3 | May | 与官方 Redis 生成的 RDB 互通 |

Should 未通过不单独否决 v0.1，但 Reviewer 可据此要求补强（工业级观感）。

---

## 4. 测试策略

### 4.1 单元测试（必须）

- RESP：空/正常/粘包/半包/null bulk/array
- 过期：边界（刚好到期）
- 各命令参数错误与 WRONGTYPE
- RDB/AOF 编解码圆件（dump→load 等价）

### 4.2 集成测试（必须）

- 真实 `RedisServer` + 临时端口 + tmp 数据目录
- RDB：写 → SAVE → 新实例加载
- AOF：写 → 新实例重放
- 复制：双实例，断言一致性

### 4.3 手工脚本（推荐，写入 VERIFICATION_REPORT）

```text
# 终端 1
python -m byo_redis --port 6379

# 终端 2（若有 redis-cli）
redis-cli -p 6379 PING
redis-cli -p 6379 SET foo bar
redis-cli -p 6379 GET foo
```

### 4.4 验证命令（任务契约）

实现完成后工程师自检：

```text
python -m compileall byo-redis
cd byo-redis && python -m pytest -q
```

QA 另按本文件章节产出 `docs/VERIFICATION_REPORT.md`，对 A1–A8 逐条给证据。

---

## 5. 验收决策树

```text
A1–A7 任一 Must FAIL?
  ├─ YES → 产品未达标；verification 失败；review 不得 pass
  └─ NO  → A8 FAIL?
            ├─ YES → 未达可发布工程底线；需修复
            └─ NO  → 功能验收通过；进入 review（工业级质量仍可提 findings）
```

---

## 6. 非验收 / 明确排除

不因下列原因判定 FAIL：

- 未实现 Cluster/Sentinel/RESP3/Lua/PubSub 等非目标
- HGETALL 字段顺序与官方不一致
- 未实现 AOF rewrite
- 性能低于官方 Redis
- 无 `redis-cli` 二进制时，已有自动化协议证明连通性（但 README 仍需给出 redis-cli 示例）

---

## 7. QA 报告模板（供 t3 使用）

```markdown
# VERIFICATION_REPORT

## 环境
- OS / Python 版本
- 提交或工作区状态

## 命令证据
- compileall: ...
- pytest: ...

## 矩阵
| ID | 结果 | 证据 |
|----|------|------|
| A1.1 | PASS | ... |

## 缺陷列表
- ...

## 结论
PASS / FAIL
```

---

## 8. 追溯矩阵（需求 → 验收）

| 需求能力 | 验收 ID |
|----------|---------|
| RESP 解析 | A1.* |
| SET/GET/EXPIRE | A2.* |
| LPUSH/RPOP | A3.* |
| HSET/HGETALL | A4.* |
| RDB | A5.* |
| AOF | A6.* |
| 主从复制 | A7.* |
| redis-cli 可连 | A1.* + A8.4 |
| 崩溃 RDB 恢复 | A5.* |
| 主从一致 | A7.* |
| 工业级可发布 | A8.* + review |
