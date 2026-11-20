# 本地消息授权账本（`solution`）

一个用 SQLite 落盘的本地消息授权账本：租户与主题隔离，订阅规则只允许**精确主题**
或**末尾一个完整段**的 `*` 通配（`deny` 优先于 `allow`）；发布可乱序，但每个分区维护
一个**不可回退的安全水位**，跨过的缺口成为**永久空洞**；策略版本**全租户单调**，切换时
**事先声明**每分区的生效偏移，必须**高于**该分区已看到的最大偏移，并且**给全部分区登记
或全部拒绝**；每条消息在提交时冻结**确切的策略版本、订阅者集合、消息规范摘要与回执**，
历史重复判定**先于**水位检查。

全部实现只用 Python 3.12 标准库与 CPU，在 Windows 原生环境下离线运行；本题不向任何真实
消息队列发请求。

---

## 1. 快速开始

```bash
# 重放 cases.json，产出 result.json
python -m solution --input cases.json --output result.json

# 跑自动化测试
python -m unittest discover -s tests -v
```

两者都不需要安装任何依赖，也不访问网络。`--workdir <目录>` 可选，用来指定每个 case 的
SQLite 文件位置（缺省为系统临时目录）。

退出码：

| 退出码 | 含义 |
| --- | --- |
| `0` | 全部 case 通过 |
| `1` | 有用例不匹配（`result.json` 仍会写出） |
| `2` | 输入文件无法读取 / 不是合法 JSON / cases.json 结构不合法（不写 result.json） |
| `3` | 运行期未捕获异常 |

---

## 2. 输入结构

### 2.1 `cases.json`

```jsonc
{
  "schema": "ledger-cases/1",
  "notes": "可选，自由文本",
  "cases": [
    {
      "case_id": "01-rule-matching-and-deny-precedence",  // 必填，全局唯一
      "description": "可选",
      "tenant": "acme",        // 可选：给缺省 tenant 的请求补上
      "requests": [ /* 请求对象数组，按顺序执行 */ ],
      "expect":   [ /* 与 requests 等长的期望数组 */ ]
    }
  ]
}
```

每个 case 使用一个**全新的** SQLite 文件（`<workdir>/<case_id>.sqlite3`），因此 case 之间
互不影响，可单独重放。

### 2.2 请求

所有请求的公共字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `op` | string | 操作名，见下表 |
| `tenant` | string | 租户，`[A-Za-z0-9][A-Za-z0-9_.-]{0,63}` |
| `request_id` | string | **写操作必填**，1..128 字符，在**租户内**唯一；读操作可省 |

| `op` | 额外字段 | 语义 |
| --- | --- | --- |
| `declare_partition` | `partition` int, `topic` string | 登记分区并把主题**永久绑定**到它。策略已存在后不允许新增分区。 |
| `register_policy` | `version` int≥1, `coverage` `[{partition, effective_offset}]`, `rules` `[{subscriber, pattern, effect}]` | 登记一整份策略版本。 |
| `publish` | `partition`, `offset`, `payload` object, 可选 `topic` 与 `policy_version` | 发布一条消息；可乱序，不推进水位。 |
| `advance_watermark` | `partition`, `target` int | 把该分区水位单调推进到 `target`，跨过的缺口永久封存。 |
| `state` | — | 读：分区/水位/消息计数/永久空洞数/已登记策略。 |
| `get_message` | `partition`, `offset` | 读：冻结的版本、摘要、订阅者集合、首次请求 ID。 |
| `get_receipt` | `request_id` | 读：该请求持久化的回执。 |

主题与规则的通配格式：

* 主题用 `.` 分段，2..16 段，每段 `[A-Za-z0-9][A-Za-z0-9_-]*`。
* 规则是**精确主题**，或**末尾恰好一个完整段**为 `*`，例如 `orders.eu.*`。
* `orders.*.eu`（中间通配）、`orders.e*`（半个段）、`*`（顶层通配）都会被
  `RULE_INVALID` 拒绝。
* `deny` 优先：同一订阅者同时命中 `allow` 与 `deny` 时，从订阅者集合里去掉。
* 通配是**按段**的，`orders.eu.*` 不匹配两段的 `orders.eu`，也不跨段匹配三段以上之外的层级。

### 2.3 `expect` 的匹配语义

`expect` 是人工判定的期望（**不由实现反推**）。匹配规则：

| 键 | 语义 |
| --- | --- |
| `class` | 精确匹配 `ok` / `rejected` / `invalid` |
| `error_code` | 精确匹配错误码 |
| `replayed` | 精确匹配（缺省不约束） |
| `result` | **子集**语义：出现的字段必须一致，dict 递归子集，list 要求长度与顺序一致 |
| `error_details` | 同样子集语义 |

子集语义让用例可以只断言关键字段（例如只断言 `policy_version` 与 `subscribers`），
但仍能精确锁定错误码与业务判定。

---

## 3. 输出结构

`result.json`：

```jsonc
{
  "schema": "ledger-result/1",
  "cases": [
    {
      "case_id": "...",
      "description": "...",
      "passed": true,
      "steps": [
        {
          "index": 0,
          "op": "publish",
          "request_id": "r3",
          "observed": {"class": "ok", "replayed": false, "result": { ... }},
          "expected": {"class": "ok", "result": { ... }},
          "match": true,
          "mismatches": []
        }
      ]
    }
  ],
  "summary": {"cases": 12, "passed": 12, "failed": 0, "steps": 98},
  "failed_case_ids": []
}
```

每个 `observed` 只包含稳定字段（`class`、`replayed`、`result` 或 `error_code`/`error_details`），
不含时间戳或临时路径，因此同一输入两次运行的结果逐字节一致。

---

## 4. 账本语义（不变量）

1. **消息身份** = `(tenant, partition, offset)`。主题在分区登记时绑定，之后不能改；
   `publish` 可选地声明 `topic`，与绑定不符即 `PARTITION_TOPIC_CONFLICT`。
2. **发布不推进水位**。水位只能由 `advance_watermark` 单调前进；`target < 当前` 是
   `WATERMARK_REGRESS`，`target == 当前` 是成功的空操作。
3. **永久空洞**。水位从 `w` 推进到 `t` 时，`[w, t)` 中**从未收到**的偏移被永久封存；
   之后对它们的发布是 `WATERMARK_BELOW_HOLE`，永远不会被接纳。
4. **历史重复先于水位检查**。若 `(partition, offset)` 已有消息：内容摘要相同 → 成功并
   返回**首次**的版本/订阅者/首次请求 ID（`duplicate: true`）；摘要不同 →
   `MESSAGE_CONFLICT`。这两条判定都发生在水位判定**之前**。
5. **版本选择**。对偏移 `o`，取 `effective_offset <= o` 中生效偏移最大（并列取版本最大）
   的版本。这就是「确切策略版本」。
6. **策略版本单调**。`version` 在租户内严格递增；否则 `VERSION_CONFLICT`。
7. **切换全成或全拒**。`coverage` 必须恰好覆盖当前**全部**已登记分区；缺项或多项 →
   `COVERAGE_PARTIAL`。任何分区不满足下列条件都会让**整次**切换失败，不留下任何行。
8. **生效偏移下界**。每个分区的 `effective_offset` 必须**严格大于**该分区已决的最大偏移
   （已见消息的 `MAX(offset)` 与 `watermark - 1` 的较大者）；否则 `VERSION_BELOW_WATERMARK`。
   同一分区上连续版本的生效偏移还必须**严格递增**，否则 `VERSION_NOT_MONOTONIC`。
9. **冻结**。消息提交时把版本号、解析出的订阅者集合、摘要一并落盘。之后新增版本或改规则，
   历史消息的 `get_message` 与重放结果都不变。
10. **请求幂等**。请求 ID 在租户内唯一：**同参**重放返回**已持久化的第一次结果**
    （`replayed: true`），**异参**返回 `REQUEST_CONFLICT` 且**不覆盖**原回执。
11. **业务失败与业务状态可区分**：
    * `class: rejected` 的**业务拒绝**（如 `MESSAGE_CONFLICT`、`WATERMARK_BELOW_HOLE`）
      也是账本状态，回执会持久化，重放原样返回（`replayed: true`）；
    * `class: invalid` 的**输入不合法**（如 `OFFSET_INVALID`、`RULE_INVALID`）不产生回执，
      也不触碰数据库。
12. **未提交中断不留下任何东西**。所有写操作在 `BEGIN IMMEDIATE` 事务中完成，
    消息/回执/水位/策略同事务提交；进程在提交前被杀，重启后数据库看不到任何痕迹。

---

## 5. 错误码

错误码一经发布含义不变。表格第二列区分它属于**业务拒绝**（会持久化回执）还是**输入无效**
（不产生回执）。

| 错误码 | 类别 | 含义 |
| --- | --- | --- |
| `INVALID_REQUEST` | 输入无效 | 请求不是 JSON 对象，或缺少 `op`/`tenant` |
| `UNKNOWN_OP` | 输入无效 | 未知操作名 |
| `REQUEST_ID_INVALID` | 输入无效 | 写操作缺少合法 `request_id` |
| `TENANT_INVALID` | 输入无效 | 租户标识不合法 |
| `TOPIC_INVALID` | 输入无效 | 主题格式不合法 |
| `PARTITION_INVALID` | 输入无效 | 分区不是 0..1000000 的整数 |
| `SUBSCRIBER_INVALID` | 输入无效 | 订阅者标识不合法 |
| `RULE_INVALID` | 输入无效 | 规则不是精确主题或末尾整段通配 |
| `RULE_DUPLICATE` | **业务拒绝** | 同一版本内重复登记相同规则 |
| `VERSION_INVALID` | 输入无效 | 版本号不是正整数 |
| `COVERAGE_INVALID` | 输入无效 | `coverage` 结构不合法 |
| `PAYLOAD_INVALID` | 输入无效 | 消息体不是 JSON 对象 |
| `OFFSET_INVALID` | 输入无效 | 偏移/目标不是非负整数 |
| `POLICY_VERSION_INVALID` | 输入无效 | `policy_version` 不是整数 |
| `TENANT_UNKNOWN` | **业务拒绝** | 租户尚未登记任何分区 |
| `UNKNOWN_PARTITION` | **业务拒绝** | 分区未登记 |
| `PARTITION_TOPIC_CONFLICT` | **业务拒绝** | 分区已绑定到另一个主题 |
| `PARTITION_AFTER_POLICY` | **业务拒绝** | 策略已存在后新增分区 |
| `PARTITION_MISSING` | **业务拒绝** | 租户没有分区，无法登记策略 |
| `COVERAGE_PARTIAL` | **业务拒绝** | 覆盖未恰好等于全部已登记分区 |
| `VERSION_CONFLICT` | **业务拒绝** | 版本不严格单调 |
| `VERSION_BELOW_WATERMARK` | **业务拒绝** | 生效偏移不高于该分区已决最大偏移 |
| `VERSION_NOT_MONOTONIC` | **业务拒绝** | 同分区连续版本生效偏移未严格递增 |
| `MESSAGE_CONFLICT` | **业务拒绝** | 同偏移已存在不同内容 |
| `WATERMARK_BELOW_HOLE` | **业务拒绝** | 偏移在安全水位下且从未收到 |
| `POLICY_UNCOVERED` | **业务拒绝** | 没有任何版本覆盖该偏移 |
| `VERSION_NOT_APPLICABLE` | **业务拒绝** | 声明的 `policy_version` 与实际覆盖版本不符 |
| `WATERMARK_REGRESS` | **业务拒绝** | 水位尝试回退 |
| `POLICY_MISSING` | **业务拒绝** | 没有策略时推进水位 |
| `REQUEST_CONFLICT` | **业务拒绝** | 同请求 ID 异参重放 |
| `RECEIPT_NOT_FOUND` | **业务拒绝** | 没有该回执 |
| `MESSAGE_NOT_FOUND` | **业务拒绝** | 没有该消息 |

响应里的 `class` 有三档：`ok`、`rejected`、`invalid`。`rejected` 一定带 `error.code`，
且写操作回执已持久化；`invalid` 不持久化。

---

## 6. 边界示例

### 6.1 规则匹配与 `deny` 优先

主题 `orders.eu.v1`，规则：

| subscriber | pattern | effect | 命中? |
| --- | --- | --- | --- |
| `billing` | `orders.eu.*` | allow | ✅ |
| `audit` | `orders.eu.*` | allow | ✅ |
| `audit` | `orders.eu.v1` | deny | ✅（deny 胜） |
| `ops` | `orders.*` | allow | ✅ |
| `exact` | `orders.eu.v1` | allow | ✅ |

最终订阅者集合为 `["billing", "exact", "ops"]`（`audit` 被去掉）。见
`cases.json` 的 `01-rule-matching-and-deny-precedence`。

### 6.2 两个分区不同生效边界

版本 2 声明 `{p0: 5, p1: 9}`：`p0` 的偏移 4 用 v1、5 用 v2；`p1` 的 8 用 v1、9 用 v2。
见 `02-two-partitions-distinct-boundaries` 与测试
`test_partition_boundaries_are_fixed_by_two_writers`。

### 6.3 整次切换失败

v2 声明 `{p0: 100, p1: 7}`，但 p1 已有偏移 7 的消息。p0 的 100 本身合法，但切换仍然
**整次**失败（`VERSION_BELOW_WATERMARK`，`details.max_seen = 7`）；`state` 里只有
v1，之后 p0 偏移 50 仍按 v1 结算。见 `03-policy-switch-fails-atomically` 与
`test_other_process_commits_first_and_fails_this_switch`。

### 6.4 永久空洞

p0 依次发布 0、2、3，再推进水位到 4：`sealed_holes = [1]`。此后发布偏移 1 得到
`WATERMARK_BELOW_HOLE`；再发已存在的偏移 0 则仍是 `duplicate: true` 的成功（历史重复
先于水位判定）。见 `04-watermark-seals-holes` 与 `05-history-duplicate-precedes-watermark`。

### 6.5 旧洞迟到 / 新旧规则重放

v1 下发布 0、2，v2 声明生效偏移 3。迟到的偏移 1 仍按 **v1** 结算（`policy_version: 1`），
偏移 3 按 **v2** 结算；重放拿到各自冻结的订阅者集合。见
`06-old-and-new-rule-replay` 与 `test_old_hole_late_arrival_is_resolved_with_old_version`。

### 6.6 幂等与冲突

同 ID 同参 → `replayed: true`，返回首次结果（即使此时规则已变）；同 ID 异参 →
`REQUEST_CONFLICT`。不同租户可以使用相同的请求 ID。见 `07-request-id-idempotency`。

### 6.7 两个刻意的边界后果

这两条都是规格直接推出来的，不是实现偏好：

* **首版生效偏移之前的偏移永远无法补上。** 若 v1 声明 `effective_offset: 10`，则偏移
  `0..9` 从未被任何版本覆盖，发布得到 `POLICY_UNCOVERED`。之后再登记 v2 时，
  `VERSION_NOT_MONOTONIC` 会拦住任何 `<= 10` 的生效偏移，所以这个空洞只能保持未覆盖。
  这与「同分区连续版本生效偏移严格递增」一致：版本不能回头改写更早的区间。
* **策略冻结后不能再新增分区。** `declare_partition` 出现在首个 `register_policy` 之后
  会得到 `PARTITION_AFTER_POLICY`。否则既有策略的 `coverage` 会不完整，破坏
  「给全部分区登记或全部拒绝」。需要更多分区时，必须在登记首个策略之前声明。

---

## 7. 并发、崩溃与测试策略

* **同步点固定先后**：并发测试不用 `sleep` 猜时序。每个工作进程执行到 `{"touch": "x",
  "wait_for": "y"}` 时先创建文件 `x`、再等文件 `y` 出现；主测试进程用同样的文件推进顺序。
  C++/Java 那种内存屏障在跨进程里不适用，文件系统就是天然的内存屏障。
* **只终止本题启动的子进程**：`tests/helpers/worker.py` 由 `subprocess.Popen` 启动，
  终止时只对该 `Popen` 对象调用 `terminate()`/`kill()`，从不按名字查找系统进程。
* **真实终止**：`crash_points` 让**实现内部**的 `hook(point)` 在指定点直接 `os._exit(70)`。
  两个关键点：
  * `publish.before_write` / `publish.after_write`：事务内、尚未提交。重启后消息、回执都
    不应存在。
  * `publish.after_commit`：事务已提交、尚未回包。重启后消息、版本冻结值、回执都应存在，
    且同 ID 重放返回**已持久化**的结果。
  * 水位与策略登记同样有 `before_write` / `after_write` / `after_commit` 三个点。
* **未提交中断＝重启后的数据库状态**：测试不检查进程内存，只用独立的 SQLite 连接（或新的
  `Ledger`）读取落盘状态来判定。
* **并发写**：所有写操作使用 `BEGIN IMMEDIATE`，配合 WAL + `busy_timeout`，跨进程串行化；
  读使用只读事务，不阻塞写者。

### 测试清单

`python -m unittest discover -s tests -v` 运行 43 个测试：

| 文件 | 覆盖 |
| --- | --- |
| `tests/test_semantics.py` | 模式/主题校验、摘要稳定性、水位空洞、版本边界、输入契约、错误码分区 |
| `tests/test_concurrency.py` | 两分区不同边界、另进程先发布导致整次切换失败、旧洞迟到 + 规则重放、**同请求 ID 跨进程 first-wins**、并发策略登记、水位与发布交错 |
| `tests/test_interleaving.py` | 双写者固定边界、另进程先提交导致切换失败、旧洞迟到按旧版本结算、旧规则重放看到冻结集合 |
| `tests/test_crash.py` | 提交前/后杀死子进程；水位与策略登记的原子性；业务拒绝回执跨进程持久化 |
| `tests/test_cli.py` | `python -m solution` 端到端、结果确定性、坏输入退出码、比对器语义 |

期望值来源：

* 纯函数测试使用独立实现重新算出摘要（`sha256(b"message\0" + canonical_json + b"\0")`），
  或直接由规格手写；
* 并发/崩溃测试的期望来自规格描述的顺序与失败条件（如「另一进程先写，因此本进程切换失败」）；
* 用例重放测试的 `expect` 是人工写在 `cases.json` 里的判定。

没有任何测试从被测实现反推期望值。

---

## 8. 实测结果

在 Windows 11 Pro 26200 上，Python 3.12.8：

```
$ python -m solution --input cases.json --output result.json
cases=12 passed=12 failed=0 steps=98

$ python -m unittest discover -s tests -v
Ran 43 tests in 5.0s
OK
```

* `cases.json` 含 12 个用例、98 个请求步骤，全部匹配；
* 连续两次运行 `result.json` 逐字节相等（`test_cases_are_stable_across_runs`）；
* 崩溃测试的 6 个用例在约 0.9 秒内完成，每个都真实启动并终止了独立子进程。

---

## 9. 目录

```
cases.json              可重放用例
result.json             最近一次 CLI 输出
solution/
  __init__.py           对外导出 Ledger / LedgerError / ERROR_CODES
  __main__.py           CLI 入口
  service.py            账本语义与请求处理
  store.py              SQLite schema、事务、回执读写
  model.py              主题/规则/摘要等纯函数
  errors.py             稳定错误码，及其「业务拒绝/输入无效」分区
  cases.py              cases.json 解析、执行、结果比对
tests/
  helpers/__init__.py   Worker（子进程）与文件同步点
  helpers/worker.py     被测试启动的工作进程
  test_*.py             自动化测试
```
