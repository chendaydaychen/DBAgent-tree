# VitaBench 系统指标、公平对比与真实负载扩容报告

## 1. 工作目标

这轮工作的目标有三点：

1. 给 VitaBench 事务实验补齐系统级指标，而不仅仅看 `tasks/s`。
2. 明确 `baseline` 与 `tree` 的公平对比口径，避免“两个方法做的不是同一件事”。
3. 扩容 `delivery` 真实负载，避免实验退化成 prompt / template 适配，而不是事务模型实验。

## 2. 实验平台

- CPU：`4 x Intel Xeon Platinum 8360H @ 3.00GHz`
- 物理核心：`96`
- 逻辑线程：`192`
- NUMA 节点：`4`
- 内存：`1.5 TB`
- 磁盘：`/home/cht` 挂载在 `7.0 TB NVMe`
- 操作系统：`Linux 3.10.0-1160.119.1.el7.x86_64`
- Python：`3.6.8`
- 编译器：`g++ 7.3.0`
- Tree-DB 服务：`/home/cht/Tree-DB/build/rundb`
- 实验端口：`TREE_DB_PORT=19092`

## 3. 这轮改造做了什么

### 3.1 系统级指标

在 `/home/cht/Tree-DB/Agent/vitabench_delivery.py` 里新增了以下 summary 字段：

- `db_abort_rate_per_attempt`
- `avg_branch_count`
- `logical_branches_per_sec`
- `avg_explore_txn_attempts_per_task`
- `avg_winner_txn_attempts_per_task`
- `avg_winner_commit_rounds_per_task`
- `avg_winner_selection_latency_sec`
- `avg_explore_phase_latency_sec`
- `avg_commit_phase_latency_sec`

这意味着现在可以把一个任务拆成三段来分析：

- 探索阶段开销
- winner 选择开销
- 最终提交阶段开销

### 3.2 公平对比口径

这轮实验把对比契约固定为：

- 同一份 VitaBench 任务
- 同一批候选店铺
- 同一套 winner 选择器
- 同一种 branch 私有 payload 结构
- 同一种 hotspot 冲突注入方式

具体来说：

- `baseline`：每个候选店铺一个普通事务串行探索，全部探索结束后再单独提交 winner
- `tree`：整道任务只有一个 tree transaction，所有候选店铺都作为 branch 私有读写，最后只提交 winner path

summary 里也显式写入了：

- `same_candidate_pool = true`
- `same_winner_selector = deterministic_rank_on_branch_payload`
- `same_private_branch_payload_shape = true`

### 3.3 真实负载扩容

原始 `delivery` 数据集平均只有 `4.10` 家店，最大只有 `9` 家店，很难支撑 `top_k=16`。

因此新增了扩容脚本：

- `/home/cht/Tree-DB/scripts/expand_vitabench_delivery.py`

这次不是简单随机补店，而是优先生成 **near-match hard negative**：

- 复制目标店铺的结构
- 修改商品名、标签、价格和属性
- 让干扰店看起来“接近正确答案”，而不是明显错误
- 剩余不足部分再用 donor 店铺补齐

扩容结果：

- 输入：`/home/cht/datasets/VitaBench/delivery/tasks.json`
- 输出：`/home/cht/datasets/VitaBench/delivery_aug_hard16/tasks.json`
- 总任务数：`100`
- 总新增店铺：`1190`
- 其中 hard negative：`884`
- donor distractor：`306`
- 扩容后每任务店铺数：固定 `16`

同时，reference winner 选择不再只依赖 `expected_product_id`，而是依据目标商品的 **名称、标签、价格、属性 profile** 对候选店中最接近的商品做确定性打分。这使得扩容后的 hard negative 真正参与 branch 比较。

## 4. 实验参数

### 4.1 原始官方对照

用于说明“官方 delivery 候选分支不够大”的对照实验：

- 数据集：官方 `delivery/tasks.json`
- `limit = 20`
- `top_k = 16`
- `mode = both`
- `agent_type = reference`
- `parallelism = 4`
- `contention_profile = hotspot`
- `interference_workers = 4`
- `baseline_winner_retry_policy = until_success`


### 4.2 扩容后主实验

- 数据集：`delivery_aug_hard16/tasks.json`
- `limit = 20`
- `top_k = 4 / 8 / 16`
- `mode = both`
- `agent_type = reference`
- `parallelism = 4`
- `contention_profile = hotspot`
- `interference_workers = 4`
- `baseline_winner_retry_policy = until_success`


## 5. 主结果

### 5.1 原始官方 `delivery` 为什么不够

在官方原始 `delivery` 上，即使设置 `top_k=16`，实际平均 branch 数也只有 `3.95`：

| 数据集 | 模式 | 平均 branch 数 | 吞吐 tasks/s |
| --- | --- | ---: | ---: |
| 官方 delivery | baseline | 3.95 | 53.28 |
| 官方 delivery | tree | 3.95 | 73.10 |
| 扩容 hard16 | baseline | 16.00 | 25.63 |
| 扩容 hard16 | tree | 16.00 | 36.93 |

这说明原始 `delivery` 更像“小分支任务”，不足以稳定研究 `16` 路分支探索。

### 5.2 扩容后 hard16 的系统级结果

| top_k | 模式 | 吞吐 tasks/s | abort rate | 平均 branch 数 | 平均事务尝试数 | 探索阶段 ms | winner 选择 ms | 提交阶段 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | baseline | 40.23 | 0.1667 | 4.0 | 6.0 | 12.83 | 0.033 | 10.38 |
| 4 | tree | 42.96 | 0.0000 | 4.0 | 1.0 | 7.64 | 0.043 | 2.92 |
| 8 | baseline | 33.23 | 0.1000 | 8.0 | 10.0 | 30.06 | 0.055 | 11.87 |
| 8 | tree | 40.57 | 0.0000 | 8.0 | 1.0 | 14.80 | 0.067 | 2.62 |
| 16 | baseline | 25.63 | 0.0556 | 16.0 | 18.0 | 60.78 | 0.105 | 13.33 |
| 16 | tree | 36.93 | 0.0000 | 16.0 | 1.0 | 21.55 | 0.116 | 2.14 |

### 5.3 tree 相对 baseline 的提升

| top_k | 吞吐提升 | 探索阶段加速 | 提交阶段加速 |
| --- | ---: | ---: | ---: |
| 4 | `1.07x` | `1.68x` | `3.55x` |
| 8 | `1.22x` | `2.03x` | `4.53x` |
| 16 | `1.44x` | `2.82x` | `6.23x` |

## 6. 如何解读这些结果

### 6.1 winner 选择不是瓶颈

winner 选择开销在三组实验里都只有 `0.03 ms - 0.12 ms` 量级，而且 baseline 与 tree 非常接近。  
这说明系统性能差异 **不是** 来自 winner 选择器本身，而是来自：

- baseline 需要为每个候选都单独开普通事务
- tree 把分支探索放进一个 tree transaction 内部

### 6.2 baseline 的代价主要堆在探索阶段和提交阶段

随着 `top_k` 从 `4` 增加到 `16`：

- baseline 平均事务尝试数从 `6.0` 增加到 `18.0`
- baseline 探索阶段从 `12.83 ms` 增加到 `60.78 ms`
- baseline 提交阶段也始终要承担 winner abort 后的重试

相对地，tree：

- 始终只有 `1.0` 次事务外壳
- 探索阶段增长更缓
- 提交阶段始终只围绕 winner path 做刷新和提交

### 6.3 abort rate 要结合执行模型来读

在 hotspot 设计下，baseline 每个任务几乎都会经历一次 winner 冲突再重做，因此：

- `db_abort_count` 在 `20` 任务实验里始终是 `20`
- 但 `db_abort_rate_per_attempt` 会随着总尝试数变大而下降

所以不能只看 abort rate 的数值大小，还要结合：

- `avg_txn_attempts_per_task`
- `explore_phase_latency`
- `commit_phase_latency`

一起判断系统负担。

### 6.4 扩容后的实验更像事务模型实验

这次扩容不是把 branch 数字机械拉大，而是同时做了两件事：

1. 把每任务候选店数量固定到 `16`
2. 让大量新增店铺成为 near-match hard negative

因此扩容后的实验更接近下面这个问题：

“在多个看起来都可能正确的候选店之间，普通事务串行多次尝试和 tree 内核分支探索，谁更高效？”

这比原始 `delivery` 更能体现事务模型本身，而不是 prompt 或模板适配。

## 7. 结论

这一轮工作得到三个明确结论：

1. 现在的 VitaBench 实验已经能输出系统级指标，能够把差异拆解到探索阶段、winner 选择阶段和提交阶段。
2. 现在的 baseline/tree 对比是公平的：同任务、同候选、同 winner 规则、同 branch payload，只改变事务边界。
3. 扩容后的 `delivery_aug_hard16` 更适合作为事务模型 benchmark；在这个更真实的多分支负载下，tree 的吞吐提升随分支数从 `1.07x` 增长到 `1.44x`，而其核心优势主要体现在探索阶段和提交阶段的系统开销明显更低。
