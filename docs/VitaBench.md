# VitaBench 实验说明

补充报告见：[VitaBench_系统指标公平对比扩容报告.md](/home/cht/Tree-DB/docs/VitaBench_系统指标公平对比扩容报告.md)

## 1. 实验目标

本实验的目标是验证：  
在同一份 VitaBench 任务、同一套候选探索逻辑下，如果只改变**事务边界的位置**，Tree 事务是否能够在多分支探索场景中获得更高的效率。

我们关注的不是“大模型会不会做任务”，而是：

- `baseline`：每探索一个候选店铺，就单独开启一个普通事务；
- `tree`：整道任务只开启一个 Tree 事务，把候选探索下沉为数据库内核中的多个 branch；
- 比较两者在吞吐、延迟和事务尝试次数上的差异。

## 2. 实验平台

本实验在一台多路服务器上完成，硬件与软件环境如下。

### 2.1 硬件规格

- CPU：`4 x Intel Xeon Platinum 8360H @ 3.00GHz`
- 总物理核心数：`96`
- 总逻辑线程数：`192`
- 每路核心数：`24`
- 超线程：`2 threads / core`
- NUMA 节点数：`4`
- 内存：`1.5 TB`
- 磁盘挂载：`/home/cht` 位于 `7.0 TB NVMe` 分区

### 2.2 软件环境

- 操作系统：`Linux 3.10.0-1160.119.1.el7.x86_64`
- Python：`3.6.8`
- 编译器：`g++-11 11.4.0`
- Tree-DB 服务端：`rundb`
- 实验通信方式：本地 `socket`，端口通过环境变量 `TREE_DB_PORT` 指定

## 3. 数据集与任务映射

### 3.1 数据来源

实验使用官方 VitaBench `delivery` 任务，并在此基础上构造扩展版本：

- 官方原始数据：
`/home/cht/datasets/VitaBench/delivery/tasks.json`
- 扩展数据：
`/home/cht/datasets/VitaBench/delivery_aug_branch16/tasks.json`

### 3.2 为什么需要扩展

官方 `delivery` 任务平均只有约 `4.1` 家店铺，难以稳定形成 `8` 或 `16` 分支。  
因此我们构造了扩展数据，使每个任务都具有 `16` 家候选店铺，从而系统性评估分支数增长时的性能变化。

扩展后的数据特征：

- 任务数：`100`
- 每任务店铺数：固定为 `16`
- 平均新增干扰店铺数：约 `11.9`

### 3.3 数据库映射方式

我们将 VitaBench delivery 任务映射为一个 KV 世界，主要包括：

- `meta:task`
- `meta:expected_orders`
- `index:stores`
- `store:<store_id>:detail`
- `product:<product_id>:detail`
- `orders:current`
- `meta:contention:hotspot`（热点冲突实验时启用）

这样可以保证：

- baseline 和 tree 使用同一份任务状态
- 两者只在事务执行形态上不同

## 4. 对比方法

### 4.1 Baseline

`baseline` 的设计是：

1. 对每个候选店铺，开启一个普通事务进行真实探索
2. 每个探索事务执行真实读写后回滚
3. 所有探索完成后，根据候选结果统一选择 winner
4. 再开启一个单独的普通事务提交 winner
5. 如果 winner 提交冲突，则持续重试直到成功

因此，在 `k` 分支场景下，baseline 的事务尝试次数理论上接近：

- `k` 次探索事务
- `1` 次 winner 提交事务
- 如有冲突，再加若干 retry

### 4.2 Tree

`tree` 的设计是：

1. 整道任务只开启一个 Tree 事务
2. 公共上下文在 root branch 上读取一次
3. 每个候选店铺对应一个子 branch
4. 每个 branch 维护真实私有读写集
5. 根据所有 branch 的候选结果统一选择 winner
6. 最终仅对 winner path 执行严格刷新与提交

因此，tree 的核心优势是：

- 分支探索共享一个事务外壳
- loser branch 不需要各自独立完成普通事务提交验证
- 最终只有 winner path 进入严格 OCC 验证与提交

## 5. 实验参数设计

### 5.1 共同设置

主实验使用如下统一参数：

- 数据集：`delivery_aug_branch16`
- 任务数：`20`
- `agent_type = reference`
- `contention_profile = hotspot`
- `parallelism = 4`
- `interference_workers = 4`
- `baseline_winner_retry_policy = until_success`

### 5.2 分支数设置

为了验证“分支数越大，tree 优势是否越明显”，我们分别设置：

- `top_k = 4`
- `top_k = 8`
- `top_k = 16`

### 5.3 为什么选择热点冲突场景

如果没有冲突，baseline 虽然要做更多事务，但很多 loser 事务成本很低，tree 的优势不容易被放大。  
热点冲突场景能够更真实地暴露两者差异：

- baseline 的 winner 事务更容易在提交阶段反复重试
- tree 只对 winner path 做最终验证，冲突面更小

## 6. 主结果


| 分支数 | 模式       | 成功率  | 吞吐 tasks/s | 平均延迟 ms | 平均事务尝试数 |
| --- | -------- | ---- | ---------- | ------- | ------- |
| 4   | baseline | 100% | 46.41      | 31.4    | 6.0     |
| 4   | tree     | 100% | 50.55      | 23.0    | 1.0     |
| 8   | baseline | 100% | 36.66      | 47.7    | 10.0    |
| 8   | tree     | 100% | 47.41      | 29.4    | 1.0     |
| 16  | baseline | 100% | 27.57      | 83.9    | 18.0    |
| 16  | tree     | 100% | 43.07      | 32.0    | 1.0     |


## 7. 结论

本实验可以总结为三点。

1. 在 VitaBench delivery 多分支探索场景中，tree 的优势不是“更容易成功”，而是“更高效地成功”。
2. 当分支数从 `4` 增长到 `8` 再到 `16` 时，baseline 的事务成本近似线性上升，而 tree 始终只维持一个事务外壳，因此其吞吐优势逐步放大。
3. 当我们进一步把 tree 的 branch 控制协议下沉到数据库内核侧后，tree 的结果更接近理论预期，说明其优势不仅来自事务语义设计，也来自控制面下沉后的实现收益。
