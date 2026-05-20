## 编译

在项目根目录执行：

```bash
cmake -S . -B build
cmake --build build -j4
```

生成的服务可执行文件是：

```bash
build/rundb
```

## 实验设置

### 1. VitaBench 数据集

```bash
/home/cht/datasets/VitaBench
```

### 2. conda 环境

```bash
conda activate tree-db
```

### 3. 模型环境变量

通过 `vLLM` 提供 OpenAI-compatible API，配置如下：

```bash
export OPENAI_BASE_URL="https://api.deepseek.com"
export OPENAI_API_KEY="sk-11a6454ac5f643d6a9267bb583ccbe79"
export OPENAI_MODEL="deepseek-v4-flash"
export TREE_DB_HOST="127.0.0.1"
export TREE_DB_PORT="19091"
export TREE_DB_BACKEND="socket"
```

