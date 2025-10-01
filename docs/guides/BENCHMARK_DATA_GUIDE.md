# Benchmark Index Data Guide for Qlib

## 问题：在哪里找到基准指数？(Where to Find Benchmark Indices?)

当你在运行Qlib策略时，可能会遇到这个错误：
```
ValueError: The benchmark ['SH000300'] does not exist. Please provide the right benchmark
```

这是因为**Qlib需要基准指数的历史数据**，但你的数据目录里没有这个指数的数据。

---

## 解决方案概览 (Solutions Overview)

### 方案1: 不使用基准 (No Benchmark) ⭐ 推荐新手
```python
# 最简单：直接设置 benchmark=None
portfolio_metric, indicator_metric = backtest(
    strategy=strategy,
    benchmark=None,  # 不使用基准
    ...
)
```

### 方案2: 下载基准数据 (Download Benchmark Data) ⭐ 推荐进阶
```python
# 使用Qlib的数据收集脚本下载
# 需要下载对应市场的基准指数数据
```

### 方案3: 使用自己的基准 (Use Your Own Benchmark)
```python
# 如果你有自己的ETF或指数数据
# 可以作为基准使用
```

---

## 详细说明 (Detailed Explanation)

### 1. 基准指数是什么？(What is a Benchmark?)

基准指数是用来**比较策略表现**的参照标准。

#### 例子
```
你的策略今年赚了15%
基准(S&P 500)今年涨了10%

结论：你的策略跑赢基准5%！👍
超额收益 = 15% - 10% = 5%
```

#### 常见基准指数

**美国市场 (US Market)**:
- **S&P 500** - 大盘股指数
  - Symbol: `^GSPC` (Yahoo Finance)
  - Symbol: `SPY` (ETF)
- **Nasdaq 100** - 科技股指数
  - Symbol: `^NDX` (Yahoo Finance)
  - Symbol: `QQQ` (ETF)
- **Dow Jones** - 道琼斯工业指数
  - Symbol: `^DJI` (Yahoo Finance)
  - Symbol: `DIA` (ETF)

**中国市场 (China Market)**:
- **CSI 300** - 沪深300
  - Symbol: `SH000300` (Qlib格式)
  - 中国最常用的基准
- **CSI 500** - 中证500
  - Symbol: `SH000905`

### 2. 为什么会报错？(Why the Error?)

Qlib的backtest函数会尝试：
1. 从你的数据目录读取基准指数的历史价格
2. 计算基准指数的收益率
3. 对比策略收益和基准收益

如果基准数据不存在 → 报错！

```python
# Qlib内部会做类似这样的操作
benchmark_prices = D.features(
    ["SH000300"],  # 基准symbol
    ["$close"],    # 收盘价
    start_time="2020-01-01",
    end_time="2024-12-31"
)

# 如果找不到SH000300的数据 → ValueError!
```

---

## 方案1: 不使用基准 (详细) ⭐

### 为什么可以不用基准？

基准不是必须的！没有基准你依然可以：
- ✓ 运行策略
- ✓ 看绝对收益
- ✓ 看夏普比率
- ✓ 看最大回撤

只是不能：
- ✗ 看超额收益
- ✗ 看信息比率(IR)

### 如何设置

#### 在backtest中设置
```python
from qlib.backtest import backtest
from qlib.backtest.executor import SimulatorExecutor

portfolio_metric, indicator_metric = backtest(
    start_time="2025-09-09",
    end_time="2025-09-29",
    strategy=strategy,
    executor=SimulatorExecutor(...),
    benchmark=None,  # ← 关键：设置为None
    account=100000000,
    exchange_kwargs={...}
)
```

#### 在配置文件中设置
```yaml
# qlib_workflow_config.yaml
port_analysis_config:
    backtest:
        start_time: "2025-09-09"
        end_time: "2025-09-29"
        account: 100000000
        benchmark: null  # ← 设置为null (YAML中的None)
        exchange_kwargs:
            freq: day
            ...
```

### 优点
- ✓ 简单，不需要额外数据
- ✓ 策略照样能运行
- ✓ 新手友好

### 缺点
- ✗ 不知道是否跑赢市场
- ✗ 少了一些分析指标

### 适合场景
- 新手学习阶段
- 只关心绝对收益
- 没有基准数据

---

## 方案2: 下载基准数据 (详细) ⭐

### 2.1 使用Qlib官方脚本下载

#### 步骤1: 查看Qlib的数据收集脚本
```bash
# Qlib提供了数据下载脚本
# 位置: qlib/scripts/data_collector/

# 查看可用的收集器
ls ~/.local/lib/python3.x/site-packages/qlib/scripts/data_collector/

# 常见的：
# - yahoo/ - Yahoo Finance数据
# - cn_data/ - 中国市场数据
```

#### 步骤2: 下载美国市场基准 (S&P 500)

**使用Yahoo Finance收集器**:
```bash
# 方法1: 下载SPY ETF作为S&P 500代理
python -m qlib.run.get_data qlib_data \
    --target_dir ~/.qlib/qlib_data/us_data \
    --region us \
    --interval 1d \
    --start 2000-01-01 \
    --end 2024-12-31

# 这会下载所有美股数据，包括SPY
```

**或者使用我们的polygon数据源**:
```bash
# 如果你已经有polygon数据
# 确保下载了SPY的数据

# 在你的数据收集中添加SPY
symbols = ["SPY", "QQQ", "DIA", ...]  # 添加基准ETF
```

#### 步骤3: 下载中国市场基准 (CSI 300)

```bash
# 使用Qlib的中国数据收集器
python -m qlib.run.get_data qlib_data \
    --target_dir ~/.qlib/qlib_data/cn_data \
    --region cn \
    --interval 1d \
    --start 2000-01-01 \
    --end 2024-12-31 \
    --include_benchmark  # ← 包含基准指数
```

### 2.2 手动添加基准数据

#### 步骤1: 下载基准数据

**使用yfinance (Python)**:
```python
import yfinance as yf
import pandas as pd

# 下载S&P 500数据
spy = yf.download("SPY", start="2020-01-01", end="2024-12-31")

# 保存为CSV
spy.to_csv("SPY.csv")

# 或者下载多个基准
benchmarks = ["SPY", "QQQ", "DIA"]
for symbol in benchmarks:
    data = yf.download(symbol, start="2020-01-01", end="2024-12-31")
    data.to_csv(f"{symbol}.csv")
```

**使用Polygon (如果你有API key)**:
```python
# 你的polygon数据收集脚本已经在做这个
# 确保symbols列表包含：
symbols = [
    "SPY",   # S&P 500 ETF
    "QQQ",   # Nasdaq 100 ETF
    "DIA",   # Dow Jones ETF
    # ... 其他股票
]
```

#### 步骤2: 转换为Qlib格式

```python
# 使用我们已有的convert_to_qlib.py脚本
# 确保基准数据也被转换

from src.transform.qlib_binary_writer import QlibBinaryWriter

writer = QlibBinaryWriter(
    duckdb_path="your_database.db",
    output_base_dir="/path/to/qlib/data"
)

# 转换数据（包括基准）
writer.convert_data_type(
    data_type="stocks_daily",  # SPY也是股票数据
    start_date="2020-01-01",
    end_date="2024-12-31"
)
```

#### 步骤3: 在策略中使用

```python
# 现在可以使用基准了
portfolio_metric, indicator_metric = backtest(
    strategy=strategy,
    benchmark="SPY",  # ← 使用SPY作为基准
    ...
)
```

### 2.3 验证基准数据是否存在

```python
# 测试脚本：检查基准数据
import qlib
from qlib.data import D

# 初始化Qlib
qlib.init(
    provider_uri="/path/to/your/qlib/data",
    region="us"
)

# 尝试读取基准数据
try:
    benchmark_data = D.features(
        ["SPY"],
        ["$close"],
        start_time="2020-01-01",
        end_time="2024-12-31"
    )
    print("✓ 基准数据存在！")
    print(f"数据点数: {len(benchmark_data)}")
    print(benchmark_data.head())
except Exception as e:
    print(f"✗ 基准数据不存在: {e}")
```

---

## 方案3: 使用替代基准 (详细)

如果你无法获取标准基准指数，可以使用**替代基准**。

### 3.1 使用ETF作为基准

```python
# 用ETF代替指数
benchmark_mapping = {
    "S&P 500":  "SPY",   # SPDR S&P 500 ETF
    "Nasdaq":   "QQQ",   # Invesco QQQ Trust
    "Dow":      "DIA",   # SPDR Dow Jones ETF
    "Russell":  "IWM",   # iShares Russell 2000
}

# 在backtest中使用
portfolio_metric, indicator_metric = backtest(
    strategy=strategy,
    benchmark="SPY",  # 用ETF代替指数
    ...
)
```

### 3.2 构建自定义基准

```python
# 如果你想用等权重的股票池作为基准
def create_equal_weight_benchmark(stocks, start_date, end_date):
    """
    创建等权重基准
    """
    # 获取所有股票的收益率
    returns = D.features(
        stocks,
        ["$close/$close[1]-1"],  # 日收益率
        start_time=start_date,
        end_time=end_date
    )

    # 计算等权重平均收益
    benchmark_return = returns.mean(axis=1)

    return benchmark_return

# 使用自定义基准
custom_benchmark = create_equal_weight_benchmark(
    stocks=["AAPL", "MSFT", "GOOGL", ...],
    start_date="2020-01-01",
    end_date="2024-12-31"
)
```

### 3.3 使用行业基准

```python
# 如果你的策略专注某个行业
# 使用该行业的ETF作为基准

sector_benchmarks = {
    "Tech":        "XLK",   # Technology Select Sector
    "Healthcare":  "XLV",   # Health Care Select Sector
    "Finance":     "XLF",   # Financial Select Sector
    "Energy":      "XLE",   # Energy Select Sector
    "Consumer":    "XLY",   # Consumer Discretionary
}

# 使用行业基准
portfolio_metric, indicator_metric = backtest(
    strategy=strategy,
    benchmark="XLK",  # 科技行业基准
    ...
)
```

---

## 推荐方案总结 (Recommended Approach)

### 对于你的QuantMini项目

#### 现状分析
```
你的数据:
✓ Polygon API 收集的美股数据
✓ 已经转换为Qlib格式
✓ 包含 ~12,000 只美股

缺少:
✗ 基准指数数据 (SPY, QQQ等)
```

#### 推荐步骤

**第1步: 短期方案 (立即可用)**
```python
# 在所有backtest调用中设置 benchmark=None
portfolio_metric, indicator_metric = backtest(
    strategy=strategy,
    benchmark=None,  # 暂时不用基准
    ...
)

# 优点：立即能运行
# 缺点：看不到超额收益
```

**第2步: 中期方案 (1-2天)**
```bash
# 1. 修改你的数据收集脚本，添加基准ETF
# 在 scripts/ingest_polygon_stocks.py 中添加：

BENCHMARK_SYMBOLS = [
    "SPY",   # S&P 500
    "QQQ",   # Nasdaq 100
    "DIA",   # Dow Jones
    "IWM",   # Russell 2000
]

# 2. 重新运行数据收集（只收集基准）
uv run python scripts/ingest_polygon_stocks.py --symbols SPY,QQQ,DIA,IWM

# 3. 转换为Qlib格式
uv run python scripts/convert_to_qlib.py --data-type stocks_daily

# 4. 现在可以使用基准了
portfolio_metric, indicator_metric = backtest(
    strategy=strategy,
    benchmark="SPY",  # 使用S&P 500
    ...
)
```

**第3步: 长期方案 (持续优化)**
```python
# 创建配置文件，根据策略选择基准
strategy_benchmarks = {
    "large_cap": "SPY",      # 大盘策略用S&P 500
    "tech_focused": "QQQ",   # 科技策略用Nasdaq
    "small_cap": "IWM",      # 小盘策略用Russell 2000
    "all_market": "SPY",     # 默认用S&P 500
}

# 自动选择合适的基准
benchmark = strategy_benchmarks.get(strategy_type, "SPY")
```

---

## 实用代码示例 (Practical Code Examples)

### 示例1: 检查是否有基准数据

```python
#!/usr/bin/env python3
"""
检查基准数据是否存在
Check if benchmark data exists
"""

import qlib
from qlib.data import D

def check_benchmark_exists(benchmark_symbol, data_path, start_date, end_date):
    """
    检查基准数据是否存在

    Args:
        benchmark_symbol: 基准代码，如 "SPY"
        data_path: Qlib数据路径
        start_date: 开始日期
        end_date: 结束日期

    Returns:
        bool: 是否存在
    """
    try:
        # 初始化Qlib
        qlib.init(provider_uri=data_path, region="us")

        # 尝试读取数据
        data = D.features(
            [benchmark_symbol],
            ["$close"],
            start_time=start_date,
            end_time=end_date
        )

        if data.empty:
            print(f"✗ {benchmark_symbol} 数据为空")
            return False

        print(f"✓ {benchmark_symbol} 数据存在")
        print(f"  数据点数: {len(data)}")
        print(f"  日期范围: {data.index.min()} 到 {data.index.max()}")
        print(f"  价格范围: ${data['$close'].min():.2f} - ${data['$close'].max():.2f}")
        return True

    except Exception as e:
        print(f"✗ {benchmark_symbol} 数据不存在")
        print(f"  错误: {e}")
        return False

if __name__ == "__main__":
    # 检查常见基准
    benchmarks = ["SPY", "QQQ", "DIA", "IWM"]
    data_path = "/Volumes/sandisk/quantmini-data/data/qlib/stocks_daily"

    print("检查基准数据...")
    print("=" * 60)

    for symbol in benchmarks:
        check_benchmark_exists(
            benchmark_symbol=symbol,
            data_path=data_path,
            start_date="2025-08-01",
            end_date="2025-09-29"
        )
        print()
```

### 示例2: 智能基准选择

```python
#!/usr/bin/env python3
"""
智能选择可用的基准
Smart benchmark selection
"""

import qlib
from qlib.data import D

def find_available_benchmark(data_path, start_date, end_date, preferred_order=None):
    """
    从候选列表中找到第一个可用的基准

    Args:
        data_path: Qlib数据路径
        start_date: 开始日期
        end_date: 结束日期
        preferred_order: 优先顺序列表

    Returns:
        str or None: 可用的基准代码，如果都不可用返回None
    """
    if preferred_order is None:
        preferred_order = ["SPY", "QQQ", "DIA", "IWM"]

    qlib.init(provider_uri=data_path, region="us")

    for benchmark in preferred_order:
        try:
            data = D.features(
                [benchmark],
                ["$close"],
                start_time=start_date,
                end_time=end_date
            )

            if not data.empty:
                print(f"✓ 使用基准: {benchmark}")
                return benchmark

        except Exception:
            continue

    print("✗ 没有找到可用的基准，建议设置 benchmark=None")
    return None

# 使用示例
if __name__ == "__main__":
    benchmark = find_available_benchmark(
        data_path="/Volumes/sandisk/quantmini-data/data/qlib/stocks_daily",
        start_date="2025-08-01",
        end_date="2025-09-29"
    )

    if benchmark:
        print(f"\n推荐配置:")
        print(f"  benchmark='{benchmark}'")
    else:
        print(f"\n推荐配置:")
        print(f"  benchmark=None")
```

### 示例3: 带基准检查的Strategy Example

```python
#!/usr/bin/env python3
"""
带基准检查的策略示例
Strategy example with benchmark check
"""

from qlib.backtest import backtest
from qlib.contrib.strategy import TopkDropoutStrategy

def backtest_with_smart_benchmark(strategy, start_time, end_time, data_path, **kwargs):
    """
    智能backtest：自动检查基准是否可用
    """
    import qlib
    from qlib.data import D

    # 初始化
    qlib.init(provider_uri=data_path, region="us")

    # 尝试找到可用的基准
    benchmark_candidates = ["SPY", "QQQ", "DIA"]
    benchmark = None

    for candidate in benchmark_candidates:
        try:
            data = D.features(
                [candidate],
                ["$close"],
                start_time=start_time,
                end_time=end_time
            )
            if not data.empty:
                benchmark = candidate
                print(f"✓ 使用基准: {benchmark}")
                break
        except:
            continue

    if benchmark is None:
        print("⚠️  没有找到基准数据，使用 benchmark=None")

    # 运行backtest
    portfolio_metric, indicator_metric = backtest(
        start_time=start_time,
        end_time=end_time,
        strategy=strategy,
        benchmark=benchmark,  # 自动选择的基准或None
        **kwargs
    )

    return portfolio_metric, indicator_metric

# 使用示例
strategy = TopkDropoutStrategy(topk=30, n_drop=5, signal=predictions)

portfolio_metric, indicator_metric = backtest_with_smart_benchmark(
    strategy=strategy,
    start_time="2025-09-09",
    end_time="2025-09-29",
    data_path="/Volumes/sandisk/quantmini-data/data/qlib/stocks_daily",
    account=100000000,
    exchange_kwargs={...}
)
```

---

## 常见问题 (FAQ)

### Q1: 一定要用基准吗？

**A:** 不一定！
- ✓ 可以设置 `benchmark=None`
- ✓ 策略照样能运行
- ✗ 只是看不到超额收益等指标

### Q2: 用哪个基准最好？

**A:** 取决于你的策略：
- 大盘股策略 → S&P 500 (SPY)
- 科技股策略 → Nasdaq 100 (QQQ)
- 小盘股策略 → Russell 2000 (IWM)
- 通用策略 → S&P 500 (SPY)

### Q3: 可以用自己的基准吗？

**A:** 可以！
- 任何有历史数据的股票/ETF都可以作为基准
- 甚至可以创建自定义的等权重基准

### Q4: 基准数据要多久的历史？

**A:**
- 至少覆盖你的回测期间
- 建议：回测期 + 额外6个月
- 例如：回测2020-2024，基准数据2019-2024

### Q5: 基准数据更新频率？

**A:**
- 日频策略 → 基准用日数据
- 分钟频策略 → 基准用分钟数据
- 月频策略 → 基准用日数据（会自动聚合）

---

## 总结 (Summary)

### 快速决策树

```
需要基准数据？
├─ 是
│  ├─ 有基准数据
│  │  └─ benchmark="SPY" ✓
│  └─ 没有基准数据
│     ├─ 可以下载
│     │  └─ 下载后使用 ✓
│     └─ 不能下载
│        └─ benchmark=None ✓
└─ 否
   └─ benchmark=None ✓
```

### 推荐行动

**立即 (今天)**:
```python
# 所有例子中使用
benchmark=None
```

**短期 (本周)**:
```bash
# 添加基准到数据收集
# 收集 SPY, QQQ, DIA, IWM

# 转换为Qlib格式
uv run python scripts/convert_to_qlib.py
```

**长期 (持续)**:
```python
# 创建智能基准选择函数
# 自动检测和使用可用基准
```

希望这个指南帮你解决了基准数据的问题！🎯
