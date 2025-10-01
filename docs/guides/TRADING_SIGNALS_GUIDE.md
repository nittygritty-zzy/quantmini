# Trading Signals Guide for TopkDropoutStrategy

## 目录 (Table of Contents)

1. [信号类型概览 (Signal Types Overview)](#信号类型概览)
2. [技术指标信号 (Technical Indicators)](#技术指标信号)
3. [基本面信号 (Fundamental Signals)](#基本面信号)
4. [机器学习信号 (ML-Based Signals)](#机器学习信号)
5. [组合信号 (Combined Signals)](#组合信号)
6. [如何选择信号 (How to Choose Signals)](#如何选择信号)

---

## 信号类型概览 (Signal Types Overview)

TopkDropoutStrategy 需要一个 **signal** (信号)，这个信号就是给每只股票的**预测分数**。

### 什么是好的信号？ (What Makes a Good Signal?)

✅ **好信号的特征**:
1. **预测性强** - 分数高的股票真的会涨
2. **稳定性好** - 不会今天说好明天说坏
3. **可解释** - 知道为什么这个股票分数高
4. **计算效率** - 能快速计算大量股票

❌ **差信号的特征**:
1. 随机噪音多
2. 过度拟合历史
3. 计算太慢
4. 滞后性太强

---

## 技术指标信号 (Technical Indicators)

### 1. 动量信号 (Momentum Signals)

#### A. ROC - Rate of Change (涨跌幅)
```python
# 计算方法
signal = (price_today - price_20days_ago) / price_20days_ago

# 优点
✓ 简单直观
✓ 捕捉趋势
✓ 计算快速

# 缺点
✗ 滞后
✗ 震荡市场表现差

# 适用场景
趋势明显的市场
```

#### B. RSI - Relative Strength Index (相对强弱指标)
```python
# 计算方法
RSI = 100 - (100 / (1 + RS))
RS = 平均涨幅 / 平均跌幅 (14天)

# 优点
✓ 识别超买超卖
✓ 范围固定 (0-100)
✓ 广泛使用

# 缺点
✗ 震荡指标，不是方向指标
✗ 需要反向理解 (RSI高可能要卖)

# 适用场景
震荡市场，寻找反转机会
```

#### C. MACD - Moving Average Convergence Divergence
```python
# 计算方法
MACD = EMA(12) - EMA(26)
Signal = EMA(MACD, 9)
Histogram = MACD - Signal

# 优点
✓ 捕捉趋势变化
✓ 有明确的买卖信号
✓ 结合了趋势和动量

# 缺点
✗ 滞后性强
✗ 参数敏感

# 适用场景
中长期趋势跟踪
```

### 2. 趋势信号 (Trend Signals)

#### A. Moving Average (移动平均线)
```python
# 计算方法
MA_signal = (price - MA(20)) / MA(20)
# 或者
MA_signal = MA(5) - MA(20)  # 金叉死叉

# 优点
✓ 平滑噪音
✓ 识别趋势方向
✓ 多种周期可选

# 缺点
✗ 滞后严重
✗ 震荡市场假信号多

# 适用场景
明确的趋势市场
```

#### B. Bollinger Bands (布林带)
```python
# 计算方法
middle = MA(20)
upper = middle + 2 * std(20)
lower = middle - 2 * std(20)
signal = (price - middle) / (upper - lower)

# 优点
✓ 考虑波动率
✓ 自适应市场变化
✓ 识别突破

# 缺点
✗ 参数选择困难
✗ 强趋势时失效

# 适用场景
波动率分析，均值回归策略
```

### 3. 成交量信号 (Volume Signals)

#### A. Volume Ratio (成交量比率)
```python
# 计算方法
volume_ratio = volume_today / avg_volume(20_days)

# 优点
✓ 确认价格变化
✓ 识别资金流向
✓ 简单有效

# 缺点
✗ 单独使用效果差
✗ 需要结合价格

# 适用场景
配合其他信号使用
```

#### B. On-Balance Volume (OBV)
```python
# 计算方法
if price_today > price_yesterday:
    OBV = OBV_yesterday + volume_today
else:
    OBV = OBV_yesterday - volume_today

# 优点
✓ 累积量价关系
✓ 领先于价格变化
✓ 识别背离

# 缺点
✗ 计算复杂
✗ 需要长期数据

# 适用场景
识别趋势确认和背离
```

---

## 基本面信号 (Fundamental Signals)

### 1. 估值指标 (Valuation Metrics)

#### A. P/E Ratio (市盈率)
```python
# 计算方法
PE = 股价 / 每股收益

# 使用方式
# 低P/E可能被低估 (价值投资)
# 高P/E可能是成长股
signal = 1 / PE  # 反转，低P/E得高分

# 优点
✓ 最常用的估值指标
✓ 直观易懂
✓ 跨行业可比

# 缺点
✗ 受会计政策影响
✗ 不同行业差异大
✗ 忽略成长性

# 适用场景
价值投资策略
```

#### B. P/B Ratio (市净率)
```python
# 计算方法
PB = 股价 / 每股净资产

# 使用方式
signal = 1 / PB  # 低市净率可能被低估

# 优点
✓ 适合资产密集型行业
✓ 相对稳定
✓ 破净股票可能有机会

# 缺点
✗ 轻资产公司不适用
✗ 账面价值可能失真

# 适用场景
银行、地产等重资产行业
```

### 2. 成长指标 (Growth Metrics)

#### A. Revenue Growth (营收增长率)
```python
# 计算方法
revenue_growth = (revenue_this_quarter - revenue_last_year_same_quarter) / revenue_last_year_same_quarter

# 使用方式
signal = revenue_growth  # 增长快的得高分

# 优点
✓ 直接反映业务增长
✓ 较难操纵
✓ 成长股的关键指标

# 缺点
✗ 不考虑盈利能力
✗ 季度波动大

# 适用场景
成长股投资
```

#### B. Earnings Growth (盈利增长率)
```python
# 计算方法
earnings_growth = (earnings_this_quarter - earnings_last_year) / abs(earnings_last_year)

# 优点
✓ 最终落实到盈利
✓ 市场关注度高
✓ 影响股价明显

# 缺点
✗ 容易被操纵
✗ 波动大

# 适用场景
盈利驱动的成长股
```

### 3. 质量指标 (Quality Metrics)

#### A. ROE - Return on Equity (净资产收益率)
```python
# 计算方法
ROE = 净利润 / 股东权益

# 使用方式
signal = ROE  # 高ROE代表高质量

# 优点
✓ 衡量盈利能力
✓ 巴菲特最爱的指标
✓ 综合性强

# 缺点
✗ 可以通过杠杆提高
✗ 不同行业差异大

# 适用场景
寻找优质企业
```

#### B. Debt-to-Equity Ratio (资产负债率)
```python
# 计算方法
debt_to_equity = 总负债 / 股东权益

# 使用方式
signal = 1 / (1 + debt_to_equity)  # 低负债得高分

# 优点
✓ 衡量财务风险
✓ 简单直观
✓ 危机时期重要

# 缺点
✗ 不同行业标准不同
✗ 不是越低越好

# 适用场景
风险控制，防御性投资
```

---

## 机器学习信号 (ML-Based Signals)

### 1. Qlib Alpha158 (158个技术因子)

#### 特点
```python
# Qlib提供的预定义因子集
from qlib.contrib.data.handler import Alpha158

# 包含
✓ 158个技术指标
✓ 价格、成交量、波动率等多维度
✓ 不同时间周期 (5天, 10天, 20天, 30天, 60天)
✓ 自动标准化

# 优点
✓ 全面覆盖技术面
✓ 经过验证有效
✓ 开箱即用
✓ 自动特征工程

# 缺点
✗ 计算量大
✗ 特征相关性高
✗ 不包含基本面

# 适用场景
技术分析为主的量化策略
```

#### Alpha158 包含的主要因子类型
```
1. 价格动量 (30个)
   - KMID, KLEN, KMID2, KUP, KLOW
   - KSFT, OPEN0, ROC系列

2. 成交量 (20个)
   - CORR系列 (价格成交量相关性)
   - CNTP, CNTN, CNTD (正负成交量天数)
   - SUMP, SUMN, SUMD (成交量和)

3. 波动率 (20个)
   - VSTD系列 (不同周期波动率)
   - WVMA (加权波动)

4. 趋势 (30个)
   - MA系列 (移动平均)
   - QTLU, QTLD (分位数指标)
   - BETA (市场相关性)

5. 其他技术指标 (58个)
   - RSI系列
   - MACD相关
   - 布林带相关
   - ...
```

### 2. LightGBM 预测

#### 使用方法
```python
# 训练模型
from qlib.contrib.model.gbdt import LGBModel

model = LGBModel(
    loss="mse",
    num_leaves=31,
    learning_rate=0.05
)
model.fit(dataset)

# 生成信号
signal = model.predict(dataset, segment="test")

# 优点
✓ 自动学习特征组合
✓ 处理非线性关系
✓ 特征重要性排序
✓ 性能优异

# 缺点
✗ 需要大量训练数据
✗ 容易过拟合
✗ 黑盒模型

# 适用场景
有足够历史数据的量化策略
```

### 3. Neural Networks (神经网络)

#### 使用方法
```python
# LSTM for time series
# Transformer for sequences
# MLP for tabular data

# 优点
✓ 捕捉复杂模式
✓ 端到端学习
✓ 处理时间序列

# 缺点
✗ 训练时间长
✗ 需要大量数据
✗ 过拟合风险高
✗ 可解释性差

# 适用场景
高频交易，复杂模式识别
```

---

## 组合信号 (Combined Signals)

### 1. 多因子组合

#### A. 简单平均
```python
# 方法
signal = (momentum_signal + value_signal + quality_signal) / 3

# 优点
✓ 简单直接
✓ 降低单一信号风险

# 缺点
✗ 权重固定
✗ 可能相互抵消
```

#### B. 加权平均
```python
# 方法
signal = 0.5 * momentum + 0.3 * value + 0.2 * quality

# 优点
✓ 可以调整重要性
✓ 根据回测优化权重

# 缺点
✗ 需要优化权重
✗ 权重可能过拟合
```

#### C. 机器学习组合
```python
# 方法: 用ML模型学习最优组合
features = [momentum, value, quality, volume, ...]
signal = model.predict(features)

# 优点
✓ 自动学习最优权重
✓ 捕捉非线性关系
✓ 自适应

# 缺点
✗ 需要训练数据
✗ 可能过拟合
```

### 2. 行业中性化

```python
# 方法: 在每个行业内排名
def industry_neutral_signal(signal, industry):
    for ind in industries:
        mask = (industry == ind)
        signal[mask] = (signal[mask] - mean(signal[mask])) / std(signal[mask])
    return signal

# 优点
✓ 减少行业暴露
✓ 纯粹的选股能力
✓ 降低系统性风险

# 缺点
✗ 可能错过行业机会
✗ 计算复杂
```

---

## 如何选择信号 (How to Choose Signals)

### 1. 根据投资风格选择

#### 价值投资 (Value Investing)
```python
推荐信号:
1. P/E Ratio (低市盈率)
2. P/B Ratio (低市净率)
3. Dividend Yield (高股息率)
4. ROE (高净资产收益率)

组合策略:
signal = 1/PE * 0.4 + 1/PB * 0.3 + dividend_yield * 0.2 + ROE * 0.1
```

#### 成长投资 (Growth Investing)
```python
推荐信号:
1. Revenue Growth (营收增长)
2. Earnings Growth (盈利增长)
3. ROE Trend (ROE增长趋势)
4. Momentum (价格动量)

组合策略:
signal = revenue_growth * 0.4 + earnings_growth * 0.3 + momentum * 0.3
```

#### 动量投资 (Momentum Investing)
```python
推荐信号:
1. ROC (涨跌幅)
2. MA Crossover (均线交叉)
3. Volume Ratio (成交量比)
4. RSI (相对强弱)

组合策略:
signal = ROC_20d * 0.5 + volume_ratio * 0.3 + RSI_signal * 0.2
```

#### 量化投资 (Quantitative)
```python
推荐信号:
1. Alpha158 + LightGBM
2. 多因子机器学习
3. 技术指标组合

组合策略:
# 使用ML自动学习
model = LGBModel()
model.fit(alpha158_features)
signal = model.predict()
```

### 2. 信号评估标准

#### A. IC (Information Coefficient)
```python
# 计算方法
IC = correlation(signal_today, return_next_day)

# 评价标准
IC > 0.05  ⭐⭐⭐⭐⭐ 优秀
IC > 0.03  ⭐⭐⭐⭐   良好
IC > 0.01  ⭐⭐⭐     可用
IC < 0.01  ⭐⭐       差
IC < 0     ⭐         无效

# 意义
IC衡量信号的预测能力
```

#### B. IR (Information Ratio)
```python
# 计算方法
IR = mean(IC) / std(IC)

# 评价标准
IR > 0.5   ⭐⭐⭐⭐⭐ 优秀
IR > 0.3   ⭐⭐⭐⭐   良好
IR > 0.1   ⭐⭐⭐     可用
IR < 0.1   ⭐⭐       差

# 意义
IR衡量信号的稳定性
```

#### C. Turnover vs Return
```python
# 权衡
高换手率 → 高交易成本 → 需要更高收益
低换手率 → 低交易成本 → 可以接受较低收益

# 评估
return_after_cost = gross_return - turnover * transaction_cost

# 目标
最大化 return_after_cost
```

### 3. 实际使用建议

#### 新手推荐
```python
# 方案1: 使用Qlib现成的
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.model.gbdt import LGBModel

dataset = DatasetH(handler=Alpha158(...))
model = LGBModel()
model.fit(dataset)
signal = model.predict(dataset)

# 优点
✓ 开箱即用
✓ 经过验证
✓ 不需要调参

# 适合
刚开始量化投资的用户
```

#### 进阶推荐
```python
# 方案2: 自定义因子 + ML
features = create_custom_features()  # 自己的因子
model = LGBModel()  # 或其他ML模型
model.fit(features)
signal = model.predict()

# 优点
✓ 可以融入自己的想法
✓ 灵活性高
✓ 可能发现alpha

# 适合
有一定经验的量化投资者
```

#### 专业推荐
```python
# 方案3: 多策略ensemble
signal_momentum = momentum_model.predict()
signal_value = value_model.predict()
signal_ml = ml_model.predict()

# 集成
signal_final = ensemble(signal_momentum, signal_value, signal_ml)

# 优点
✓ 降低单一策略风险
✓ 更稳定
✓ 适应不同市场环境

# 适合
专业量化团队
```

---

## 常见问题 (FAQ)

### Q1: 我应该用几个信号？

```
初学者: 1个
- 专注理解一个信号
- 推荐: Alpha158 + LightGBM

中级: 2-3个
- 技术 + 基本面结合
- 例如: 动量 + 价值 + 质量

高级: 3-5个
- 多维度覆盖
- 使用ML自动组合
```

### Q2: 技术指标好还是基本面好？

```
技术指标:
✓ 反应快
✓ 高频交易适用
✓ 数据容易获取
✗ 噪音多
✗ 可能失效

基本面:
✓ 稳定
✓ 长期有效
✓ 逻辑清晰
✗ 更新慢
✗ 数据获取难

建议: 两者结合！
```

### Q3: 信号多久更新一次？

```
日频策略: 每天更新
- TopkDropoutStrategy通常用日频
- 使用收盘价计算信号

分钟频策略: 每分钟更新
- 高频交易
- 需要快速计算

月频策略: 每月更新
- 基本面策略
- 换手率低
```

### Q4: 如何避免过拟合？

```
1. 使用简单信号
   - 复杂不一定好

2. 分离训练和测试集
   - 严格不能提前看测试数据

3. 增加正则化
   - ML模型加L1/L2

4. 实盘验证
   - 纸面交易验证信号
```

---

## 推荐的信号组合方案

### 方案A: 极简版（新手）
```python
# 只用Qlib的Alpha158
handler = Alpha158(start_time="2020-01-01", end_time="2024-12-31")
model = LGBModel()
signal = model.predict()

策略配置:
topk = 30
n_drop = 3
预期: IR ~ 0.2-0.4
```

### 方案B: 均衡版（进阶）
```python
# 技术 + 基本面
momentum = calculate_momentum()  # ROC 20天
value = calculate_value()        # P/E反转
quality = calculate_quality()    # ROE

signal = 0.4 * momentum + 0.3 * value + 0.3 * quality

策略配置:
topk = 50
n_drop = 5
预期: IR ~ 0.3-0.5
```

### 方案C: 专业版（高级）
```python
# 多模型ensemble
model1 = LGBModel()  # 技术指标模型
model2 = XGBModel()  # 基本面模型
model3 = NNModel()   # 深度学习模型

signal1 = model1.predict()
signal2 = model2.predict()
signal3 = model3.predict()

# 动态权重
weights = optimize_weights(signal1, signal2, signal3)
signal = weights[0]*signal1 + weights[1]*signal2 + weights[2]*signal3

策略配置:
topk = 100
n_drop = 10
预期: IR ~ 0.5+
```

---

## 总结 (Summary)

### 关键要点

1. **没有完美的信号**
   - 所有信号都有失效的时候
   - 需要持续监控和调整

2. **简单往往更好**
   - 复杂的信号容易过拟合
   - 从简单开始，逐步优化

3. **组合降低风险**
   - 多个信号互相补充
   - 降低单一信号失效的影响

4. **回测必不可少**
   - 必须用历史数据验证
   - 但要警惕过拟合

5. **持续学习**
   - 市场在变化
   - 信号需要不断更新

### 开始建议

```python
# 第一步: 从Alpha158开始
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.model.gbdt import LGBModel

# 第二步: 训练模型
dataset = DatasetH(handler=Alpha158(...))
model = LGBModel()
model.fit(dataset)

# 第三步: 生成信号
signal = model.predict(dataset, segment="test")

# 第四步: 使用TopkDropoutStrategy
strategy = TopkDropoutStrategy(
    topk=30,
    n_drop=5,
    signal=signal
)

# 第五步: 回测评估
# 分析结果，调整参数

# 第六步: 实盘小资金验证
# 确认有效后逐步增加资金
```

祝你投资成功！🎯📈
