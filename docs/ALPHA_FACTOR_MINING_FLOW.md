# Alpha 因子挖掘完整流程说明

本文档说明在当前项目中，从登录到产出高质量 Alpha 的**完整因子挖掘流程**，并以 **CHN TOP2000U** 为例给出具体操作步骤。

---

## 一、流程总览

```
登录 BRAIN
    ↓
选择区域 (如 CHN) → 确定 Universe (如 TOP2000U)、Delay
    ↓
获取该区域可用数据集 → 选择要用的数据集 (如 pv1, fund1, …)
    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│  Alpha Factory Pipeline（推荐：一条龙）                                   │
│  AI 分析数据集 → 推荐 fields → 模板调度 → Alpha 生成 → 去重 → 聚类        │
│       → 批量回测 → 筛选 → 自愈修复 → 保存高质量 Alpha + research_report   │
└─────────────────────────────────────────────────────────────────────────┘
    ↓
结果：research/<输出目录>/
      - research_report.json   （模板数、生成数、去重数、聚类数、回测成功数）
      - high_quality_alphas.json
      - alpha_cluster_report.json
      - 回测明细、报告等
```

---

## 二、CHN TOP2000U 实操步骤

### 前置准备

1. **依赖与配置**
   - 安装：`pip install -r requirements.txt`
   - 凭证：在 `config/credentials.json` 中配置 `["你的邮箱", "你的密码"]`
   - （若用 AI 分析）在 `config/api_keys.json` 中配置 AI API Key

2. **CHN 默认参数**（已在 `config/settings.py` 中配置）
   - **Universe**：`TOP2000U`
   - **Delay**：`1`

---

### 方式 A：一条龙——Alpha Factory Pipeline（推荐）

适合：希望一键完成「选字段 → 生成 → 回测 → 筛选 → 自愈 → 出报告」的完整链路。

**交互式（菜单）：**

```bash
python main.py
```

1. 选择 **1. 登录 BRAIN 平台**，完成登录。
2. 选择 **11. Alpha Factory Pipeline（全流程）**。
3. 按提示输入：
   - **区域代码**：`CHN`
   - **Universe**：直接回车即用默认 `TOP2000U`
   - **Delay**：直接回车即用默认 `1`
   - **数据集 ID**：输入一个或多个，逗号分隔，例如：
     - 单数据集：`pv1`
     - 多数据集：`pv1,fund1` 或 `pv1,analyst15`
4. 程序会自动执行：
   - AI 分析 CHN 下所选数据集的 metadata，得到推荐 fields
   - 模板调度（每轮约 20~30 个模板，来自 `config/alpha_config.py`）
   - Alpha 批量生成 → 去重 → 聚类
   - 批量回测 → 按 Sharpe/Fitness/Turnover 筛选 → 失败项自愈补考
   - 保存高质量 Alpha，并写入 **research_report.json**

**关于「metadata」和「推荐 fields」的说明**（详见下方 **2.1 节**）：
- **Metadata 在哪看**：Pipeline 会在输出目录下保存 `metadata_snapshot.json`（构建出的数据集/字段元数据）和 `ai_analysis_result.json`（AI 返回的推荐字段与中和数据集推荐），便于核对。
- **推荐 fields**：指 AI 根据 metadata 选出的、适合做 Alpha 因子挖掘的**字段列表**（含 `dataset_id`、`field_id`、`reason`、`priority`），这些字段会填入模板（如 `rank(ts_mean({field}, {window}))`）参与后续批量生成与回测。
- **Coverage 短时的设计**：当某数据集或字段 coverage 不足时，AI 会输出 **neutralization_datasets**（建议补充的数据集）；当前 Pipeline 会把这些建议写入 `ai_analysis_result.json`。若你希望「用 AI 推荐的新数据集再跑一轮」，可把该数据集 ID 加入后重新执行一次 Pipeline（见 2.1 节）。

**命令行（无需交互）：**

```bash
# 登录后自动跑全流程，结果输出到 research/alpha_factory_CHN_<数据集>_<时间戳>/
python main.py pipeline --region CHN --datasets pv1

# 多数据集
python main.py pipeline -r CHN -d pv1,fund1

# 指定输出目录
python main.py pipeline -r CHN -d pv1 --output-dir research/my_chn_study

# 不跑自愈（仅回测+筛选，不 AI 修复失败项）
python main.py pipeline -r CHN -d pv1 --no-self-heal
```

**结果查看：**

- 打开输出目录下的 `research_report.json`，可看到本轮的：
  - `templates_used`：使用的模板数
  - `alpha_generated`：生成 Alpha 数
  - `after_dedup`：去重后数量
  - `clusters`：聚类数量
  - `backtest_success`：回测成功数
- 高质量 Alpha 列表：`high_quality_alphas.json`
- 聚类分布：`alpha_cluster_report.json`
- **metadata / AI 分析**（便于核对「metadata 在哪」「推荐 fields / 中和数据集」）：
  - `metadata_snapshot.json`：本轮构建的数据集与字段元数据（dataset_metadata、field_metadata）
  - `ai_analysis_result.json`：AI 分析结果，含 `recommended_fields` 与 `neutralization_datasets`（coverage 短时 AI 推荐的补充数据集）

---

### 方式 B：分步操作（先看数据再决定）

适合：想先查看 CHN 有哪些数据集、哪些字段，再手动决定用哪些数据集做挖掘。

**第一步：登录并查看 CHN 可用数据集**

```bash
python main.py
# 选 1 登录
# 选 2 查看/获取区域数据集
# 区域输入：CHN
```

或 CLI：

```bash
python main.py datasets --region CHN
```

从输出中记下你要用的 **数据集 ID**（如 `pv1`、`fund1`、`analyst15` 等）。

**第二步：查看某数据集的字段（可选）**

```bash
# 菜单 3：查看/获取数据集字段，按提示选 CHN、pv1
# 或 CLI：
python main.py fields --region CHN --dataset pv1
```

用于确认该数据集在 CHN 下有哪些字段，便于理解后续 AI 推荐的 fields。

**第三步：跑 Alpha Factory Pipeline**

数据集选好后，按 **方式 A** 操作：

- 菜单选 **11**，区域填 `CHN`，数据集填上一步选好的 ID（如 `pv1` 或 `pv1,fund1`）；
- 或直接：`python main.py pipeline -r CHN -d pv1`（可替换为你的数据集组合）。

**第四步：查看报告与高质量 Alpha**

- 进入 `research/alpha_factory_CHN_xxx/` 查看 `research_report.json`、`high_quality_alphas.json` 及回测报告。

---

### 2.1 Metadata、推荐 fields 与「coverage 短时自动找数据集」说明

#### Metadata 是什么、在哪看？

- **含义**：对当前所选数据集（如 CHN 下的 pv1、fund1）的**两级元数据**：
  - **dataset_metadata**：数据集级，如 `dataset_id`、`dataset_name`、`category`、`coverage`、`region`、`frequency`。
  - **field_metadata**：字段级，如 `dataset_id`、`field_id`、`field_name`、`description`、`coverage`、`type`。
- **构建位置**：由 `ai/metadata_builder.build_metadata_for_region_datasets()` 在内存中构建，原始数据来自 BRAIN API（并会写入 `cache/regions/`、`cache/dataset_fields/` 等缓存）。
- **在 Pipeline 里怎么看**：运行 Alpha Factory Pipeline 后，在**当次输出目录**下会生成：
  - **metadata_snapshot.json**：本轮构建的 dataset_metadata + field_metadata 快照；
  - **ai_analysis_result.json**：AI 分析结果，含 `recommended_fields` 与 `neutralization_datasets`。
- 若不做 Pipeline、只想看某区域某数据集的原始缓存，可查看 `cache/regions/<region>.json`、`cache/dataset_fields/<region>_<dataset>.json`（具体路径以 `config/settings.py` 中 CACHE_DIR 为准）。

#### 「推荐 fields」指的是什么？

- **定义**：AI 根据上述 metadata，从当前数据集的字段里筛选出的、适合做 Alpha 因子挖掘的**字段列表**。
- **每条内容**：`dataset_id`、`field_id`（或 `field_name`）、`reason`（推荐理由）、`priority`（1–5，5 最优先）。
- **用途**：在「模板调度」之后，会用这些 `field_id` 去填模板里的占位符（如 `{field}`、`{field1}`、`{field2}`），再配合 window 等参数，批量生成具体 Alpha 表达式并回测。  
  例如：推荐字段里有 `close`、`volume`，模板有 `rank(ts_mean({field}, {window}))`，就会生成 `rank(ts_mean(close, 5))`、`rank(ts_mean(volume, 10))` 等，用于后续去重、聚类和回测。

#### 设计初衷：coverage 短时 AI 找新数据集搭配研究

- **你的初衷**：构建 metadata → 当输入的数据集 **data coverage 过短**时，AI **自己快速找到合适的新数据集**搭配研究，找出有效字段组合信号 → 用于后续 Alpha 批量生成。
- **当前实现**：
  - **已实现**：  
    - 构建 metadata 时包含每个数据集/字段的 **coverage**；  
    - AI 分析时使用 `config/alpha_config.COVERAGE_THRESHOLD`（默认 0.6），并明确要求：若某数据集或字段 coverage &lt; 阈值，可在 **neutralization_datasets** 中推荐其他数据集用于**中和或弥补**。  
    - AI 返回结果中有 **recommended_fields**（推荐字段）和 **neutralization_datasets**（建议补充的数据集及理由）。
  - **尚未自动化的部分**：  
    Pipeline 目前会保存 AI 的完整分析结果（含 `neutralization_datasets`）到 `ai_analysis_result.json`，但**不会**自动把「AI 推荐的新数据集」加入当前 run、重新拉 metadata、再跑一轮分析。也就是说：**AI 会给出“可搭配的新数据集”建议，但需要你根据建议手动把新数据集 ID 加入，再跑一次 Pipeline**，才能实现「用新数据集搭配、重新分析、再生成 Alpha」。
- **如何实现「coverage 短时用新数据集搭配研究」**：  
  1. 跑完一次 Pipeline 后，打开输出目录下的 **ai_analysis_result.json**，查看 **neutralization_datasets**。  
  2. 若 AI 推荐了补充数据集（如 `fund1`、`analyst15`），把你**当前用的数据集 ID 与推荐的数据集 ID 一起**作为新输入，再跑一次 Pipeline，例如：  
     `python main.py pipeline -r CHN -d pv1,fund1`（在原先只有 pv1 的基础上加上 fund1）。  
  3. 新的一轮会基于「pv1 + fund1」重新构建 metadata、重新让 AI 推荐字段（可能包含来自 fund1 的字段），再继续模板生成与回测。

若后续希望做成「AI 推荐了 neutralization_datasets 后自动扩展数据集并再跑一轮」，可以在 Pipeline 里加一步判断：当 `neutralization_datasets` 非空时，将推荐的数据集 ID 并入当前 `dataset_ids`，重新调用 `step_ai_analysis`（或整轮 Pipeline），并在文档中说明该行为。

---

## 三、如何选择 CHN 下的数据集

- **按需求选**：价量用 `pv1`，基金相关用 `fund1`，分析师用 `analyst15` 等（以 BRAIN 实际提供为准）。
- **先少后多**：建议先用单数据集（如 `pv1`）跑通 Pipeline，再尝试多数据集（如 `pv1,fund1`）。
- **看缓存**：`cache/regions/`、`cache/dataset_fields/` 下会有 CHN 的缓存，可辅助确认可用数据集与字段。

---

## 四、流程与模块对应关系（CHN 同理）

| 步骤           | 项目中的实现 |
|----------------|--------------|
| 登录           | `SessionManager.login` / 菜单 1 |
| 选区域/Universe | 输入 CHN → 默认 TOP2000U、delay=1 |
| 选数据集       | 输入 pv1 或 pv1,fund1 等 |
| AI 分析        | `ai/alpha_factory_pipeline.step_ai_analysis`（metadata_builder + data_analysis） |
| 推荐 fields    | 上一步输出 `recommended_fields` |
| 模板调度       | `step_template_schedule`（config：templates_per_round、TEMPLATE_SCHEDULE_DISTRIBUTION） |
| Alpha 生成     | `step_alpha_generation` → `alpha_generator.generate_alphas_from_expressions` |
| 去重           | `step_dedup` → `alpha_deduplicator.deduplicate` |
| 聚类           | `step_cluster` → `alpha_cluster.run_cluster_and_report` |
| 批量回测       | `step_backtest` → `backtest_loop.run_batch_backtest` |
| 筛选           | `step_filter` → `backtest_loop.filter_high_value`（MIN_SHARPE/MIN_FITNESS/MAX_TURNOVER） |
| 自愈修复       | `step_self_heal` → `researcher_brain.diagnose_and_fix` + 补考 |
| 保存与报告     | `step_save_high_quality` + `research_report.json` |

---

## 五、小结：针对 CHN TOP2000U 的最简路径

1. 配置好 `config/credentials.json`（及可选 `config/api_keys.json`）。
2. 运行：`python main.py pipeline -r CHN -d pv1`（将 `pv1` 换成你要的数据集或列表，如 `pv1,fund1`）。
3. 在 `research/alpha_factory_CHN_<数据集>_<时间戳>/` 下查看：
   - **research_report.json**：整体统计；
   - **high_quality_alphas.json**：通过筛选的 Alpha 列表。

若希望先了解 CHN 有哪些数据集再选，可先执行 `python main.py datasets --region CHN`，再按上面第 2、3 步执行。
