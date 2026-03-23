"""
AIAC2025_v2 - BRAIN 量化因子挖掘平台 统一入口
支持交互式菜单和命令行参数两种模式

用法:
  python main.py              # 交互式菜单模式
  python main.py datasets     # 命令行: 获取数据集
  python main.py fields       # 命令行: 获取字段
  python main.py backtest     # 命令行: 执行回测
"""
import argparse
import json
import sys
from pathlib import Path

# 确保项目根目录在 Python path 中
PROJECT_ROOT = Path(__file__).parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))

from core.session_manager import SessionManager
from core.data_manager import DataManager
from core.alpha_builder import AlphaBuilder
from core.backtest_runner import BacktestRunner
from config.settings import REGION_DEFAULTS


def print_banner():
    """打印欢迎横幅"""
    print()
    print("=" * 60)
    print("  🧠 BRAIN 量化因子挖掘平台 v2.0")
    print("  WorldQuant BRAIN Alpha Research Tool")
    print("=" * 60)
    print()


def print_menu():
    """打印主菜单"""
    print("┌─────────────────────────────────────┐")
    print("│           主菜单                     │")
    print("├─────────────────────────────────────┤")
    print("│  1. 登录 BRAIN 平台                  │")
    print("│  2. 查看/获取区域数据集               │")
    print("│  3. 查看/获取数据集字段               │")
    print("│  4. 获取操作符列表                   │")
    print("│  5. 构建 Alpha 表达式                 │")
    print("│  6. 批量回测                         │")
    print("│  7. 查看回测报告                     │")
    print("│  8. 缓存状态                        │")
    print("├──── AI / 高级功能 ───────────────────┤")
    print("│  9. Alpha Factory Pipeline（推荐）   │")
    print("│  10. 回测结果筛选/导出                │")
    print("│  11. Alpha 优化器                    │")
    print("│  12. Alpha 组合优化                  │")
    print("│  13. AI 数据集配置生成器              │")
    print("│  0. 退出                            │")
    print("└─────────────────────────────────────┘")


def handle_login():
    """处理登录"""
    if SessionManager.is_logged_in():
        timeout = SessionManager.check_timeout()
        print(f"✅ 已登录 (剩余 {timeout} 秒)")
        re = input("是否重新登录? (y/N): ").strip().lower()
        if re != "y":
            return
    SessionManager.login()


def handle_datasets():
    """处理数据集查询"""
    session = SessionManager.get_session()
    print("\n可用区域:", ", ".join(REGION_DEFAULTS.keys()))
    region = input("请输入区域代码 (如 USA/IND/CHN): ").strip().upper()

    if region not in REGION_DEFAULTS:
        print(f"⚠️ 未知区域: {region}, 将尝试使用默认值继续")
        default_uni, default_delay = "TOP3000", "1"
    else:
        default_uni = REGION_DEFAULTS[region]["universe"]
        default_delay = str(REGION_DEFAULTS[region]["delay"])

    universe = input(f"请输入 Universe (默认 {default_uni}): ").strip() or default_uni
    try:
        delay_str = input(f"请输入 Delay (默认 {default_delay}): ").strip() or default_delay
        delay = int(delay_str)
    except ValueError:
        delay = int(default_delay)

    print("\n💡 提示: '强制刷新缓存' 会忽略本地已保存的旧数据，向平台重新请求最新列表。")
    refresh = input("是否强制刷新缓存? (y/N): ").strip().lower() == "y"
    datasets = DataManager.get_datasets(session, region, universe=universe, delay=delay, force_refresh=refresh)

    if datasets is not None and not datasets.empty:
        print(f"\n📊 {region} 区域共 {len(datasets)} 个数据集:")
        # 显示关键列
        display_cols = []
        for col in ["id", "name", "category", "coverage", "subcategory"]:
            if col in datasets.columns:
                display_cols.append(col)
        if display_cols:
            print(datasets[display_cols].to_string(index=False, max_rows=30))
        else:
            print(datasets.head(30).to_string(index=False))
    else:
        print("❌ 未获取到数据集 (可能该区域在此 Universe/Delay 下无数据，或拼写错误)")


def handle_fields():
    """处理字段查询"""
    session = SessionManager.get_session()

    # 显示已缓存的区域
    cached = DataManager.list_cached_regions()
    if cached:
        print(f"\n已缓存的区域: {', '.join(cached)}")

    region = input("请输入区域代码 (如 USA/IND/CHN): ").strip().upper()
    dataset_id = input("请输入数据集ID (如 pv1, analyst4, fundamental17): ").strip()

    if not dataset_id:
        print("❌ 数据集ID不能为空")
        return

    if region not in REGION_DEFAULTS:
        default_uni, default_delay = "TOP3000", "1"
    else:
        default_uni = REGION_DEFAULTS[region]["universe"]
        default_delay = str(REGION_DEFAULTS[region]["delay"])

    universe = input(f"请输入 Universe (默认 {default_uni}): ").strip() or default_uni
    try:
        delay_str = input(f"请输入 Delay (默认 {default_delay}): ").strip() or default_delay
        delay = int(delay_str)
    except ValueError:
        delay = int(default_delay)

    print("\n💡 提示: '强制刷新缓存' 会忽略本地已保存的旧数据，向平台重新请求最新列表。")
    refresh = input("是否强制刷新缓存? (y/N): ").strip().lower() == "y"
    fields = DataManager.get_fields(session, region, dataset_id, universe=universe, delay=delay, force_refresh=refresh)

    if fields is not None and not fields.empty:
        print(f"\n📊 {region}/{dataset_id} 共 {len(fields)} 个字段:")
        display_cols = []
        for col in ["id", "description", "coverage", "type"]:
            if col in fields.columns:
                display_cols.append(col)
        if display_cols:
            # 按覆盖率排序显示
            if "coverage" in fields.columns:
                fields_sorted = fields.sort_values("coverage", ascending=False)
            else:
                fields_sorted = fields
            print(fields_sorted[display_cols].to_string(index=False, max_rows=50))
        else:
            print(fields.head(50).to_string(index=False))
    else:
        print("❌ 未获取到字段 (可能数据集不存在，或拼写错误)")


def handle_operators():
    """处理操作符查询"""
    session = SessionManager.get_session()
    refresh = input("是否强制刷新缓存? (y/N): ").strip().lower() == "y"
    operators = DataManager.get_operators(session, force_refresh=refresh)

    if operators is not None and len(operators) > 0:
        print(f"\n📊 共 {len(operators)} 个操作符:")
        display_cols = []
        for col in ["name", "scope", "description"]:
            if col in operators.columns:
                display_cols.append(col)
        if display_cols:
            print(operators[display_cols].to_string(index=False, max_rows=40))
        else:
            print(operators.head(40).to_string(index=False))
    else:
        print("❌ 未获取到操作符")


def handle_build_alpha():
    """处理Alpha构建"""
    print("\n📋 Alpha 构建方式:")
    print("  1. 手动输入表达式")
    print("  2. 从模板生成")

    choice = input("请选择 (1/2): ").strip()

    if choice == "1":
        expression = input("请输入Alpha表达式: ").strip()
        if not expression:
            print("❌ 表达式不能为空")
            return

        region = input("区域代码 (默认 USA): ").strip().upper() or "USA"

        # 验证
        result = AlphaBuilder.validate_expression(expression)
        AlphaBuilder.print_validation(result)

        if result["valid"]:
            config = AlphaBuilder.build_config(expression, region)
            print(f"\n✅ Alpha配置已生成:")
            print(f"   表达式: {expression}")
            print(f"   区域: {region}")
            print(f"   Universe: {config['settings']['universe']}")

            # 是否立即回测
            run_now = input("\n是否立即回测? (y/N): ").strip().lower()
            if run_now == "y":
                session = SessionManager.get_session()
                result = BacktestRunner.run_single(session, config)
                if result["success"]:
                    print(f"\n{'=' * 50}")
                    print(f"  Alpha ID: {result['alpha_id']}")
                    print(f"  Sharpe:   {result['sharpe']:.3f}")
                    print(f"  Fitness:  {result['fitness']:.3f}")
                    print(f"  Turnover: {result['turnover']:.2%}")
                    print(f"  失败检查: {result['n_failed']} 个")
                    if result['failed_checks']:
                        for fc in result['failed_checks']:
                            print(f"    ❌ {fc}")
                    print(f"{'=' * 50}")

    elif choice == "2":
        AlphaBuilder.list_templates()
        template_name = input("请输入模板名称: ").strip()
        region = input("区域代码 (默认 USA): ").strip().upper() or "USA"

        # 需要字段列表
        fields_input = input("请输入字段列表 (逗号分隔): ").strip()
        if not fields_input:
            print("❌ 字段列表不能为空")
            return

        fields = [f.strip() for f in fields_input.split(",")]
        configs = AlphaBuilder.generate_from_template(template_name, fields, region)

        if configs:
            print(f"\n预览前5个:")
            for i, c in enumerate(configs[:5], 1):
                print(f"  {i}. {c.get('regular', 'N/A')}")

            run_now = input(f"\n是否批量回测全部 {len(configs)} 个? (y/N): ").strip().lower()
            if run_now == "y":
                session = SessionManager.get_session()
                results = BacktestRunner.run_batch(session, configs)
                # 保存结果
                BacktestRunner.save_research(results, region, "template")


def handle_batch_backtest():
    """处理批量回测（支持断点续传和抽样）"""
    from datetime import datetime
    from pathlib import Path

    print("\n📋 批量回测来源:")
    print("  1. 从JSON文件加载表达式")
    print("  2. 手动输入多个表达式")

    choice = input("请选择 (1/2): ").strip()
    region = input("区域代码 (默认 USA): ").strip().upper() or "USA"

    if choice == "1":
        file_path = input("JSON文件路径: ").strip()
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            alpha_metadata = None  # 用于保存溯源元数据

            if isinstance(data, list):
                if isinstance(data[0], str):
                    expressions = data
                elif isinstance(data[0], dict):
                    expressions = [d.get("expression", d.get("regular", "")) for d in data]
                    # 保留原始数据作为元数据
                    alpha_metadata = data
                else:
                    expressions = data
            else:
                print("❌ JSON格式不支持，请使用表达式字符串列表")
                return
        except Exception as e:
            print(f"❌ 读取文件失败: {e}")
            return

    elif choice == "2":
        print("请输入Alpha表达式 (每行一个, 空行结束):")
        expressions = []
        alpha_metadata = None  # 手动输入没有元数据
        while True:
            line = input().strip()
            if not line:
                break
            expressions.append(line)
        if not expressions:
            print("❌ 未输入任何表达式")
            return
    else:
        return

    print(f"\n共 {len(expressions)} 个表达式待回测")

    # 准备输出目录
    dataset = input("数据集名称 (用于归档, 默认 mixed): ").strip() or "mixed"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"research/{region}_{dataset}_{timestamp}"

    # 检查是否有未完成的回测
    progress_file = Path(output_dir) / "progress.json"
    resume = False

    if progress_file.exists():
        print("\n⚠️  发现未完成的回测任务")
        resume_choice = input("是否续传? (y/N): ").strip().lower()
        if resume_choice == "y":
            resume = True
            print("✅ 将从上次中断处继续")

    # 回测模式选择（仅在非续传模式下）
    sample_mode = "all"
    sample_count = None

    if not resume:
        print("\n📊 回测模式:")
        print("  1. 全部回测")
        print("  2. 回测前 N 个")
        print("  3. 随机抽样 N 个")

        mode_choice = input("请选择 (1-3, 默认 1): ").strip() or "1"

        if mode_choice == "2":
            sample_mode = "first_n"
            sample_count = int(input("请输入数量: ").strip())
            print(f"✅ 将回测前 {sample_count} 个")
        elif mode_choice == "3":
            sample_mode = "random_n"
            sample_count = int(input("请输入数量: ").strip())
            print(f"✅ 将随机抽样 {sample_count} 个")

    # 回测参数准备
    backtest_params = {}

    # 干预窗口：回测参数
    def display_backtest_params(params):
        print(f"\n{'=' * 80}")
        print("🔧 回测参数:")
        print(f"{'=' * 80}")
        if not params:
            print("  使用默认参数（从 region 推断）")
        else:
            for key, value in params.items():
                print(f"  {key} = {value}")

    action, backtest_params = intervention_gate(
        "回测参数",
        display_backtest_params,
        edit_backtest_params,
        backtest_params
    )

    if action == "cancel":
        print("❌ 用户取消操作")
        return

    # 构建配置（传入用户修改后的参数）
    configs = AlphaBuilder.build_batch_configs(expressions, region, **backtest_params)

    # 执行回测
    session = SessionManager.get_session()
    runner = BacktestRunner()

    results = runner.run_batch(
        session,
        configs,
        output_dir=output_dir,
        sample_mode=sample_mode,
        sample_count=sample_count,
        resume=resume,
        alpha_metadata=alpha_metadata
    )

    # ==================== 自愈系统：错误修复与重试 ====================

    # 提取失败案例
    failed_alphas = [r for r in results if not r.get("success", False) and r.get("error_analysis")]

    if failed_alphas:
        print(f"\n⚠️  检测到 {len(failed_alphas)} 个失败的 Alpha")
        print(f"{'=' * 60}")

        # 显示失败统计
        error_types = {}
        for failed in failed_alphas:
            error_analysis = failed.get("error_analysis", {})
            error_category = error_analysis.get("error_category", "unknown")
            error_types[error_category] = error_types.get(error_category, 0) + 1

        print("📊 错误类型统计:")
        for error_cat, count in error_types.items():
            print(f"  - {error_cat}: {count} 个")

        # 询问是否启用自愈系统
        retry_choice = input("\n是否启用自愈系统尝试修复? (Y/n): ").strip().lower()

        if retry_choice != "n":
            print(f"\n🔧 [Self-Healing] 启动自愈系统...")
            print(f"{'=' * 60}")

            # 初始化 AI 研究员
            from ai.researcher_brain import AIResearcher
            researcher = AIResearcher()

            # 提取数据集信息（从第一个配置中获取）
            dataset_id = input("数据集 ID (如 pv1): ").strip()
            if not dataset_id:
                print("❌ 未提供数据集 ID，无法修复")
            else:
                fixed_alphas = []
                fix_logs = []

                for idx, failed in enumerate(failed_alphas, 1):
                    error_analysis = failed.get("error_analysis", {})
                    error_category = error_analysis.get("error_category", "unknown")
                    affected_entity = error_analysis.get("affected_entity", "unknown")

                    print(f"\n[{idx}/{len(failed_alphas)}] [Self-Healing] 检测到 {error_category} 错误: '{affected_entity}'")

                    # 诊断修复
                    try:
                        fix_result = researcher.diagnose_and_fix(
                            error_analysis,
                            failed,
                            session,
                            region,
                            dataset_id
                        )

                        if fix_result["success"]:
                            print(f"  ✅ {fix_result['fix_log']}")
                            print(f"  📊 修复置信度: {fix_result['confidence']:.2f}")

                            if fix_result.get("risk_warning"):
                                print(f"  ⚠️  风险提示: {fix_result['risk_warning']}")

                            if fix_result["confidence"] > 0.6:
                                fixed_alphas.append(fix_result["fixed_alpha"])
                                fix_logs.append({
                                    "original_expression": failed.get("expression", ""),
                                    "fixed_expression": fix_result["fixed_alpha"].get("expression", ""),
                                    "fix_log": fix_result["fix_log"],
                                    "confidence": fix_result["confidence"]
                                })
                            else:
                                print(f"  ⚠️  置信度过低，跳过修复")
                        else:
                            print(f"  ❌ {fix_result['fix_log']}")

                    except Exception as e:
                        print(f"  ❌ 修复失败: {e}")

                # 重新提交"补考"回测
                if fixed_alphas:
                    print(f"\n{'=' * 60}")
                    print(f"🔄 [Self-Healing] 正在提交 {len(fixed_alphas)} 个修复后的 Alpha 进行补考回测...")
                    print(f"{'=' * 60}")

                    # 构建修复后的配置
                    fixed_configs = []
                    for fixed_alpha in fixed_alphas:
                        # 保持原有配置，只替换表达式
                        fixed_config = AlphaBuilder.build_config(
                            fixed_alpha.get("expression", ""),
                            region
                        )
                        fixed_configs.append(fixed_config)

                    # 执行补考回测
                    retry_results = runner.run_batch(
                        session,
                        fixed_configs,
                        output_dir=output_dir + "_retry",
                        sample_mode="all"
                    )

                    # 统计补考结果
                    retry_success = [r for r in retry_results if r.get("success", False)]
                    retry_high_value = [r for r in retry_success if r["sharpe"] >= 1.0]

                    print(f"\n{'=' * 60}")
                    print(f"📊 [Self-Healing] 补考结果:")
                    print(f"  - 提交: {len(fixed_alphas)} 个")
                    print(f"  - 成功: {len(retry_success)} 个")
                    print(f"  - 高价值 (Sharpe≥1.0): {len(retry_high_value)} 个")
                    print(f"{'=' * 60}")

                    # 合并结果
                    results.extend(retry_results)

                    # 保存修复日志
                    fix_log_file = Path(output_dir) / "self_healing_log.json"
                    fix_log_file.parent.mkdir(parents=True, exist_ok=True)
                    with open(fix_log_file, "w", encoding="utf-8") as f:
                        json.dump(fix_logs, f, ensure_ascii=False, indent=2)
                    print(f"\n💾 修复日志已保存: {fix_log_file}")

                else:
                    print(f"\n⚠️  没有可修复的 Alpha（置信度过低或不支持的错误类型）")

    # 保存研究归档
    if results:
        BacktestRunner.save_research(results, region, dataset)
    else:
        print("\n⚠️  无回测结果，未生成归档")


def handle_view_report():
    """处理查看报告"""
    print("\n📋 查看方式:")
    print("  1. 输入Alpha ID查看")
    print("  2. 查看最近研究报告")

    choice = input("请选择 (1/2): ").strip()

    if choice == "1":
        alpha_id = input("Alpha ID: ").strip()
        if not alpha_id:
            return
        session = SessionManager.get_session()
        result = BacktestRunner.get_report(session, alpha_id)
        if result["success"]:
            print(f"\n{'=' * 50}")
            print(f"  Alpha ID:  {result['alpha_id']}")
            print(f"  表达式:    {result['expression']}")
            print(f"  Sharpe:    {result['sharpe']:.3f}")
            print(f"  Fitness:   {result['fitness']:.3f}")
            print(f"  Turnover:  {result['turnover']:.2%}")
            print(f"  Returns:   {result['returns']:.4f}")
            print(f"  Drawdown:  {result['drawdown']:.4f}")
            print(f"  Long/Short: {result['long_count']}/{result['short_count']}")
            print(f"  通过检查:  {result['n_passed']} 个")
            print(f"  失败检查:  {result['n_failed']} 个")
            for fc in result.get("failed_checks", []):
                print(f"    ❌ {fc}")
            print(f"{'=' * 50}")
        else:
            print("❌ 获取报告失败")

    elif choice == "2":
        from config.settings import RESEARCH_DIR
        if RESEARCH_DIR.exists():
            dirs = sorted(RESEARCH_DIR.iterdir(), reverse=True)
            if dirs:
                print("\n📁 最近的研究:")
                for i, d in enumerate(dirs[:10], 1):
                    print(f"  {i}. {d.name}")
                    report = d / "report.md"
                    if report.exists():
                        print(f"     → {report}")
            else:
                print("暂无研究记录")
        else:
            print("暂无研究记录")


def print_analysis_result(result):
    """格式化打印数据分析结果（使用 Markdown 表格）"""
    print(f"\n{'=' * 80}")
    print("📊 AI 筛选的核心字段:")
    print(f"{'=' * 80}")

    # 核心字段表格
    core_fields = result.get("core_fields", [])
    if core_fields:
        print("\n| # | 字段名 | 语义类型 | 平台类型 | 金融逻辑 | 预期方向 | 覆盖率 | 时间覆盖 | 建议操作符 |")
        print("|---|--------|----------|----------|----------|----------|--------|----------|------------|")
        for i, field in enumerate(core_fields, 1):
            field_name = field.get('field_name', 'N/A')
            field_type = field.get('field_type', 'N/A')
            data_type = field.get('data_type', 'N/A')
            logic = field.get('logic', 'N/A')
            if len(logic) > 40:
                logic = logic[:37] + "..."
            direction = field.get('expected_direction', 'N/A')

            # 格式化覆盖率
            cov = field.get('coverage')
            cov_str = f"{cov:.1%}" if cov is not None else "N/A"
            if field.get('coverage_warning'):
                cov_str += "⚠️"

            dcov = field.get('dateCoverage')
            dcov_str = f"{dcov:.1%}" if dcov is not None else "N/A"
            if field.get('date_coverage_warning'):
                dcov_str += "⚠️"

            operators = ', '.join(field.get('suggested_operators', [])[:3])
            print(f"| {i} | {field_name} | {field_type} | {data_type} | {logic} | {direction} | {cov_str} | {dcov_str} | {operators} |")

    # 字段组合表格
    field_combinations = result.get("field_combinations", [])
    if field_combinations:
        print(f"\n{'=' * 80}")
        print("🔗 推荐的字段组合:")
        print(f"{'=' * 80}")
        print("\n| # | 字段组合 | 金融逻辑 | 类型 |")
        print("|---|----------|----------|------|")
        for i, combo in enumerate(field_combinations, 1):
            combination = combo.get('combination', 'N/A')
            logic = combo.get('logic', 'N/A')
            if len(logic) > 50:
                logic = logic[:47] + "..."
            combo_type = combo.get('type', 'N/A')
            print(f"| {i} | {combination} | {logic} | {combo_type} |")

    # 推荐操作符
    operators = result.get('available_operators', [])
    if operators:
        print(f"\n{'=' * 80}")
        print(f"🔧 推荐操作符: {', '.join(operators)}")
        print(f"{'=' * 80}")


# ==================== 老板干预系统 ====================

def intervention_gate(gate_name, display_fn, edit_fn, data):
    """
    通用干预窗口：展示 → 等待用户输入 A/E/C → 编辑 → 循环

    Args:
        gate_name: 干预窗口名称（如 "分析结果"）
        display_fn: 展示函数 display_fn(data)
        edit_fn: 编辑函数 edit_fn(data, instruction) -> data
        data: 待干预的数据

    Returns:
        (action, data): action 为 "confirm" 或 "cancel"
    """
    while True:
        # 展示数据
        display_fn(data)

        # 等待用户输入
        print(f"\n{'=' * 80}")
        print(f"🚪 {gate_name} 干预窗口")
        print(f"{'=' * 80}")
        print("  [A] 确认并继续")
        print("  [E] 编辑修改")
        print("  [C] 取消操作")

        choice = input("\n请选择 (A/E/C): ").strip().upper()

        if choice == "A":
            return ("confirm", data)
        elif choice == "C":
            return ("cancel", data)
        elif choice == "E":
            instruction = input("\n请输入修改指令（自然语言）: ").strip()
            if not instruction:
                print("❌ 指令不能为空")
                continue

            try:
                print(f"\n🔧 正在处理修改指令...")
                data = edit_fn(data, instruction)
                print(f"✅ 修改完成，重新展示结果")
            except Exception as e:
                print(f"❌ 修改失败: {e}")
                import traceback
                traceback.print_exc()
        else:
            print("❌ 无效选择，请输入 A/E/C")


def edit_analysis_fields(data, instruction):
    """
    编辑分析结果的字段列表

    Args:
        data: 分析结果字典
        instruction: 用户指令

    Returns:
        修改后的 data
    """
    import re
    from ai.researcher_brain import AIResearcher

    core_fields = data.get("core_fields", [])

    # 简单指令正则解析
    # 删除/移除 XXX
    if re.search(r'(删除|移除)\s+(\w+)', instruction, re.IGNORECASE):
        match = re.search(r'(删除|移除)\s+(\w+)', instruction, re.IGNORECASE)
        field_to_remove = match.group(2)
        original_count = len(core_fields)
        core_fields = [f for f in core_fields if f.get("field_name") != field_to_remove]
        data["core_fields"] = core_fields
        print(f"  已删除字段: {field_to_remove} (剩余 {len(core_fields)}/{original_count})")
        return data

    # 加入/添加 XXX
    if re.search(r'(加入|添加)\s+(\w+)', instruction, re.IGNORECASE):
        match = re.search(r'(加入|添加)\s+(\w+)', instruction, re.IGNORECASE)
        field_to_add = match.group(2)
        # 简单追加（需要用户提供完整字段信息，这里仅做演示）
        print(f"⚠️  简单添加字段 {field_to_add}，请手动补充字段信息")
        core_fields.append({
            "field_name": field_to_add,
            "field_type": "Unknown",
            "logic": "用户手动添加",
            "expected_direction": "neutral",
            "suggested_operators": []
        })
        data["core_fields"] = core_fields
        return data

    # 修改方向: 把 XXX 的方向改为 YYY
    if re.search(r'把\s+(\w+)\s+的方向改为\s+(\w+)', instruction, re.IGNORECASE):
        match = re.search(r'把\s+(\w+)\s+的方向改为\s+(\w+)', instruction, re.IGNORECASE)
        field_name = match.group(1)
        new_direction = match.group(2)
        for field in core_fields:
            if field.get("field_name") == field_name:
                field["expected_direction"] = new_direction
                print(f"  已修改 {field_name} 的方向为: {new_direction}")
                break
        data["core_fields"] = core_fields
        return data

    # 复杂指令 → 调用 AI 重构
    print(f"  复杂指令，调用 AI 重构字段列表...")
    researcher = AIResearcher()

    # 构建 Prompt
    prompt = f"""
你是一位量化研究员。用户对以下字段分析结果提出了修改要求，请按照要求调整字段列表。

**当前字段列表：**
{json.dumps(core_fields, ensure_ascii=False, indent=2)}

**用户指令：**
{instruction}

**任务要求：**
1. 根据用户指令调整字段列表（增删改）
2. 保持字段格式一致
3. 输出完整的字段列表（JSON 格式）

**输出格式（必须为 JSON）：**
{{
    "core_fields": [
        {{
            "field_name": "...",
            "field_type": "...",
            "logic": "...",
            "expected_direction": "...",
            "suggested_operators": [...]
        }},
        ...
    ]
}}

请严格按照 JSON 格式输出，不要包含任何其他文字。
"""

    result = researcher._call_ai(prompt, json_mode=True)
    data["core_fields"] = result.get("core_fields", core_fields)
    print(f"  AI 重构完成，字段数量: {len(data['core_fields'])}")
    return data


def edit_strategy(data, instruction):
    """
    编辑策略配置

    Args:
        data: 策略配置字典
        instruction: 用户指令

    Returns:
        修改后的 data
    """
    from ai.researcher_brain import AIResearcher

    researcher = AIResearcher()

    # 构建 Prompt（强调 generation_script 必须与 templates 同步）
    prompt = f"""
你是一位量化策略设计师。用户对以下策略配置提出了修改要求，请按照要求调整策略。

**当前策略配置：**
{json.dumps(data, ensure_ascii=False, indent=2)}

**用户指令：**
{instruction}

**任务要求：**
1. 根据用户指令调整策略配置（修改模板、字段规则、参数范围等）
2. **重要**：如果修改了 templates，必须同步更新 generation_script
3. 保持配置格式一致
4. 输出完整的策略配置（JSON 格式）

**输出格式（必须为 JSON）：**
{{
    "strategy_name": "...",
    "strategy_description": "...",
    "strategy_type": "...",
    "templates": [...],
    "generation_script": "...",
    "backtest_params": {{...}}
}}

请严格按照 JSON 格式输出，不要包含任何其他文字。
"""

    result = researcher._call_ai(prompt, json_mode=True)
    print(f"  AI 重构完成")
    return result


def edit_backtest_params(params, instruction):
    """
    编辑回测参数

    Args:
        params: 回测参数字典
        instruction: 用户指令

    Returns:
        修改后的 params
    """
    import re

    # 支持 key=value 格式
    kv_pattern = r'(\w+)\s*=\s*([^\s]+)'
    matches = re.findall(kv_pattern, instruction)

    if matches:
        for key, value in matches:
            # 类型转换
            if value.replace('.', '', 1).isdigit():
                if '.' in value:
                    params[key] = float(value)
                else:
                    params[key] = int(value)
            else:
                params[key] = value
            print(f"  已修改参数: {key} = {value}")
        return params

    # 支持自然语言（如 "把 decay 改为 3"）
    nl_pattern = r'把\s+(\w+)\s+改为\s+([^\s]+)'
    match = re.search(nl_pattern, instruction)
    if match:
        key = match.group(1)
        value = match.group(2)
        if value.replace('.', '', 1).isdigit():
            if '.' in value:
                params[key] = float(value)
            else:
                params[key] = int(value)
        else:
            params[key] = value
        print(f"  已修改参数: {key} = {value}")
        return params

    # 无法解析 → 逐项编辑模式
    print(f"  无法解析指令，进入逐项编辑模式")
    print(f"\n当前参数:")
    for key, value in params.items():
        print(f"  {key} = {value}")

    print(f"\n请逐项输入新值（直接回车保持不变）:")
    for key in params.keys():
        new_value = input(f"  {key} (当前: {params[key]}): ").strip()
        if new_value:
            if new_value.replace('.', '', 1).isdigit():
                if '.' in new_value:
                    params[key] = float(new_value)
                else:
                    params[key] = int(new_value)
            else:
                params[key] = new_value

    return params


def display_strategy(strategy_config):
    """展示策略配置（提取自原有代码）"""
    print(f"\n{'=' * 80}")
    print("📋 策略配置:")
    print(f"{'=' * 80}")
    print(f"策略名称: {strategy_config.get('strategy_name', 'N/A')}")
    print(f"策略描述: {strategy_config.get('strategy_description', 'N/A')}")
    print(f"策略类型: {strategy_config.get('strategy_type', 'N/A')}")

    # 显示多个模板
    templates = strategy_config.get("templates", [])
    if templates:
        print(f"\n📐 表达式模板（共 {len(templates)} 个）:")
        for i, tmpl in enumerate(templates, 1):
            print(f"\n  模板 {i}: {tmpl.get('template_type', 'N/A')}")
            print(f"  表达式: {tmpl.get('template', 'N/A')}")
            print(f"  描述: {tmpl.get('description', 'N/A')}")

            field_rules = tmpl.get("field_rules", {})
            if field_rules:
                print(f"  字段规则:")
                for placeholder, rule in field_rules.items():
                    candidates = rule.get("candidates", [])
                    print(f"    - {placeholder}: {', '.join(candidates)}")

            window_ranges = tmpl.get("window_ranges", {})
            if window_ranges:
                print(f"  窗口参数:")
                for param, values in window_ranges.items():
                    print(f"    - {param}: {values}")

    # 显示回测参数
    backtest_params = strategy_config.get("backtest_params", {})
    if backtest_params:
        print(f"\n🔧 回测参数:")
        for key, value in backtest_params.items():
            print(f"  - {key}: {value}")


def handle_alpha_factory_pipeline():
    """
    Alpha Factory Pipeline 全流程：
    AI 数据分析 → 模板调度 → Alpha 批量生成 → 去重 → 聚类 → 批量回测 → 筛选 → 错误自愈 → 保存高质量 Alpha，
    并输出 research_report.json。
    """
    from config.settings import RESEARCH_DIR
    from ai.alpha_factory_pipeline import run_pipeline

    print("\n=== 🏭 Alpha Factory Pipeline（全流程 + 研究报告）===")
    print("流程: AI 分析选字段 → 模板调度(20~30) → 生成 → 去重 → 聚类 → 回测 → 筛选 → 自愈 → 保存高质量 Alpha → research_report.json")

    if not SessionManager.is_logged_in():
        print("❌ 请先登录（菜单 1）")
        return
    session = SessionManager.get_session()

    print("\n可用区域:", ", ".join(REGION_DEFAULTS.keys()))
    region = input("请输入区域代码 (如 USA/CHN/IND): ").strip().upper()
    if not region:
        print("❌ 区域代码不能为空")
        return
    defaults = REGION_DEFAULTS.get(region, {"universe": "TOP3000", "delay": 1})
    universe = input(f"Universe (默认 {defaults['universe']}): ").strip() or defaults["universe"]
    try:
        delay = int(input(f"Delay (默认 {defaults['delay']}): ").strip() or str(defaults["delay"]))
    except ValueError:
        delay = defaults["delay"]

    dataset_input = input("请输入数据集 ID，多个用逗号分隔 (如 pv1): ").strip()
    if not dataset_input:
        print("❌ 至少输入一个数据集 ID")
        return
    dataset_ids = [x.strip() for x in dataset_input.split(",") if x.strip()]
    if not dataset_ids:
        print("❌ 数据集 ID 无效")
        return

    output_dir = RESEARCH_DIR / f"alpha_factory_{region}_{'_'.join(dataset_ids)}_{__import__('datetime').datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"\n📂 输出目录: {output_dir}")

    try:
        state = run_pipeline(
            session,
            region,
            dataset_ids,
            DataManager,
            output_dir=output_dir,
            universe=universe,
            delay=delay,
            templates_per_round=None,
            run_self_heal_flag=True,
            steps=None,
        )
    except Exception as e:
        print(f"❌ Pipeline 执行异常: {e}")
        import traceback
        traceback.print_exc()
        return

    report_path = output_dir / "research_report.json"
    if report_path.exists():
        import json
        with open(report_path, "r", encoding="utf-8") as f:
            report = json.load(f)
        print(f"\n📋 研究报告已生成: {report_path}")
        print(f"  templates_used: {report.get('templates_used', 0)}")
        print(f"  alpha_generated: {report.get('alpha_generated', 0)}")
        print(f"  after_dedup: {report.get('after_dedup', 0)}")
        print(f"  clusters: {report.get('clusters', 0)}")
        print(f"  backtest_success: {report.get('backtest_success', 0)}")
    high = state.get("high_value") or []
    if high:
        print(f"\n✅ 高质量 Alpha 数量: {len(high)}（已保存至 {output_dir / 'high_quality_alphas.json'}）")


def handle_filter_backtest_results():
    """
    回测结果筛选 / 导出：
    从 research 目录中加载所有回测结果，按条件筛选。
    支持 results.json 和 backtest_results.json 两种格式。
    """
    from pathlib import Path
    import json as _json
    from config.settings import MIN_SHARPE, MIN_FITNESS, MAX_TURNOVER

    print("\n=== 🔍 回测结果筛选 / 导出 ===")

    # 选择筛选模式
    print("\n筛选模式：")
    print("  1. 从指定目录筛选")
    print("  2. 从所有 research 目录汇总筛选")
    mode = input("请选择 (1/2, 默认1): ").strip() or "1"

    results = []

    if mode == "2":
        # 汇总所有目录
        research_base = Path("research")
        if research_base.exists():
            for subdir in research_base.iterdir():
                if not subdir.is_dir():
                    continue
                # 尝试加载 backtest_results.json 或 results.json
                for filename in ["backtest_results.json", "results.json"]:
                    fpath = subdir / filename
                    if fpath.exists():
                        try:
                            with open(fpath, "r", encoding="utf-8") as f:
                                data = _json.load(f)
                            if isinstance(data, list):
                                for r in data:
                                    r["_source"] = str(subdir.name)
                                results.extend(data)
                                print(f"  加载 {subdir.name}: {len(data)} 条")
                        except Exception as e:
                            print(f"  ⚠️ 加载失败 {subdir.name}: {e}")
    else:
        # 指定目录
        research_dir_input = input("请输入 research 子目录路径（如 research/USA_test_xxx）: ").strip()
        if not research_dir_input:
            print("❌ 路径不能为空")
            return
        research_dir = Path(research_dir_input)
        if not research_dir.exists() or not research_dir.is_dir():
            print(f"❌ 目录不存在: {research_dir}")
            return

        # 尝试加载 backtest_results.json 或 results.json
        for filename in ["backtest_results.json", "results.json"]:
            results_file = research_dir / filename
            if results_file.exists():
                try:
                    with open(results_file, "r", encoding="utf-8") as f:
                        results = _json.load(f)
                    print(f"✅ 已加载 {filename}: {len(results)} 条结果")
                    break
                except Exception as e:
                    print(f"❌ 读取失败: {e}")

    if not results:
        print("❌ 未找到任何回测结果")
        return

    # 显示统计信息
    successful = [r for r in results if r.get("success") or r.get("alpha_id")]
    sharpes = [r.get("sharpe", 0) or 0 for r in successful]
    print(f"\n📊 统计: 共 {len(results)} 条, 成功 {len(successful)} 条")
    if sharpes:
        print(f"   Sharpe 范围: {min(sharpes):.3f} ~ {max(sharpes):.3f}")

    # 筛选条件
    def _read_float(prompt: str, default=None):
        s = input(prompt).strip()
        if not s:
            return default
        try:
            return float(s)
        except ValueError:
            print("⚠️ 输入非法，使用默认值")
            return default

    print(f"\n请输入筛选条件（回车使用默认值）：")
    min_sharpe = _read_float(f"最小 Sharpe (默认 {MIN_SHARPE}): ", MIN_SHARPE)
    min_fitness = _read_float(f"最小 Fitness (默认 {MIN_FITNESS}): ", MIN_FITNESS)
    max_turnover = _read_float(f"最大 Turnover% (默认 {MAX_TURNOVER*100:.0f}%): ", MAX_TURNOVER * 100)
    if max_turnover and max_turnover > 1:
        max_turnover = max_turnover / 100  # 转换为小数

    # 执行筛选
    filtered = [
        r for r in results
        if r.get("alpha_id")
        and (r.get("sharpe", 0) or 0) >= min_sharpe
        and (r.get("fitness", 0) or 0) >= min_fitness
        and (r.get("turnover", 1) or 1) <= max_turnover
    ]

    print(f"\n✅ 筛选结果: {len(filtered)}/{len(successful)} 条符合条件")

    if not filtered:
        # 分析原因
        low_sharpe = len([r for r in successful if (r.get("sharpe", 0) or 0) < min_sharpe])
        low_fitness = len([r for r in successful if (r.get("fitness", 0) or 0) < min_fitness])
        high_turnover = len([r for r in successful if (r.get("turnover", 1) or 1) > max_turnover])
        print("\n未达标原因分析：")
        if low_sharpe:
            print(f"  - {low_sharpe} 个 Sharpe < {min_sharpe}")
        if low_fitness:
            print(f"  - {low_fitness} 个 Fitness < {min_fitness}")
        if high_turnover:
            print(f"  - {high_turnover} 个 Turnover > {max_turnover:.0%}")
        return

    # 按Sharpe排序展示
    filtered_sorted = sorted(filtered, key=lambda r: r.get("sharpe", 0) or 0, reverse=True)
    top_n = min(20, len(filtered_sorted))

    print(f"\n🏆 Top {top_n} Alpha（按 Sharpe 排序）：")
    print("-" * 90)
    print(f"{'#':<4} {'Alpha ID':<12} {'Sharpe':>8} {'Fitness':>8} {'Turnover':>10} {'Expression'}")
    print("-" * 90)

    for i, r in enumerate(filtered_sorted[:top_n], 1):
        sharpe = r.get("sharpe", 0) or 0
        fitness = r.get("fitness", 0) or 0
        turnover = r.get("turnover", 0) or 0
        alpha_id = r.get("alpha_id", "N/A")
        expr = r.get("expression", "") or ""
        if len(expr) > 45:
            expr = expr[:42] + "..."
        print(f"{i:<4} {alpha_id:<12} {sharpe:>8.3f} {fitness:>8.3f} {turnover:>9.2%} {expr}")

    # 导出选项
    print("\n导出选项：")
    print("  1. 导出 alpha_id 列表")
    print("  2. 导出完整结果")
    print("  0. 不导出")
    export_choice = input("请选择: ").strip()

    if export_choice == "1":
        alpha_ids = [r.get("alpha_id") for r in filtered_sorted if r.get("alpha_id")]
        out_file = Path("research") / f"filtered_alpha_ids_{_json.dumps(min_sharpe).replace('.', '_')}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            _json.dump(alpha_ids, f, indent=2)
        print(f"✅ 已导出 {len(alpha_ids)} 个 alpha_id 到 {out_file}")
    elif export_choice == "2":
        out_file = Path("research") / f"filtered_results_{_json.dumps(min_sharpe).replace('.', '_')}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            _json.dump(filtered_sorted, f, ensure_ascii=False, indent=2)
        print(f"✅ 已导出完整结果到 {out_file}")


def handle_alpha_optimizer():
    """
    Alpha 优化器：对已有 Alpha 进行多轮迭代优化
    目标：降低 Turnover、改善 Fitness、通过所有检查项
    """
    from ai.alpha_optimizer import optimize_alpha, batch_optimize
    from config.settings import RESEARCH_DIR

    print("\n=== 🔧 Alpha 优化器 ===")
    print("对已有 Alpha 进行多轮迭代优化，目标是让 Alpha 通过 BRAIN 平台检查项。")
    print("优化策略: 参数调整 → 表达式平滑 → 组合优化 → AI 深度优化")

    if not SessionManager.is_logged_in():
        print("❌ 请先登录（菜单 1）")
        return
    session = SessionManager.get_session()

    print("\n请选择模式:")
    print("  1. 单个 Alpha 优化")
    print("  2. 批量 Alpha 优化（多个 Alpha ID）")
    mode = input("请选择 (1/2): ").strip()

    if mode == "2":
        # 批量模式
        alpha_ids_input = input("请输入 Alpha ID（多个用逗号分隔）: ").strip()
        alpha_ids = [x.strip() for x in alpha_ids_input.split(",") if x.strip()]
        if not alpha_ids:
            print("❌ Alpha ID 不能为空")
            return
    else:
        # 单个模式
        alpha_id = input("请输入 Alpha ID: ").strip()
        if not alpha_id:
            print("❌ Alpha ID 不能为空")
            return
        alpha_ids = [alpha_id]

    print("\n可用区域:", ", ".join(REGION_DEFAULTS.keys()))
    region = input("请输入区域代码 (如 USA): ").strip().upper()
    if not region:
        print("❌ 区域代码不能为空")
        return

    try:
        max_rounds = int(input("最大优化轮数 (默认 5): ").strip() or "5")
    except ValueError:
        max_rounds = 5

    # 专项优化选项
    print("\n专项优化选项：")
    print("  1. 常规优化（自动选择策略）")
    print("  2. 专项优化（指定参数）")
    opt_mode = input("请选择 (1/2, 默认1): ").strip() or "1"

    config = {}
    if opt_mode == "2":
        # 固定中性化
        neut_input = input("固定中性化 (如 FAST/INDUSTRY/MARKET，回车跳过): ").strip().upper()
        if neut_input:
            config["fixed_neutralization"] = neut_input
            print(f"  [配置] 固定中性化: {neut_input}")

        # 指定优化策略
        print("\n优化策略：")
        print("  1. 自动选择（推荐）")
        print("  2. 参数优化 (param_tune)")
        print("  3. 表达式平滑 (smoothing)")
        print("  4. 组合优化 (combination)")
        print("  5. AI 深度优化 (ai_deep)")
        strat_input = input("请选择策略 (1-5): ").strip()
        if strat_input in ["2", "3", "4", "5"]:
            strategy_map = {"2": "param_tune", "3": "smoothing", "4": "combination", "5": "ai_deep"}
            config["force_strategy"] = strategy_map[strat_input]
            print(f"  [配置] 强制策略: {strategy_map[strat_input]}")

    output_dir = RESEARCH_DIR / "optimizations"
    print(f"\n输出目录: {output_dir}")

    # 传递配置（如果有专项设置）
    config_to_pass = config if config else None

    if len(alpha_ids) == 1:
        # 单个优化
        print(f"\n开始优化 Alpha: {alpha_ids[0]}")
        history = optimize_alpha(
            session,
            alpha_ids[0],
            region,
            max_rounds=max_rounds,
            output_dir=output_dir,
            config=config_to_pass,
        )
        histories = [history]
    else:
        # 批量优化
        print(f"\n开始批量优化 {len(alpha_ids)} 个 Alpha...")
        stop_on_success = input("找到达标 Alpha 后是否停止? (Y/n): ").strip().lower() != "n"
        histories = batch_optimize(
            session,
            alpha_ids,
            region,
            max_rounds=max_rounds,
            output_dir=output_dir,
            stop_on_success=stop_on_success,
            config=config_to_pass,
        )

    # 打印结果汇总
    print(f"\n{'='*60}")
    print("📊 优化结果汇总")
    print(f"{'='*60}")

    success_count = sum(1 for h in histories if h.final_status == "success")
    print(f"成功达标: {success_count}/{len(histories)}")

    for i, h in enumerate(histories, 1):
        print(f"\n--- Alpha {i}: {h.original_alpha_id} ---")
        print(f"  原始: Sharpe={h.original_metrics.get('sharpe', 0):.2f}, "
              f"Turnover={h.original_metrics.get('turnover', 0):.1%}")
        if h.best_metrics:
            print(f"  最佳: Sharpe={h.best_metrics.get('sharpe', 0):.2f}, "
                  f"Turnover={h.best_metrics.get('turnover', 0):.1%}")
        print(f"  优化轮数: {len(h.records)}, 状态: {h.final_status}")

        if h.best_alpha and h.final_status == "success":
            print(f"  ✅ 达标 Alpha ID: {h.best_alpha.get('alpha_id', 'N/A')}")
            print(f"  表达式: {h.best_alpha.get('expression', 'N/A')[:60]}...")


def handle_batch_combination_optimizer():
    """
    批量组合优化：分析多个 Alpha 的表达式结构，自动分组，
    通过 AI 分析生成组合创新方案。
    """
    from ai.alpha_optimizer import batch_combine_optimize
    from config.settings import RESEARCH_DIR

    print("\n=== 🔀 Alpha 组合优化 ===")
    print("分析多个 Alpha 的表达式结构，AI 生成组合创新方案。")
    print("适用场景：多个高 Sharpe 但 Turnover 过高的 Alpha，需要组合优化。")

    if not SessionManager.is_logged_in():
        print("❌ 请先登录（菜单 1）")
        return
    session = SessionManager.get_session()

    # 输入 Alpha ID 列表
    print("\n请输入要组合优化的 Alpha ID（来自同一数据集，多个用逗号分隔）:")
    print("示例: d5mzpxVj,KP6z3bdp,rKLGY73J")
    alpha_ids_input = input("Alpha ID 列表: ").strip()
    alpha_ids = [x.strip() for x in alpha_ids_input.split(",") if x.strip()]

    if not alpha_ids:
        print("❌ Alpha ID 不能为空")
        return

    if len(alpha_ids) < 2:
        print("❌ 组合优化至少需要 2 个 Alpha")
        return

    print(f"\n已输入 {len(alpha_ids)} 个 Alpha ID")

    print("\n可用区域:", ", ".join(REGION_DEFAULTS.keys()))
    region = input("请输入区域代码 (如 USA): ").strip().upper()
    if not region:
        print("❌ 区域代码不能为空")
        return

    try:
        max_combinations = int(input("最大生成组合数 (默认 10): ").strip() or "10")
    except ValueError:
        max_combinations = 10

    output_dir = RESEARCH_DIR / "optimizations"
    print(f"\n📂 输出目录: {output_dir}")

    print(f"\n{'='*60}")
    print("开始批量组合优化...")
    print(f"{'='*60}")

    result = batch_combine_optimize(
        session,
        alpha_ids,
        region,
        output_dir=output_dir,
        max_combinations=max_combinations,
    )

    # 打印结果
    print(f"\n{'='*60}")
    print("📊 组合优化结果")
    print(f"{'='*60}")

    print(f"\n源 Alpha 分析:")
    print(f"  输入 Alpha: {len(alpha_ids)} 个")
    print(f"  表达式分组: {len(result.source_groups)} 组")

    for i, g in enumerate(result.source_groups, 1):
        print(f"\n  组 {i}: {len(g.alphas)} 个变体")
        print(f"    表达式: {g.expression[:50]}...")
        print(f"    平均 Sharpe: {g.avg_sharpe:.2f}")
        print(f"    平均 Turnover: {g.avg_turnover:.1%}")

    print(f"\n生成结果:")
    print(f"  生成表达式: {len(result.generated_expressions)} 个")
    print(f"  回测成功: {sum(1 for r in result.backtest_results if r.get('success'))} 个")
    print(f"  达标 Alpha: {len(result.qualified_alphas)} 个")

    if result.qualified_alphas:
        print(f"\n✅ 达标 Alpha 列表:")
        for i, a in enumerate(result.qualified_alphas, 1):
            print(f"  {i}. {a.get('alpha_id', 'N/A')}")
            print(f"     Sharpe: {a.get('sharpe', 0):.2f}, Turnover: {a.get('turnover', 0):.1%}")
            print(f"     表达式: {a.get('expression', 'N/A')[:60]}...")
    else:
        print(f"\n⚠️ 未找到达标 Alpha")
        # 显示最佳结果
        if result.backtest_results:
            best = max(result.backtest_results, key=lambda r: (r.get('sharpe', 0) or 0) - (r.get('turnover', 0) or 1) * 10)
            print(f"\n最佳候选:")
            print(f"  Sharpe: {best.get('sharpe', 0):.2f}, Turnover: {best.get('turnover', 0):.1%}")
            print(f"  表达式: {best.get('expression', 'N/A')[:60]}...")


def handle_dataset_config_generator():
    """
    AI 数据集配置生成器：
    自动分析新数据集并生成研究方向和模板配置。
    """
    from ai.researcher_brain import AIResearcher
    from ai.metadata_builder import build_field_metadata
    from pathlib import Path
    import json

    print("\n=== 🔮 AI 数据集配置生成器 ===")
    print("自动分析新数据集并生成研究方向和模板配置。")

    if not SessionManager.is_logged_in():
        print("❌ 请先登录（菜单 1）")
        return
    session = SessionManager.get_session()

    # 输入数据集信息
    print("\n可用区域:", ", ".join(REGION_DEFAULTS.keys()))
    region = input("请输入区域代码 (如 USA): ").strip().upper()
    if not region:
        print("❌ 区域代码不能为空")
        return

    dataset_id = input("请输入数据集 ID (如 analyst10): ").strip().lower()
    if not dataset_id:
        print("❌ 数据集 ID 不能为空")
        return

    # 检查是否已存在配置
    from ai.template_loader import load_guidance
    existing_guidance = load_guidance(dataset_id)
    if existing_guidance:
        print(f"\n⚠️  数据集 '{dataset_id}' 已有配置文件")
        overwrite = input("是否覆盖? (y/N): ").strip().lower()
        if overwrite != "y":
            print("已取消")
            return

    # 配置参数
    defaults = REGION_DEFAULTS.get(region, {"universe": "TOP3000", "delay": 1})
    universe = input(f"Universe (默认 {defaults['universe']}): ").strip() or defaults["universe"]
    try:
        delay = int(input(f"Delay (默认 {defaults['delay']}): ").strip() or str(defaults["delay"]))
    except ValueError:
        delay = defaults["delay"]

    try:
        num_directions = int(input("研究方向数量 (默认 4): ").strip() or "4")
        num_templates = int(input("模板数量 (默认 20): ").strip() or "20")
    except ValueError:
        num_directions, num_templates = 4, 20

    # 获取数据集信息
    print(f"\n🔍 获取数据集 {region}/{dataset_id} 信息...")
    datasets_df = DataManager.get_datasets(session, region, universe=universe, delay=delay)
    if datasets_df is None or datasets_df.empty:
        print("❌ 未获取到数据集列表")
        return

    # 查找数据集
    dataset_row = datasets_df[datasets_df["id"].str.lower() == dataset_id]
    if dataset_row.empty:
        print(f"❌ 未找到数据集 '{dataset_id}'")
        print("可用数据集:", datasets_df["id"].head(20).tolist())
        return

    dataset_name = dataset_row.iloc[0].get("name", dataset_id)
    dataset_description = dataset_row.iloc[0].get("description", "")

    # 获取字段列表
    print(f"🔍 获取字段列表...")
    fields_df = DataManager.get_fields(session, region, dataset_id, universe=universe, delay=delay)
    if fields_df is None or fields_df.empty:
        print("❌ 未获取到字段列表")
        return

    print(f"   数据集: {dataset_name}")
    print(f"   字段数: {len(fields_df)}")

    # 构建字段 metadata
    fields_metadata = []
    for _, row in fields_df.iterrows():
        fields_metadata.append({
            "field_id": row.get("id", ""),
            "field_name": row.get("name", ""),
            "type": row.get("type", "MATRIX"),
            "coverage": row.get("coverage", 0),
            "description": row.get("description", ""),
        })

    # 获取操作符列表
    operators_df = DataManager.get_operators(session)
    operators_list = operators_df["name"].tolist() if operators_df is not None else []

    # 初始化 AI 研究员
    try:
        researcher = AIResearcher()
        print(f"\n✅ AI 研究员已初始化（使用模型: {researcher.provider}）")
    except Exception as e:
        print(f"❌ AI 初始化失败: {e}")
        return

    # 执行配置生成
    print(f"\n{'='*60}")
    print(f"开始生成配置...")
    print(f"  研究方向: {num_directions} 个")
    print(f"  模板: {num_templates} 个")
    print(f"{'='*60}")

    try:
        result = researcher.generate_dataset_config(
            region=region,
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            dataset_description=dataset_description,
            fields_metadata=fields_metadata,
            operators_list=operators_list,
            num_templates=num_templates,
            num_directions=num_directions,
        )
    except Exception as e:
        print(f"❌ 配置生成失败: {e}")
        import traceback
        traceback.print_exc()
        return

    # === 干预窗口 A：研究方向 ===
    def display_research_directions(data):
        directions = data.get("research_directions", [])
        print(f"\n{'='*70}")
        print(f"📊 研究方向（共 {len(directions)} 个）")
        print(f"{'='*70}")
        for i, d in enumerate(directions, 1):
            print(f"\n{i}. {d.get('name', 'N/A')}")
            print(f"   描述: {d.get('description', 'N/A')}")
            print(f"   字段模式: {', '.join(d.get('field_patterns', []))}")
            print(f"   建议操作符: {', '.join(d.get('suggested_operators', []))}")
            print(f"   Alpha 逻辑: {d.get('alpha_logic', 'N/A')}")

    action, result = intervention_gate(
        "研究方向",
        display_research_directions,
        _edit_config_list,
        result
    )
    if action == "cancel":
        return

    # === 干预窗口 B：优先字段 ===
    def display_priority_fields(data):
        fields = data.get("priority_fields", [])
        print(f"\n{'='*70}")
        print(f"📋 优先字段（共 {len(fields)} 个）")
        print(f"{'='*70}")
        print("\n| # | 字段 ID | 类型 | 数据类型 | 覆盖率 | 逻辑 |")
        print("|---|---------|------|----------|--------|------|")
        for i, f in enumerate(fields, 1):
            cov = f.get('coverage')
            cov_str = f"{cov:.1%}" if cov else "N/A"
            logic = f.get('logic', '')[:25]
            print(f"| {i} | {f.get('field_id', 'N/A')} | {f.get('field_type', 'N/A')} | "
                  f"{f.get('data_type', 'N/A')} | {cov_str} | {logic} |")

    action, result = intervention_gate(
        "优先字段",
        display_priority_fields,
        _edit_config_list,
        result
    )
    if action == "cancel":
        return

    # === 干预窗口 C：模板列表 ===
    def display_templates(data):
        templates = data.get("templates", [])
        print(f"\n{'='*70}")
        print(f"📐 模板列表（共 {len(templates)} 个）")
        print(f"{'='*70}")
        for i, t in enumerate(templates, 1):
            print(f"\n{i}. [{t.get('category', 'N/A')}] {t.get('name', 'N/A')}")
            print(f"   表达式: {t.get('expression', 'N/A')}")
            print(f"   描述: {t.get('description', 'N/A')}")

    action, result = intervention_gate(
        "模板列表",
        display_templates,
        _edit_config_list,
        result
    )
    if action == "cancel":
        return

    # === 保存配置 ===
    save_choice = input("\n是否保存配置文件? (Y/n): ").strip().lower()
    if save_choice == "n":
        print("已取消保存")
        return

    # 保存文件
    output_dir = Path("config/dataset_templates")
    output_dir.mkdir(parents=True, exist_ok=True)

    templates_file = output_dir / f"{dataset_id}_templates.json"
    guidance_file = output_dir / f"{dataset_id}_guidance.json"

    with open(templates_file, "w", encoding="utf-8") as f:
        json.dump(result["templates"], f, ensure_ascii=False, indent=2)

    with open(guidance_file, "w", encoding="utf-8") as f:
        json.dump(result["guidance"], f, ensure_ascii=False, indent=2)

    print(f"\n✅ 配置已保存:")
    print(f"   模板文件: {templates_file}")
    print(f"   引导文件: {guidance_file}")

    # 显示使用说明
    print(f"\n📖 使用说明:")
    print(f"   运行 Pipeline 时使用 --template-mode specialized")
    print(f"   python main.py pipeline --region {region} --datasets {dataset_id} --template-mode specialized")


def _edit_config_list(data, instruction):
    """简单的配置列表编辑函数"""
    # 简单实现：直接返回原数据
    # 复杂编辑可以通过 AI 实现
    print(f"  收到编辑指令: {instruction}")
    print(f"  （暂不支持自动编辑，请手动修改保存后的配置文件）")
    return data


def print_multi_analysis_result(result):
    """格式化打印多数据集联合分析结果"""
    # 数据集维度分析
    dataset_dims = result.get("dataset_dimensions", {})
    if dataset_dims:
        print(f"\n{'=' * 80}")
        print("[Dataset] 数据集维度分析:")
        print(f"{'=' * 80}")
        for ds_id, dim in dataset_dims.items():
            print(f"  {ds_id}: {dim}")

    # 核心字段表格（含来源数据集列）
    print(f"\n{'=' * 80}")
    print("📊 AI 筛选的核心字段（多数据集联合）:")
    print(f"{'=' * 80}")

    core_fields = result.get("core_fields", [])
    if core_fields:
        print("\n| # | 字段名 | 来源 | 语义类型 | 平台类型 | 金融逻辑 | 预期方向 | 覆盖率 | 时间覆盖 | 建议操作符 |")
        print("|---|--------|------|----------|----------|----------|----------|--------|----------|------------|")
        for i, field in enumerate(core_fields, 1):
            field_name = field.get('field_name', 'N/A')
            source = field.get('source_dataset', 'N/A')
            field_type = field.get('field_type', 'N/A')
            data_type = field.get('data_type', 'N/A')
            logic = field.get('logic', 'N/A')
            if len(logic) > 35:
                logic = logic[:32] + "..."
            direction = field.get('expected_direction', 'N/A')

            cov = field.get('coverage')
            cov_str = f"{cov:.1%}" if cov is not None else "N/A"
            if field.get('coverage_warning'):
                cov_str += "⚠️"

            dcov = field.get('dateCoverage')
            dcov_str = f"{dcov:.1%}" if dcov is not None else "N/A"
            if field.get('date_coverage_warning'):
                dcov_str += "⚠️"

            operators = ', '.join(field.get('suggested_operators', [])[:3])
            print(f"| {i} | {field_name} | {source} | {field_type} | {data_type} | {logic} | {direction} | {cov_str} | {dcov_str} | {operators} |")

    # 字段组合表格（标注跨数据集）
    field_combinations = result.get("field_combinations", [])
    if field_combinations:
        print(f"\n{'=' * 80}")
        print("🔗 推荐的字段组合:")
        print(f"{'=' * 80}")
        print("\n| # | 字段组合 | 金融逻辑 | 类型 | 跨数据集 |")
        print("|---|----------|----------|------|----------|")
        for i, combo in enumerate(field_combinations, 1):
            combination = combo.get('combination', 'N/A')
            logic = combo.get('logic', 'N/A')
            if len(logic) > 45:
                logic = logic[:42] + "..."
            combo_type = combo.get('type', 'N/A')
            cross = "✅" if combo.get('cross_dataset') else ""
            print(f"| {i} | {combination} | {logic} | {combo_type} | {cross} |")

    # 推荐操作符
    operators = result.get('available_operators', [])
    if operators:
        print(f"\n{'=' * 80}")
        print(f"🔧 推荐操作符: {', '.join(operators)}")
        print(f"{'=' * 80}")


def handle_multi_dataset_analysis(researcher, region, universe, delay, data_type):
    """处理多数据集联合分析"""
    from config.ai_config import MULTI_DATASET_CONFIG
    from config.settings import AI_GENERATED_DIR
    from datetime import datetime

    print(f"\n=== [Multi] 多数据集联合分析 ===")
    print(f"区域: {region}, Universe: {universe}, Delay: {delay}\n")

    # 1. 选择数据集组合
    print("请选择数据集组合:")
    combos = MULTI_DATASET_CONFIG["recommended_combos"]
    combo_keys = list(combos.keys())
    for i, (name, ds_list) in enumerate(combos.items(), 1):
        print(f"  {i}. {name} ({', '.join(ds_list)})")
    print(f"  {len(combos) + 1}. 手动输入")

    combo_choice = input(f"\n请选择 (1-{len(combos) + 1}): ").strip()

    if combo_choice.isdigit() and 1 <= int(combo_choice) <= len(combos):
        selected_key = combo_keys[int(combo_choice) - 1]
        dataset_ids = combos[selected_key]
        print(f"✅ 已选择: {selected_key} → {dataset_ids}")
    elif combo_choice == str(len(combos) + 1):
        ds_input = input("请输入数据集 ID（逗号分隔，如 pv1,analyst15）: ").strip()
        if not ds_input:
            print("❌ 数据集不能为空")
            return
        dataset_ids = [s.strip() for s in ds_input.split(",") if s.strip()]
        if len(dataset_ids) < 2:
            print("❌ 至少需要 2 个数据集")
            return
        if len(dataset_ids) > MULTI_DATASET_CONFIG["max_datasets"]:
            print(f"❌ 最多支持 {MULTI_DATASET_CONFIG['max_datasets']} 个数据集")
            return
    else:
        print("❌ 无效选择")
        return

    # 2. 确认
    print(f"\n将分析以下数据集: {', '.join(dataset_ids)}")
    confirm = input("确认开始? (Y/n): ").strip().lower()
    if confirm == "n":
        print("❌ 已取消")
        return

    # 3. 调用分析
    print(f"\n🔍 开始多数据集联合分析...")
    result = researcher.analyze_multi_datasets(
        region, dataset_ids,
        universe=universe, delay=delay, data_type=data_type
    )

    # 4. 干预窗口
    action, result = intervention_gate(
        "多数据集分析结果",
        print_multi_analysis_result,
        edit_analysis_fields,
        result
    )

    if action == "cancel":
        print("❌ 用户取消操作")
        return

    # 5. 保存分析结果
    save_choice = input("\n是否保存分析结果? (y/N): ").strip().lower()
    if save_choice == "y":
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        ds_tag = "_".join(sorted(dataset_ids))
        output_file = AI_GENERATED_DIR / f"analysis_{region}_MULTI_{ds_tag}_{timestamp}.json"

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        print(f"✅ 分析结果已保存: {output_file}")


def handle_strategy_build():
    """处理策略构建"""
    from ai.researcher_brain import AIResearcher
    from config.settings import AI_GENERATED_DIR
    from datetime import datetime
    import glob

    print("\n=== 📐 策略构建 ===")

    # 1. 列出已保存的分析结果
    analysis_files = sorted(glob.glob(str(AI_GENERATED_DIR / "analysis_*.json")), reverse=True)

    if not analysis_files:
        print("❌ 没有找到分析结果文件")
        print("请先运行 '2. 数据分析（指定数据集）' 并保存分析结果")
        return

    print("\n已保存的分析结果:")
    for i, file_path in enumerate(analysis_files[:10], 1):
        file_name = Path(file_path).name
        print(f"  {i}. {file_name}")

    # 2. 用户选择分析结果
    try:
        choice = int(input("\n请选择分析结果 (输入序号): ").strip())
        if choice < 1 or choice > len(analysis_files[:10]):
            print("❌ 无效的选择")
            return
        selected_file = analysis_files[choice - 1]
    except ValueError:
        print("❌ 请输入有效的数字")
        return

    # 3. 读取分析结果
    with open(selected_file, "r", encoding="utf-8") as f:
        analysis_result = json.load(f)

    print(f"\n✅ 已加载分析结果: {Path(selected_file).name}")

    # 干预窗口 C：加载分析结果后
    action, analysis_result = intervention_gate(
        "已加载的分析结果",
        print_analysis_result,
        edit_analysis_fields,
        analysis_result
    )

    if action == "cancel":
        print("❌ 用户取消操作")
        return

    # 4. AI 推荐策略方向
    try:
        researcher = AIResearcher()
        print(f"✅ AI 研究员已初始化（使用模型: {researcher.provider}）")

        print(f"\n🤔 AI 正在分析数据特征并推荐策略方向...")
        recommendation = researcher.recommend_strategy(analysis_result)

        # 显示数据特征分析
        data_chars = recommendation.get("data_characteristics", {})
        print(f"\n{'=' * 80}")
        print("📊 数据特征分析:")
        print(f"{'=' * 80}")
        print(f"数据集类型: {data_chars.get('dataset_type', 'N/A')}")
        print(f"数据频率: {data_chars.get('field_frequency', 'N/A')}")
        print(f"数据维度: {data_chars.get('data_dimension', 'N/A')}")
        print(f"因果强度: {data_chars.get('causal_strength', 'N/A')}")
        print(f"Alpha 潜力: {data_chars.get('alpha_potential', 'N/A')}")

        # 显示推荐策略
        recommended_strategies = recommendation.get("recommended_strategies", [])
        print(f"\n{'=' * 80}")
        print(f"💡 AI 推荐的策略方向（共 {len(recommended_strategies)} 个）:")
        print(f"{'=' * 80}")

        for i, strategy in enumerate(recommended_strategies, 1):
            priority_icon = "⭐" * (4 - strategy.get("priority", 3))
            print(f"\n{i}. {strategy.get('strategy_name', 'N/A')} {priority_icon}")
            print(f"   推荐理由: {strategy.get('reason', 'N/A')}")
            print(f"   预期效果: {strategy.get('expected_effect', 'N/A')}")
            print(f"   潜在风险: {strategy.get('potential_risk', 'N/A')}")
            print(f"   置信度: {strategy.get('confidence', 'N/A')}")

        # 显示 AI 自动选择
        auto_select = recommendation.get("auto_select", "")
        print(f"\n{'=' * 80}")
        print(f"🎯 AI 自动选择: {auto_select}")
        print(f"{'=' * 80}")

        # 5. 用户选择策略方向
        print("\n请选择策略方向:")
        print("  0. 使用 AI 自动选择（推荐）")
        for i, strategy in enumerate(recommended_strategies, 1):
            print(f"  {i}. {strategy.get('strategy_name', 'N/A')}")
        print(f"  {len(recommended_strategies) + 1}. 自定义策略方向")

        strategy_choice = input(f"\n请选择 (0-{len(recommended_strategies) + 1}, 默认 0): ").strip() or "0"

        if strategy_choice == "0":
            strategy_focus = auto_select
            print(f"✅ 使用 AI 自动选择: {strategy_focus}")
        elif strategy_choice.isdigit() and 1 <= int(strategy_choice) <= len(recommended_strategies):
            strategy_focus = recommended_strategies[int(strategy_choice) - 1].get("strategy_name", "动量反转")
            print(f"✅ 已选择: {strategy_focus}")
        elif strategy_choice == str(len(recommended_strategies) + 1):
            strategy_focus = input("请输入自定义策略方向: ").strip()
            if not strategy_focus:
                print("❌ 策略方向不能为空")
                return
        else:
            print("❌ 无效的选择，使用 AI 自动选择")
            strategy_focus = auto_select

        # 6. 提取上下文信息（用于智能参数继承）
        context = analysis_result.get("context", {})
        region = context.get("region", "USA")
        universe = context.get("universe", "TOP3000")
        delay = context.get("delay", 1)

        print(f"\n🔨 开始构建策略...")
        print(f"策略方向: {strategy_focus}")
        print(f"回测参数: Region={region}, Universe={universe}, Delay={delay}")

        strategy_config = researcher.build_strategy(
            analysis_result,
            strategy_focus,
            region=region,
            universe=universe,
            delay=delay
        )

        # 干预窗口：策略设计
        action, strategy_config = intervention_gate(
            "策略设计",
            display_strategy,
            edit_strategy,
            strategy_config
        )

        if action == "cancel":
            print("❌ 用户取消操作")
            return

        # 7. 展示策略配置（已在干预窗口中展示，这里跳过）
        # 原有的展示代码已被 display_strategy() 函数替代

        # 7.5 AI 推荐回测参数（新增）
        print(f"\n🤔 AI 正在分析策略特征并推荐最优回测参数...")
        params_recommendation = researcher.recommend_backtest_params(
            strategy_config,
            analysis_result,
            region=region,
            universe=universe,
            delay=delay
        )

        # 显示策略特征分析
        strategy_chars = params_recommendation.get("strategy_characteristics", {})
        print(f"\n{'=' * 80}")
        print("📊 策略特征分析:")
        print(f"{'=' * 80}")
        print(f"策略周期: {strategy_chars.get('strategy_period', 'N/A')}")
        print(f"窗口范围: {strategy_chars.get('window_range', 'N/A')}")
        print(f"操作符重点: {strategy_chars.get('operator_focus', 'N/A')}")
        print(f"复杂度: {strategy_chars.get('complexity', 'N/A')}")

        # 显示数据特征分析
        data_chars = params_recommendation.get("data_characteristics", {})
        print(f"\n数据特征分析:")
        print(f"数据频率: {data_chars.get('data_frequency', 'N/A')}")
        print(f"波动性: {data_chars.get('volatility', 'N/A')}")
        print(f"缺失值: {data_chars.get('missing_values', 'N/A')}")
        print(f"行业分布: {data_chars.get('industry_distribution', 'N/A')}")

        # 显示推荐参数
        recommended_params = params_recommendation.get("recommended_params", {})
        print(f"\n{'=' * 80}")
        print("💡 AI 推荐的回测参数:")
        print(f"{'=' * 80}")

        for param_name, param_info in recommended_params.items():
            value = param_info.get("value", "N/A")
            reason = param_info.get("reason", "N/A")
            confidence = param_info.get("confidence", "N/A")
            print(f"\n{param_name}: {value} (置信度: {confidence})")
            print(f"  理由: {reason}")

        # 显示风险警告
        risk_warnings = params_recommendation.get("risk_warnings", [])
        if risk_warnings:
            print(f"\n{'=' * 80}")
            print("⚠️  风险提示:")
            print(f"{'=' * 80}")
            for i, warning in enumerate(risk_warnings, 1):
                print(f"{i}. {warning}")

        # 显示最终配置
        final_config = params_recommendation.get("final_config", {})
        print(f"\n{'=' * 80}")
        print("🎯 AI 推荐的最终回测参数:")
        print(f"{'=' * 80}")
        for key, value in final_config.items():
            print(f"  {key}: {value}")
        print(f"{'=' * 80}")

        # 询问是否使用 AI 推荐的参数
        use_ai_params = input("\n是否使用 AI 推荐的回测参数? (Y/n): ").strip().lower()
        if use_ai_params != 'n':
            # 更新策略配置中的回测参数
            strategy_config['backtest_params'] = final_config
            print("✅ 已采用 AI 推荐的回测参数")
        else:
            print("✅ 保持原有回测参数")

        # 8. 询问是否保存
        save_choice = input("\n是否保存策略配置? (y/N): ").strip().lower()
        if save_choice == "y":
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            strategy_name = strategy_config.get('strategy_name', 'strategy').replace(' ', '_')
            output_file = AI_GENERATED_DIR / f"strategy_{strategy_name}_{timestamp}.json"

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(strategy_config, f, ensure_ascii=False, indent=2)

            print(f"✅ 策略配置已保存: {output_file}")

    except Exception as e:
        print(f"❌ 策略构建失败: {e}")
        import traceback
        traceback.print_exc()


def display_optimization_stats(stats):
    """格式化展示回测统计摘要"""
    print(f"\n{'=' * 70}")
    print("📊 回测结果统计摘要")
    print(f"{'=' * 70}")

    print(f"\n总数: {stats['total']}  |  有效: {stats['valid_count']}  |  "
          f"成功(Sharpe≥1.0): {stats['success_count']}  |  "
          f"Sharpe为负: {stats['negative_count']}")
    print(f"成功率: {stats['success_rate']:.1%}")

    s = stats["sharpe"]
    f = stats["fitness"]
    t = stats["turnover"]
    r = stats["returns"]

    print(f"\n{'指标':<12} {'均值':>8} {'中位数':>8} {'最大':>8} {'最小':>8}")
    print("-" * 48)
    print(f"{'Sharpe':<12} {s['mean']:>8.3f} {s['median']:>8.3f} {s['max']:>8.3f} {s['min']:>8.3f}")
    print(f"{'Fitness':<12} {f['mean']:>8.3f} {f['median']:>8.3f} {f['max']:>8.3f} {f['min']:>8.3f}")
    print(f"{'Turnover':<12} {t['mean']:>8.3f} {t['median']:>8.3f} {t['max']:>8.3f} {t['min']:>8.3f}")
    print(f"{'Returns':<12} {r['mean']:>8.4f} {'':>8} {r['max']:>8.4f} {r['min']:>8.4f}")

    # 模板类型统计
    ts = stats.get("template_type_stats", {})
    if ts:
        print(f"\n{'模板类型':<28} {'数量':>5} {'Sharpe均值':>10} {'Sharpe最大':>10} {'Turnover均值':>12}")
        print("-" * 70)
        for ttype, tstat in sorted(ts.items(), key=lambda x: x[1]["sharpe_mean"], reverse=True):
            print(f"{ttype:<28} {tstat['count']:>5} {tstat['sharpe_mean']:>10.3f} "
                  f"{tstat['sharpe_max']:>10.3f} {tstat['turnover_mean']:>12.3f}")

    # Top N
    print(f"\n🏆 Top {len(stats['top_n'])} 案例:")
    for i, r in enumerate(stats["top_n"], 1):
        print(f"  {i}. Sharpe={r.get('sharpe', 0):.3f}  Fitness={r.get('fitness', 0):.3f}  "
              f"Turnover={r.get('turnover', 0):.3f}  [{r.get('template_type', 'N/A')}]")
        print(f"     {r.get('expression', 'N/A')[:70]}")

    # Bottom N
    print(f"\n📉 Bottom {len(stats['bottom_n'])} 案例:")
    for i, r in enumerate(stats["bottom_n"], 1):
        print(f"  {i}. Sharpe={r.get('sharpe', 0):.3f}  Fitness={r.get('fitness', 0):.3f}  "
              f"Turnover={r.get('turnover', 0):.3f}  [{r.get('template_type', 'N/A')}]")
        print(f"     {r.get('expression', 'N/A')[:70]}")

    print(f"{'=' * 70}")


def handle_optimization():
    """处理回测分析与闭环优化"""
    from ai.researcher_brain import AIResearcher
    from ai.strategy_generator import StrategyGenerator
    from config.settings import RESEARCH_DIR, AI_GENERATED_DIR
    from config.ai_config import OPTIMIZATION_CONFIG
    from datetime import datetime
    import glob as glob_mod

    print("\n=== 🔄 回测分析与闭环优化 ===")

    # 1. 扫描 research/ 目录，列出所有含 results.json 的回测目录
    research_dirs = []
    for results_file in sorted(Path(RESEARCH_DIR).glob("*/results.json"), reverse=True):
        research_dirs.append(results_file.parent)

    if not research_dirs:
        print("❌ 没有找到回测结果")
        print("请先执行批量回测（菜单 6 或 AI 挖掘流程）")
        return

    print("\n已有的回测结果:")
    for i, d in enumerate(research_dirs[:15], 1):
        results_file = d / "results.json"
        try:
            with open(results_file, "r", encoding="utf-8") as f:
                count = len(json.load(f))
        except Exception:
            count = "?"
        print(f"  {i}. {d.name}  ({count} 条结果)")

    # 2. 用户选择
    try:
        choice = int(input("\n请选择回测结果 (输入序号): ").strip())
        if choice < 1 or choice > len(research_dirs[:15]):
            print("❌ 无效的选择")
            return
        selected_dir = research_dirs[choice - 1]
    except ValueError:
        print("❌ 请输入有效的数字")
        return

    # 3. 读取 results.json
    results_file = selected_dir / "results.json"
    with open(results_file, "r", encoding="utf-8") as f:
        results = json.load(f)

    print(f"\n✅ 已加载: {selected_dir.name} ({len(results)} 条结果)")

    # 4. 统计分析
    stats = AIResearcher.analyze_backtest_results(results)

    if stats.get("error"):
        print(f"❌ {stats['error']}")
        return

    display_optimization_stats(stats)

    # 5. 询问是否进行 AI 优化
    optimize_choice = input("\n是否进行 AI 闭环优化? (y/N): ").strip().lower()
    if optimize_choice != "y":
        print("已退出")
        return

    # 5a. 可选：加载原始策略文件
    original_strategy = {}
    strategy_files = sorted(
        glob_mod.glob(str(AI_GENERATED_DIR / "strategy_*.json")),
        reverse=True
    )

    if strategy_files:
        print("\n可选：加载原始策略文件（用于对比优化）")
        print("  0. 跳过（不加载原始策略）")
        for i, fp in enumerate(strategy_files[:10], 1):
            print(f"  {i}. {Path(fp).name}")

        strat_choice = input("请选择 (0 跳过): ").strip() or "0"
        if strat_choice.isdigit() and 1 <= int(strat_choice) <= len(strategy_files[:10]):
            with open(strategy_files[int(strat_choice) - 1], "r", encoding="utf-8") as f:
                original_strategy = json.load(f)
            print(f"✅ 已加载原始策略: {Path(strategy_files[int(strat_choice) - 1]).name}")

    # 迭代优化循环
    max_iterations = OPTIMIZATION_CONFIG["max_iterations"]
    current_results = results

    for iteration in range(1, max_iterations + 1):
        print(f"\n{'=' * 70}")
        print(f"🔄 优化迭代 第 {iteration}/{max_iterations} 轮")
        print(f"{'=' * 70}")

        # 5b. 初始化 AI 研究员，调用 optimize_strategy()
        try:
            researcher = AIResearcher()
            print(f"✅ AI 研究员已初始化（使用模型: {researcher.provider}）")
            print(f"\n🤔 AI 正在分析回测结果并生成优化策略...")

            opt_result = researcher.optimize_strategy(current_results, original_strategy)
        except Exception as e:
            print(f"❌ AI 优化失败: {e}")
            import traceback
            traceback.print_exc()
            return

        if opt_result.get("error"):
            print(f"❌ {opt_result['error']}")
            return

        # 5c. 展示 AI 分析结果
        print(f"\n{'=' * 70}")
        print("🧠 AI 分析结果")
        print(f"{'=' * 70}")
        print(f"\n整体分析: {opt_result.get('overall_analysis', 'N/A')}")
        print(f"\n成功案例总结: {opt_result.get('success_cases_summary', 'N/A')}")
        print(f"\n失败案例总结: {opt_result.get('failure_cases_summary', 'N/A')}")

        suggestions = opt_result.get("optimization_suggestions", [])
        if suggestions:
            print(f"\n💡 优化建议:")
            for i, sug in enumerate(suggestions, 1):
                if isinstance(sug, dict):
                    print(f"  {i}. [{sug.get('priority', '中')}] {sug.get('suggestion', 'N/A')}")
                    print(f"     预期改善: {sug.get('expected_improvement', 'N/A')}")
                else:
                    print(f"  {i}. {sug}")

        # 检查 updated_strategy
        updated_strategy = opt_result.get("updated_strategy", {})
        if not updated_strategy.get("generation_script"):
            print("\n⚠️  AI 返回的优化策略缺少 generation_script，无法自动生成 Alpha")
            print("请手动调整策略后重试")
            return

        print(f"\n📐 优化后的策略: {updated_strategy.get('strategy_name', 'N/A')}")
        templates = updated_strategy.get("templates", [])
        print(f"   模板数量: {len(templates)}")
        for i, tmpl in enumerate(templates, 1):
            print(f"   {i}. [{tmpl.get('template_type', 'N/A')}] {tmpl.get('template', 'N/A')[:60]}")

        # 5d. 询问是否执行改进策略
        execute_choice = input("\n是否执行改进策略（生成新 Alpha 并回测）? (y/N): ").strip().lower()
        if execute_choice != "y":
            # 仅保存策略
            save_choice = input("是否保存优化策略? (y/N): ").strip().lower()
            if save_choice == "y":
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                strat_name = updated_strategy.get("strategy_name", "optimized").replace(" ", "_")
                output_file = AI_GENERATED_DIR / f"strategy_{strat_name}_{timestamp}.json"
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(updated_strategy, f, ensure_ascii=False, indent=2)
                print(f"✅ 优化策略已保存: {output_file}")
            return

        # 5e. 保存改进策略 → 生成新 Alpha → 回测
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            strat_name = updated_strategy.get("strategy_name", "optimized").replace(" ", "_")

            # 保存策略
            strat_file = AI_GENERATED_DIR / f"strategy_{strat_name}_{timestamp}.json"
            with open(strat_file, "w", encoding="utf-8") as f:
                json.dump(updated_strategy, f, ensure_ascii=False, indent=2)
            print(f"\n✅ 优化策略已保存: {strat_file}")

            # 生成新 Alpha
            print(f"\n🔧 正在生成新 Alpha...")
            generator = StrategyGenerator()
            alphas_file = AI_GENERATED_DIR / f"alphas_{strat_name}_{timestamp}.json"
            count = generator.generate_alphas(updated_strategy, str(alphas_file))
            print(f"✅ 成功生成 {count} 个 Alpha")

            # 询问是否立即回测
            backtest_choice = input("\n是否立即回测? (y/N): ").strip().lower()
            if backtest_choice != "y":
                print(f"Alpha 文件已保存: {alphas_file}")
                print("可稍后通过菜单 6 执行回测")
                return

            # 执行回测
            print(f"\n🚀 开始回测...")
            from core.session_manager import SessionManager
            from core.alpha_builder import AlphaBuilder

            if not SessionManager.is_logged_in():
                print("❌ 请先登录 BRAIN 平台")
                return
            session = SessionManager.get_session()

            with open(alphas_file, "r", encoding="utf-8") as f:
                alphas_list = json.load(f)

            # 提取区域信息
            bp = updated_strategy.get("backtest_params", {})
            region = bp.get("region", "USA")

            expressions = [a.get("expression", "") for a in alphas_list if a.get("expression")]
            configs = AlphaBuilder.build_batch_configs(expressions, region)

            # 应用回测参数
            for cfg in configs:
                for key in ["decay", "truncation", "neutralization", "pasteurization", "unit_handling", "nan_handling"]:
                    if key in bp:
                        cfg["settings"][key] = bp[key]

            new_results = BacktestRunner.run_batch(session, configs)
            dataset_name = f"opt_iter{iteration}"
            BacktestRunner.save_research(new_results, region, dataset_name)

            print(f"\n✅ 回测完成，共 {len(new_results)} 条结果")

            # 展示新结果统计
            new_stats = AIResearcher.analyze_backtest_results(new_results)
            if not new_stats.get("error"):
                display_optimization_stats(new_stats)

                # 检查是否达标
                if new_stats["success_rate"] >= OPTIMIZATION_CONFIG["target_success_rate"]:
                    print(f"\n🎉 成功率 {new_stats['success_rate']:.1%} 已达标！优化完成。")
                    return

                if new_stats["sharpe"]["max"] >= OPTIMIZATION_CONFIG["target_sharpe"]:
                    print(f"\n✅ 最高 Sharpe {new_stats['sharpe']['max']:.3f} 已达标！")

            # 询问是否继续迭代
            if iteration < max_iterations:
                continue_choice = input(f"\n是否继续优化（还剩 {max_iterations - iteration} 轮）? (y/N): ").strip().lower()
                if continue_choice != "y":
                    print("优化结束")
                    return
                # 更新数据用于下一轮
                current_results = new_results
                original_strategy = updated_strategy
            else:
                print(f"\n已达到最大迭代次数 ({max_iterations} 轮)，优化结束")

        except Exception as e:
            print(f"❌ 执行失败: {e}")
            import traceback
            traceback.print_exc()
            return


def handle_alpha_generation():
    """处理批量生成 Alpha"""
    from ai.strategy_generator import StrategyGenerator
    from config.settings import AI_GENERATED_DIR
    from datetime import datetime
    import glob

    print("\n=== 🔧 批量生成 Alpha ===")

    # 1. 列出已保存的策略配置（包含 strategy_ 和 test_strategy_ 文件）
    strategy_files = sorted(
        glob.glob(str(AI_GENERATED_DIR / "strategy_*.json")) +
        glob.glob(str(AI_GENERATED_DIR / "test_strategy_*.json")),
        reverse=True
    )

    if not strategy_files:
        print("❌ 没有找到策略配置文件")
        print("请先运行 '3. 策略构建' 并保存策略配置")
        return

    print("\n已保存的策略配置:")
    for i, file_path in enumerate(strategy_files[:10], 1):
        file_name = Path(file_path).name
        print(f"  {i}. {file_name}")

    # 2. 用户选择策略配置
    try:
        choice = int(input("\n请选择策略配置 (输入序号): ").strip())
        if choice < 1 or choice > len(strategy_files[:10]):
            print("❌ 无效的选择")
            return
        selected_file = strategy_files[choice - 1]
    except ValueError:
        print("❌ 请输入有效的数字")
        return

    # 3. 读取策略配置
    with open(selected_file, "r", encoding="utf-8") as f:
        strategy_config = json.load(f)

    print(f"\n✅ 已加载策略配置: {Path(selected_file).name}")

    # 4. 显示预估生成数量并确认
    estimated_count = strategy_config.get("estimated_count", 0)
    print(f"\n预估生成数量: {estimated_count} 个 Alpha")

    if estimated_count > 500:
        print(f"⚠️ 生成数量较多，可能需要较长时间")

    confirm = input("\n是否继续生成? (y/N): ").strip().lower()
    if confirm != "y":
        print("已取消")
        return

    # 5. 生成 Alpha
    try:
        generator = StrategyGenerator()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        strategy_name = strategy_config.get('strategy_name', 'alphas').replace(' ', '_')
        output_file = AI_GENERATED_DIR / f"alphas_{strategy_name}_{timestamp}.json"

        print(f"\n🔧 开始生成 Alpha...")
        count = generator.generate_alphas(strategy_config, str(output_file))

        print(f"\n✅ 成功生成 {count} 个 Alpha")
        print(f"文件已保存: {output_file}")

        # 6. 验证表达式
        validate_choice = input("\n是否验证表达式语法? (y/N): ").strip().lower()
        if validate_choice == "y":
            print(f"\n🔍 开始验证表达式...")
            result = generator.validate_expressions(str(output_file))

            print(f"\n{'=' * 60}")
            print("📊 验证结果:")
            print(f"{'=' * 60}")
            print(f"总数: {result['total']}")
            print(f"有效: {result['valid']} ({result['valid']/result['total']*100:.1f}%)")
            print(f"无效: {result['invalid']} ({result['invalid']/result['total']*100:.1f}%)")

            if result['invalid'] > 0:
                print(f"\n前 {min(len(result['invalid_expressions']), 10)} 个无效表达式:")
                for item in result['invalid_expressions']:
                    print(f"  [{item['index']}] {item['expression'][:50]}...")
                    print(f"      错误: {item['error']}")

            print(f"{'=' * 60}")

    except Exception as e:
        print(f"❌ Alpha 生成失败: {e}")
        import traceback
        traceback.print_exc()


def interactive_mode():
    """交互式菜单主循环"""
    print_banner()

    # 显示缓存状态
    DataManager.print_cache_status()

    while True:
        print_menu()
        choice = input("请选择操作 [0-12]: ").strip()

        try:
            if choice == "0":
                print("\n👋 再见！")
                break
            elif choice == "1":
                handle_login()
            elif choice == "2":
                handle_datasets()
            elif choice == "3":
                handle_fields()
            elif choice == "4":
                handle_operators()
            elif choice == "5":
                handle_build_alpha()
            elif choice == "6":
                handle_batch_backtest()
            elif choice == "7":
                handle_view_report()
            elif choice == "8":
                DataManager.print_cache_status()
            elif choice == "9":
                handle_alpha_factory_pipeline()
            elif choice == "10":
                handle_filter_backtest_results()
            elif choice == "11":
                handle_alpha_optimizer()
            elif choice == "12":
                handle_batch_combination_optimizer()
            elif choice == "13":
                handle_dataset_config_generator()
            else:
                print("❌ 无效选项，请重新选择")
        except KeyboardInterrupt:
            print("\n\n⚠️ 操作已取消")
        except Exception as e:
            print(f"\n❌ 错误: {e}")
            import traceback
            traceback.print_exc()

        print()


def cli_mode():
    """命令行参数模式"""
    parser = argparse.ArgumentParser(
        description="BRAIN 量化因子挖掘平台 v2.0"
    )
    subparsers = parser.add_subparsers(dest="command")

    # datasets 子命令
    ds_parser = subparsers.add_parser("datasets", help="获取区域数据集")
    ds_parser.add_argument("--region", "-r", required=True, help="区域代码")
    ds_parser.add_argument("--refresh", action="store_true", help="强制刷新缓存")

    # fields 子命令
    fi_parser = subparsers.add_parser("fields", help="获取数据集字段")
    fi_parser.add_argument("--region", "-r", required=True, help="区域代码")
    fi_parser.add_argument("--dataset", "-d", required=True, help="数据集ID")
    fi_parser.add_argument("--refresh", action="store_true", help="强制刷新缓存")

    # operators 子命令
    op_parser = subparsers.add_parser("operators", help="获取操作符列表")
    op_parser.add_argument("--refresh", action="store_true", help="强制刷新缓存")

    # backtest 子命令
    bt_parser = subparsers.add_parser("backtest", help="批量回测")
    bt_parser.add_argument("--file", "-f", required=True, help="表达式JSON文件")
    bt_parser.add_argument("--region", "-r", required=True, help="区域代码")
    bt_parser.add_argument("--dataset", "-d", default="mixed", help="数据集名称")

    # report 子命令
    rp_parser = subparsers.add_parser("report", help="查看Alpha报告")
    rp_parser.add_argument("--alpha-id", "-a", required=True, help="Alpha ID")

    # pipeline 子命令 — Alpha Factory 全流程
    pipe_parser = subparsers.add_parser("pipeline", help="Alpha Factory Pipeline（全流程 + research_report.json）")
    pipe_parser.add_argument("--region", "-r", required=True, help="区域代码（如 USA）")
    pipe_parser.add_argument("--datasets", "-d", required=True, help="数据集 ID，逗号分隔（如 pv1 或 pv1,analyst15）")
    pipe_parser.add_argument("--output-dir", "-o", default=None, help="输出目录（默认 research/alpha_factory_<timestamp>）")
    pipe_parser.add_argument("--no-self-heal", action="store_true", help="不执行错误自愈")
    pipe_parser.add_argument("--resume", action="store_true", help="从已有输出目录恢复回测进度")
    pipe_parser.add_argument("--template-mode", choices=["default", "specialized"], default="default",
                             help="模板模式: default(通用模板) 或 specialized(针对性模板，按数据集自动查找)")
    pipe_parser.add_argument("--templates", default=None,
                             help="自定义模板文件路径（优先级最高，覆盖 template-mode）")
    pipe_parser.add_argument("--templates-per-round", "-t", type=int, default=None,
                             help="每轮使用的模板数量（默认 25）")
    pipe_parser.add_argument("--max-fields", "-f", type=int, default=None,
                             help="最大推荐字段数（默认 15）")

    # optimize 子命令 — Alpha 优化器
    opt_parser = subparsers.add_parser("optimize", help="Alpha 优化器（多轮迭代优化）")
    opt_parser.add_argument("--alpha-id", "-a", required=True, help="要优化的 Alpha ID")
    opt_parser.add_argument("--region", "-r", required=True, help="区域代码（如 USA）")
    opt_parser.add_argument("--max-rounds", "-m", type=int, default=5, help="最大优化轮数（默认 5）")
    opt_parser.add_argument("--neutralization", "-n", default=None, help="固定中性化（如 FAST、INDUSTRY、MARKET），不指定则自动搜索")
    opt_parser.add_argument("--strategy", "-s", default=None, choices=["param_tune", "smoothing", "combination", "ai_deep"], help="强制优化策略（不指定则自动选择）")
    opt_parser.add_argument("--output-dir", "-o", default=None, help="输出目录")

    # combine 子命令 — 批量组合优化
    combine_parser = subparsers.add_parser("combine", help="Alpha 组合优化（批量组合创新）")
    combine_parser.add_argument("--alpha-ids", "-a", required=True, help="Alpha ID 列表，逗号分隔")
    combine_parser.add_argument("--region", "-r", required=True, help="区域代码（如 USA）")
    combine_parser.add_argument("--max-combinations", "-m", type=int, default=10, help="最大生成组合数（默认 10）")
    combine_parser.add_argument("--output-dir", "-o", default=None, help="输出目录")

    args = parser.parse_args()

    if args.command is None:
        return False  # 没有子命令，进入交互模式

    if args.command == "pipeline":
        session = SessionManager.login()
        from config.settings import RESEARCH_DIR
        from pathlib import Path
        from datetime import datetime
        from ai.alpha_factory_pipeline import run_pipeline

        # 获取配置（命令行参数优先，否则使用 None 让 pipeline 自动处理）
        tpr = args.templates_per_round
        max_fields = args.max_fields

        dataset_ids = [x.strip() for x in args.datasets.split(",") if x.strip()]
        out = Path(args.output_dir) if args.output_dir else RESEARCH_DIR / f"alpha_factory_{args.region}_{'_'.join(dataset_ids)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # 打印运行配置
        print(f"\n{'='*50}")
        print("Pipeline 运行配置")
        print(f"{'='*50}")
        print(f"  区域: {args.region.upper()}")
        print(f"  数据集: {', '.join(dataset_ids)}")
        print(f"  模板模式: {args.template_mode}")
        print(f"  每轮模板数: {tpr if tpr else '自动(根据字段数动态调整)'}")
        print(f"  最大字段数: {max_fields if max_fields else '自动(根据字段数动态调整)'}")
        print(f"  输出目录: {out}")
        print(f"{'='*50}\n")

        state = run_pipeline(
            session, args.region.upper(), dataset_ids, DataManager,
            output_dir=out, run_self_heal_flag=not args.no_self_heal, steps=None,
            template_mode=args.template_mode,
            templates_path=args.templates,
            resume=args.resume,
            templates_per_round=tpr,
            max_recommended_fields=max_fields,
        )
        report_path = Path(state["output_dir"]) / "research_report.json"
        if report_path.exists():
            with open(report_path, "r", encoding="utf-8") as f:
                report = json.load(f)
            print(json.dumps(report, ensure_ascii=False, indent=2))
        return True

    elif args.command == "optimize":
        from ai.alpha_optimizer import optimize_alpha
        from pathlib import Path
        session = SessionManager.login()
        output_dir = Path(args.output_dir) if args.output_dir else Path("research")

        # 构建配置（包含固定中性化和强制策略）
        config = None
        if args.neutralization or args.strategy:
            config = {}
            if args.neutralization:
                config["fixed_neutralization"] = args.neutralization.upper()
                print(f"[配置] 固定中性化: {args.neutralization.upper()}")
            if args.strategy:
                config["force_strategy"] = args.strategy
                print(f"[配置] 强制策略: {args.strategy}")

        history = optimize_alpha(
            session,
            args.alpha_id,
            args.region.upper(),
            max_rounds=args.max_rounds,
            output_dir=output_dir,
            config=config,
        )
        print(f"\n{'='*50}")
        print("优化结果:")
        print(f"  原始 Alpha: {history.original_alpha_id}")
        print(f"  原始 Sharpe: {history.original_metrics.get('sharpe', 0):.2f}")
        print(f"  原始 Turnover: {history.original_metrics.get('turnover', 0):.1%}")
        if history.best_metrics:
            print(f"  最佳 Sharpe: {history.best_metrics.get('sharpe', 0):.2f}")
            print(f"  最佳 Turnover: {history.best_metrics.get('turnover', 0):.1%}")
        print(f"  优化轮数: {len(history.records)}")
        print(f"  最终状态: {history.final_status}")
        print(f"{'='*50}")
        return True

    elif args.command == "combine":
        from ai.alpha_optimizer import batch_combine_optimize
        from pathlib import Path
        session = SessionManager.login()
        output_dir = Path(args.output_dir) if args.output_dir else Path("research/optimizations")
        alpha_ids = [x.strip() for x in args.alpha_ids.split(",") if x.strip()]
        if not alpha_ids:
            print("❌ Alpha ID 列表为空")
            return True

        print(f"\n{'='*50}")
        print(f"批量组合优化")
        print(f"  输入 Alpha: {len(alpha_ids)} 个")
        print(f"  区域: {args.region.upper()}")
        print(f"  最大组合数: {args.max_combinations}")
        print(f"{'='*50}\n")

        result = batch_combine_optimize(
            session,
            alpha_ids,
            args.region.upper(),
            output_dir=output_dir,
            max_combinations=args.max_combinations,
        )

        print(f"\n{'='*50}")
        print("组合优化结果:")
        print(f"  表达式分组: {len(result.source_groups)} 组")
        print(f"  生成表达式: {len(result.generated_expressions)} 个")
        print(f"  达标 Alpha: {len(result.qualified_alphas)} 个")
        print(f"{'='*50}")

        if result.qualified_alphas:
            print("\n达标 Alpha 列表:")
            for i, a in enumerate(result.qualified_alphas, 1):
                print(f"  {i}. {a.get('alpha_id', 'N/A')}: "
                      f"Sharpe={a.get('sharpe', 0):.2f}, "
                      f"Turnover={a.get('turnover', 0):.1%}")
        return True

    session = SessionManager.login()

    if args.command == "datasets":
        datasets = DataManager.get_datasets(session, args.region, force_refresh=args.refresh)
        print(datasets.to_string())

    elif args.command == "fields":
        fields = DataManager.get_fields(session, args.region, args.dataset, force_refresh=args.refresh)
        print(fields.to_string())

    elif args.command == "operators":
        operators = DataManager.get_operators(session, force_refresh=args.refresh)
        print(operators.to_string())

    elif args.command == "backtest":
        with open(args.file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list) and isinstance(data[0], str):
            expressions = data
        elif isinstance(data, list) and isinstance(data[0], dict):
            expressions = [d.get("expression", d.get("regular", "")) for d in data]
        else:
            print("❌ 不支持的JSON格式")
            return True

        configs = AlphaBuilder.build_batch_configs(expressions, args.region)
        results = BacktestRunner.run_batch(session, configs)
        BacktestRunner.save_research(results, args.region, args.dataset)

    elif args.command == "report":
        result = BacktestRunner.get_report(session, args.alpha_id)
        if result["success"]:
            print(json.dumps(result, indent=2, ensure_ascii=False, default=str))

    return True


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cli_mode()
    else:
        interactive_mode()
