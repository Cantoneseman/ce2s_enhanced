"""
论文插图生成脚本（中文版）
读取实验结果 CSV 文件，生成论文级别的中文插图

生成的图表：
- 图 1: 系统整体性能对比 (fig1_macro_performance.png)
- 图 2: 网络抖动鲁棒性 (fig2_jitter_robustness.png)
- 图 3: 数据类型适应性 (fig3_datatype_adaptability.png)
- 图 4: 关键模块消融实验 (fig4_ablation_study.png)
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path


# ========== 全局字体设置（必须在最开头，防止中文乱码）==========

plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'SimSun', 'Arial']
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题


# ========== 通用设置 ==========

def setup_plot_style():
    """设置论文级别的绘图风格"""
    # 使用 seaborn 风格
    try:
        plt.style.use('seaborn-v0_8-whitegrid')
    except OSError:
        try:
            plt.style.use('seaborn-whitegrid')
        except OSError:
            pass
    
    # 重新设置中文字体（style.use 可能会覆盖）
    plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'SimSun', 'Arial']
    plt.rcParams['axes.unicode_minus'] = False
    
    # 其他设置
    plt.rcParams.update({
        'font.size': 11,
        'axes.titlesize': 12,
        'axes.labelsize': 11,
        'xtick.labelsize': 10,
        'ytick.labelsize': 10,
        'legend.fontsize': 10,
        'figure.titlesize': 14,
        'figure.dpi': 150,
        'savefig.dpi': 300,
        'savefig.bbox': 'tight',
        'axes.grid': True,
        'grid.alpha': 0.3,
    })


# 配色方案
COLORS = {
    'proposed': '#2878B5',      # 深蓝色 - 本文方法/完整系统
    'fixed': '#9AC9DB',         # 浅灰蓝 - 固定分块/消融变体
    'random': '#C82423',        # 红色 - 随机调度
    'smart': '#2878B5',         # 蓝色 - 智能调度
}

# 中文标签映射
METHOD_LABELS = {
    'Fixed-size Chunking': '固定分块',
    'Proposed (FastCDC)': '本文方法',
    'Random': '随机调度',
    'Smart (Proposed)': '本文智能调度',
}

DATATYPE_LABELS = {
    'Code': '源代码',
    'PDF': 'PDF文档',
    'Binary': '二进制文件',
}

ABLATION_LABELS = {
    'Proposed (Full System)': '完整系统 (CE2S)',
    'w/o SmartScheduler': '无智能调度',
    'w/o FastCDC': '无 FastCDC',
}

# 结果文件路径
RESULTS_DIR = Path("results")
FIGURES_DIR = Path("figures")


def ensure_figures_dir():
    """确保 figures 目录存在"""
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)


def get_column_value(df, possible_names, default=None):
    """
    从 DataFrame 中获取可能存在的列名
    
    Args:
        df: DataFrame
        possible_names: 可能的列名列表
        default: 默认值
        
    Returns:
        列名或默认值
    """
    for name in possible_names:
        if name in df.columns:
            return name
    return default


# ========== 图 1: 系统整体性能对比 ==========

def plot_figure1_macro_performance():
    """
    图 1: 系统整体性能对比
    
    数据源: experiment_results_final.csv (Phase='Macro-benchmark')
    内容: 双子图 - (a) 传输耗时对比 (b) 去重率对比
    """
    print("\n" + "="*60)
    print("📊 生成图 1: 系统整体性能对比")
    print("="*60)
    
    # 尝试多个可能的文件名
    csv_path = None
    for filename in ['experiment_results_final.csv', 'experiment_results.csv']:
        path = RESULTS_DIR / filename
        if path.exists():
            csv_path = path
            break
    
    if csv_path is None:
        print("   ❌ 未找到实验结果文件")
        return
    
    df = pd.read_csv(csv_path)
    print(f"   📄 读取数据: {csv_path.name}, {len(df)} 行")
    print(f"   📋 列名: {list(df.columns)}")
    
    # 筛选 Macro-benchmark 数据（如果存在 Phase 列）
    if 'Phase' in df.columns:
        macro_df = df[df['Phase'] == 'Macro-benchmark']
        if len(macro_df) > 0:
            df = macro_df
            print(f"   📋 筛选 Macro-benchmark: {len(df)} 行")
    
    # 识别方法列名（可能是 Method 或 Experiment）
    method_col = get_column_value(df, ['Method', 'Experiment'], 'Method')
    if method_col not in df.columns:
        print(f"   ❌ 未找到方法列 (Method/Experiment)")
        return
    
    # 识别时间列名
    time_col = get_column_value(df, ['Time(s)', 'Total Time (s)', 'TotalTime(s)'], 'Time(s)')
    if time_col not in df.columns:
        print(f"   ❌ 未找到时间列")
        return
    
    # 识别去重率列名
    dedup_col = get_column_value(df, ['DedupRatio(%)', 'Dedup Ratio (%)', 'Dedup(%)'], None)
    
    # 去重，保留最后一次实验数据
    df = df.drop_duplicates(subset=[method_col], keep='last')
    print(f"   📋 去重后: {len(df)} 行")
    
    # 只保留 Fixed 和 Proposed 相关的数据
    mask = df[method_col].str.contains('Fixed|Proposed|FastCDC', case=False, regex=True, na=False)
    df = df[mask]
    
    if len(df) < 2:
        print("   ❌ 数据不足（需要 Fixed 和 Proposed 两组），跳过绘图")
        return
    
    # 创建双子图
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))
    
    # 准备数据
    methods = df[method_col].tolist()
    times = df[time_col].tolist()
    dedups = df[dedup_col].tolist() if dedup_col else [0] * len(methods)
    
    # 中文标签和配色
    labels = []
    colors = []
    for m in methods:
        if 'FastCDC' in str(m) or 'Proposed' in str(m):
            labels.append('本文方法')
            colors.append(COLORS['proposed'])
        else:
            labels.append('固定分块')
            colors.append(COLORS['fixed'])
    
    # 子图 (a): 传输耗时对比
    bars1 = ax1.bar(labels, times, color=colors, edgecolor='black', linewidth=0.8)
    ax1.set_ylabel('传输耗时 (秒)')
    ax1.set_title('(a) 传输耗时对比')
    ax1.set_ylim(0, max(times) * 1.25)
    
    for bar, val in zip(bars1, times):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(times)*0.02,
                f'{val:.3f}s', ha='center', va='bottom', fontsize=9)
    
    # 子图 (b): 去重率对比
    bars2 = ax2.bar(labels, dedups, color=colors, edgecolor='black', linewidth=0.8)
    ax2.set_ylabel('去重率 (%)')
    ax2.set_title('(b) 去重率对比')
    ax2.set_ylim(0, 100)
    
    for bar, val in zip(bars2, dedups):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                f'{val:.1f}%', ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    
    # 保存
    output_path = FIGURES_DIR / "fig1_macro_performance.png"
    plt.savefig(output_path)
    plt.savefig(FIGURES_DIR / "fig1_macro_performance.pdf")
    print(f"   ✅ 已保存: {output_path}")
    
    plt.close()


# ========== 图 2: 网络抖动鲁棒性 ==========

def plot_figure2_jitter_robustness():
    """
    图 2: 网络抖动鲁棒性
    
    数据源: jitter_test_results.csv (只保留 Random 和 Smart)
    内容: 折线图 - 不同 Jitter 级别下的平均耗时
    """
    print("\n" + "="*60)
    print("📊 生成图 2: 网络抖动鲁棒性")
    print("="*60)
    
    csv_path = RESULTS_DIR / "jitter_test_results.csv"
    if not csv_path.exists():
        print(f"   ❌ 文件不存在: {csv_path}")
        return
    
    df = pd.read_csv(csv_path)
    print(f"   📄 读取数据: {len(df)} 行")
    print(f"   📋 列名: {list(df.columns)}")
    
    # 仅保留 Random 和 Smart
    df = df[df['Method'].isin(['Random', 'Smart (Proposed)'])]
    print(f"   📋 筛选 Random & Smart: {len(df)} 行")
    
    if len(df) == 0:
        print("   ❌ 数据为空，跳过绘图")
        return
    
    # 识别时间列名
    time_col = get_column_value(df, ['AvgTime(s)', 'Avg Time (s)', 'Time(s)'], 'AvgTime(s)')
    
    fig, ax = plt.subplots(figsize=(8, 5))
    
    # 绘制折线图
    for method in ['Random', 'Smart (Proposed)']:
        method_data = df[df['Method'] == method].sort_values('Jitter(ms)')
        
        if len(method_data) == 0:
            continue
        
        if method == 'Random':
            color, marker, linestyle = COLORS['random'], 'o', '--'
            label = '随机调度'
        else:
            color, marker, linestyle = COLORS['smart'], 's', '-'
            label = '本文智能调度'
        
        ax.plot(method_data['Jitter(ms)'], method_data[time_col],
               marker=marker, markersize=8, linewidth=2,
               color=color, linestyle=linestyle, label=label)
    
    ax.set_xlabel('网络抖动幅度 (标准差 ms)')
    ax.set_ylabel('平均上传耗时 (秒)')
    ax.set_title('网络抖动对传输效率的影响')
    
    jitter_levels = sorted(df['Jitter(ms)'].unique())
    ax.set_xticks(jitter_levels)
    ax.set_xticklabels([str(int(j)) for j in jitter_levels])
    
    ax.legend(loc='upper left', framealpha=0.9)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    output_path = FIGURES_DIR / "fig2_jitter_robustness.png"
    plt.savefig(output_path)
    plt.savefig(FIGURES_DIR / "fig2_jitter_robustness.pdf")
    print(f"   ✅ 已保存: {output_path}")
    
    plt.close()


# ========== 图 3: 数据类型适应性 ==========

def plot_figure3_datatype_adaptability():
    """
    图 3: 数据类型适应性
    
    数据源: datatype_test_results.csv
    内容: 分组柱状图 - 不同数据类型下的去重率
    """
    print("\n" + "="*60)
    print("📊 生成图 3: 数据类型适应性")
    print("="*60)
    
    csv_path = RESULTS_DIR / "datatype_test_results.csv"
    if not csv_path.exists():
        print(f"   ❌ 文件不存在: {csv_path}")
        return
    
    df = pd.read_csv(csv_path)
    print(f"   📄 读取数据: {len(df)} 行")
    print(f"   📋 列名: {list(df.columns)}")
    
    if len(df) == 0:
        print("   ❌ 数据为空，跳过绘图")
        return
    
    fig, ax = plt.subplots(figsize=(8, 5))
    
    # 数据类型顺序
    data_types = ['Code', 'PDF', 'Binary']
    data_types = [dt for dt in data_types if dt in df['DataType'].values]
    
    if len(data_types) == 0:
        print("   ❌ 无有效数据类型，跳过绘图")
        return
    
    x = np.arange(len(data_types))
    width = 0.35
    
    fixed_values = []
    proposed_values = []
    
    for dt in data_types:
        dt_data = df[df['DataType'] == dt]
        
        fixed_row = dt_data[dt_data['Method'].str.contains('Fixed', case=False, na=False)]
        fixed_val = fixed_row['DedupRatio(%)'].values[0] if len(fixed_row) > 0 else 0
        fixed_values.append(fixed_val)
        
        proposed_row = dt_data[dt_data['Method'].str.contains('FastCDC|Proposed', case=False, regex=True, na=False)]
        proposed_val = proposed_row['DedupRatio(%)'].values[0] if len(proposed_row) > 0 else 0
        proposed_values.append(proposed_val)
    
    # 中文标签
    x_labels = [DATATYPE_LABELS.get(dt, dt) for dt in data_types]
    
    bars1 = ax.bar(x - width/2, fixed_values, width, 
                   label='固定分块', color=COLORS['fixed'],
                   edgecolor='black', linewidth=0.8)
    bars2 = ax.bar(x + width/2, proposed_values, width,
                   label='本文方法', color=COLORS['proposed'],
                   edgecolor='black', linewidth=0.8)
    
    for bar, val in zip(bars1, fixed_values):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
               f'{val:.1f}%', ha='center', va='bottom', fontsize=9)
    
    for bar, val in zip(bars2, proposed_values):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
               f'{val:.1f}%', ha='center', va='bottom', fontsize=9)
    
    ax.set_xlabel('数据类型')
    ax.set_ylabel('去重率 (%)')
    ax.set_title('不同数据类型下的去重效率')
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels)
    ax.set_ylim(0, 100)
    ax.legend(loc='upper right', framealpha=0.9)
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    
    output_path = FIGURES_DIR / "fig3_datatype_adaptability.png"
    plt.savefig(output_path)
    plt.savefig(FIGURES_DIR / "fig3_datatype_adaptability.pdf")
    print(f"   ✅ 已保存: {output_path}")
    
    plt.close()


# ========== 图 4: 关键模块消融实验 ==========

def plot_figure4_ablation_study():
    """
    图 4: 关键模块消融实验
    
    数据源: experiment_results.csv
    列名: Experiment (不是 Method)
    内容: 柱状图 - 消融实验对性能的影响
    """
    print("\n" + "="*60)
    print("📊 生成图 4: 关键模块消融实验")
    print("="*60)
    
    csv_path = RESULTS_DIR / "experiment_results.csv"
    if not csv_path.exists():
        print(f"   ❌ 文件不存在: {csv_path}")
        return
    
    df = pd.read_csv(csv_path)
    print(f"   📄 读取数据: {len(df)} 行")
    print(f"   📋 列名: {list(df.columns)}")
    
    if len(df) == 0:
        print("   ❌ 数据为空，跳过绘图")
        return
    
    # 识别实验名称列（Experiment 或 Method）
    exp_col = get_column_value(df, ['Experiment', 'Method'], None)
    if exp_col is None:
        print("   ❌ 未找到 Experiment 或 Method 列")
        return
    print(f"   📋 使用列: {exp_col}")
    
    # 识别时间列
    time_col = get_column_value(df, ['Total Time (s)', 'Time(s)', 'TotalTime(s)'], None)
    if time_col is None:
        print("   ❌ 未找到时间列")
        return
    print(f"   📋 时间列: {time_col}")
    
    # 应用中文标签映射
    label_map = {
        'Proposed (Full System)': '完整系统 (CE2S)',
        'w/o SmartScheduler': '无智能调度',
        'w/o FastCDC': '无 FastCDC',
    }
    df['Label'] = df[exp_col].replace(label_map)
    
    # 对于未映射的值，保持原样
    df['Label'] = df['Label'].fillna(df[exp_col])
    
    print(f"   📋 实验组: {df['Label'].tolist()}")
    
    fig, ax = plt.subplots(figsize=(8, 5))
    
    labels = df['Label'].tolist()
    times = df[time_col].tolist()
    
    # 配色：完整系统深蓝，其他浅灰
    colors = [COLORS['proposed'] if '完整系统' in str(lbl) else COLORS['fixed'] for lbl in labels]
    
    bars = ax.bar(labels, times, color=colors, edgecolor='black', linewidth=0.8)
    
    for bar, val in zip(bars, times):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(times)*0.02,
               f'{val:.3f}s', ha='center', va='bottom', fontsize=9)
    
    ax.set_ylabel('总上传耗时 (秒)')
    ax.set_title('消融实验：关键模块对性能的影响')
    ax.set_ylim(0, max(times) * 1.25)
    ax.grid(True, alpha=0.3, axis='y')
    
    # 🔥 修改处：将 rotation 改为 0，标签就会水平显示
    plt.xticks(rotation=0)
    
    plt.tight_layout()
    
    output_path = FIGURES_DIR / "fig4_ablation_study.png"
    plt.savefig(output_path)
    plt.savefig(FIGURES_DIR / "fig4_ablation_study.pdf")
    print(f"   ✅ 已保存: {output_path}")
    
    plt.close()
# ========== 主函数 ==========

def main():
    """生成所有论文插图"""
    print("="*70)
    print("  论文插图生成脚本（中文版）")
    print("  Paper Figure Generation Script (Chinese)")
    print("="*70)
    
    # 设置绘图风格
    setup_plot_style()
    
    # 创建输出目录
    ensure_figures_dir()
    print(f"\n📁 输出目录: {FIGURES_DIR.absolute()}")
    
    # 检查数据文件
    print(f"\n📂 数据目录: {RESULTS_DIR.absolute()}")
    if RESULTS_DIR.exists():
        csv_files = list(RESULTS_DIR.glob("*.csv"))
        print(f"   发现 {len(csv_files)} 个 CSV 文件:")
        for f in csv_files:
            print(f"   - {f.name}")
    else:
        print("   ⚠️ 数据目录不存在")
    
    # 生成各图
    plot_figure1_macro_performance()
    plot_figure2_jitter_robustness()
    plot_figure3_datatype_adaptability()
    plot_figure4_ablation_study()
    
    # 总结
    print("\n" + "="*70)
    print("✅ 所有插图生成完成！")
    print("="*70)
    
    # 列出生成的文件
    if FIGURES_DIR.exists():
        output_files = list(FIGURES_DIR.glob("*"))
        print(f"\n📊 生成的图片 ({len(output_files)} 个):")
        for f in sorted(output_files):
            print(f"   - {f.name}")


if __name__ == "__main__":
    main()
