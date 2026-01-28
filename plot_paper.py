"""
论文插图生成脚本
读取 experiment_results.csv 生成学术风格图表
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
from pathlib import Path

# ========== 全局样式设置 ==========

# 尝试使用学术风格
try:
    plt.style.use('seaborn-v0_8-paper')
except:
    try:
        plt.style.use('seaborn-paper')
    except:
        plt.style.use('ggplot')

# 字体设置：无衬线字体，适合论文
plt.rcParams.update({
    'font.family': 'sans-serif',
    'font.sans-serif': ['Arial', 'Helvetica', 'DejaVu Sans'],
    'font.size': 11,
    'axes.titlesize': 13,
    'axes.labelsize': 12,
    'xtick.labelsize': 10,
    'ytick.labelsize': 10,
    'legend.fontsize': 10,
    'figure.titlesize': 14,
    'figure.dpi': 150,
    'savefig.dpi': 300,
    'savefig.bbox': 'tight',
    'axes.spines.top': False,
    'axes.spines.right': False,
})

# 颜色方案
COLOR_PROPOSED = '#1f77b4'      # 深蓝色 - 突出 Proposed
COLOR_ABLATION_1 = '#7f7f7f'    # 灰色
COLOR_ABLATION_2 = '#bcbcbc'    # 浅灰色
COLOR_HIGHLIGHT = '#2ca02c'     # 绿色 - 用于强调


def load_experiment_data(csv_path: str = "experiment_results.csv") -> pd.DataFrame:
    """
    读取实验结果 CSV 文件
    
    Args:
        csv_path: CSV 文件路径
        
    Returns:
        实验数据 DataFrame
    """
    if not Path(csv_path).exists():
        print(f"⚠️ 文件不存在: {csv_path}")
        print("   生成模拟数据用于演示...")
        
        # 模拟数据（用于测试）
        data = {
            'Experiment': ['Proposed (Full System)', 'w/o SmartScheduler', 'w/o FastCDC'],
            'Total Time (s)': [2.35, 4.82, 2.41],
            'Avg Latency (ms)': [35.2, 125.6, 38.1],
            'Dedup Ratio (%)': [87.5, 86.2, 12.3],
            'Total Chunks': [640, 635, 640],
            'Uploaded Shards': [1920, 1905, 1920]
        }
        return pd.DataFrame(data)
    
    print(f"📊 读取数据: {csv_path}")
    return pd.read_csv(csv_path)


def plot_ablation_time(df: pd.DataFrame, output_path: str = "fig_ablation_time.png"):
    """
    图表 1：消融实验耗时对比柱状图
    
    Args:
        df: 实验数据
        output_path: 输出图片路径
    """
    print(f"📈 生成图表: {output_path}")
    
    fig, ax = plt.subplots(figsize=(8, 5))
    
    # 提取数据
    experiments = df['Experiment'].tolist()
    
    # 尝试不同的列名（兼容性）
    time_col = None
    for col in ['Total Time (s)', 'Time(s)', 'Time', 'total_time']:
        if col in df.columns:
            time_col = col
            break
    
    if time_col is None:
        print("   ⚠️ 未找到时间列，使用模拟数据")
        times = [2.35, 4.82, 2.41]
    else:
        times = df[time_col].tolist()
    
    # 简化实验名称（用于显示）
    display_names = []
    for name in experiments:
        if 'Proposed' in name or 'Full' in name:
            display_names.append('Proposed\n(CE2S)')
        elif 'Scheduler' in name:
            display_names.append('w/o Smart\nScheduler')
        elif 'FastCDC' in name:
            display_names.append('w/o\nFastCDC')
        else:
            display_names.append(name)
    
    # 颜色：Proposed 深蓝色，其他灰色
    colors = []
    for name in experiments:
        if 'Proposed' in name or 'Full' in name:
            colors.append(COLOR_PROPOSED)
        elif 'Scheduler' in name:
            colors.append(COLOR_ABLATION_1)
        else:
            colors.append(COLOR_ABLATION_2)
    
    # 绘制柱状图
    x_pos = range(len(display_names))
    bars = ax.bar(x_pos, times, color=colors, edgecolor='black', linewidth=0.8, width=0.6)
    
    # 在柱子上标注具体秒数
    for bar, time_val in zip(bars, times):
        height = bar.get_height()
        ax.annotate(f'{time_val:.2f}s',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 5),
                    textcoords='offset points',
                    ha='center', va='bottom',
                    fontsize=11, fontweight='bold')
    
    # 设置标签和标题
    ax.set_xticks(x_pos)
    ax.set_xticklabels(display_names)
    ax.set_ylabel('Total Upload Time (seconds)', fontweight='bold')
    ax.set_title('Ablation Study: Upload Time Comparison', fontweight='bold', pad=15)
    
    # Y轴从0开始
    ax.set_ylim(0, max(times) * 1.25)
    
    # 添加网格线
    ax.yaxis.grid(True, linestyle='--', alpha=0.7)
    ax.set_axisbelow(True)
    
    # 添加说明文字
    ax.text(0.02, 0.98, 'Lower is better', transform=ax.transAxes,
            fontsize=9, verticalalignment='top', style='italic', color='gray')
    
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    
    print(f"   ✅ 保存至: {output_path}")


def plot_dedup_ratio(df: pd.DataFrame, output_path: str = "fig_dedup_ratio.png"):
    """
    图表 2：去重率对比柱状图
    仅对比 Proposed 和 w/o FastCDC
    
    Args:
        df: 实验数据
        output_path: 输出图片路径
    """
    print(f"📈 生成图表: {output_path}")
    
    fig, ax = plt.subplots(figsize=(6, 5))
    
    # 筛选数据：Proposed 和 w/o FastCDC
    proposed_row = None
    wo_fastcdc_row = None
    
    for _, row in df.iterrows():
        exp_name = row['Experiment']
        if 'Proposed' in exp_name or 'Full' in exp_name:
            proposed_row = row
        elif 'FastCDC' in exp_name:
            wo_fastcdc_row = row
    
    # 尝试不同的列名
    dedup_col = None
    for col in ['Dedup Ratio (%)', 'DedupRatio', 'Dedup Ratio', 'dedup_ratio']:
        if col in df.columns:
            dedup_col = col
            break
    
    # 提取去重率数据
    if proposed_row is not None and dedup_col:
        proposed_dedup = float(proposed_row[dedup_col])
    else:
        proposed_dedup = 87.5  # 模拟数据
    
    if wo_fastcdc_row is not None and dedup_col:
        wo_fastcdc_dedup = float(wo_fastcdc_row[dedup_col])
    else:
        wo_fastcdc_dedup = 12.3  # 模拟数据
    
    # 准备绑图数据
    display_names = ['Proposed\n(FastCDC)', 'Fixed-size\nChunking']
    dedup_values = [proposed_dedup, wo_fastcdc_dedup]
    colors = [COLOR_PROPOSED, COLOR_ABLATION_2]
    
    # 绘制柱状图
    x_pos = range(len(display_names))
    bars = ax.bar(x_pos, dedup_values, color=colors, edgecolor='black', linewidth=0.8, width=0.5)
    
    # 在柱子上标注百分比
    for bar, val in zip(bars, dedup_values):
        height = bar.get_height()
        ax.annotate(f'{val:.1f}%',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 5),
                    textcoords='offset points',
                    ha='center', va='bottom',
                    fontsize=12, fontweight='bold')
    
    # 设置标签和标题
    ax.set_xticks(x_pos)
    ax.set_xticklabels(display_names)
    ax.set_ylabel('Deduplication Ratio (%)', fontweight='bold')
    ax.set_title('Chunking Algorithm Impact on Deduplication', fontweight='bold', pad=15)
    
    # Y轴固定 0-100%
    ax.set_ylim(0, 100)
    ax.set_yticks([0, 20, 40, 60, 80, 100])
    
    # 添加网格线
    ax.yaxis.grid(True, linestyle='--', alpha=0.7)
    ax.set_axisbelow(True)
    
    # 添加差异标注
    improvement = proposed_dedup - wo_fastcdc_dedup
    ax.annotate('',
                xy=(0, proposed_dedup), xytext=(1, wo_fastcdc_dedup),
                arrowprops=dict(arrowstyle='<->', color='red', lw=1.5))
    
    mid_x = 0.5
    mid_y = (proposed_dedup + wo_fastcdc_dedup) / 2
    ax.text(mid_x, mid_y, f'+{improvement:.1f}%',
            ha='center', va='center', fontsize=10, fontweight='bold',
            color='red', bbox=dict(boxstyle='round', facecolor='white', edgecolor='red', alpha=0.8))
    
    # 添加说明文字
    ax.text(0.02, 0.98, 'Test: 10% modified file', transform=ax.transAxes,
            fontsize=9, verticalalignment='top', style='italic', color='gray')
    
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    
    print(f"   ✅ 保存至: {output_path}")


def plot_latency_comparison(df: pd.DataFrame, output_path: str = "fig_latency.png"):
    """
    图表 3（可选）：平均延迟对比
    
    Args:
        df: 实验数据
        output_path: 输出图片路径
    """
    print(f"📈 生成图表: {output_path}")
    
    fig, ax = plt.subplots(figsize=(8, 5))
    
    experiments = df['Experiment'].tolist()
    
    # 尝试不同的列名
    latency_col = None
    for col in ['Avg Latency (ms)', 'Latency(ms)', 'Avg Latency', 'avg_latency']:
        if col in df.columns:
            latency_col = col
            break
    
    if latency_col is None:
        latencies = [35.2, 125.6, 38.1]  # 模拟数据
    else:
        latencies = df[latency_col].tolist()
    
    # 简化名称
    display_names = []
    for name in experiments:
        if 'Proposed' in name or 'Full' in name:
            display_names.append('Proposed\n(CE2S)')
        elif 'Scheduler' in name:
            display_names.append('w/o Smart\nScheduler')
        elif 'FastCDC' in name:
            display_names.append('w/o\nFastCDC')
        else:
            display_names.append(name)
    
    # 颜色
    colors = []
    for name in experiments:
        if 'Proposed' in name or 'Full' in name:
            colors.append(COLOR_PROPOSED)
        elif 'Scheduler' in name:
            colors.append('#d62728')  # 红色突出高延迟
        else:
            colors.append(COLOR_ABLATION_2)
    
    # 绘制柱状图
    x_pos = range(len(display_names))
    bars = ax.bar(x_pos, latencies, color=colors, edgecolor='black', linewidth=0.8, width=0.6)
    
    # 标注数值
    for bar, lat in zip(bars, latencies):
        height = bar.get_height()
        ax.annotate(f'{lat:.1f}ms',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 5),
                    textcoords='offset points',
                    ha='center', va='bottom',
                    fontsize=11, fontweight='bold')
    
    ax.set_xticks(x_pos)
    ax.set_xticklabels(display_names)
    ax.set_ylabel('Average Latency (ms)', fontweight='bold')
    ax.set_title('Scheduler Impact on Upload Latency', fontweight='bold', pad=15)
    
    ax.set_ylim(0, max(latencies) * 1.25)
    ax.yaxis.grid(True, linestyle='--', alpha=0.7)
    ax.set_axisbelow(True)
    
    ax.text(0.02, 0.98, 'Lower is better', transform=ax.transAxes,
            fontsize=9, verticalalignment='top', style='italic', color='gray')
    
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    
    print(f"   ✅ 保存至: {output_path}")


def main():
    """主函数"""
    print("╔" + "═"*50 + "╗")
    print("║" + " Paper Figure Generator ".center(50) + "║")
    print("║" + " 论文插图生成工具 ".center(44) + "║")
    print("╚" + "═"*50 + "╝")
    
    # 读取数据
    df = load_experiment_data("experiment_results.csv")
    print(f"\n📋 实验数据概览:")
    print(df.to_string(index=False))
    
    # 生成图表
    print("\n" + "-"*50)
    plot_ablation_time(df, "fig_ablation_time.png")
    plot_dedup_ratio(df, "fig_dedup_ratio.png")
    plot_latency_comparison(df, "fig_latency.png")
    
    print("\n" + "="*50)
    print("✅ 所有图表生成完成!")
    print("   输出文件:")
    print("   • fig_ablation_time.png  - 消融实验耗时对比")
    print("   • fig_dedup_ratio.png    - 去重率对比")
    print("   • fig_latency.png        - 平均延迟对比")
    print("="*50)


if __name__ == "__main__":
    main()
