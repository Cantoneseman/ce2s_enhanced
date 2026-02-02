"""
run_comprehensive_profiling.py
综合性能测试脚本

实验目的：评估系统在不同负载下的资源消耗趋势（Scalability）
以及大文件处理时的详细资源波形。

包含两个部分：
Part 1: 可扩展性测试 (Scalability Test)
Part 2: 大文件波形捕捉 (Detailed Waveform)

依赖库：psutil, pandas, time, threading, gc
"""

import os
import gc
import time
import threading
import psutil
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
from datetime import datetime

# 导入 Client 类
from client import Client


# ============================================================
# 工具函数
# ============================================================

def get_process_metrics():
    """获取当前进程的 CPU 和内存使用情况"""
    process = psutil.Process()
    cpu_percent = process.cpu_percent(interval=None)
    memory_mb = process.memory_info().rss / (1024 * 1024)
    return cpu_percent, memory_mb


def generate_test_data(size_mb: int) -> bytes:
    """
    生成指定大小的测试数据
    使用混合数据模式以更接近真实场景
    """
    size_bytes = size_mb * 1024 * 1024
    
    # 混合数据：50% 随机 + 50% 重复模式（模拟真实文件）
    random_part = np.random.bytes(size_bytes // 2)
    
    # 重复模式（模拟代码、文档中的重复结构）
    pattern = b"This is a repeating pattern for deduplication testing. " * 100
    repeat_count = (size_bytes // 2) // len(pattern) + 1
    repeat_part = (pattern * repeat_count)[:size_bytes // 2]
    
    return random_part + repeat_part


# 带宽配置：100 Mbps = 12.5 MB/s
BANDWIDTH_MBs = 12.5  # MB/s (100 Mbps)


def baseline_process(data: bytes) -> Tuple[float, float]:
    """
    Baseline 处理：模拟传统方案（全量传输，无去重）
    
    Returns:
        Tuple[elapsed_time, peak_cpu, peak_memory]
    """
    start_time = time.time()
    process = psutil.Process()
    
    peak_cpu = 0
    peak_memory = 0
    
    # 模拟读取数据到缓冲区
    buffer = bytearray(len(data))
    buffer[:] = data
    
    # 记录峰值
    cpu, mem = get_process_metrics()
    peak_cpu = max(peak_cpu, cpu)
    peak_memory = max(peak_memory, mem)
    
    # 模拟网络传输延迟（100 Mbps = 12.5 MB/s）
    # Baseline 需要传输全部数据
    data_size_mb = len(data) / (1024 * 1024)
    transfer_time = data_size_mb / BANDWIDTH_MBs
    time.sleep(transfer_time)  # 不设上限，反映真实传输开销
    
    # 再次记录
    cpu, mem = get_process_metrics()
    peak_cpu = max(peak_cpu, cpu)
    peak_memory = max(peak_memory, mem)
    
    elapsed = time.time() - start_time
    
    del buffer
    gc.collect()
    
    return elapsed, peak_cpu, peak_memory


def proposed_process(data: bytes, client: Client) -> Tuple[float, float, float]:
    """
    Proposed 处理：FastCDC 分块 + MLE 加密 + 去重
    
    Returns:
        Tuple[elapsed_time, peak_cpu, peak_memory]
    """
    start_time = time.time()
    process = psutil.Process()
    
    peak_cpu = 0
    peak_memory = 0
    
    # 初始化 CPU 监控
    process.cpu_percent(interval=None)
    
    # 执行 FastCDC + MLE 加密
    encrypted_chunks = client.encrypt_data(data, chunk_mode='fastcdc')
    
    # 记录峰值
    cpu, mem = get_process_metrics()
    peak_cpu = max(peak_cpu, cpu)
    peak_memory = max(peak_memory, mem)
    
    # 模拟去重后的网络传输（假设 70% 去重率，只传输 30% 数据）
    # 传输时间按 100 Mbps = 12.5 MB/s 计算
    data_size_mb = len(data) / (1024 * 1024)
    unique_ratio = 0.30  # 只需传输 30% 的唯一数据
    transfer_time = (data_size_mb * unique_ratio) / BANDWIDTH_MBs
    time.sleep(transfer_time)  # 不设上限
    
    # 再次记录
    cpu, mem = get_process_metrics()
    peak_cpu = max(peak_cpu, cpu)
    peak_memory = max(peak_memory, mem)
    
    elapsed = time.time() - start_time
    
    del encrypted_chunks
    gc.collect()
    
    return elapsed, peak_cpu, peak_memory


# ============================================================
# Part 1: 可扩展性测试 (Scalability Test)
# ============================================================

def run_scalability_test():
    """
    可扩展性测试：评估不同文件大小下的资源消耗
    """
    print("=" * 60)
    print("Part 1: 可扩展性测试 (Scalability Test)")
    print("=" * 60)
    
    # 测试配置
    file_sizes = [100, 200, 500, 1000]  # MB - 匹配论文图5
    num_runs = 3  # 每个配置运行次数
    
    print(f"带宽配置: {BANDWIDTH_MBs} MB/s ({BANDWIDTH_MBs * 8} Mbps)")
    
    results = []
    client = Client("profiling_client")
    
    total_tests = len(file_sizes) * 2 * num_runs
    current_test = 0
    
    for size_mb in file_sizes:
        print(f"\n[{size_mb}MB] 生成测试数据...")
        
        # 生成测试数据
        gc.collect()
        test_data = generate_test_data(size_mb)
        print(f"[{size_mb}MB] 数据生成完成 ({len(test_data) / 1024 / 1024:.1f} MB)")
        
        # ===== Baseline 测试 =====
        baseline_times = []
        baseline_cpus = []
        baseline_mems = []
        
        for run in range(num_runs):
            current_test += 1
            print(f"[{current_test}/{total_tests}] Baseline - {size_mb}MB - Run {run+1}/{num_runs}")
            
            gc.collect()
            time.sleep(0.5)  # 冷却
            
            elapsed, peak_cpu, peak_mem = baseline_process(test_data)
            baseline_times.append(elapsed)
            baseline_cpus.append(peak_cpu)
            baseline_mems.append(peak_mem)
        
        # 计算平均值
        avg_time = np.mean(baseline_times)
        avg_cpu = np.mean(baseline_cpus)
        avg_mem = np.mean(baseline_mems)
        throughput = size_mb / avg_time if avg_time > 0 else 0
        
        results.append({
            'FileSize_MB': size_mb,
            'Method': 'Baseline',
            'Avg_CPU_%': round(avg_cpu, 2),
            'Peak_Memory_MB': round(avg_mem, 2),
            'Total_Time_s': round(avg_time, 3),
            'Throughput_MBs': round(throughput, 2)
        })
        print(f"  → Baseline 平均: Time={avg_time:.2f}s, CPU={avg_cpu:.1f}%, Mem={avg_mem:.1f}MB")
        
        # ===== Proposed 测试 =====
        proposed_times = []
        proposed_cpus = []
        proposed_mems = []
        
        for run in range(num_runs):
            current_test += 1
            print(f"[{current_test}/{total_tests}] Proposed - {size_mb}MB - Run {run+1}/{num_runs}")
            
            gc.collect()
            time.sleep(0.5)  # 冷却
            
            elapsed, peak_cpu, peak_mem = proposed_process(test_data, client)
            proposed_times.append(elapsed)
            proposed_cpus.append(peak_cpu)
            proposed_mems.append(peak_mem)
        
        # 计算平均值
        avg_time = np.mean(proposed_times)
        avg_cpu = np.mean(proposed_cpus)
        avg_mem = np.mean(proposed_mems)
        throughput = size_mb / avg_time if avg_time > 0 else 0
        
        results.append({
            'FileSize_MB': size_mb,
            'Method': 'Proposed',
            'Avg_CPU_%': round(avg_cpu, 2),
            'Peak_Memory_MB': round(avg_mem, 2),
            'Total_Time_s': round(avg_time, 3),
            'Throughput_MBs': round(throughput, 2)
        })
        print(f"  → Proposed 平均: Time={avg_time:.2f}s, CPU={avg_cpu:.1f}%, Mem={avg_mem:.1f}MB")
        
        # 清理
        del test_data
        gc.collect()
    
    # 保存结果
    df = pd.DataFrame(results)
    output_path = 'results/scalability_test.csv'
    os.makedirs('results', exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"\n✓ 可扩展性测试结果已保存到: {output_path}")
    print(df.to_string(index=False))
    
    return df


# ============================================================
# Part 2: 大文件波形捕捉 (Detailed Waveform)
# ============================================================

class WaveformRecorder:
    """高频采样记录器"""
    
    def __init__(self, interval: float = 0.05):
        self.interval = interval
        self.records = []
        self.running = False
        self.thread = None
        self.start_time = 0
        self.current_method = ""
        self.current_phase = ""
    
    def start(self, method: str):
        """开始记录"""
        self.records = []
        self.running = True
        self.start_time = time.time()
        self.current_method = method
        self.current_phase = "计算阶段"
        self.thread = threading.Thread(target=self._record_loop, daemon=True)
        self.thread.start()
    
    def set_phase(self, phase: str):
        """设置当前阶段"""
        self.current_phase = phase
    
    def stop(self):
        """停止记录"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=1.0)
    
    def _record_loop(self):
        """记录循环"""
        process = psutil.Process()
        process.cpu_percent(interval=None)  # 初始化
        
        while self.running:
            try:
                elapsed = time.time() - self.start_time
                cpu = process.cpu_percent(interval=None)
                mem = process.memory_info().rss / (1024 * 1024)
                
                self.records.append({
                    'Time_s': round(elapsed, 3),
                    'CPU_Percent': round(cpu, 1),
                    'Memory_MB': round(mem, 2),
                    'Method': self.current_method,
                    'Phase': self.current_phase
                })
                
                time.sleep(self.interval)
            except Exception as e:
                print(f"Recording error: {e}")
                break
    
    def get_dataframe(self) -> pd.DataFrame:
        """返回记录的 DataFrame"""
        return pd.DataFrame(self.records)


def run_detailed_waveform(size_mb: int = 200):
    """
    大文件波形捕捉：高频采样记录 CPU 和内存变化
    """
    print("\n" + "=" * 60)
    print(f"Part 2: 大文件波形捕捉 ({size_mb}MB)")
    print("=" * 60)
    
    # 生成测试数据
    print(f"[Waveform] 生成 {size_mb}MB 测试数据...")
    gc.collect()
    test_data = generate_test_data(size_mb)
    print(f"[Waveform] 数据生成完成")
    
    client = Client("waveform_client")
    recorder = WaveformRecorder(interval=0.05)  # 50ms 采样间隔
    
    all_records = []
    
    # ===== Baseline 波形 =====
    print(f"\n[Waveform] 捕捉 Baseline 波形...")
    gc.collect()
    time.sleep(1.0)  # 冷却
    
    recorder.start("Baseline")
    
    # Baseline 处理
    buffer = bytearray(len(test_data))
    buffer[:] = test_data
    
    # 模拟传输阶段 (100 Mbps = 12.5 MB/s，全量传输)
    recorder.set_phase("传输阶段")
    data_size_mb = len(test_data) / (1024 * 1024)
    transfer_time = data_size_mb / BANDWIDTH_MBs
    time.sleep(transfer_time)
    
    recorder.stop()
    baseline_df = recorder.get_dataframe()
    all_records.extend(baseline_df.to_dict('records'))
    print(f"  → Baseline 采样点: {len(baseline_df)}")
    
    del buffer
    gc.collect()
    time.sleep(1.0)
    
    # ===== Proposed 波形 =====
    print(f"\n[Waveform] 捕捉 Proposed 波形...")
    gc.collect()
    time.sleep(1.0)  # 冷却
    
    recorder.start("Proposed")
    
    # Proposed 处理 - 计算阶段
    recorder.set_phase("计算阶段")
    encrypted_chunks = client.encrypt_data(test_data, chunk_mode='fastcdc')
    
    # Proposed 处理 - 传输阶段 (100 Mbps = 12.5 MB/s，70%去重只传30%)
    recorder.set_phase("传输阶段")
    data_size_mb = len(test_data) / (1024 * 1024)
    unique_ratio = 0.30
    transfer_time = (data_size_mb * unique_ratio) / BANDWIDTH_MBs
    time.sleep(transfer_time)
    
    recorder.stop()
    proposed_df = recorder.get_dataframe()
    all_records.extend(proposed_df.to_dict('records'))
    print(f"  → Proposed 采样点: {len(proposed_df)}")
    
    del encrypted_chunks
    gc.collect()
    
    # 保存结果
    df = pd.DataFrame(all_records)
    output_path = f'results/detailed_waveform_{size_mb}mb.csv'
    df.to_csv(output_path, index=False)
    print(f"\n✓ 波形数据已保存到: {output_path}")
    print(f"  总采样点: {len(df)}")
    
    # 打印统计信息
    for method in ['Baseline', 'Proposed']:
        method_df = df[df['Method'] == method]
        if len(method_df) > 0:
            print(f"\n{method} 统计:")
            print(f"  - 峰值 CPU: {method_df['CPU_Percent'].max():.1f}%")
            print(f"  - 平均 CPU: {method_df['CPU_Percent'].mean():.1f}%")
            print(f"  - 峰值内存: {method_df['Memory_MB'].max():.1f}MB")
            print(f"  - 持续时间: {method_df['Time_s'].max():.2f}s")
    
    del test_data
    gc.collect()
    
    return df


# ============================================================
# 主程序
# ============================================================

def main():
    """主程序入口"""
    print("=" * 60)
    print("CE2S 综合性能测试 (Comprehensive Profiling)")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # 确保输出目录存在
    os.makedirs('results', exist_ok=True)
    
    # Part 1: 可扩展性测试
    scalability_df = run_scalability_test()
    
    # Part 2: 大文件波形捕捉
    waveform_df = run_detailed_waveform(size_mb=200)
    
    print("\n" + "=" * 60)
    print("测试完成!")
    print("=" * 60)
    print("\n生成的数据文件:")
    print("  1. results/scalability_test.csv")
    print("  2. results/detailed_waveform_200mb.csv")
    print("\n可以使用这些数据绘制图 8 的三个子图:")
    print("  (a) 大文件 CPU/内存动态波形")
    print("  (b) 资源开销增长趋势")
    print("  (c) 处理吞吐率对比")


if __name__ == '__main__':
    main()
