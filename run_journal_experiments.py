#!/usr/bin/env python3
"""
CE2S Enhanced - 期刊论文综合实验脚本
=====================================
实验 A: 真实去重率测试 (对应图 5)
实验 B: 资源可扩展性测试 (对应图 8)
"""

import gc
import os
import sys
import csv
import time
import psutil
import threading
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Any, Tuple
from collections import defaultdict

# 导入项目模块
from client import Client, FASTCDC_AVAILABLE

# 配置路径
BASE_DIR = Path(__file__).parent
TEST_DATA_DIR = BASE_DIR / "test_data"
RESULTS_DIR = BASE_DIR / "results"

# Linux Kernel 数据集路径
LINUX_KERNEL_DIR = TEST_DATA_DIR / "linux_kernel" / "linux-5.10.1"

# 合成数据集路径
SYNTHETIC_DIR = TEST_DATA_DIR / "synthetic"
SYNTHETIC_FILES = ["100MB.bin", "200MB.bin", "500MB.bin", "1GB.bin"]

# 固定分块大小 (用于 Fixed-Size Chunking 对比)
FIXED_CHUNK_SIZE = 8192  # 8 KB


@dataclass
class ResourceMonitor:
    """资源监控器：监控 CPU 和内存使用"""
    
    interval: float = 0.1  # 采样间隔（秒）
    
    # 监控数据
    cpu_samples: List[float] = field(default_factory=list)
    memory_samples: List[float] = field(default_factory=list)
    peak_memory: float = 0.0
    
    # 控制标志
    _running: bool = False
    _thread: threading.Thread = None
    
    def _monitor_loop(self):
        """监控循环"""
        process = psutil.Process()
        
        while self._running:
            try:
                # CPU 使用率
                cpu_percent = process.cpu_percent(interval=None)
                self.cpu_samples.append(cpu_percent)
                
                # 内存使用（MB）
                memory_info = process.memory_info()
                memory_mb = memory_info.rss / (1024 * 1024)
                self.memory_samples.append(memory_mb)
                
                # 更新峰值
                if memory_mb > self.peak_memory:
                    self.peak_memory = memory_mb
                
                time.sleep(self.interval)
            except Exception:
                break
    
    def start(self):
        """开始监控"""
        self.cpu_samples = []
        self.memory_samples = []
        self.peak_memory = 0.0
        self._running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
    
    def stop(self) -> Dict[str, float]:
        """停止监控并返回统计结果"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=1.0)
        
        return {
            'peak_memory_mb': self.peak_memory,
            'avg_cpu_percent': sum(self.cpu_samples) / len(self.cpu_samples) if self.cpu_samples else 0,
            'avg_memory_mb': sum(self.memory_samples) / len(self.memory_samples) if self.memory_samples else 0,
            'sample_count': len(self.cpu_samples)
        }


def format_size(size: int) -> str:
    """格式化文件大小"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} TB"


def collect_files(directory: Path, extensions: List[str] = None) -> List[Path]:
    """
    收集目录下的所有文件
    
    Args:
        directory: 目录路径
        extensions: 文件扩展名过滤（可选）
    
    Returns:
        文件路径列表
    """
    files = []
    for file_path in directory.rglob('*'):
        if file_path.is_file():
            if extensions is None or file_path.suffix.lower() in extensions:
                files.append(file_path)
    return sorted(files)


# =============================================================================
# 实验 A: 真实去重率测试
# =============================================================================

def fixed_size_chunking(data: bytes, chunk_size: int = FIXED_CHUNK_SIZE) -> List[bytes]:
    """
    固定大小分块
    
    Args:
        data: 数据
        chunk_size: 块大小
    
    Returns:
        分块列表
    """
    chunks = []
    offset = 0
    while offset < len(data):
        chunk = data[offset:offset + chunk_size]
        chunks.append(chunk)
        offset += chunk_size
    return chunks


def calculate_dedup_ratio(chunks: List[bytes]) -> Tuple[float, int, int]:
    """
    计算去重率
    
    Args:
        chunks: 分块列表
    
    Returns:
        (去重率, 唯一块数, 总块数)
    """
    import hashlib
    
    fingerprints = set()
    total_chunks = len(chunks)
    
    for chunk in chunks:
        fp = hashlib.sha256(chunk).hexdigest()
        fingerprints.add(fp)
    
    unique_chunks = len(fingerprints)
    
    if total_chunks == 0:
        return 0.0, 0, 0
    
    # 去重率 = 1 - (唯一块数 / 总块数)
    dedup_ratio = 1.0 - (unique_chunks / total_chunks)
    
    return dedup_ratio, unique_chunks, total_chunks


def run_experiment_a():
    """
    实验 A: 真实去重率测试
    
    对比 Fixed-Size Chunking 和 FastCDC (Proposed) 的去重率
    """
    print("\n" + "=" * 70)
    print("实验 A: 真实去重率测试 (Linux Kernel 数据集)")
    print("=" * 70)
    
    # 检查数据集
    if not LINUX_KERNEL_DIR.exists():
        print(f"✗ 数据集不存在: {LINUX_KERNEL_DIR}")
        print("  请先运行 prepare_datasets.py 准备数据")
        return False
    
    # 收集文件
    print("\n[1/4] 收集文件...")
    files = collect_files(LINUX_KERNEL_DIR)
    print(f"  找到 {len(files):,} 个文件")
    
    if len(files) == 0:
        print("✗ 未找到任何文件")
        return False
    
    # 读取所有文件内容
    print("\n[2/4] 读取文件内容...")
    all_data = []
    total_size = 0
    
    for i, file_path in enumerate(files):
        try:
            with open(file_path, 'rb') as f:
                data = f.read()
                all_data.append(data)
                total_size += len(data)
        except Exception as e:
            pass  # 跳过无法读取的文件
        
        if (i + 1) % 5000 == 0:
            print(f"  已处理 {i + 1:,}/{len(files):,} 个文件...")
    
    print(f"  总数据量: {format_size(total_size)}")
    
    # 合并所有数据
    combined_data = b''.join(all_data)
    del all_data
    gc.collect()
    
    # Fixed-Size Chunking
    print("\n[3/4] Fixed-Size Chunking 分块...")
    start_time = time.time()
    fixed_chunks = fixed_size_chunking(combined_data, FIXED_CHUNK_SIZE)
    fixed_time = time.time() - start_time
    
    fixed_dedup_ratio, fixed_unique, fixed_total = calculate_dedup_ratio(fixed_chunks)
    print(f"  分块数: {fixed_total:,}")
    print(f"  唯一块数: {fixed_unique:,}")
    print(f"  去重率: {fixed_dedup_ratio * 100:.2f}%")
    print(f"  耗时: {fixed_time:.2f}s")
    
    del fixed_chunks
    gc.collect()
    
    # FastCDC (Proposed)
    print("\n[4/4] FastCDC (Proposed) 分块...")
    client = Client()
    
    start_time = time.time()
    fastcdc_chunks = client._chunk_data(combined_data, mode='fastcdc')
    fastcdc_time = time.time() - start_time
    
    fastcdc_dedup_ratio, fastcdc_unique, fastcdc_total = calculate_dedup_ratio(fastcdc_chunks)
    print(f"  分块数: {fastcdc_total:,}")
    print(f"  唯一块数: {fastcdc_unique:,}")
    print(f"  去重率: {fastcdc_dedup_ratio * 100:.2f}%")
    print(f"  耗时: {fastcdc_time:.2f}s")
    
    del fastcdc_chunks
    del combined_data
    gc.collect()
    
    # 保存结果
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    result_file = RESULTS_DIR / "real_world_dedup.csv"
    
    with open(result_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Method', 'Total_Files', 'Total_Size_Bytes', 'Total_Size_Human',
            'Total_Chunks', 'Unique_Chunks', 'Dedup_Ratio', 'Time_Seconds'
        ])
        writer.writerow([
            'Fixed-Size', len(files), total_size, format_size(total_size),
            fixed_total, fixed_unique, f"{fixed_dedup_ratio * 100:.2f}%", f"{fixed_time:.2f}"
        ])
        writer.writerow([
            'FastCDC (Proposed)', len(files), total_size, format_size(total_size),
            fastcdc_total, fastcdc_unique, f"{fastcdc_dedup_ratio * 100:.2f}%", f"{fastcdc_time:.2f}"
        ])
    
    print(f"\n✓ 结果已保存到: {result_file}")
    
    # 打印对比摘要
    print("\n" + "-" * 50)
    print("去重率对比摘要:")
    print("-" * 50)
    print(f"{'方法':<25} {'去重率':>15} {'分块数':>15}")
    print("-" * 50)
    print(f"{'Fixed-Size Chunking':<25} {fixed_dedup_ratio * 100:>14.2f}% {fixed_total:>15,}")
    print(f"{'FastCDC (Proposed)':<25} {fastcdc_dedup_ratio * 100:>14.2f}% {fastcdc_total:>15,}")
    print("-" * 50)
    
    improvement = fastcdc_dedup_ratio - fixed_dedup_ratio
    print(f"\nFastCDC 去重率提升: {improvement * 100:+.2f}%")
    
    return True


# =============================================================================
# 实验 B: 资源可扩展性测试
# =============================================================================

def run_baseline_processing(file_path: Path) -> Tuple[float, Dict[str, float]]:
    """
    Baseline 处理：仅读取 + 模拟发送
    
    Args:
        file_path: 文件路径
    
    Returns:
        (耗时, 资源统计)
    """
    monitor = ResourceMonitor(interval=0.05)
    monitor.start()
    
    start_time = time.time()
    
    # 读取文件
    with open(file_path, 'rb') as f:
        data = f.read()
    
    # 模拟发送（遍历数据）
    chunk_size = 64 * 1024  # 64 KB
    offset = 0
    while offset < len(data):
        chunk = data[offset:offset + chunk_size]
        # 模拟网络传输开销
        _ = len(chunk)
        offset += chunk_size
    
    elapsed = time.time() - start_time
    stats = monitor.stop()
    
    del data
    gc.collect()
    
    return elapsed, stats


def run_proposed_processing(file_path: Path, client: Client) -> Tuple[float, Dict[str, float], Dict[str, Any]]:
    """
    Proposed 处理：FastCDC + MLE + 模拟发送
    
    Args:
        file_path: 文件路径
        client: Client 实例
    
    Returns:
        (耗时, 资源统计, 处理统计)
    """
    monitor = ResourceMonitor(interval=0.05)
    monitor.start()
    
    start_time = time.time()
    
    # 读取文件
    with open(file_path, 'rb') as f:
        data = f.read()
    
    # FastCDC 分块 + MLE 加密
    encrypted_chunks = client.encrypt_data(data, chunk_mode='fastcdc')
    
    # 模拟发送（遍历加密块）
    total_encrypted_size = 0
    for chunk in encrypted_chunks:
        total_encrypted_size += len(chunk['data'])
        # 模拟网络传输开销
        _ = chunk['fingerprint']
    
    elapsed = time.time() - start_time
    stats = monitor.stop()
    
    # 收集处理统计
    processing_stats = {
        'num_chunks': len(encrypted_chunks),
        'original_size': len(data),
        'encrypted_size': total_encrypted_size
    }
    
    del data
    del encrypted_chunks
    gc.collect()
    
    return elapsed, stats, processing_stats


def run_experiment_b():
    """
    实验 B: 资源可扩展性测试
    
    测试不同文件规模下的峰值内存、CPU 和耗时
    """
    print("\n" + "=" * 70)
    print("实验 B: 资源可扩展性测试 (合成数据集)")
    print("=" * 70)
    
    # 检查数据集
    if not SYNTHETIC_DIR.exists():
        print(f"✗ 数据集目录不存在: {SYNTHETIC_DIR}")
        print("  请先运行 prepare_datasets.py 准备数据")
        return False
    
    # 检查文件
    missing_files = []
    for filename in SYNTHETIC_FILES:
        file_path = SYNTHETIC_DIR / filename
        if not file_path.exists():
            missing_files.append(filename)
    
    if missing_files:
        print(f"✗ 缺少文件: {', '.join(missing_files)}")
        print("  请先运行 prepare_datasets.py 准备数据")
        return False
    
    # 初始化
    client = Client()
    results = []
    
    print(f"\n将测试 {len(SYNTHETIC_FILES)} 个文件...")
    print(f"FastCDC 可用: {FASTCDC_AVAILABLE}")
    
    for i, filename in enumerate(SYNTHETIC_FILES, 1):
        file_path = SYNTHETIC_DIR / filename
        file_size = file_path.stat().st_size
        
        print(f"\n[{i}/{len(SYNTHETIC_FILES)}] 测试 {filename} ({format_size(file_size)})")
        print("-" * 50)
        
        # 预热 GC
        gc.collect()
        time.sleep(0.5)
        
        # Baseline 测试
        print("  Baseline (读取+发送)...")
        baseline_time, baseline_stats = run_baseline_processing(file_path)
        print(f"    耗时: {baseline_time:.2f}s")
        print(f"    峰值内存: {baseline_stats['peak_memory_mb']:.1f} MB")
        print(f"    平均 CPU: {baseline_stats['avg_cpu_percent']:.1f}%")
        
        # 清理内存
        gc.collect()
        time.sleep(0.5)
        
        # Proposed 测试
        print("  Proposed (FastCDC+MLE+发送)...")
        proposed_time, proposed_stats, proc_stats = run_proposed_processing(file_path, client)
        print(f"    耗时: {proposed_time:.2f}s")
        print(f"    峰值内存: {proposed_stats['peak_memory_mb']:.1f} MB")
        print(f"    平均 CPU: {proposed_stats['avg_cpu_percent']:.1f}%")
        print(f"    分块数: {proc_stats['num_chunks']:,}")
        
        # 记录结果
        results.append({
            'file_name': filename,
            'file_size_bytes': file_size,
            'file_size_human': format_size(file_size),
            # Baseline
            'baseline_time_s': baseline_time,
            'baseline_peak_memory_mb': baseline_stats['peak_memory_mb'],
            'baseline_avg_cpu': baseline_stats['avg_cpu_percent'],
            # Proposed
            'proposed_time_s': proposed_time,
            'proposed_peak_memory_mb': proposed_stats['peak_memory_mb'],
            'proposed_avg_cpu': proposed_stats['avg_cpu_percent'],
            'proposed_num_chunks': proc_stats['num_chunks'],
            # 比较
            'time_overhead': proposed_time - baseline_time,
            'memory_overhead': proposed_stats['peak_memory_mb'] - baseline_stats['peak_memory_mb']
        })
        
        # 强制 GC
        gc.collect()
        time.sleep(1)
    
    # 保存结果
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    result_file = RESULTS_DIR / "scalability_new.csv"
    
    with open(result_file, 'w', newline='', encoding='utf-8') as f:
        if results:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
    
    print(f"\n✓ 结果已保存到: {result_file}")
    
    # 打印摘要表格
    print("\n" + "-" * 90)
    print("可扩展性测试摘要:")
    print("-" * 90)
    print(f"{'文件':<12} {'大小':>10} {'Baseline':>20} {'Proposed':>20} {'内存开销':>15}")
    print(f"{'':12} {'':>10} {'时间(s) / 内存(MB)':>20} {'时间(s) / 内存(MB)':>20} {'(MB)':>15}")
    print("-" * 90)
    
    for r in results:
        print(f"{r['file_name']:<12} "
              f"{r['file_size_human']:>10} "
              f"{r['baseline_time_s']:>8.2f} / {r['baseline_peak_memory_mb']:>8.1f} "
              f"{r['proposed_time_s']:>8.2f} / {r['proposed_peak_memory_mb']:>8.1f} "
              f"{r['memory_overhead']:>+14.1f}")
    
    print("-" * 90)
    
    return True


# =============================================================================
# 主函数
# =============================================================================

def main():
    """主函数"""
    print("\n" + "╔" + "═" * 68 + "╗")
    print("║" + " CE2S Enhanced - 期刊论文综合实验 ".center(68) + "║")
    print("╚" + "═" * 68 + "╝")
    
    print(f"\n配置信息:")
    print(f"  FastCDC 可用: {FASTCDC_AVAILABLE}")
    print(f"  数据目录: {TEST_DATA_DIR}")
    print(f"  结果目录: {RESULTS_DIR}")
    
    success_a = False
    success_b = False
    
    # 实验 A
    try:
        success_a = run_experiment_a()
    except Exception as e:
        print(f"\n✗ 实验 A 出错: {e}")
        import traceback
        traceback.print_exc()
    
    # 清理内存
    gc.collect()
    time.sleep(2)
    
    # 实验 B
    try:
        success_b = run_experiment_b()
    except Exception as e:
        print(f"\n✗ 实验 B 出错: {e}")
        import traceback
        traceback.print_exc()
    
    # 总结
    print("\n" + "=" * 70)
    print("实验完成总结")
    print("=" * 70)
    print(f"  实验 A (真实去重率): {'✓ 完成' if success_a else '✗ 失败'}")
    print(f"  实验 B (资源可扩展性): {'✓ 完成' if success_b else '✗ 失败'}")
    
    if success_a:
        print(f"\n  结果文件:")
        print(f"    - {RESULTS_DIR / 'real_world_dedup.csv'}")
    if success_b:
        print(f"    - {RESULTS_DIR / 'scalability_new.csv'}")
    
    print("\n" + "=" * 70)
    
    if not (success_a and success_b):
        sys.exit(1)


if __name__ == "__main__":
    main()
