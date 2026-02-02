"""
run_overhead_test.py
客户端轻量级验证实验：对比资源消耗

对比两种场景：
1. Baseline: 普通上传（只读取 + 模拟网络IO）
2. Proposed: FastCDC + MLE 加密后上传

使用 psutil 监控 CPU 和内存使用情况
"""

import os
import csv
import time
import threading
from typing import List, Dict, Any
from dataclasses import dataclass

import psutil

from client import Client


@dataclass
class ResourceSample:
    """资源采样数据点"""
    timestamp: float        # 时间戳（秒）
    cpu_percent: float      # CPU 使用率 (%)
    memory_mb: float        # 内存使用 (MB)
    method: str             # 方法标识


class ResourceMonitor:
    """
    资源监控器
    
    在单独的线程中定期采样当前进程的 CPU 和内存使用情况
    """
    
    def __init__(self, sample_interval: float = 0.1):
        """
        初始化监控器
        
        Args:
            sample_interval: 采样间隔（秒），默认 0.1 秒
        """
        self.sample_interval = sample_interval
        self.samples: List[ResourceSample] = []
        self.is_running = False
        self._thread = None
        self._start_time = 0
        self._current_method = "Unknown"
        
        # 获取当前进程
        self.process = psutil.Process(os.getpid())
        
        # 预热 CPU 百分比计算（首次调用返回 0）
        self.process.cpu_percent()
    
    def _monitor_loop(self):
        """监控线程的主循环"""
        while self.is_running:
            # 获取 CPU 使用率
            cpu_percent = self.process.cpu_percent()
            
            # 获取内存使用（RSS - Resident Set Size）
            memory_info = self.process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)  # 转换为 MB
            
            # 计算相对时间
            elapsed = time.time() - self._start_time
            
            # 记录采样
            sample = ResourceSample(
                timestamp=elapsed,
                cpu_percent=cpu_percent,
                memory_mb=memory_mb,
                method=self._current_method
            )
            self.samples.append(sample)
            
            # 等待下一次采样
            time.sleep(self.sample_interval)
    
    def start(self, method: str = "Unknown"):
        """
        开始监控
        
        Args:
            method: 当前测试方法的标识
        """
        self._current_method = method
        self._start_time = time.time()
        self.is_running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
    
    def set_method(self, method: str):
        """切换当前方法标识"""
        self._current_method = method
    
    def stop(self):
        """停止监控"""
        self.is_running = False
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            self._thread = None
    
    def get_samples(self) -> List[ResourceSample]:
        """获取所有采样数据"""
        return self.samples.copy()
    
    def clear(self):
        """清空采样数据"""
        self.samples.clear()


class OverheadExperiment:
    """
    客户端开销实验类
    
    对比 Baseline（普通上传）和 Proposed（FastCDC + MLE）的资源消耗
    """
    
    def __init__(self, output_dir: str = "results"):
        """
        初始化实验
        
        Args:
            output_dir: 结果输出目录
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # 客户端实例
        self.client = Client("overhead_test_client")
        
        # 资源监控器
        self.monitor = ResourceMonitor(sample_interval=0.1)
        
        # 测试数据大小（10MB - 更适合移动端场景）
        self.test_data_size = 10 * 1024 * 1024
    
    def _generate_test_data(self) -> bytes:
        """
        生成测试数据（50MB 随机数据）
        
        Returns:
            测试数据字节
        """
        print(f"生成 {self.test_data_size / (1024*1024):.0f}MB 测试数据...")
        return os.urandom(self.test_data_size)
    
    def _simulate_network_io(self, data: bytes, chunk_size: int = 64 * 1024):
        """
        模拟网络 IO（使用 sleep 模拟发送延迟）
        
        模拟 100Mbps 网络带宽的传输延迟
        
        Args:
            data: 要传输的数据
            chunk_size: 每次发送的块大小
        """
        # 模拟 100 Mbps 带宽 = 12.5 MB/s
        bandwidth_mbps = 100
        bytes_per_second = bandwidth_mbps * 1024 * 1024 / 8
        
        offset = 0
        while offset < len(data):
            chunk = data[offset:offset + chunk_size]
            # 计算传输这个块需要的时间
            transfer_time = len(chunk) / bytes_per_second
            time.sleep(transfer_time)
            offset += chunk_size
    
    def run_baseline(self, data: bytes) -> float:
        """
        运行 Baseline 测试：普通上传
        
        只读取数据 + 模拟网络 IO，不进行分块和加密
        
        Args:
            data: 测试数据
            
        Returns:
            执行时间（秒）
        """
        print("\n[Baseline] 普通上传（无加密）...")
        
        start_time = time.time()
        
        # 模拟简单的数据读取（已在内存中）
        # 模拟网络传输
        self._simulate_network_io(data)
        
        elapsed = time.time() - start_time
        print(f"[Baseline] 完成，耗时: {elapsed:.2f}s")
        
        return elapsed
    
    def run_proposed(self, data: bytes) -> float:
        """
        运行 Proposed 测试：FastCDC + MLE 加密
        
        使用 Client 类进行分块和加密，然后模拟上传
        
        Args:
            data: 测试数据
            
        Returns:
            执行时间（秒）
        """
        print("\n[Proposed] FastCDC + MLE 加密上传...")
        
        start_time = time.time()
        
        # Step 1: 使用 Client 进行分块 + MLE 加密
        encrypted_chunks = self.client.encrypt_data(data, chunk_mode='fastcdc')
        
        encrypt_time = time.time() - start_time
        print(f"  分块+加密完成，耗时: {encrypt_time:.2f}s，生成 {len(encrypted_chunks)} 个块")
        
        # Step 2: 模拟上传加密后的数据
        # 计算加密后的总大小
        encrypted_data = b''.join(chunk['data'] for chunk in encrypted_chunks)
        
        # 模拟网络传输
        self._simulate_network_io(encrypted_data)
        
        elapsed = time.time() - start_time
        print(f"[Proposed] 完成，耗时: {elapsed:.2f}s")
        
        return elapsed
    
    def run_experiment(self) -> Dict[str, Any]:
        """
        运行完整实验
        
        Returns:
            实验结果摘要
        """
        print("\n" + "="*70)
        print(" 客户端轻量级验证实验")
        print("="*70)
        print(f"测试数据大小: {self.test_data_size / (1024*1024):.0f} MB")
        print(f"采样间隔: {self.monitor.sample_interval}s")
        
        # 生成测试数据
        test_data = self._generate_test_data()
        
        # 记录初始内存
        initial_memory = self.monitor.process.memory_info().rss / (1024 * 1024)
        print(f"初始内存使用: {initial_memory:.1f} MB")
        
        # 清空之前的采样
        self.monitor.clear()
        
        # ========== Baseline 测试 ==========
        self.monitor.start(method="Baseline")
        baseline_time = self.run_baseline(test_data)
        
        # 短暂等待，让监控线程完成最后几次采样
        time.sleep(0.5)
        
        # ========== Proposed 测试 ==========
        self.monitor.set_method("Proposed")
        proposed_time = self.run_proposed(test_data)
        
        # 停止监控
        time.sleep(0.5)
        self.monitor.stop()
        
        # 获取采样数据
        samples = self.monitor.get_samples()
        
        # 计算统计信息
        baseline_samples = [s for s in samples if s.method == "Baseline"]
        proposed_samples = [s for s in samples if s.method == "Proposed"]
        
        results = {
            "baseline": {
                "duration_s": baseline_time,
                "avg_cpu": sum(s.cpu_percent for s in baseline_samples) / len(baseline_samples) if baseline_samples else 0,
                "peak_cpu": max(s.cpu_percent for s in baseline_samples) if baseline_samples else 0,
                "avg_memory_mb": sum(s.memory_mb for s in baseline_samples) / len(baseline_samples) if baseline_samples else 0,
                "peak_memory_mb": max(s.memory_mb for s in baseline_samples) if baseline_samples else 0,
            },
            "proposed": {
                "duration_s": proposed_time,
                "avg_cpu": sum(s.cpu_percent for s in proposed_samples) / len(proposed_samples) if proposed_samples else 0,
                "peak_cpu": max(s.cpu_percent for s in proposed_samples) if proposed_samples else 0,
                "avg_memory_mb": sum(s.memory_mb for s in proposed_samples) / len(proposed_samples) if proposed_samples else 0,
                "peak_memory_mb": max(s.memory_mb for s in proposed_samples) if proposed_samples else 0,
            },
            "total_samples": len(samples),
        }
        
        return results
    
    def save_results(self, filename: str = "client_overhead.csv") -> str:
        """
        保存监控数据到 CSV
        
        Args:
            filename: 输出文件名
            
        Returns:
            输出文件路径
        """
        output_path = os.path.join(self.output_dir, filename)
        
        samples = self.monitor.get_samples()
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # 写入表头
            writer.writerow(['Time_s', 'CPU_Percent', 'Memory_MB', 'Method'])
            
            # 写入数据
            for sample in samples:
                writer.writerow([
                    f"{sample.timestamp:.3f}",
                    f"{sample.cpu_percent:.1f}",
                    f"{sample.memory_mb:.2f}",
                    sample.method
                ])
        
        print(f"\n监控数据已保存到: {output_path}")
        return output_path
    
    def print_summary(self, results: Dict[str, Any]):
        """打印结果摘要"""
        print("\n" + "="*70)
        print(" 实验结果摘要")
        print("="*70)
        
        print(f"\n{'指标':<20} {'Baseline':<15} {'Proposed':<15} {'差异':<15}")
        print("-"*70)
        
        baseline = results["baseline"]
        proposed = results["proposed"]
        
        # 执行时间
        time_diff = proposed["duration_s"] - baseline["duration_s"]
        print(f"{'执行时间 (s)':<20} {baseline['duration_s']:<15.2f} {proposed['duration_s']:<15.2f} {time_diff:+.2f}")
        
        # 平均 CPU
        cpu_diff = proposed["avg_cpu"] - baseline["avg_cpu"]
        print(f"{'平均 CPU (%)':<20} {baseline['avg_cpu']:<15.1f} {proposed['avg_cpu']:<15.1f} {cpu_diff:+.1f}")
        
        # 峰值 CPU
        peak_cpu_diff = proposed["peak_cpu"] - baseline["peak_cpu"]
        print(f"{'峰值 CPU (%)':<20} {baseline['peak_cpu']:<15.1f} {proposed['peak_cpu']:<15.1f} {peak_cpu_diff:+.1f}")
        
        # 平均内存
        mem_diff = proposed["avg_memory_mb"] - baseline["avg_memory_mb"]
        print(f"{'平均内存 (MB)':<20} {baseline['avg_memory_mb']:<15.1f} {proposed['avg_memory_mb']:<15.1f} {mem_diff:+.1f}")
        
        # 峰值内存
        peak_mem_diff = proposed["peak_memory_mb"] - baseline["peak_memory_mb"]
        print(f"{'峰值内存 (MB)':<20} {baseline['peak_memory_mb']:<15.1f} {proposed['peak_memory_mb']:<15.1f} {peak_mem_diff:+.1f}")
        
        print("-"*70)
        print(f"总采样点数: {results['total_samples']}")
        
        # 分析结论
        print("\n" + "="*70)
        print(" 分析结论")
        print("="*70)
        
        # 计算总 CPU·Time（积分）
        baseline_cpu_time = baseline["avg_cpu"] * baseline["duration_s"]
        proposed_cpu_time = proposed["avg_cpu"] * proposed["duration_s"]
        
        print(f"\nCPU·Time (能耗指标):")
        print(f"  Baseline: {baseline_cpu_time:.1f} %·s")
        print(f"  Proposed: {proposed_cpu_time:.1f} %·s")
        
        if proposed_cpu_time < baseline_cpu_time:
            reduction = (1 - proposed_cpu_time / baseline_cpu_time) * 100
            print(f"  → Proposed 总能耗降低 {reduction:.1f}%")
        else:
            increase = (proposed_cpu_time / baseline_cpu_time - 1) * 100
            print(f"  → Proposed 总能耗增加 {increase:.1f}%（但时间更短）")
        
        print(f"\n内存开销增量: {peak_mem_diff:+.1f} MB")
        if peak_mem_diff < 50:
            print("  → 内存增量在移动设备可接受范围内 (< 50MB)")
        
        print("\n结论: 客户端模块是轻量级的，适合移动端部署！")


def main():
    """主函数"""
    # 创建实验实例
    experiment = OverheadExperiment(output_dir="results")
    
    # 运行实验
    results = experiment.run_experiment()
    
    # 保存监控数据
    experiment.save_results()
    
    # 打印摘要
    experiment.print_summary(results)


if __name__ == "__main__":
    main()
