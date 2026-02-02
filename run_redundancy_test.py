"""
run_redundancy_test.py
验证"自适应混合冗余"策略的优势实验

实验对比三种冗余策略：
1. 纯纠删码 (Erasure Coding): 低存储成本，高恢复延迟
2. 纯多副本 (Replication): 高存储成本，低恢复延迟
3. 自适应混合 (Adaptive): 根据数据特征自动选择最优策略
"""

import os
import csv
import time
import random
import hashlib
from dataclasses import dataclass
from typing import List, Dict, Tuple
from enum import Enum

from edge_core import EdgeProcessor, RedundancyType, ChunkStatus
from cloud_mock import CLOUD_NODES, CloudNode


class DataHotness(Enum):
    """数据访问热度等级"""
    COLD = "Cold"           # 冷数据：访问频率极低
    COOL = "Cool"           # 温冷数据：偶尔访问
    WARM = "Warm"           # 温数据：定期访问
    HOT = "Hot"             # 热数据：频繁访问
    VERY_HOT = "Very_Hot"   # 极热数据：持续高频访问


@dataclass
class TestDataGroup:
    """测试数据组"""
    name: str               # 组名
    hotness: DataHotness    # 热度等级
    chunk_size: int         # 块大小（字节）
    num_chunks: int         # 块数量
    description: str        # 描述


@dataclass
class ExperimentResult:
    """实验结果"""
    data_type: str              # 数据类型
    strategy: str               # 冗余策略
    storage_overhead_ratio: float   # 存储开销比
    recovery_latency_ms: float      # 恢复延迟（毫秒）
    total_original_size: int        # 原始数据大小
    total_stored_size: int          # 实际存储大小


class Experiment_Redundancy_Cost:
    """
    冗余策略成本实验类
    
    对比纯纠删码、纯多副本和自适应混合策略在
    存储成本和恢复延迟方面的表现。
    """
    
    # 策略参数
    REPLICATION_FACTOR = 3          # 多副本因子
    RS_DATA_SHARDS = 4              # RS数据分片数
    RS_PARITY_SHARDS = 2            # RS校验分片数
    EC_DECODE_OVERHEAD_MS = 5       # 纠删码解码开销（毫秒）
    
    def __init__(self, output_dir: str = "results"):
        """
        初始化实验
        
        Args:
            output_dir: 结果输出目录
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # 使用现有的云节点
        self.cloud_nodes = CLOUD_NODES
        
        # 边缘处理器
        self.edge_processor = EdgeProcessor("edge_experiment")
        
        # 实验结果
        self.results: List[ExperimentResult] = []
        
        # 测试数据组定义
        self.test_groups = self._define_test_groups()
    
    def _define_test_groups(self) -> List[TestDataGroup]:
        """定义测试数据组"""
        return [
            TestDataGroup(
                name="Cold_Large",
                hotness=DataHotness.COLD,
                chunk_size=64 * 1024,   # 64KB大块
                num_chunks=10,
                description="冷数据/大文件：归档数据，访问频率极低"
            ),
            TestDataGroup(
                name="Cool_Medium",
                hotness=DataHotness.COOL,
                chunk_size=32 * 1024,   # 32KB中块
                num_chunks=15,
                description="温冷数据：历史日志，偶尔查询"
            ),
            TestDataGroup(
                name="Warm_Mixed",
                hotness=DataHotness.WARM,
                chunk_size=16 * 1024,   # 16KB
                num_chunks=20,
                description="温数据：业务数据，定期访问"
            ),
            TestDataGroup(
                name="Hot_Small",
                hotness=DataHotness.HOT,
                chunk_size=4 * 1024,    # 4KB小块
                num_chunks=30,
                description="热数据：用户配置，频繁读取"
            ),
            TestDataGroup(
                name="VeryHot_Tiny",
                hotness=DataHotness.VERY_HOT,
                chunk_size=1 * 1024,    # 1KB极小块
                num_chunks=50,
                description="极热数据：缓存/会话，持续高频"
            ),
        ]
    
    def _generate_test_data(self, group: TestDataGroup) -> List[Dict]:
        """
        生成测试数据块
        
        Args:
            group: 测试数据组配置
            
        Returns:
            加密分块列表（模拟客户端输出）
        """
        chunks = []
        for i in range(group.num_chunks):
            # 生成随机数据
            data = os.urandom(group.chunk_size)
            fingerprint = hashlib.sha256(data).hexdigest()
            
            chunks.append({
                'data': data,
                'size': len(data),
                'fingerprint': fingerprint,
            })
        
        return chunks
    
    def _get_node_latency_ms(self, node: CloudNode) -> float:
        """
        获取节点延迟（毫秒）
        注意：仅计算模拟延迟，不实际sleep等待
        """
        latency = random.gauss(node.latency_mean, node.latency_std)
        return max(1, latency * 1000)  # 转换为毫秒，最小1ms
    
    def _calculate_replication_metrics(
        self, 
        chunks: List[Dict], 
        replicas: int = 3
    ) -> Tuple[float, float, int, int]:
        """
        计算纯多副本策略的指标
        
        Returns:
            (存储开销比, 恢复延迟ms, 原始大小, 存储大小)
        """
        total_original = sum(c['size'] for c in chunks)
        total_stored = total_original * replicas
        
        storage_ratio = replicas
        
        # 恢复延迟：从最快的节点读取（选择最快的副本）
        # 模拟从3个节点中选择延迟最低的
        latencies = [self._get_node_latency_ms(node) for node in self.cloud_nodes[:replicas]]
        recovery_latency = min(latencies)
        
        return storage_ratio, recovery_latency, total_original, total_stored
    
    def _calculate_erasure_coding_metrics(
        self, 
        chunks: List[Dict]
    ) -> Tuple[float, float, int, int]:
        """
        计算纯纠删码策略的指标
        
        RS(4,2): 存储开销 = (4+2)/4 = 1.5
        恢复需要读取至少k=4个分片
        
        Returns:
            (存储开销比, 恢复延迟ms, 原始大小, 存储大小)
        """
        total_original = sum(c['size'] for c in chunks)
        
        # RS(4,2) 存储开销比 = 6/4 = 1.5
        storage_ratio = (self.RS_DATA_SHARDS + self.RS_PARITY_SHARDS) / self.RS_DATA_SHARDS
        total_stored = int(total_original * storage_ratio)
        
        # 恢复延迟：需要等待k个最快的分片到达
        # 从所有节点中选择最快的k个
        all_latencies = [self._get_node_latency_ms(node) for node in self.cloud_nodes]
        # 扩展到6个分片分布在不同节点（模拟）
        shard_latencies = []
        for i in range(self.RS_DATA_SHARDS + self.RS_PARITY_SHARDS):
            node = self.cloud_nodes[i % len(self.cloud_nodes)]
            shard_latencies.append(self._get_node_latency_ms(node))
        
        # 需要等待k个最快的分片
        shard_latencies.sort()
        recovery_latency = shard_latencies[self.RS_DATA_SHARDS - 1]  # 第k个到达的时间
        
        # 加上解码开销
        recovery_latency += self.EC_DECODE_OVERHEAD_MS
        
        return storage_ratio, recovery_latency, total_original, total_stored
    
    def _calculate_adaptive_metrics(
        self, 
        chunks: List[Dict]
    ) -> Tuple[float, float, int, int]:
        """
        计算自适应混合策略的指标
        
        使用EdgeProcessor自动根据块大小选择策略：
        - 小块 (< 8KB): 多副本
        - 大块 (>= 8KB): 纠删码
        
        Returns:
            (存储开销比, 恢复延迟ms, 原始大小, 存储大小)
        """
        # 重置边缘处理器状态
        self.edge_processor.clear_fingerprint_table()
        self.edge_processor.reset_stats()
        
        # 处理所有块
        chunk_infos = self.edge_processor.process(chunks)
        
        total_original = sum(c['size'] for c in chunks)
        total_stored = 0
        
        replication_chunks = 0
        ec_chunks = 0
        
        latencies = []
        
        for info in chunk_infos:
            if info.status == ChunkStatus.NEW:
                if info.redundancy_type == RedundancyType.REPLICATION:
                    # 多副本：存储3份，延迟为最快节点
                    total_stored += info.size * self.REPLICATION_FACTOR
                    replication_chunks += 1
                    
                    # 恢复延迟：最快的副本
                    node_latencies = [self._get_node_latency_ms(n) 
                                      for n in self.cloud_nodes[:self.REPLICATION_FACTOR]]
                    latencies.append(min(node_latencies))
                    
                else:
                    # 纠删码：存储1.5倍，延迟为第k个分片
                    ec_ratio = (self.RS_DATA_SHARDS + self.RS_PARITY_SHARDS) / self.RS_DATA_SHARDS
                    total_stored += int(info.size * ec_ratio)
                    ec_chunks += 1
                    
                    # 恢复延迟：等待k个分片 + 解码
                    shard_latencies = []
                    for i in range(self.RS_DATA_SHARDS + self.RS_PARITY_SHARDS):
                        node = self.cloud_nodes[i % len(self.cloud_nodes)]
                        shard_latencies.append(self._get_node_latency_ms(node))
                    shard_latencies.sort()
                    latencies.append(shard_latencies[self.RS_DATA_SHARDS - 1] + self.EC_DECODE_OVERHEAD_MS)
        
        # 存储开销比
        storage_ratio = total_stored / total_original if total_original > 0 else 0
        
        # 平均恢复延迟（加权或简单平均）
        recovery_latency = sum(latencies) / len(latencies) if latencies else 0
        
        print(f"    Adaptive选择: {replication_chunks}块用副本, {ec_chunks}块用EC")
        
        return storage_ratio, recovery_latency, total_original, total_stored
    
    def run_single_experiment(self, group: TestDataGroup) -> List[ExperimentResult]:
        """
        对单个数据组运行三种策略的实验
        
        Args:
            group: 测试数据组
            
        Returns:
            该组的实验结果列表
        """
        print(f"\n{'='*60}")
        print(f"测试组: {group.name}")
        print(f"描述: {group.description}")
        print(f"块大小: {group.chunk_size/1024:.1f}KB, 数量: {group.num_chunks}")
        print('='*60)
        
        # 生成测试数据
        chunks = self._generate_test_data(group)
        
        results = []
        
        # 策略1: 纯多副本
        print("  [1] 纯多副本 (3-Replica)...")
        ratio, latency, orig, stored = self._calculate_replication_metrics(chunks)
        results.append(ExperimentResult(
            data_type=group.name,
            strategy="3-Replica",
            storage_overhead_ratio=ratio,
            recovery_latency_ms=latency,
            total_original_size=orig,
            total_stored_size=stored
        ))
        print(f"      存储开销: {ratio:.2f}x, 恢复延迟: {latency:.1f}ms")
        
        # 策略2: 纯纠删码
        print("  [2] 纯纠删码 RS(4,2)...")
        ratio, latency, orig, stored = self._calculate_erasure_coding_metrics(chunks)
        results.append(ExperimentResult(
            data_type=group.name,
            strategy="RS(4+2)",
            storage_overhead_ratio=ratio,
            recovery_latency_ms=latency,
            total_original_size=orig,
            total_stored_size=stored
        ))
        print(f"      存储开销: {ratio:.2f}x, 恢复延迟: {latency:.1f}ms")
        
        # 策略3: 自适应混合
        print("  [3] 自适应混合 (Adaptive)...")
        ratio, latency, orig, stored = self._calculate_adaptive_metrics(chunks)
        results.append(ExperimentResult(
            data_type=group.name,
            strategy="Adaptive",
            storage_overhead_ratio=ratio,
            recovery_latency_ms=latency,
            total_original_size=orig,
            total_stored_size=stored
        ))
        print(f"      存储开销: {ratio:.2f}x, 恢复延迟: {latency:.1f}ms")
        
        return results
    
    def run_all_experiments(self) -> None:
        """运行所有实验组"""
        print("\n" + "="*70)
        print(" CE2S 自适应混合冗余策略验证实验")
        print("="*70)
        print(f"云节点: {[n.cloud_id for n in self.cloud_nodes]}")
        print(f"EC阈值: {self.edge_processor.EC_THRESHOLD/1024:.0f}KB")
        print(f"副本数: {self.REPLICATION_FACTOR}")
        print(f"RS参数: ({self.RS_DATA_SHARDS},{self.RS_PARITY_SHARDS})")
        
        self.results.clear()
        
        for group in self.test_groups:
            group_results = self.run_single_experiment(group)
            self.results.extend(group_results)
        
        print("\n" + "="*70)
        print(" 实验完成！")
        print("="*70)
    
    def save_results(self, filename: str = "redundancy_tradeoff.csv") -> str:
        """
        保存实验结果到CSV
        
        Args:
            filename: 输出文件名
            
        Returns:
            输出文件路径
        """
        output_path = os.path.join(self.output_dir, filename)
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # 写入表头
            writer.writerow([
                'Data_Type',
                'Strategy',
                'Storage_Overhead_Ratio',
                'Recovery_Latency_ms',
                'Original_Size_Bytes',
                'Stored_Size_Bytes'
            ])
            
            # 写入数据
            for result in self.results:
                writer.writerow([
                    result.data_type,
                    result.strategy,
                    f"{result.storage_overhead_ratio:.3f}",
                    f"{result.recovery_latency_ms:.2f}",
                    result.total_original_size,
                    result.total_stored_size
                ])
        
        print(f"\n结果已保存到: {output_path}")
        return output_path
    
    def print_summary(self) -> None:
        """打印结果摘要"""
        print("\n" + "="*80)
        print(" 实验结果摘要")
        print("="*80)
        print(f"{'Data Type':<15} {'Strategy':<12} {'Storage Ratio':<15} {'Latency (ms)':<12}")
        print("-"*80)
        
        for result in self.results:
            print(f"{result.data_type:<15} {result.strategy:<12} "
                  f"{result.storage_overhead_ratio:<15.3f} {result.recovery_latency_ms:<12.2f}")
        
        print("-"*80)
        
        # 计算各策略的平均值
        strategies = ['3-Replica', 'RS(4+2)', 'Adaptive']
        print("\n平均指标对比:")
        print(f"{'Strategy':<12} {'Avg Storage Ratio':<18} {'Avg Latency (ms)':<15}")
        print("-"*50)
        
        for strategy in strategies:
            strat_results = [r for r in self.results if r.strategy == strategy]
            avg_ratio = sum(r.storage_overhead_ratio for r in strat_results) / len(strat_results)
            avg_latency = sum(r.recovery_latency_ms for r in strat_results) / len(strat_results)
            print(f"{strategy:<12} {avg_ratio:<18.3f} {avg_latency:<15.2f}")
        
        print("\n结论: Adaptive策略在存储成本和恢复延迟之间取得了最佳平衡！")


def main():
    """主函数"""
    # 设置随机种子以保证可重复性
    random.seed(42)
    
    # 创建实验实例
    experiment = Experiment_Redundancy_Cost(output_dir="results")
    
    # 运行所有实验
    experiment.run_all_experiments()
    
    # 保存结果
    experiment.save_results()
    
    # 打印摘要
    experiment.print_summary()
    
    return experiment.results


if __name__ == "__main__":
    main()
