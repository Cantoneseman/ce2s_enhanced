"""
云边端存储系统 - 主实验模块
整合 Client、EdgeProcessor、SmartScheduler、CloudNode 进行对比实验
"""

import os
import time
import random
import threading
from pathlib import Path
from typing import List, Dict, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm

# 导入系统模块
from client import Client
from edge_core import EdgeProcessor, ChunkStatus, RedundancyType
from scheduler import SmartScheduler
from cloud_mock import CLOUD_NODES, CloudNode


@dataclass
class ExperimentResult:
    """实验结果"""
    scenario: str                    # 场景名称
    total_time: float               # 总耗时（秒）
    avg_latency: float              # 平均延迟（秒）
    total_chunks: int               # 总块数
    uploaded_chunks: int            # 实际上传块数
    total_bytes: int                # 总数据量
    uploaded_bytes: int             # 实际上传数据量
    cloud_distribution: Dict[str, int]  # 各云节点上传分布


class CE2SExperiment:
    """
    云边端存储系统实验类
    
    实验流程：
    Client 加密 -> Edge FastCDC 分块 -> Edge 混合冗余 -> Scheduler 调度 -> Cloud 上传
    """
    
    def __init__(self, output_dir: str = "./experiment_output"):
        """初始化实验环境"""
        self.client = Client(client_id="exp_client")
        self.edge = EdgeProcessor(edge_id="exp_edge")
        self.scheduler = SmartScheduler(cloud_nodes=CLOUD_NODES)
        
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 实验统计
        self.results: List[ExperimentResult] = []
    
    def generate_test_file(self, size_mb: float = 5.0) -> bytes:
        """
        生成随机测试文件
        
        Args:
            size_mb: 文件大小（MB）
            
        Returns:
            随机二进制数据
        """
        size_bytes = int(size_mb * 1024 * 1024)
        print(f"📁 生成 {size_mb}MB 随机测试数据...")
        return os.urandom(size_bytes)
    
    def generate_modified_file(self, original: bytes, modify_ratio: float = 0.1) -> bytes:
        """
        生成修改后的文件（用于测试去重）
        
        Args:
            original: 原始数据
            modify_ratio: 修改比例
            
        Returns:
            修改后的数据
        """
        data = bytearray(original)
        modify_size = int(len(data) * modify_ratio)
        
        # 随机选择位置进行修改
        start_pos = random.randint(0, len(data) - modify_size)
        new_content = os.urandom(modify_size)
        data[start_pos:start_pos + modify_size] = new_content
        
        print(f"📝 修改了 {modify_ratio*100:.0f}% 的内容 ({modify_size} bytes)")
        return bytes(data)
    
    def run_pipeline(self, data: bytes, scenario: str, 
                     use_smart_scheduler: bool = True) -> ExperimentResult:
        """
        运行完整的数据处理和上传流程
        
        Args:
            data: 原始数据
            scenario: 场景名称
            use_smart_scheduler: 是否使用智能调度
            
        Returns:
            实验结果
        """
        print(f"\n{'='*60}")
        print(f"🚀 运行场景: {scenario}")
        print(f"{'='*60}")
        
        start_time = time.time()
        cloud_distribution = {node.cloud_id: 0 for node in CLOUD_NODES}
        total_latency = 0.0
        uploaded_chunks = 0
        uploaded_bytes = 0
        
        # Step 1: Client 加密
        print("\n[1/4] 🔐 客户端加密 (AES-GCM)...")
        encrypted_data, key, nonce = self.client.encrypt_data(data)
        print(f"      原始: {len(data)} bytes -> 密文: {len(encrypted_data)} bytes")
        
        # Step 2: Edge FastCDC 分块
        print("\n[2/4] 🔪 边缘节点 FastCDC 分块...")
        chunks = self.edge.process(encrypted_data)
        print(f"      总块数: {len(chunks)}")
        
        new_chunks = [c for c in chunks if c.status == ChunkStatus.NEW]
        ref_chunks = [c for c in chunks if c.status == ChunkStatus.REF]
        print(f"      新块: {len(new_chunks)}, 引用块: {len(ref_chunks)}")
        
        # Step 3 & 4: 冗余编码 + 云端上传
        print("\n[3/4] 🔄 混合冗余编码...")
        print("[4/4] ☁️  上传到云存储...")
        
        # 准备上传任务列表
        upload_tasks = []
        
        # 智能调度：预先获取 Top 3 节点列表，轮流分配任务以利用并发
        if use_smart_scheduler:
            best_nodes = self.scheduler.select_best_nodes(k=3)
            best_node_list = [node for node, score in best_nodes]
            print(f"      智能调度选择节点: {[n.cloud_id for n in best_node_list]}")
        
        task_idx = 0
        for chunk in new_chunks:
            # 选择云节点
            if use_smart_scheduler:
                # 智能调度：轮流使用 Top 3 节点，充分利用并发
                target_node = best_node_list[task_idx % len(best_node_list)]
            else:
                # 随机选择
                target_node = random.choice(CLOUD_NODES)
            
            # 为每个分片创建上传任务
            for shard_idx, shard in enumerate(chunk.shards):
                filename = f"{chunk.fingerprint[:16]}_{shard_idx}.shard"
                
                if use_smart_scheduler:
                    # 分片也轮流分配到不同节点
                    shard_target = best_node_list[task_idx % len(best_node_list)]
                    upload_tasks.append((shard_target, shard, filename))
                else:
                    upload_tasks.append((target_node, shard, filename))
                task_idx += 1
        
        # 线程安全的统计更新锁
        stats_lock = threading.Lock()
        
        def upload_shard(task):
            """单个分片上传任务"""
            target_node, shard, filename = task
            result = target_node.upload(shard, filename)
            return {
                'cloud_id': target_node.cloud_id,
                'latency': result['latency'],
                'size': len(shard)
            }
        
        # 使用 ThreadPoolExecutor 并发上传
        with ThreadPoolExecutor(max_workers=20) as executor:
            # 批量提交所有任务
            futures = {executor.submit(upload_shard, task): task for task in upload_tasks}
            
            # 使用 tqdm 显示进度条
            with tqdm(total=len(futures), desc="      上传进度", unit="shard") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    
                    # 线程安全地更新统计
                    with stats_lock:
                        total_latency += result['latency']
                        uploaded_chunks += 1
                        uploaded_bytes += result['size']
                        cloud_distribution[result['cloud_id']] += 1
                        
                        # 更新调度器统计（用于 EWMA）
                        if use_smart_scheduler:
                            self.scheduler.update_stats(result['cloud_id'], result['latency'])
                    
                    pbar.update(1)
        
        end_time = time.time()
        total_time = end_time - start_time
        avg_latency = total_latency / uploaded_chunks if uploaded_chunks > 0 else 0
        
        # 创建结果
        result = ExperimentResult(
            scenario=scenario,
            total_time=total_time,
            avg_latency=avg_latency,
            total_chunks=len(chunks),
            uploaded_chunks=uploaded_chunks,
            total_bytes=len(encrypted_data),
            uploaded_bytes=uploaded_bytes,
            cloud_distribution=cloud_distribution
        )
        
        self.results.append(result)
        return result
    
    def print_result(self, result: ExperimentResult) -> None:
        """打印单个实验结果"""
        print(f"\n📊 {result.scenario} 结果:")
        print(f"   总耗时: {result.total_time:.3f}s")
        print(f"   平均延迟: {result.avg_latency*1000:.2f}ms")
        print(f"   上传块数: {result.uploaded_chunks}")
        print(f"   上传数据量: {result.uploaded_bytes/1024:.2f}KB")
        print(f"   云节点分布: {result.cloud_distribution}")
    
    def compare_results(self, baseline: ExperimentResult, 
                        proposed: ExperimentResult) -> None:
        """对比两种场景的结果"""
        print("\n" + "="*60)
        print("📈 对比分析")
        print("="*60)
        
        # 耗时对比
        time_diff = baseline.total_time - proposed.total_time
        time_improve = (time_diff / baseline.total_time) * 100 if baseline.total_time > 0 else 0
        print(f"\n⏱️  总耗时:")
        print(f"   Baseline:  {baseline.total_time:.3f}s")
        print(f"   Proposed:  {proposed.total_time:.3f}s")
        print(f"   提升: {time_improve:+.2f}%")
        
        # 延迟对比
        latency_diff = baseline.avg_latency - proposed.avg_latency
        latency_improve = (latency_diff / baseline.avg_latency) * 100 if baseline.avg_latency > 0 else 0
        print(f"\n🌐 平均延迟:")
        print(f"   Baseline:  {baseline.avg_latency*1000:.2f}ms")
        print(f"   Proposed:  {proposed.avg_latency*1000:.2f}ms")
        print(f"   提升: {latency_improve:+.2f}%")
        
        # 云节点分布
        print(f"\n☁️  云节点分布:")
        print(f"   Baseline:  {baseline.cloud_distribution}")
        print(f"   Proposed:  {proposed.cloud_distribution}")
    
    def run_dedup_experiment(self, original_data: bytes) -> None:
        """
        运行去重实验：上传修改了 10% 内容的文件
        
        注意：去重测试在原始数据层面进行（不加密），
        因为加密使用随机 key/nonce 会导致相同数据产生不同密文。
        """
        print("\n" + "="*60)
        print("🔄 去重效果实验")
        print("="*60)
        
        # 使用新的边缘处理器进行去重测试
        dedup_edge = EdgeProcessor(edge_id="exp_edge_dedup")
        
        # Step 1: 先处理原始数据，建立指纹表
        print("\n📥 第一次上传：处理原始数据...")
        chunks_first = dedup_edge.process(original_data)
        first_stats = dedup_edge.get_stats()
        print(f"   总块数: {first_stats['total_chunks']}, 新块: {first_stats['new_chunks']}")
        
        # Step 2: 复制原始数据并只修改中间 10%
        print("\n📝 生成修改后的数据（只修改中间 10%）...")
        modified_data = bytearray(original_data)
        
        # 计算中间 10% 的位置
        data_len = len(modified_data)
        modify_size = int(data_len * 0.1)  # 10% 的数据量
        start_pos = (data_len - modify_size) // 2  # 从中间开始
        
        # 用随机数据覆盖中间部分
        new_content = os.urandom(modify_size)
        modified_data[start_pos:start_pos + modify_size] = new_content
        modified_data = bytes(modified_data)
        
        print(f"   原始数据大小: {data_len} bytes")
        print(f"   修改范围: 字节 {start_pos} ~ {start_pos + modify_size}")
        print(f"   修改大小: {modify_size} bytes ({modify_size/data_len*100:.1f}%)")
        
        # Step 3: 重置统计（但保留指纹表用于去重检测）
        dedup_edge.reset_stats()
        
        # Step 4: 处理修改后的数据
        print("\n📤 第二次上传：处理修改后的数据...")
        chunks_second = dedup_edge.process(modified_data)
        
        stats = dedup_edge.get_stats()
        
        print(f"\n📊 去重统计:")
        print(f"   总块数: {stats['total_chunks']}")
        print(f"   新块: {stats['new_chunks']}")
        print(f"   引用块（去重）: {stats['ref_chunks']}")
        print(f"   去重节省: {stats['bytes_saved']} bytes")
        
        # 计算去重率
        if stats['total_chunks'] > 0:
            dedup_ratio = stats['ref_chunks'] / stats['total_chunks'] * 100
        else:
            dedup_ratio = 0
        print(f"   去重率: {dedup_ratio:.2f}%")
        
        # 计算实际上传数据量减少
        new_upload_bytes = sum(c.size for c in chunks_second if c.status == ChunkStatus.NEW)
        total_bytes = sum(c.size for c in chunks_second)
        reduction = (1 - new_upload_bytes / total_bytes) * 100 if total_bytes > 0 else 0
        
        print(f"\n   原始需上传: {total_bytes} bytes")
        print(f"   实际需上传: {new_upload_bytes} bytes")
        print(f"   数据量减少: {reduction:.2f}%")


def main():
    """主函数：运行对比实验"""
    print("╔" + "═"*58 + "╗")
    print("║" + " CE2S: Cloud-Edge-End Storage System ".center(58) + "║")
    print("║" + " 云边端存储系统 - 对比实验 ".center(52) + "║")
    print("╚" + "═"*58 + "╝")
    
    # 初始化实验
    experiment = CE2SExperiment()
    
    # 生成 5MB 测试数据
    test_data = experiment.generate_test_file(size_mb=5.0)
    
    # ============================================
    # 场景 A: Baseline - 随机选择云节点
    # ============================================
    # 重置边缘处理器
    experiment.edge = EdgeProcessor(edge_id="exp_edge_baseline")
    baseline_result = experiment.run_pipeline(
        data=test_data,
        scenario="场景A: Baseline (随机选择)",
        use_smart_scheduler=False
    )
    experiment.print_result(baseline_result)
    
    # ============================================
    # 场景 B: Proposed - 智能调度
    # ============================================
    # 重置边缘处理器和调度器
    experiment.edge = EdgeProcessor(edge_id="exp_edge_proposed")
    experiment.scheduler = SmartScheduler(cloud_nodes=CLOUD_NODES)
    
    # 预热调度器：模拟一些历史数据
    print("\n🔥 预热调度器 (模拟历史延迟数据)...")
    for _ in range(10):
        for node in CLOUD_NODES:
            fake_latency = random.gauss(node.latency_mean, node.latency_std)
            experiment.scheduler.update_stats(node.cloud_id, max(0, fake_latency))
    
    proposed_result = experiment.run_pipeline(
        data=test_data,
        scenario="场景B: Proposed (智能调度)",
        use_smart_scheduler=True
    )
    experiment.print_result(proposed_result)
    
    # ============================================
    # 对比分析
    # ============================================
    experiment.compare_results(baseline_result, proposed_result)
    
    # ============================================
    # 去重实验
    # ============================================
    # 使用 Proposed 场景的边缘处理器（保留指纹表）
    experiment.run_dedup_experiment(test_data)
    
    print("\n" + "="*60)
    print("✅ 实验完成!")
    print("="*60)


if __name__ == "__main__":
    main()
