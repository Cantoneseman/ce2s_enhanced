"""
实验 3：网络抖动（Jitter）稳定性测试

评估不同调度算法在网络波动条件下的性能表现：
- Random: 随机选择云节点
- Greedy: 贪心选择基准延迟最低的节点（不考虑实时波动）
- Smart (Proposed): EWMA 预测 + QoS-Cost 感知调度

实验变量：Jitter Level (0, 50, 100, 200, 400 ms)
"""

import os
import sys
import time
import random
import csv
from pathlib import Path
from typing import List, Dict, Callable, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# 导入项目模块
from client import Client
from edge_core import EdgeProcessor, ChunkStatus
from cloud_mock import CloudNode, CLOUD_NODES
from scheduler import SmartScheduler


# ========== 调度策略定义 ==========

def strategy_random(nodes: List[CloudNode], scheduler: SmartScheduler = None) -> CloudNode:
    """
    随机调度策略
    
    随机选择一个云节点，不考虑任何性能指标。
    作为最简单的 baseline。
    """
    return random.choice(nodes)


def strategy_greedy(nodes: List[CloudNode], scheduler: SmartScheduler = None) -> CloudNode:
    """
    贪心调度策略
    
    选择基准延迟 (latency_mean) 最低的节点。
    不考虑实时网络波动，模拟简单的静态配置方案。
    """
    return min(nodes, key=lambda n: n.latency_mean)


def strategy_smart(nodes: List[CloudNode], scheduler: SmartScheduler) -> CloudNode:
    """
    智能调度策略 (Proposed)
    
    使用 SmartScheduler 基于 EWMA 预测和 QoS-Cost 多目标优化选择节点。
    能够适应网络波动，动态调整节点选择。
    """
    best_nodes = scheduler.select_best_nodes(k=1)
    return best_nodes[0][0]  # 返回最优节点


# ========== 云节点配置工具 ==========

def configure_jitter(nodes: List[CloudNode], jitter_ms: float) -> None:
    """
    配置所有云节点的抖动级别
    
    Args:
        nodes: 云节点列表
        jitter_ms: 抖动标准差（毫秒）
    """
    jitter_sec = jitter_ms / 1000.0  # 转换为秒
    for node in nodes:
        node.latency_std = jitter_sec


def reset_nodes_config(nodes: List[CloudNode], original_configs: List[Dict]) -> None:
    """
    恢复云节点的原始配置
    
    Args:
        nodes: 云节点列表
        original_configs: 原始配置列表
    """
    for node, config in zip(nodes, original_configs):
        node.latency_mean = config['latency_mean']
        node.latency_std = config['latency_std']


def save_original_configs(nodes: List[CloudNode]) -> List[Dict]:
    """保存云节点的原始配置"""
    return [
        {'latency_mean': n.latency_mean, 'latency_std': n.latency_std}
        for n in nodes
    ]


# ========== 上传模拟 ==========

def simulate_single_upload(
    node: CloudNode, 
    data: bytes, 
    chunk_id: int
) -> Tuple[str, float]:
    """
    模拟单个分片上传
    
    Returns:
        (cloud_id, actual_latency)
    """
    start = time.time()
    filename = f"jitter_test_{chunk_id}.shard"
    node.upload(data, filename)
    actual_latency = time.time() - start
    return node.cloud_id, actual_latency


def run_upload_test(
    chunks: List[bytes],
    nodes: List[CloudNode],
    strategy_fn: Callable,
    scheduler: SmartScheduler = None,
    warmup_rounds: int = 5,
    desc: str = "上传测试"
) -> Tuple[float, float]:
    """
    运行上传测试
    
    Args:
        chunks: 数据块列表
        nodes: 云节点列表
        strategy_fn: 调度策略函数
        scheduler: SmartScheduler 实例（仅 strategy_smart 需要）
        warmup_rounds: 调度器预热轮数
        desc: 进度条描述
        
    Returns:
        (total_time, avg_latency): 总耗时和平均延迟
    """
    # 预热调度器（仅对 Smart 策略有效）
    if scheduler is not None:
        for _ in range(warmup_rounds):
            for node in nodes:
                fake_latency = random.gauss(node.latency_mean, node.latency_std)
                scheduler.update_stats(node.cloud_id, max(0, fake_latency))
    
    total_latency = 0.0
    start_time = time.time()
    
    # 使用线程池并发上传
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        
        for i, chunk_data in enumerate(chunks):
            # 使用策略选择节点
            selected_node = strategy_fn(nodes, scheduler)
            
            # 提交上传任务
            future = executor.submit(
                simulate_single_upload, 
                selected_node, 
                chunk_data, 
                i
            )
            futures.append(future)
        
        # 收集结果
        for future in tqdm(as_completed(futures), total=len(futures), desc=desc):
            cloud_id, actual_latency = future.result()
            total_latency += actual_latency
            
            # 更新调度器统计（仅对 Smart 策略）
            if scheduler is not None:
                scheduler.update_stats(cloud_id, actual_latency)
    
    total_time = time.time() - start_time
    avg_latency = total_latency / len(chunks) if chunks else 0
    
    return total_time, avg_latency


# ========== 主实验流程 ==========

def run_jitter_experiment(
    data_size_mb: float = 5.0,
    jitter_levels: List[int] = None,
    iterations: int = 3,
    output_dir: str = "results"
) -> List[Dict]:
    """
    运行抖动稳定性实验
    
    Args:
        data_size_mb: 测试数据大小（MB）
        jitter_levels: 抖动级别列表（毫秒）
        iterations: 每个配置的迭代次数
        output_dir: 结果输出目录
        
    Returns:
        实验结果列表
    """
    if jitter_levels is None:
        jitter_levels = [0, 50, 100, 200, 400]
    
    print("=" * 70)
    print("  实验 3：网络抖动（Jitter）稳定性测试")
    print("=" * 70)
    print(f"数据大小: {data_size_mb} MB")
    print(f"抖动级别: {jitter_levels} ms")
    print(f"迭代次数: {iterations}")
    print("=" * 70)
    
    # 生成测试数据
    print("\n📦 生成测试数据...")
    data_size = int(data_size_mb * 1024 * 1024)
    test_data = os.urandom(data_size)
    print(f"   数据大小: {len(test_data):,} bytes")
    
    # 端侧分块 + MLE 加密
    print("\n🔐 端侧分块 + MLE 加密...")
    client = Client(client_id="jitter_test")
    encrypted_chunks = client.encrypt_data(test_data, chunk_mode='fastcdc')
    chunk_data_list = [c['data'] for c in encrypted_chunks]
    print(f"   分块数: {len(chunk_data_list)}")
    print(f"   平均块大小: {sum(len(c) for c in chunk_data_list) / len(chunk_data_list):.0f} bytes")
    
    # 保存原始配置
    original_configs = save_original_configs(CLOUD_NODES)
    
    # 策略定义
    strategies = [
        ("Random", strategy_random),
        ("Greedy", strategy_greedy),
        ("Smart (Proposed)", strategy_smart),
    ]
    
    results = []
    
    # 遍历抖动级别
    for jitter_ms in jitter_levels:
        print(f"\n{'='*70}")
        print(f"🌊 Jitter Level: {jitter_ms} ms")
        print(f"{'='*70}")
        
        # 配置抖动
        configure_jitter(CLOUD_NODES, jitter_ms)
        print(f"   已配置所有节点 latency_std = {jitter_ms} ms")
        
        # 显示当前节点配置
        for node in CLOUD_NODES:
            print(f"   - {node.cloud_id}: mean={node.latency_mean*1000:.0f}ms, "
                  f"std={node.latency_std*1000:.0f}ms")
        
        # 测试每种策略
        for strategy_name, strategy_fn in strategies:
            print(f"\n   📊 测试策略: {strategy_name}")
            
            time_sum = 0.0
            latency_sum = 0.0
            
            for iter_idx in range(iterations):
                # 创建新的调度器实例
                scheduler = SmartScheduler(cloud_nodes=CLOUD_NODES) if strategy_name == "Smart (Proposed)" else None
                
                # 运行上传测试
                total_time, avg_latency = run_upload_test(
                    chunks=chunk_data_list,
                    nodes=CLOUD_NODES,
                    strategy_fn=strategy_fn,
                    scheduler=scheduler,
                    desc=f"      Iter {iter_idx+1}/{iterations}"
                )
                
                time_sum += total_time
                latency_sum += avg_latency
                
                print(f"      迭代 {iter_idx+1}: 总耗时={total_time:.3f}s, 平均延迟={avg_latency*1000:.1f}ms")
            
            # 计算平均值
            avg_time = time_sum / iterations
            avg_lat = latency_sum / iterations
            
            print(f"   ✅ {strategy_name}: 平均总耗时={avg_time:.3f}s, 平均延迟={avg_lat*1000:.1f}ms")
            
            # 记录结果
            results.append({
                "Jitter(ms)": jitter_ms,
                "Method": strategy_name,
                "AvgTime(s)": round(avg_time, 3),
                "AvgLatency(ms)": round(avg_lat * 1000, 2)
            })
    
    # 恢复原始配置
    reset_nodes_config(CLOUD_NODES, original_configs)
    
    # 保存结果
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    output_file = Path(output_dir) / "jitter_test_results.csv"
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["Jitter(ms)", "Method", "AvgTime(s)", "AvgLatency(ms)"])
        writer.writeheader()
        writer.writerows(results)
    
    print(f"\n{'='*70}")
    print(f"✅ 实验完成！结果已保存至: {output_file}")
    print(f"{'='*70}")
    
    # 打印结果总结
    print("\n📈 结果总结:")
    print(f"{'Jitter(ms)':<12} {'Method':<20} {'AvgTime(s)':<12} {'AvgLatency(ms)':<15}")
    print("-" * 60)
    for r in results:
        print(f"{r['Jitter(ms)']:<12} {r['Method']:<20} {r['AvgTime(s)']:<12.3f} {r['AvgLatency(ms)']:<15.2f}")
    
    # 计算 Smart 相对于其他策略的提升
    print("\n🎯 Smart (Proposed) 性能对比:")
    for jitter_ms in jitter_levels:
        jitter_results = [r for r in results if r['Jitter(ms)'] == jitter_ms]
        smart_result = next(r for r in jitter_results if r['Method'] == 'Smart (Proposed)')
        
        for r in jitter_results:
            if r['Method'] != 'Smart (Proposed)':
                improvement = (r['AvgTime(s)'] - smart_result['AvgTime(s)']) / r['AvgTime(s)'] * 100
                print(f"   Jitter={jitter_ms}ms: vs {r['Method']}: {improvement:+.1f}%")
    
    return results


if __name__ == "__main__":
    # 运行实验
    results = run_jitter_experiment(
        data_size_mb=5.0,
        jitter_levels=[0, 50, 100, 200, 400],
        iterations=3,
        output_dir="results"
    )
