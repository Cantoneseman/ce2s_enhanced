"""
智能调度器模块
实现 QoS-Cost Aware 的云节点选择策略
"""

from typing import List, Dict, Optional, Tuple
from cloud_mock import CloudNode, CLOUD_NODES


class SmartScheduler:
    """
    智能调度器：基于 EWMA 延迟预测和成本感知的多目标优化调度
    
    核心创新点：
    - 使用指数加权移动平均 (EWMA) 预测网络延迟
    - 多目标打分函数平衡 QoS（延迟）和成本
    """
    
    def __init__(self, cloud_nodes: List[CloudNode] = None, alpha: float = 0.7,
                 latency_weight: float = 0.8, cost_weight: float = 0.2):
        """
        初始化调度器
        
        Args:
            cloud_nodes: 可用的云节点列表，默认使用 CLOUD_NODES
            alpha: EWMA 平滑因子，值越大越重视最近的观测值
            latency_weight: 延迟在评分中的权重（默认0.8，优先保证低延迟）
            cost_weight: 成本在评分中的权重（默认0.2）
        """
        self.cloud_nodes = cloud_nodes or CLOUD_NODES
        self.alpha = alpha
        self.latency_weight = latency_weight
        self.cost_weight = cost_weight
        
        # 历史延迟记录：{cloud_id: [latency1, latency2, ...]}
        self.latency_history: Dict[str, List[float]] = {
            node.cloud_id: [] for node in self.cloud_nodes
        }
        
        # EWMA 预测值缓存：{cloud_id: ewma_value}
        self.ewma_cache: Dict[str, Optional[float]] = {
            node.cloud_id: None for node in self.cloud_nodes
        }
        
        # 云节点映射：{cloud_id: CloudNode}
        self.node_map: Dict[str, CloudNode] = {
            node.cloud_id: node for node in self.cloud_nodes
        }
    
    def update_stats(self, cloud_id: str, actual_latency: float) -> None:
        """
        更新云节点的历史延迟数据并刷新 EWMA 缓存
        
        Args:
            cloud_id: 云节点标识
            actual_latency: 实际观测到的延迟（秒）
        """
        if cloud_id not in self.latency_history:
            raise ValueError(f"Unknown cloud_id: {cloud_id}")
        
        # 记录历史延迟
        self.latency_history[cloud_id].append(actual_latency)
        
        # 更新 EWMA 缓存
        # 公式：EWMA_t = α × X_t + (1 - α) × EWMA_{t-1}
        prev_ewma = self.ewma_cache[cloud_id]
        if prev_ewma is None:
            # 首次观测，直接使用当前值
            self.ewma_cache[cloud_id] = actual_latency
        else:
            self.ewma_cache[cloud_id] = (
                self.alpha * actual_latency + (1 - self.alpha) * prev_ewma
            )
    
    def predict_latency(self, cloud_id: str) -> float:
        """
        使用 EWMA 预测云节点的下一次延迟
        
        EWMA 公式：$EWMA_t = \\alpha \\times X_t + (1 - \\alpha) \\times EWMA_{t-1}$
        
        Args:
            cloud_id: 云节点标识
            
        Returns:
            预测的延迟值（秒）
        """
        if cloud_id not in self.node_map:
            raise ValueError(f"Unknown cloud_id: {cloud_id}")
        
        # 如果有 EWMA 缓存，直接返回
        if self.ewma_cache[cloud_id] is not None:
            return self.ewma_cache[cloud_id]
        
        # 如果没有历史数据，使用节点的配置均值作为初始预测
        return self.node_map[cloud_id].latency_mean
    
    def _normalize_values(self, values: List[float]) -> List[float]:
        """
        Min-Max 归一化，用于多目标评分
        
        Args:
            values: 原始值列表
            
        Returns:
            归一化后的值列表 [0, 1]
        """
        min_val = min(values)
        max_val = max(values)
        
        if max_val == min_val:
            return [0.5] * len(values)  # 所有值相同时返回中间值
        
        return [(v - min_val) / (max_val - min_val) for v in values]
    
    def calculate_score(self, cloud_id: str, normalize: bool = False,
                        all_latencies: List[float] = None, 
                        all_costs: List[float] = None) -> float:
        """
        计算云节点的综合评分
        
        评分公式：$Score = 0.6 \\times PredictedLatency + 0.4 \\times Cost$
        
        Args:
            cloud_id: 云节点标识
            normalize: 是否使用归一化值（用于跨节点比较）
            all_latencies: 所有节点的预测延迟（归一化时需要）
            all_costs: 所有节点的成本（归一化时需要）
            
        Returns:
            综合评分，分数越低越好
        """
        predicted_latency = self.predict_latency(cloud_id)
        cost = self.node_map[cloud_id].cost_per_gb
        
        if normalize and all_latencies and all_costs:
            # 使用归一化值
            idx = [n.cloud_id for n in self.cloud_nodes].index(cloud_id)
            norm_latencies = self._normalize_values(all_latencies)
            norm_costs = self._normalize_values(all_costs)
            return (self.latency_weight * norm_latencies[idx] + 
                    self.cost_weight * norm_costs[idx])
        
        # 原始值加权求和
        return self.latency_weight * predicted_latency + self.cost_weight * cost
    
    def select_best_nodes(self, k: int = 1, normalize: bool = True) -> List[Tuple[CloudNode, float]]:
        """
        选择最优的 k 个云节点
        
        评分公式：$Score = 0.6 \\times PredictedLatency + 0.4 \\times Cost$
        
        Args:
            k: 返回的节点数量
            normalize: 是否对延迟和成本进行归一化（推荐开启以平衡量纲）
            
        Returns:
            按评分排序的 (CloudNode, score) 元组列表，分数越低越好
        """
        # 获取所有节点的预测延迟和成本
        all_latencies = [self.predict_latency(n.cloud_id) for n in self.cloud_nodes]
        all_costs = [n.cost_per_gb for n in self.cloud_nodes]
        
        # 计算每个节点的评分
        scores = []
        for i, node in enumerate(self.cloud_nodes):
            if normalize:
                score = self.calculate_score(
                    node.cloud_id, normalize=True,
                    all_latencies=all_latencies, all_costs=all_costs
                )
            else:
                score = self.calculate_score(node.cloud_id, normalize=False)
            scores.append((node, score))
        
        # 按评分排序（升序，分数越低越好）
        scores.sort(key=lambda x: x[1])
        
        return scores[:k]
    
    def upload_with_scheduling(self, data: bytes, filename: str, 
                                k: int = 1) -> List[dict]:
        """
        使用智能调度进行上传
        
        Args:
            data: 要上传的数据
            filename: 文件名
            k: 上传到前 k 个最优节点（冗余存储）
            
        Returns:
            上传结果列表
        """
        best_nodes = self.select_best_nodes(k)
        results = []
        
        for node, score in best_nodes:
            # 执行上传
            result = node.upload(data, filename)
            
            # 更新历史统计
            self.update_stats(node.cloud_id, result['latency'])
            
            result['predicted_score'] = score
            results.append(result)
        
        return results
    
    def get_stats_summary(self) -> Dict[str, dict]:
        """
        获取所有云节点的统计摘要
        
        Returns:
            包含各节点统计信息的字典
        """
        summary = {}
        for cloud_id in self.latency_history:
            history = self.latency_history[cloud_id]
            summary[cloud_id] = {
                "sample_count": len(history),
                "ewma_prediction": self.ewma_cache[cloud_id],
                "avg_latency": sum(history) / len(history) if history else None,
                "min_latency": min(history) if history else None,
                "max_latency": max(history) if history else None,
                "cost_per_gb": self.node_map[cloud_id].cost_per_gb
            }
        return summary
    
    def __repr__(self):
        return (f"SmartScheduler(nodes={len(self.cloud_nodes)}, α={self.alpha}, "
                f"weights=[latency:{self.latency_weight}, cost:{self.cost_weight}])")


# 默认调度器实例
scheduler = SmartScheduler()


if __name__ == "__main__":
    # 测试代码
    print("=== 智能调度器测试 ===")
    print(scheduler)
    
    # 模拟一些历史数据
    print("\n--- 模拟历史延迟数据 ---")
    import random
    for _ in range(5):
        for node in CLOUD_NODES:
            fake_latency = random.gauss(node.latency_mean, node.latency_std)
            scheduler.update_stats(node.cloud_id, max(0, fake_latency))
    
    # 显示预测延迟
    print("\n--- EWMA 延迟预测 ---")
    for node in CLOUD_NODES:
        pred = scheduler.predict_latency(node.cloud_id)
        print(f"{node.cloud_id}: 预测延迟 = {pred*1000:.2f}ms")
    
    # 选择最优节点
    print("\n--- 选择最优节点 (Top 2) ---")
    best = scheduler.select_best_nodes(k=2)
    for node, score in best:
        print(f"{node.cloud_id}: Score = {score:.4f}")
    
    # 智能上传测试
    print("\n--- 智能上传测试 ---")
    test_data = b"Test data for smart scheduling"
    results = scheduler.upload_with_scheduling(test_data, "smart_test.txt", k=2)
    for r in results:
        print(f"{r['cloud_id']}: latency={r['latency']*1000:.2f}ms, score={r['predicted_score']:.4f}")
    
    # 统计摘要
    print("\n--- 统计摘要 ---")
    stats = scheduler.get_stats_summary()
    for cloud_id, info in stats.items():
        print(f"{cloud_id}: samples={info['sample_count']}, "
              f"EWMA={info['ewma_prediction']*1000:.2f}ms" if info['ewma_prediction'] else f"{cloud_id}: 无数据")
