"""
云存储节点模拟模块
使用本地文件夹模拟云存储，支持延迟和成本建模
"""

import os
import time
import random
from pathlib import Path


class CloudNode:
    """云存储节点模拟类"""
    
    def __init__(self, cloud_id: str, latency_mean: float, latency_std: float, 
                 cost_per_gb: float, storage_dir: str = "./cloud_storage"):
        """
        初始化云节点
        
        Args:
            cloud_id: 云节点唯一标识
            latency_mean: 平均网络延迟（秒）
            latency_std: 延迟波动标准差（秒）
            cost_per_gb: 每GB存储成本（模拟值）
            storage_dir: 本地模拟存储根目录
        """
        self.cloud_id = cloud_id
        self.latency_mean = latency_mean
        self.latency_std = latency_std
        self.cost_per_gb = cost_per_gb
        
        # 创建云节点专属存储目录
        self.storage_path = Path(storage_dir) / cloud_id
        self.storage_path.mkdir(parents=True, exist_ok=True)
    
    def _simulate_latency(self) -> float:
        """模拟网络延迟（基于正态分布）"""
        latency = random.gauss(self.latency_mean, self.latency_std)
        # 确保延迟非负
        return max(0, latency)
    
    def upload(self, data: bytes, filename: str) -> dict:
        """
        上传数据到云存储（模拟）
        
        Args:
            data: 要上传的数据（字节）
            filename: 文件名
            
        Returns:
            包含上传结果的字典
        """
        # 模拟网络延迟
        latency = self._simulate_latency()
        time.sleep(latency)
        
        # 写入本地磁盘
        file_path = self.storage_path / filename
        with open(file_path, 'wb') as f:
            f.write(data)
        
        # 计算存储成本
        size_gb = len(data) / (1024 ** 3)
        cost = size_gb * self.cost_per_gb
        
        return {
            "cloud_id": self.cloud_id,
            "filename": filename,
            "size_bytes": len(data),
            "latency": latency,
            "cost": cost,
            "path": str(file_path)
        }
    
    def download(self, filename: str) -> bytes:
        """
        从云存储下载数据（模拟）
        
        Args:
            filename: 文件名
            
        Returns:
            文件数据（字节）
        """
        # 模拟网络延迟
        latency = self._simulate_latency()
        time.sleep(latency)
        
        file_path = self.storage_path / filename
        with open(file_path, 'rb') as f:
            return f.read()
    
    def __repr__(self):
        return (f"CloudNode(id={self.cloud_id}, latency={self.latency_mean}±{self.latency_std}s, "
                f"cost={self.cost_per_gb}/GB)")


# ========== 初始化 4 个云节点实例 ==========

# 阿里云：非常快，贵
aliyun = CloudNode(
    cloud_id="aliyun",
    latency_mean=0.02,   # 20ms 平均延迟（非常快）
    latency_std=0.005,   # 5ms 波动
    cost_per_gb=0.8      # 高成本
)

# 腾讯云：快，中等成本
tencent = CloudNode(
    cloud_id="tencent",
    latency_mean=0.05,   # 50ms 平均延迟（快）
    latency_std=0.01,    # 10ms 波动
    cost_per_gb=0.5      # 中等成本
)

# 华为云：中等延迟，便宜
huawei = CloudNode(
    cloud_id="huawei",
    latency_mean=0.10,   # 100ms 平均延迟（中）
    latency_std=0.02,    # 20ms 波动
    cost_per_gb=0.3      # 便宜
)

# 百度云：非常慢（故障节点），非常便宜
baidu = CloudNode(
    cloud_id="baidu",
    latency_mean=0.50,   # 500ms 平均延迟（非常慢/故障）
    latency_std=0.10,    # 100ms 波动
    cost_per_gb=0.1      # 非常便宜
)

# 云节点列表，便于统一管理
CLOUD_NODES = [aliyun, tencent, huawei, baidu]


if __name__ == "__main__":
    # 测试代码
    print("=== 云节点列表 ===")
    for node in CLOUD_NODES:
        print(node)
    
    print("\n=== 测试上传 ===")
    test_data = b"Hello, Cloud Storage Simulation!"
    result = aliyun.upload(test_data, "test.txt")
    print(f"上传结果: {result}")
