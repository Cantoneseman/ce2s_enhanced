"""
实验 4：数据类型去重效果测试

评估 FastCDC vs Fixed-size Chunking 在不同数据类型下的去重效果：
- Code: 源代码文件（高可压缩性、结构化）
- PDF/Documents: 文档文件（混合内容）
- Binary: 二进制文件（低冗余）

实验设计：
1. 加载各类型数据集
2. 生成 Version 2（中间插入 5% 新数据）
3. 对比两种分块方法的去重率
"""

import os
import csv
import time
from pathlib import Path
from typing import Dict, List, Tuple, Any

# 导入项目模块
from client import Client
from edge_core import EdgeProcessor, ChunkStatus


# ========== 数据集路径配置 ==========

DATASET_PATHS = {
    "Code": "test_data/code",
    "PDF": "real_docs",
    "Binary": "test_data/binary",
}


# ========== 数据加载模块 ==========

def load_folder_data(folder_path: str, max_size_mb: float = 10.0) -> bytes:
    """
    加载文件夹中的所有文件，合并为单个 bytes 对象
    
    Args:
        folder_path: 文件夹路径
        max_size_mb: 最大加载大小（MB），防止内存溢出
        
    Returns:
        合并后的文件内容
    """
    folder = Path(folder_path)
    
    if not folder.exists():
        print(f"   ⚠️ 文件夹不存在: {folder_path}")
        return None
    
    all_data = bytearray()
    file_count = 0
    max_size = int(max_size_mb * 1024 * 1024 * 10)
    
    # 递归遍历所有文件
    for file_path in sorted(folder.rglob("*")):
        if file_path.is_file():
            try:
                with open(file_path, 'rb') as f:
                    file_data = f.read()
                    
                    # 检查是否超过最大大小
                    if len(all_data) + len(file_data) > max_size:
                        remaining = max_size - len(all_data)
                        if remaining > 0:
                            all_data.extend(file_data[:remaining])
                        print(f"   ⚠️ 达到最大大小限制 ({max_size_mb} MB)，停止加载")
                        break
                    
                    all_data.extend(file_data)
                    file_count += 1
            except (PermissionError, IOError) as e:
                print(f"   ⚠️ 无法读取文件: {file_path} ({e})")
    
    if len(all_data) == 0:
        return None
    
    return bytes(all_data)


def generate_code_dataset(folder_path: str = "test_data/code", 
                          target_size_mb: float = 2.0) -> bytes:
    """
    生成代码数据集
    
    如果文件夹存在则加载，否则生成模拟代码文件
    """
    folder = Path(folder_path)
    
    # 尝试加载已有数据
    existing_data = load_folder_data(folder_path)
    if existing_data and len(existing_data) > 100 * 1024:  # 至少 100KB
        return existing_data
    
    # 生成模拟代码
    print(f"   📝 生成模拟代码数据...")
    folder.mkdir(parents=True, exist_ok=True)
    
    target_size = int(target_size_mb * 1024 * 1024)
    
    # 代码模板
    code_templates = [
        b'''"""
Module: data_processor.py
Description: Data processing utilities for CE2S system
"""

import hashlib
import json
from typing import List, Dict, Optional
from dataclasses import dataclass

@dataclass
class DataChunk:
    """Represents a data chunk with metadata."""
    chunk_id: int
    fingerprint: str
    size: int
    data: bytes
    
    def to_dict(self) -> Dict:
        return {
            "chunk_id": self.chunk_id,
            "fingerprint": self.fingerprint,
            "size": self.size
        }

class DataProcessor:
    """Processes data chunks for storage."""
    
    def __init__(self, chunk_size: int = 8192):
        self.chunk_size = chunk_size
        self.processed_count = 0
    
    def process(self, data: bytes) -> List[DataChunk]:
        """Process data into chunks."""
        chunks = []
        offset = 0
        
        while offset < len(data):
            chunk_data = data[offset:offset + self.chunk_size]
            fingerprint = hashlib.sha256(chunk_data).hexdigest()
            
            chunk = DataChunk(
                chunk_id=len(chunks),
                fingerprint=fingerprint,
                size=len(chunk_data),
                data=chunk_data
            )
            chunks.append(chunk)
            offset += self.chunk_size
            self.processed_count += 1
        
        return chunks

''',
        b'''"""
Module: scheduler.py
Description: Intelligent scheduling for cloud node selection
"""

import random
from typing import List, Tuple
from collections import defaultdict

class EWMAPredictor:
    """EWMA-based latency predictor."""
    
    def __init__(self, alpha: float = 0.7):
        self.alpha = alpha
        self.predictions = {}
    
    def update(self, node_id: str, latency: float) -> float:
        if node_id not in self.predictions:
            self.predictions[node_id] = latency
        else:
            self.predictions[node_id] = (
                self.alpha * latency + 
                (1 - self.alpha) * self.predictions[node_id]
            )
        return self.predictions[node_id]
    
    def predict(self, node_id: str, default: float = 100.0) -> float:
        return self.predictions.get(node_id, default)

class SmartScheduler:
    """QoS-Cost aware scheduler."""
    
    def __init__(self, nodes: List[dict], 
                 latency_weight: float = 0.8,
                 cost_weight: float = 0.2):
        self.nodes = nodes
        self.latency_weight = latency_weight
        self.cost_weight = cost_weight
        self.predictor = EWMAPredictor()
    
    def select_nodes(self, k: int = 3) -> List[dict]:
        """Select top-k nodes based on score."""
        scores = []
        for node in self.nodes:
            latency = self.predictor.predict(node["id"], node["latency"])
            cost = node["cost"]
            score = self.latency_weight * latency + self.cost_weight * cost * 100
            scores.append((node, score))
        
        scores.sort(key=lambda x: x[1])
        return [node for node, _ in scores[:k]]

''',
        b'''"""
Module: storage_client.py
Description: Cloud storage client implementation
"""

import os
import time
import hashlib
from pathlib import Path
from typing import Optional, Dict, Any

class StorageClient:
    """Client for cloud storage operations."""
    
    def __init__(self, endpoint: str, api_key: str):
        self.endpoint = endpoint
        self.api_key = api_key
        self.upload_count = 0
        self.download_count = 0
    
    def upload(self, data: bytes, filename: str) -> Dict[str, Any]:
        """Upload data to cloud storage."""
        fingerprint = hashlib.sha256(data).hexdigest()
        
        # Simulate network latency
        time.sleep(0.01)
        
        self.upload_count += 1
        
        return {
            "status": "success",
            "filename": filename,
            "size": len(data),
            "fingerprint": fingerprint,
            "timestamp": time.time()
        }
    
    def download(self, filename: str) -> Optional[bytes]:
        """Download data from cloud storage."""
        # Simulate network latency
        time.sleep(0.01)
        
        self.download_count += 1
        
        # Return mock data
        return b"mock_data"
    
    def get_stats(self) -> Dict[str, int]:
        return {
            "uploads": self.upload_count,
            "downloads": self.download_count
        }

''',
    ]
    
    # 生成内容
    content = bytearray()
    template_idx = 0
    
    while len(content) < target_size:
        content.extend(code_templates[template_idx % len(code_templates)])
        content.extend(b"\n" + b"#" * 80 + b"\n\n")
        template_idx += 1
    
    return bytes(content[:target_size])


def generate_binary_dataset(folder_path: str = "test_data/binary",
                            target_size_mb: float = 2.0) -> bytes:
    """
    生成二进制数据集
    
    如果文件夹存在则加载，否则生成随机二进制数据
    """
    folder = Path(folder_path)
    
    # 尝试加载已有数据
    existing_data = load_folder_data(folder_path)
    if existing_data and len(existing_data) > 100 * 1024:
        return existing_data
    
    # 生成模拟二进制数据
    print(f"   📝 生成模拟二进制数据...")
    folder.mkdir(parents=True, exist_ok=True)
    
    target_size = int(target_size_mb * 1024 * 1024)
    
    # 混合生成：部分随机，部分重复块（模拟真实二进制文件）
    content = bytearray()
    
    # 生成一些可能重复的块
    repeat_blocks = [os.urandom(8192) for _ in range(10)]
    
    while len(content) < target_size:
        # 70% 随机数据，30% 重复块
        if random.random() < 0.7:
            content.extend(os.urandom(random.randint(4096, 16384)))
        else:
            content.extend(random.choice(repeat_blocks))
    
    return bytes(content[:target_size])


def generate_pdf_dataset(folder_path: str = "real_docs",
                         target_size_mb: float = 2.0) -> bytes:
    """
    生成文档数据集
    
    如果文件夹存在则加载，否则生成模拟文档数据
    """
    folder = Path(folder_path)
    
    # 尝试加载已有数据
    existing_data = load_folder_data(folder_path)
    if existing_data and len(existing_data) > 100 * 1024:
        return existing_data
    
    # 生成模拟文档数据
    print(f"   📝 生成模拟文档数据...")
    folder.mkdir(parents=True, exist_ok=True)
    
    target_size = int(target_size_mb * 1024 * 1024)
    
    # 文档段落模板
    paragraphs = [
        b"=" * 80 + b"\n" + b"CHAPTER 1: Introduction to Cloud Storage\n" + b"=" * 80 + b"\n\n",
        b"Cloud storage has revolutionized the way we store and manage data. The ability to access files from anywhere has transformed both personal and enterprise computing. " * 15 + b"\n\n",
        b"Edge computing brings computation closer to data sources, reducing latency and bandwidth consumption. This paradigm shift enables real-time processing of IoT data streams. " * 15 + b"\n\n",
        b"-" * 80 + b"\n" + b"Section 1.1: System Architecture\n" + b"-" * 80 + b"\n\n",
        b"Our proposed system implements a three-tier architecture. The client layer handles encryption, the edge layer performs deduplication, and the cloud layer provides persistent storage. " * 12 + b"\n\n",
        b"=" * 80 + b"\n" + b"CHAPTER 2: Deduplication Algorithms\n" + b"=" * 80 + b"\n\n",
        b"Content-Defined Chunking (CDC) algorithms divide data at content-dependent boundaries. Unlike fixed-size chunking, CDC maintains chunk alignment even when data is inserted or deleted. " * 15 + b"\n\n",
        b"FastCDC is a gear-based CDC algorithm that achieves significant speedup over traditional Rabin fingerprinting while maintaining comparable deduplication ratios. " * 15 + b"\n\n",
        # 模拟嵌入的图片/表格
        b"\n[Figure 1: Architecture Diagram]\n" + os.urandom(4096) + b"\n",
        b"\n[Table 1: Performance Comparison]\n" + os.urandom(2048) + b"\n",
    ]
    
    content = bytearray()
    para_idx = 0
    
    while len(content) < target_size:
        content.extend(paragraphs[para_idx % len(paragraphs)])
        para_idx += 1
    
    return bytes(content[:target_size])


# ========== 数据集加载器 ==========

def load_dataset(data_type: str) -> bytes:
    """
    根据数据类型加载数据集
    
    Args:
        data_type: 数据类型 ("Code", "PDF", "Binary")
        
    Returns:
        数据集内容
    """
    folder_path = DATASET_PATHS.get(data_type)
    
    if data_type == "Code":
        return generate_code_dataset(folder_path)
    elif data_type == "PDF":
        return generate_pdf_dataset(folder_path)
    elif data_type == "Binary":
        return generate_binary_dataset(folder_path)
    else:
        raise ValueError(f"Unknown data type: {data_type}")


def create_version_2_data(raw_data: bytes, insert_ratio: float = 0.05) -> bytes:
    """
    模拟版本迭代：在数据中间插入新内容
    
    Args:
        raw_data: 原始数据（版本 1）
        insert_ratio: 插入数据占原始数据的比例（默认 5%）
        
    Returns:
        修改后的数据（版本 2）
    """
    data_len = len(raw_data)
    # 加上奇数偏移量 (+13 字节)，确保插入操作打乱固定分块的边界对齐
    # 防止 insert_size 刚好是分块大小的整数倍导致 Fixed Chunking 意外对齐
    insert_size = int(data_len * insert_ratio) + 13
    insert_pos = data_len // 2  # 在正中间插入
    
    # 生成要插入的新内容
    new_content = bytearray()
    new_content.extend(b"\n\n[VERSION 2 UPDATE - BEGIN]\n")
    new_content.extend(b"This content was added in version 2.\n" * 10)
    new_content.extend(b"[VERSION 2 UPDATE - END]\n\n")
    
    # 填充剩余部分用随机数据
    remaining = insert_size - len(new_content)
    if remaining > 0:
        new_content.extend(os.urandom(remaining))
    
    new_content = bytes(new_content[:insert_size])
    
    # 执行插入操作
    version_2_data = raw_data[:insert_pos] + new_content + raw_data[insert_pos:]
    
    return version_2_data


# ========== 去重测试核心逻辑 ==========

def run_dedup_test(
    raw_data: bytes,
    data_v2: bytes,
    chunk_mode: str
) -> Tuple[float, int, int, int]:
    """
    运行去重测试
    
    Args:
        raw_data: 原始数据 (V1)
        data_v2: 修改后数据 (V2)
        chunk_mode: 分块模式 ('fastcdc' 或 'fixed')
        
    Returns:
        (dedup_ratio, total_chunks, new_chunks, ref_chunks)
    """
    # 初始化组件
    client = Client(client_id=f"datatype_test_{chunk_mode}")
    edge = EdgeProcessor(edge_id=f"edge_{chunk_mode}")
    
    # Phase 1: 处理 V1 数据（建立指纹表）
    encrypted_chunks_v1 = client.encrypt_data(raw_data, chunk_mode=chunk_mode)
    chunks_v1 = edge.process(encrypted_chunks_v1)
    
    # Phase 2: 处理 V2 数据（测试去重）
    edge.reset_stats()
    encrypted_chunks_v2 = client.encrypt_data(data_v2, chunk_mode=chunk_mode)
    chunks_v2 = edge.process(encrypted_chunks_v2)
    
    # 统计
    total_chunks = len(chunks_v2)
    new_chunks = sum(1 for c in chunks_v2 if c.status == ChunkStatus.NEW)
    ref_chunks = sum(1 for c in chunks_v2 if c.status == ChunkStatus.REF)
    
    dedup_ratio = ref_chunks / total_chunks if total_chunks > 0 else 0
    
    return dedup_ratio, total_chunks, new_chunks, ref_chunks


# ========== 主实验流程 ==========

def run_datatype_experiment(
    iterations: int = 3,
    output_dir: str = "results"
) -> List[Dict]:
    """
    运行数据类型去重效果实验
    
    Args:
        iterations: 每个配置的迭代次数
        output_dir: 结果输出目录
        
    Returns:
        实验结果列表
    """
    print("=" * 70)
    print("  实验 4：数据类型去重效果测试")
    print("=" * 70)
    print(f"数据类型: {list(DATASET_PATHS.keys())}")
    print(f"分块方法: Fixed-size, FastCDC (Proposed)")
    print(f"迭代次数: {iterations}")
    print("=" * 70)
    
    # 分块方法配置
    methods = [
        ("Fixed-size Chunking", "fixed"),
        ("Proposed (FastCDC)", "fastcdc"),
    ]
    
    results = []
    
    # 遍历数据类型
    for data_type in DATASET_PATHS.keys():
        print(f"\n{'='*70}")
        print(f"📁 数据类型: {data_type}")
        print(f"   路径: {DATASET_PATHS[data_type]}")
        print(f"{'='*70}")
        
        # 加载数据集
        print(f"\n   📂 加载数据集...")
        raw_data = load_dataset(data_type)
        
        if raw_data is None:
            print(f"   ❌ 无法加载数据，跳过")
            continue
        
        print(f"   ✅ 数据大小: {len(raw_data) / 1024:.1f} KB")
        
        # 生成 V2 数据（插入 5% 新内容）
        print(f"\n   📝 生成 Version 2 数据（插入 5%）...")
        data_v2 = create_version_2_data(raw_data, insert_ratio=0.05)
        print(f"   ✅ V2 数据大小: {len(data_v2) / 1024:.1f} KB (+{(len(data_v2) - len(raw_data)) / 1024:.1f} KB)")
        
        # 测试每种分块方法
        for method_name, chunk_mode in methods:
            print(f"\n   🧪 测试方法: {method_name} (mode={chunk_mode})")
            
            dedup_sum = 0.0
            total_chunks_last = 0
            new_chunks_last = 0
            ref_chunks_last = 0
            
            for iter_idx in range(iterations):
                dedup_ratio, total, new, ref = run_dedup_test(
                    raw_data, data_v2, chunk_mode
                )
                
                dedup_sum += dedup_ratio
                total_chunks_last = total
                new_chunks_last = new
                ref_chunks_last = ref
                
                print(f"      迭代 {iter_idx+1}: 去重率={dedup_ratio*100:.2f}% "
                      f"(总块={total}, 新块={new}, 引用块={ref})")
            
            # 计算平均去重率
            avg_dedup = dedup_sum / iterations
            
            print(f"   ✅ {method_name}: 平均去重率={avg_dedup*100:.2f}%")
            
            # 记录结果
            results.append({
                "DataType": data_type,
                "Method": method_name,
                "DedupRatio(%)": round(avg_dedup * 100, 2),
                "TotalChunks": total_chunks_last,
                "NewChunks": new_chunks_last,
                "RefChunks": ref_chunks_last
            })
    
    # 保存结果
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    output_file = Path(output_dir) / "datatype_test_results.csv"
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ["DataType", "Method", "DedupRatio(%)", "TotalChunks", "NewChunks", "RefChunks"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    print(f"\n{'='*70}")
    print(f"✅ 实验完成！结果已保存至: {output_file}")
    print(f"{'='*70}")
    
    # 打印结果总结
    print("\n📈 结果总结:")
    print(f"{'DataType':<12} {'Method':<25} {'DedupRatio(%)':<15}")
    print("-" * 55)
    for r in results:
        print(f"{r['DataType']:<12} {r['Method']:<25} {r['DedupRatio(%)']:<15.2f}")
    
    # 计算 FastCDC 相对于 Fixed 的提升
    print("\n🎯 FastCDC vs Fixed 去重率提升:")
    for data_type in DATASET_PATHS.keys():
        type_results = [r for r in results if r['DataType'] == data_type]
        if len(type_results) >= 2:
            fixed = next((r for r in type_results if 'Fixed' in r['Method']), None)
            fastcdc = next((r for r in type_results if 'FastCDC' in r['Method']), None)
            
            if fixed and fastcdc:
                improvement = fastcdc['DedupRatio(%)'] - fixed['DedupRatio(%)']
                print(f"   {data_type}: +{improvement:.2f}%")
    
    return results


if __name__ == "__main__":
    import random
    random.seed(42)  # 可重复性
    
    # 运行实验
    results = run_datatype_experiment(
        iterations=3,
        output_dir="results"
    )
