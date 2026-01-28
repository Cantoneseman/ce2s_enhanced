"""
消融实验脚本
对比分析：智能调度 vs 随机调度、FastCDC vs 固定分块
"""

import os
import csv
import time
import random
import threading
from pathlib import Path
from typing import Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from tqdm import tqdm

# 导入系统模块
from client import Client
from edge_core import EdgeProcessor, ChunkStatus, ChunkInfo
from scheduler import SmartScheduler
from cloud_mock import CloudNode, CLOUD_NODES


@dataclass
class ExperimentResult:
    """实验结果"""
    exp_name: str           # 实验名称
    total_time: float       # 总耗时（秒）
    avg_latency: float      # 平均延迟（秒）
    dedup_ratio: float      # 去重率
    total_chunks: int       # 总块数
    uploaded_shards: int    # 上传分片数
    cloud_distribution: Dict[str, int]  # 云节点分布


# ============================================================
# 数据准备模块
# ============================================================

def generate_synthetic_workload(path: str = "./test_data/synthetic_workload.bin", 
                                 size_mb: float = 5.0) -> bytes:
    """
    生成结构化模拟数据（代码+文档混合）
    
    生成包含英文文本段落、模拟代码片段和少量二进制数据的混合文件，
    模拟真实的"代码+文档"工作负载结构。
    
    Args:
        path: 输出文件路径
        size_mb: 目标文件大小（MB）
        
    Returns:
        生成的文件内容（字节）
    """
    file_path = Path(path)
    
    if file_path.exists():
        print(f"📄 读取已有工作负载文件: {path}")
        with open(file_path, 'rb') as f:
            return f.read()
    
    print(f"📝 生成结构化模拟工作负载: {path}")
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    target_size = int(size_mb * 1024 * 1024)
    
    # ========== 文档段落模板 ==========
    doc_paragraphs = [
        b"=" * 80 + b"\n" + b"CHAPTER 1: Introduction to Cloud-Edge-End Storage Systems\n" + b"=" * 80 + b"\n\n",
        b"Cloud storage has revolutionized the way we store and manage data. The ability to access files from anywhere has transformed both personal and enterprise computing. " * 10 + b"\n\n",
        b"Edge computing brings computation closer to data sources, reducing latency and bandwidth consumption. This paradigm shift enables real-time processing of IoT data streams. " * 10 + b"\n\n",
        b"-" * 80 + b"\n" + b"Section 1.1: System Architecture Overview\n" + b"-" * 80 + b"\n\n",
        b"Our proposed CE2S (Cloud-Edge-End Storage) system implements a three-tier architecture. The client layer handles encryption, the edge layer performs deduplication, and the cloud layer provides persistent storage. " * 8 + b"\n\n",
        b"=" * 80 + b"\n" + b"CHAPTER 2: Deduplication Algorithms\n" + b"=" * 80 + b"\n\n",
        b"Content-Defined Chunking (CDC) algorithms divide data at content-dependent boundaries. Unlike fixed-size chunking, CDC maintains chunk alignment even when data is inserted or deleted. " * 10 + b"\n\n",
        b"FastCDC is a gear-based CDC algorithm that achieves 10x speedup over traditional Rabin fingerprinting while maintaining comparable deduplication ratios. " * 10 + b"\n\n",
    ]
    
    # ========== 代码片段模板 ==========
    code_snippets = [
        b'''
# ============================================================
# cloud_storage.py - Cloud Storage Client Implementation
# ============================================================

import hashlib
import requests
from typing import Optional, Dict, List

class CloudStorageClient:
    """Client for interacting with cloud storage services."""
    
    def __init__(self, endpoint: str, api_key: str):
        self.endpoint = endpoint
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {api_key}"})
    
    def upload_chunk(self, chunk_id: str, data: bytes) -> Dict:
        """Upload a data chunk to cloud storage."""
        fingerprint = hashlib.sha256(data).hexdigest()
        response = self.session.post(
            f"{self.endpoint}/chunks/{chunk_id}",
            data=data,
            headers={"X-Fingerprint": fingerprint}
        )
        return response.json()
    
    def download_chunk(self, chunk_id: str) -> Optional[bytes]:
        """Download a chunk from cloud storage."""
        response = self.session.get(f"{self.endpoint}/chunks/{chunk_id}")
        if response.status_code == 200:
            return response.content
        return None

''',
        b'''
# ============================================================
# edge_processor.py - Edge Node Processing Module
# ============================================================

import zlib
from dataclasses import dataclass
from enum import Enum
from typing import List, Tuple

class ChunkType(Enum):
    NEW = "new"
    DUPLICATE = "duplicate"

@dataclass
class ProcessedChunk:
    chunk_id: int
    fingerprint: str
    chunk_type: ChunkType
    data: bytes
    compressed_size: int

class EdgeProcessor:
    """Edge node processor for data deduplication and compression."""
    
    def __init__(self, compression_level: int = 6):
        self.compression_level = compression_level
        self.fingerprint_index: Dict[str, int] = {}
    
    def process_chunks(self, chunks: List[bytes]) -> List[ProcessedChunk]:
        """Process a list of data chunks."""
        results = []
        for idx, chunk in enumerate(chunks):
            fingerprint = self._compute_fingerprint(chunk)
            if fingerprint in self.fingerprint_index:
                chunk_type = ChunkType.DUPLICATE
                compressed = b""
            else:
                chunk_type = ChunkType.NEW
                compressed = zlib.compress(chunk, self.compression_level)
                self.fingerprint_index[fingerprint] = idx
            
            results.append(ProcessedChunk(
                chunk_id=idx,
                fingerprint=fingerprint,
                chunk_type=chunk_type,
                data=chunk if chunk_type == ChunkType.NEW else b"",
                compressed_size=len(compressed)
            ))
        return results

''',
        b'''
# ============================================================
# scheduler.py - Intelligent Cloud Node Scheduler
# ============================================================

import random
from typing import List, Tuple, Optional
from dataclasses import dataclass

@dataclass
class CloudNode:
    node_id: str
    latency_ms: float
    cost_per_gb: float
    available_capacity_gb: float

class SmartScheduler:
    """EWMA-based intelligent scheduler for cloud node selection."""
    
    def __init__(self, alpha: float = 0.7):
        self.alpha = alpha
        self.ewma_latency: Dict[str, float] = {}
    
    def update_latency(self, node_id: str, observed_latency: float) -> None:
        """Update EWMA latency prediction for a node."""
        if node_id not in self.ewma_latency:
            self.ewma_latency[node_id] = observed_latency
        else:
            prev = self.ewma_latency[node_id]
            self.ewma_latency[node_id] = self.alpha * observed_latency + (1 - self.alpha) * prev
    
    def select_best_nodes(self, nodes: List[CloudNode], k: int = 3) -> List[CloudNode]:
        """Select top-k nodes based on predicted latency and cost."""
        scores = []
        for node in nodes:
            pred_latency = self.ewma_latency.get(node.node_id, node.latency_ms)
            score = 0.7 * pred_latency + 0.3 * node.cost_per_gb * 100
            scores.append((node, score))
        scores.sort(key=lambda x: x[1])
        return [node for node, _ in scores[:k]]

''',
        b'''
# ============================================================
# config.py - System Configuration
# ============================================================

import os
from pathlib import Path

# Storage paths
STORAGE_ROOT = Path(os.getenv("CE2S_STORAGE_ROOT", "./storage"))
CLOUD_STORAGE_PATH = STORAGE_ROOT / "cloud"
EDGE_CACHE_PATH = STORAGE_ROOT / "edge_cache"
LOG_PATH = STORAGE_ROOT / "logs"

# Chunking parameters
CHUNK_MIN_SIZE = 4 * 1024      # 4 KB
CHUNK_AVG_SIZE = 8 * 1024      # 8 KB
CHUNK_MAX_SIZE = 16 * 1024     # 16 KB

# Erasure coding parameters
EC_DATA_SHARDS = 4
EC_PARITY_SHARDS = 2

# Cloud nodes configuration
CLOUD_NODES = [
    {"id": "aliyun", "endpoint": "https://oss.aliyun.com", "latency": 20, "cost": 0.8},
    {"id": "tencent", "endpoint": "https://cos.tencent.com", "latency": 50, "cost": 0.5},
    {"id": "huawei", "endpoint": "https://obs.huawei.com", "latency": 100, "cost": 0.3},
    {"id": "baidu", "endpoint": "https://bos.baidu.com", "latency": 500, "cost": 0.1},
]

''',
    ]
    
    # ========== 组装内容 ==========
    content = bytearray()
    
    # 添加文件头
    header = b"""################################################################################
#                                                                              #
#                  CE2S: Cloud-Edge-End Storage System                         #
#                  Synthetic Workload for Ablation Study                       #
#                                                                              #
################################################################################

"""
    content.extend(header)
    
    # 交替添加文档和代码，模拟真实项目结构
    doc_idx = 0
    code_idx = 0
    section = 0
    
    while len(content) < target_size:
        section += 1
        
        # 添加文档段落
        content.extend(doc_paragraphs[doc_idx % len(doc_paragraphs)])
        doc_idx += 1
        
        # 每隔几个文档段落添加代码
        if section % 2 == 0:
            content.extend(b"\n" + b"```python\n")
            content.extend(code_snippets[code_idx % len(code_snippets)])
            content.extend(b"```\n\n")
            code_idx += 1
        
        # 偶尔添加二进制数据（模拟图片/附件）
        if section % 5 == 0:
            binary_size = random.randint(2048, 8192)
            content.extend(b"\n[Binary Data Block: " + str(binary_size).encode() + b" bytes]\n")
            content.extend(os.urandom(binary_size))
            content.extend(b"\n[End Binary Block]\n\n")
    
    # 截断到目标大小
    content = bytes(content[:target_size])
    
    # 保存文件
    with open(file_path, 'wb') as f:
        f.write(content)
    
    print(f"   ✅ 生成完成: {len(content) / 1024 / 1024:.2f} MB")
    return content


def load_real_dataset(folder_path: str = "./real_docs") -> bytes:
    """
    加载真实文件夹数据
    
    遍历指定文件夹下所有文件，将它们读取并拼接成一个巨大的 bytearray。
    
    Args:
        folder_path: 文件夹路径
        
    Returns:
        拼接后的所有文件内容（字节）
    """
    folder = Path(folder_path)
    
    if not folder.exists():
        print(f"⚠️ 文件夹不存在: {folder_path}")
        print("   将使用合成数据替代")
        return generate_synthetic_workload()
    
    print(f"📂 加载真实数据集: {folder_path}")
    
    all_data = bytearray()
    file_count = 0
    
    # 递归遍历所有文件
    for file_path in folder.rglob("*"):
        if file_path.is_file():
            try:
                with open(file_path, 'rb') as f:
                    file_data = f.read()
                    all_data.extend(file_data)
                    file_count += 1
            except (PermissionError, IOError) as e:
                print(f"   ⚠️ 无法读取文件: {file_path} ({e})")
    
    total_size_mb = len(all_data) / (1024 * 1024)
    print(f"   ✅ 加载完成: {file_count} 个文件, 总大小: {total_size_mb:.2f} MB")
    
    if len(all_data) == 0:
        print("   ⚠️ 文件夹为空，使用合成数据替代")
        return generate_synthetic_workload()
    
    return bytes(all_data)


def create_version_2_data(raw_data: bytes) -> bytes:
    """
    模拟版本迭代：在数据中间插入新内容
    
    模拟用户编辑场景：在数据流的正中间插入（Insert）相当于原大小 10% 的新随机数据。
    注意：这是插入操作，会造成后续数据偏移，不是覆盖操作。
    
    这种插入操作对于测试 FastCDC 的优势至关重要：
    - FastCDC（内容定义分块）：插入后，只有插入点附近的块会改变，后续块边界自动重新对齐
    - 固定分块：插入后，所有后续块都会偏移，导致全部失效
    
    Args:
        raw_data: 原始数据（版本 1）
        
    Returns:
        修改后的数据（版本 2）
    """
    data_len = len(raw_data)
    insert_size = int(data_len * 0.1)  # 插入 10% 大小的新数据
    insert_pos = data_len // 2          # 在正中间插入
    
    print(f"📝 创建版本 2 数据（模拟编辑）:")
    print(f"   原始大小: {data_len / 1024 / 1024:.2f} MB")
    print(f"   插入位置: 字节 {insert_pos} (正中间)")
    print(f"   插入大小: {insert_size} bytes ({insert_size / 1024:.1f} KB)")
    
    # 生成要插入的新内容（模拟用户新增的代码/文档）
    new_content = bytearray()
    
    # 添加一些结构化的插入内容
    new_content.extend(b"\n\n")
    new_content.extend(b"=" * 60 + b"\n")
    new_content.extend(b"[NEW SECTION INSERTED - Version 2 Update]\n")
    new_content.extend(b"=" * 60 + b"\n\n")
    new_content.extend(b"This section was added in version 2 of the document.\n" * 20)
    new_content.extend(b"\n# New code added:\n")
    new_content.extend(b"def new_feature():\n")
    new_content.extend(b"    '''Feature added in v2'''\n")
    new_content.extend(b"    pass\n\n")
    
    # 填充剩余部分用随机数据
    remaining = insert_size - len(new_content)
    if remaining > 0:
        new_content.extend(os.urandom(remaining))
    
    # 截断到目标大小
    new_content = bytes(new_content[:insert_size])
    
    # 执行插入操作：前半部分 + 新内容 + 后半部分
    version_2_data = raw_data[:insert_pos] + new_content + raw_data[insert_pos:]
    
    print(f"   新版本大小: {len(version_2_data) / 1024 / 1024:.2f} MB")
    print(f"   大小增加: +{insert_size / 1024:.1f} KB (+10%)")
    
    return version_2_data


# ============================================================
# 保留原有的 generate_mock_real_file 作为兼容性别名
# ============================================================

def generate_mock_real_file(path: str = "./test_data/real_sample.pdf") -> bytes:
    """
    生成模拟真实文件
    
    如果文件已存在则读取，否则生成一个 5MB 的模拟文件。
    使用重复文本段落模拟真实文档结构（而非全随机乱码）。
    
    Args:
        path: 文件路径
        
    Returns:
        文件内容（字节）
    """
    file_path = Path(path)
    
    if file_path.exists():
        print(f"📄 读取已有测试文件: {path}")
        with open(file_path, 'rb') as f:
            return f.read()
    
    print(f"📝 生成模拟真实文件: {path}")
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    # 目标大小：5MB
    target_size = 5 * 1024 * 1024
    
    # 创建多个不同的文本段落（模拟文档结构）
    paragraphs = [
        b"=" * 80 + b"\n" + b"CHAPTER 1: Introduction to Cloud-Edge-End Storage Systems\n" + b"=" * 80 + b"\n\n",
        b"Cloud storage has revolutionized the way we store and manage data. " * 20 + b"\n\n",
        b"Edge computing brings computation closer to data sources, reducing latency. " * 20 + b"\n\n",
        b"The integration of cloud and edge creates a powerful hybrid architecture. " * 20 + b"\n\n",
        b"-" * 80 + b"\n" + b"Section 1.1: System Architecture\n" + b"-" * 80 + b"\n\n",
        b"Our proposed CE2S system consists of three layers: client, edge, and cloud. " * 15 + b"\n\n",
        b"Data flows from clients through edge nodes before reaching cloud storage. " * 15 + b"\n\n",
        b"=" * 80 + b"\n" + b"CHAPTER 2: Deduplication and Chunking Algorithms\n" + b"=" * 80 + b"\n\n",
        b"Content-Defined Chunking (CDC) provides better deduplication than fixed-size. " * 20 + b"\n\n",
        b"FastCDC improves chunking speed while maintaining high deduplication ratio. " * 20 + b"\n\n",
        b"-" * 80 + b"\n" + b"Section 2.1: SHA-256 Fingerprinting\n" + b"-" * 80 + b"\n\n",
        b"Each chunk is hashed using SHA-256 to create a unique fingerprint. " * 15 + b"\n\n",
        b"Duplicate chunks are detected by comparing fingerprints in a hash table. " * 15 + b"\n\n",
        b"=" * 80 + b"\n" + b"CHAPTER 3: Intelligent Scheduling\n" + b"=" * 80 + b"\n\n",
        b"EWMA-based latency prediction enables proactive node selection. " * 20 + b"\n\n",
        b"Multi-objective optimization balances QoS and cost requirements. " * 20 + b"\n\n",
        # 添加一些随机二进制数据（模拟嵌入的图片/图表）
        os.urandom(8192),
        b"\n\n" + b"Figure 1: System Architecture Diagram\n\n",
        os.urandom(4096),
        b"\n\n" + b"Table 1: Performance Comparison\n\n",
    ]
    
    # 组装文件内容（重复段落以达到目标大小）
    content = bytearray()
    paragraph_idx = 0
    
    while len(content) < target_size:
        content.extend(paragraphs[paragraph_idx % len(paragraphs)])
        paragraph_idx += 1
    
    # 截断到目标大小
    content = bytes(content[:target_size])
    
    # 保存文件
    with open(file_path, 'wb') as f:
        f.write(content)
    
    print(f"   生成完成: {len(content) / 1024 / 1024:.2f} MB")
    return content


# ============================================================
# 实验核心运行逻辑
# ============================================================

def run_experiment_phase(
    phase_name: str,
    raw_data: bytes,
    iterations: int = 1
) -> List[Dict]:
    """
    运行实验阶段：对比 Fixed (传统) vs Proposed (本文方法)
    
    实验流程：
    1. 使用 create_version_2_data 生成修改后的 data_v2
    2. 对比两组实验配置：
       - Group A (Fixed): use_fastcdc=False, use_smart_scheduler=True
       - Group B (Proposed): use_fastcdc=True, use_smart_scheduler=True
    3. 每组先跑 raw_data（预热/基准），再跑 data_v2
    4. 计算 data_v2 上传时的去重率
    
    Args:
        phase_name: 实验阶段名称
        raw_data: 原始测试数据（版本 1）
        iterations: 迭代次数（用于计算平均值）
        
    Returns:
        结果字典列表，包含 Phase, Method, Time(s), DedupRatio(%)
    """
    print("\n" + "╔" + "═"*58 + "╗")
    print("║" + f" Experiment Phase: {phase_name} ".center(58) + "║")
    print("╚" + "═"*58 + "╝")
    
    # 生成版本 2 数据（中间插入 10%）
    print("\n📝 准备测试数据...")
    data_v2 = create_version_2_data(raw_data)
    
    # 实验配置
    experiment_configs = [
        {
            "name": "Fixed-size Chunking",
            "short_name": "Fixed",
            "use_fastcdc": False,
            "use_smart_scheduler": True
        },
        {
            "name": "Proposed (FastCDC)",
            "short_name": "Proposed",
            "use_fastcdc": True,
            "use_smart_scheduler": True
        }
    ]
    
    results = []
    
    for config in experiment_configs:
        print(f"\n{'='*60}")
        print(f"🧪 实验组: {config['name']}")
        print(f"   FastCDC: {config['use_fastcdc']}, SmartScheduler: {config['use_smart_scheduler']}")
        print(f"{'='*60}")
        
        total_time_sum = 0.0
        dedup_ratio_sum = 0.0
        
        for iter_idx in range(iterations):
            if iterations > 1:
                print(f"\n--- 迭代 {iter_idx + 1}/{iterations} ---")
            
            # ========== 组件初始化 ==========
            client = Client(client_id=f"phase_{phase_name}_{config['short_name']}")
            edge = EdgeProcessor(edge_id=f"edge_{config['short_name']}")
            scheduler = SmartScheduler(cloud_nodes=CLOUD_NODES)
            
            # 预热调度器
            for _ in range(5):
                for node in CLOUD_NODES:
                    fake_latency = random.gauss(node.latency_mean, node.latency_std)
                    scheduler.update_stats(node.cloud_id, max(0, fake_latency))
            
            chunk_mode = 'fastcdc' if config['use_fastcdc'] else 'fixed'
            
            # ========== Phase 1: 处理 raw_data（预热/基准）==========
            print(f"\n[Phase 1] 📤 上传原始数据 (Version 1)...")
            
            # 端侧分块 + 块级 MLE 加密
            # Client 负责分块（FastCDC 或 Fixed）和加密
            # 返回加密块列表：[{data, size, fingerprint, key}, ...]
            encrypted_chunks_v1 = client.encrypt_data(raw_data, chunk_mode=chunk_mode)
            
            # 边缘处理（去重检测 + 冗余编码）
            # EdgeProcessor 只负责去重，不再分块
            chunks_v1 = edge.process(encrypted_chunks_v1)
            new_chunks_v1 = [c for c in chunks_v1 if c.status == ChunkStatus.NEW]
            
            print(f"      模式: {chunk_mode}")
            print(f"      总块数: {len(chunks_v1)}, 新块: {len(new_chunks_v1)}")
            
            # 模拟上传（并发）
            upload_time_v1 = _simulate_upload(
                new_chunks_v1, 
                scheduler if config['use_smart_scheduler'] else None,
                desc="      V1上传"
            )
            print(f"      V1 上传耗时: {upload_time_v1:.3f}s")
            
            # ========== Phase 2: 处理 data_v2（测试去重）==========
            print(f"\n[Phase 2] 📤 上传修改后数据 (Version 2)...")
            
            # 重置统计（保留指纹表）
            edge.reset_stats()
            
            start_time = time.time()
            
            # 端侧分块 + 块级 MLE 加密
            # 由于 MLE + 内容定义分块：
            # - 未修改的明文块 → 相同密文块 → 被 EdgeProcessor 去重
            # - 修改过的明文块 → 不同密文块 → 作为新块上传
            encrypted_chunks_v2 = client.encrypt_data(data_v2, chunk_mode=chunk_mode)
            
            # 边缘处理（去重检测）
            chunks_v2 = edge.process(encrypted_chunks_v2)
            
            new_chunks_v2 = [c for c in chunks_v2 if c.status == ChunkStatus.NEW]
            ref_chunks_v2 = [c for c in chunks_v2 if c.status == ChunkStatus.REF]
            
            # 计算去重率
            dedup_ratio = len(ref_chunks_v2) / len(chunks_v2) if chunks_v2 else 0
            
            print(f"      模式: {chunk_mode}")
            print(f"      总块数: {len(chunks_v2)}")
            print(f"      新块: {len(new_chunks_v2)}, 引用块: {len(ref_chunks_v2)}")
            print(f"      去重率: {dedup_ratio * 100:.2f}%")
            
            # 模拟上传（只上传新块）
            upload_time_v2 = _simulate_upload(
                new_chunks_v2,
                scheduler if config['use_smart_scheduler'] else None,
                desc="      V2上传"
            )
            
            total_time = time.time() - start_time
            print(f"      V2 上传耗时: {upload_time_v2:.3f}s")
            print(f"      总处理耗时: {total_time:.3f}s")
            
            total_time_sum += total_time
            dedup_ratio_sum += dedup_ratio
        
        # 计算平均值
        avg_time = total_time_sum / iterations
        avg_dedup = dedup_ratio_sum / iterations
        
        # 记录结果
        result = {
            "Phase": phase_name,
            "Method": config['name'],
            "Time(s)": round(avg_time, 3),
            "DedupRatio(%)": round(avg_dedup * 100, 2),
            "TotalChunks": len(chunks_v2),
            "NewChunks": len(new_chunks_v2),
            "RefChunks": len(ref_chunks_v2)
        }
        results.append(result)
        
        print(f"\n📊 {config['name']} 结果:")
        print(f"   平均耗时: {avg_time:.3f}s")
        print(f"   去重率: {avg_dedup * 100:.2f}%")
    
    # 打印对比总结
    print("\n" + "="*60)
    print("📈 Phase 结果对比")
    print("="*60)
    print(f"{'Method':<25} {'Time(s)':<12} {'Dedup(%)':<12}")
    print("-"*60)
    for r in results:
        print(f"{r['Method']:<25} {r['Time(s)']:<12.3f} {r['DedupRatio(%)']:<12.2f}")
    
    # 计算提升
    if len(results) >= 2:
        fixed_result = results[0]
        proposed_result = results[1]
        
        dedup_improvement = proposed_result['DedupRatio(%)'] - fixed_result['DedupRatio(%)']
        print("-"*60)
        print(f"🎯 FastCDC 去重率提升: +{dedup_improvement:.2f}%")
    
    return results


def _simulate_upload(
    chunks: List[ChunkInfo],
    scheduler: SmartScheduler = None,
    desc: str = "上传"
) -> float:
    """
    模拟并发上传（内部辅助函数）
    
    Args:
        chunks: 需要上传的数据块列表
        scheduler: 调度器（None 则随机选择）
        desc: 进度条描述
        
    Returns:
        上传耗时（秒）
    """
    if not chunks:
        return 0.0
    
    # 准备上传任务
    upload_tasks = []
    
    if scheduler:
        best_nodes = scheduler.select_best_nodes(k=3)
        best_node_list = [node for node, score in best_nodes]
    else:
        best_node_list = None
    
    task_idx = 0
    for chunk in chunks:
        for shard_idx, shard in enumerate(chunk.shards):
            if best_node_list:
                target_node = best_node_list[task_idx % len(best_node_list)]
            else:
                target_node = random.choice(CLOUD_NODES)
            
            filename = f"{chunk.fingerprint[:16]}_{shard_idx}.shard"
            upload_tasks.append((target_node, shard, filename))
            task_idx += 1
    
    if not upload_tasks:
        return 0.0
    
    # 线程安全锁
    stats_lock = threading.Lock()
    total_latency = 0.0
    
    def upload_shard(task):
        target_node, shard, filename = task
        result = target_node.upload(shard, filename)
        return result['latency']
    
    start_time = time.time()
    
    # 并发上传
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(upload_shard, task): task for task in upload_tasks}
        
        with tqdm(total=len(futures), desc=desc, unit="shard", leave=False) as pbar:
            for future in as_completed(futures):
                latency = future.result()
                with stats_lock:
                    total_latency += latency
                pbar.update(1)
    
    return time.time() - start_time


def run_single_experiment(
    exp_name: str,
    use_fastcdc: bool,
    use_smart_scheduler: bool,
    raw_data: bytes
) -> ExperimentResult:
    """
    运行单次消融实验
    
    Args:
        exp_name: 实验名称
        use_fastcdc: 是否使用 FastCDC（否则使用固定分块）
        use_smart_scheduler: 是否使用智能调度（否则随机选择）
        raw_data: 原始测试数据
        
    Returns:
        实验结果
    """
    print(f"\n{'='*60}")
    print(f"🧪 实验: {exp_name}")
    print(f"   FastCDC: {use_fastcdc}, SmartScheduler: {use_smart_scheduler}")
    print(f"{'='*60}")
    
    # ========== 组件初始化 ==========
    client = Client(client_id=f"ablation_{exp_name}")
    edge = EdgeProcessor(edge_id=f"edge_{exp_name}")
    scheduler = SmartScheduler(cloud_nodes=CLOUD_NODES)
    
    # 预热调度器
    for _ in range(5):
        for node in CLOUD_NODES:
            fake_latency = random.gauss(node.latency_mean, node.latency_std)
            scheduler.update_stats(node.cloud_id, max(0, fake_latency))
    
    # 统计变量
    cloud_distribution = {node.cloud_id: 0 for node in CLOUD_NODES}
    total_latency = 0.0
    uploaded_shards = 0
    
    start_time = time.time()
    
    # ========== Step 1: 加密 ==========
    print("\n[1/3] 🔐 客户端加密...")
    encrypted_data, key, nonce = client.encrypt_data(raw_data)
    
    # ========== Step 2: 边缘处理 ==========
    print("[2/3] 🔪 边缘处理...")
    chunk_mode = 'fastcdc' if use_fastcdc else 'fixed'
    chunks = edge.process(encrypted_data, mode=chunk_mode)
    
    new_chunks = [c for c in chunks if c.status == ChunkStatus.NEW]
    print(f"      模式: {chunk_mode}, 总块数: {len(chunks)}, 新块: {len(new_chunks)}")
    
    # ========== Step 3: 上传 ==========
    print("[3/3] ☁️  并发上传...")
    
    # 准备上传任务
    upload_tasks = []
    
    if use_smart_scheduler:
        # 智能调度：选择 Top 3 节点
        best_nodes = scheduler.select_best_nodes(k=3)
        best_node_list = [node for node, score in best_nodes]
        print(f"      智能调度选择: {[n.cloud_id for n in best_node_list]}")
    
    task_idx = 0
    for chunk in new_chunks:
        for shard_idx, shard in enumerate(chunk.shards):
            if use_smart_scheduler:
                target_node = best_node_list[task_idx % len(best_node_list)]
            else:
                target_node = random.choice(CLOUD_NODES)
            
            filename = f"{chunk.fingerprint[:16]}_{shard_idx}.shard"
            upload_tasks.append((target_node, shard, filename))
            task_idx += 1
    
    # 线程安全锁
    stats_lock = threading.Lock()
    
    def upload_shard(task):
        target_node, shard, filename = task
        result = target_node.upload(shard, filename)
        return {
            'cloud_id': target_node.cloud_id,
            'latency': result['latency'],
            'size': len(shard)
        }
    
    # 并发上传
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(upload_shard, task): task for task in upload_tasks}
        
        with tqdm(total=len(futures), desc="      上传进度", unit="shard") as pbar:
            for future in as_completed(futures):
                result = future.result()
                
                with stats_lock:
                    total_latency += result['latency']
                    uploaded_shards += 1
                    cloud_distribution[result['cloud_id']] += 1
                    
                    if use_smart_scheduler:
                        scheduler.update_stats(result['cloud_id'], result['latency'])
                
                pbar.update(1)
    
    upload_time = time.time() - start_time
    
    # ========== 去重率测试（第二次上传） ==========
    print("\n[去重测试] 修改 10% 数据后重新处理...")
    
    # 复制并修改中间 10%
    modified_data = bytearray(raw_data)
    data_len = len(modified_data)
    modify_size = int(data_len * 0.1)
    start_pos = (data_len - modify_size) // 2
    modified_data[start_pos:start_pos + modify_size] = os.urandom(modify_size)
    modified_data = bytes(modified_data)
    
    # 重置边缘处理器统计（保留指纹表）
    edge.reset_stats()
    
    # 处理修改后的数据（不加密，直接测试分块去重）
    chunks_second = edge.process(modified_data, mode=chunk_mode)
    
    # 计算去重率
    ref_chunks = [c for c in chunks_second if c.status == ChunkStatus.REF]
    dedup_ratio = len(ref_chunks) / len(chunks_second) if chunks_second else 0
    
    print(f"      总块数: {len(chunks_second)}, 引用块: {len(ref_chunks)}")
    print(f"      去重率: {dedup_ratio * 100:.2f}%")
    
    total_time = time.time() - start_time
    avg_latency = total_latency / uploaded_shards if uploaded_shards > 0 else 0
    
    # 创建结果
    result = ExperimentResult(
        exp_name=exp_name,
        total_time=total_time,
        avg_latency=avg_latency,
        dedup_ratio=dedup_ratio,
        total_chunks=len(chunks),
        uploaded_shards=uploaded_shards,
        cloud_distribution=cloud_distribution
    )
    
    print(f"\n📊 {exp_name} 结果:")
    print(f"   总耗时: {total_time:.3f}s")
    print(f"   平均延迟: {avg_latency*1000:.2f}ms")
    print(f"   去重率: {dedup_ratio*100:.2f}%")
    print(f"   云节点分布: {cloud_distribution}")
    
    return result


def save_results_to_csv(results: List[ExperimentResult], output_path: str = "experiment_results.csv"):
    """
    将实验结果保存为 CSV 文件
    """
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # 写入表头
        writer.writerow([
            'Experiment', 'Total Time (s)', 'Avg Latency (ms)', 
            'Dedup Ratio (%)', 'Total Chunks', 'Uploaded Shards',
            'Aliyun', 'Tencent', 'Huawei', 'Baidu'
        ])
        
        # 写入数据
        for r in results:
            writer.writerow([
                r.exp_name,
                f"{r.total_time:.3f}",
                f"{r.avg_latency*1000:.2f}",
                f"{r.dedup_ratio*100:.2f}",
                r.total_chunks,
                r.uploaded_shards,
                r.cloud_distribution.get('aliyun', 0),
                r.cloud_distribution.get('tencent', 0),
                r.cloud_distribution.get('huawei', 0),
                r.cloud_distribution.get('baidu', 0)
            ])
    
    print(f"\n💾 结果已保存至: {output_path}")


def print_comparison_table(results: List[ExperimentResult]):
    """
    打印对比分析表
    """
    print("\n" + "="*80)
    print("📈 消融实验对比分析")
    print("="*80)
    
    # 表头
    print(f"\n{'Experiment':<25} {'Time(s)':<10} {'Latency(ms)':<12} {'Dedup(%)':<10} {'Shards':<10}")
    print("-" * 80)
    
    # 数据行
    for r in results:
        print(f"{r.exp_name:<25} {r.total_time:<10.3f} {r.avg_latency*1000:<12.2f} "
              f"{r.dedup_ratio*100:<10.2f} {r.uploaded_shards:<10}")
    
    print("-" * 80)
    
    # 分析
    if len(results) >= 3:
        proposed = results[0]
        wo_scheduler = results[1]
        wo_fastcdc = results[2]
        
        print("\n🔍 关键发现:")
        
        # 调度器效果
        scheduler_effect = (wo_scheduler.total_time - proposed.total_time) / wo_scheduler.total_time * 100
        print(f"   • 智能调度器: 减少 {scheduler_effect:.1f}% 上传耗时 "
              f"({wo_scheduler.total_time:.2f}s → {proposed.total_time:.2f}s)")
        
        # FastCDC 效果
        fastcdc_effect = proposed.dedup_ratio - wo_fastcdc.dedup_ratio
        print(f"   • FastCDC: 提升 {fastcdc_effect*100:.1f}% 去重率 "
              f"({wo_fastcdc.dedup_ratio*100:.1f}% → {proposed.dedup_ratio*100:.1f}%)")


def main():
    """
    主函数：运行完整的消融实验
    
    实验流程：
    1. Phase 1: Micro-benchmark (合成数据, 5MB)
    2. Phase 2: Macro-benchmark (真实数据, real_docs文件夹)
    3. 保存所有结果到 CSV
    """
    print("╔" + "═"*62 + "╗")
    print("║" + " CE2S: Cloud-Edge-End Storage System ".center(62) + "║")
    print("║" + " Comprehensive Ablation Study ".center(62) + "║")
    print("║" + " 综合消融实验：FastCDC vs Fixed-size Chunking ".center(50) + "║")
    print("╚" + "═"*62 + "╝")
    
    all_results = []
    
    # ============================================================
    # Phase 1: Micro-benchmark (合成数据)
    # ============================================================
    print("\n")
    print("█" * 62)
    print("█" + " PHASE 1: Micro-benchmark (Synthetic Data) ".center(60) + "█")
    print("█" * 62)
    
    # 生成合成工作负载
    synthetic_data = generate_synthetic_workload(
        path="./test_data/synthetic.dat",
        size_mb=5.0
    )
    
    # 运行实验
    phase1_results = run_experiment_phase(
        phase_name="Micro-benchmark",
        raw_data=synthetic_data,
        iterations=1
    )
    all_results.extend(phase1_results)
    
    # ============================================================
    # Phase 2: Macro-benchmark (真实数据)
    # ============================================================
    print("\n")
    print("█" * 62)
    print("█" + " PHASE 2: Macro-benchmark (Real Data) ".center(60) + "█")
    print("█" * 62)
    
    real_docs_path = "./real_docs"
    
    if Path(real_docs_path).exists() and any(Path(real_docs_path).iterdir()):
        print(f"\n📂 发现真实数据文件夹: {real_docs_path}")
        
        # 加载真实数据集
        real_data = load_real_dataset(real_docs_path)
        
        # 运行实验
        phase2_results = run_experiment_phase(
            phase_name="Macro-benchmark",
            raw_data=real_data,
            iterations=1
        )
        all_results.extend(phase2_results)
    else:
        print(f"\n⚠️ 真实数据文件夹不存在或为空: {real_docs_path}")
        print("   跳过 Macro-benchmark 阶段")
        print("   提示：将 PDF/文档文件放入 real_docs 文件夹即可运行此阶段")
    
    # ============================================================
    # 保存结果到 CSV
    # ============================================================
    print("\n")
    print("█" * 62)
    print("█" + " RESULTS SUMMARY ".center(60) + "█")
    print("█" * 62)
    
    # 计算吞吐量并保存
    save_final_results(all_results, output_path="experiment_results_final.csv")
    
    # 打印最终汇总表
    print_final_summary(all_results)
    
    print("\n" + "="*62)
    print("✅ 所有实验完成!")
    print("   结果文件: experiment_results_final.csv")
    print("="*62)


def save_final_results(results: List[Dict], output_path: str = "experiment_results_final.csv"):
    """
    保存最终实验结果到 CSV
    
    CSV 字段：Phase, Method, Time(s), DedupRatio(%), Throughput(MB/s)
    """
    print(f"\n💾 保存结果到: {output_path}")
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # 写入表头
        writer.writerow([
            'Phase', 'Method', 'Time(s)', 'DedupRatio(%)', 
            'TotalChunks', 'NewChunks', 'RefChunks', 'Throughput(MB/s)'
        ])
        
        # 写入数据
        for r in results:
            # 估算数据大小（基于块数和平均块大小 8KB）
            estimated_size_mb = r.get('TotalChunks', 0) * 8 / 1024  # 8KB per chunk
            time_s = r.get('Time(s)', 1)
            throughput = estimated_size_mb / time_s if time_s > 0 else 0
            
            writer.writerow([
                r.get('Phase', ''),
                r.get('Method', ''),
                f"{r.get('Time(s)', 0):.3f}",
                f"{r.get('DedupRatio(%)', 0):.2f}",
                r.get('TotalChunks', 0),
                r.get('NewChunks', 0),
                r.get('RefChunks', 0),
                f"{throughput:.2f}"
            ])
    
    print(f"   ✅ 保存成功")


def print_final_summary(results: List[Dict]):
    """
    打印最终汇总表
    """
    print("\n" + "="*80)
    print("📊 实验结果汇总")
    print("="*80)
    
    # 表头
    print(f"\n{'Phase':<18} {'Method':<25} {'Time(s)':<10} {'Dedup(%)':<10} {'Chunks':<8}")
    print("-"*80)
    
    # 按 Phase 分组显示
    current_phase = None
    for r in results:
        phase = r.get('Phase', '')
        if phase != current_phase:
            if current_phase is not None:
                print("-"*80)
            current_phase = phase
        
        print(f"{phase:<18} {r.get('Method', ''):<25} "
              f"{r.get('Time(s)', 0):<10.3f} "
              f"{r.get('DedupRatio(%)', 0):<10.2f} "
              f"{r.get('TotalChunks', 0):<8}")
    
    print("-"*80)
    
    # 关键发现分析
    print("\n🔍 关键发现:")
    
    # 按 Phase 分析
    phases = set(r.get('Phase', '') for r in results)
    
    for phase in phases:
        phase_results = [r for r in results if r.get('Phase', '') == phase]
        
        fixed_result = next((r for r in phase_results if 'Fixed' in r.get('Method', '')), None)
        proposed_result = next((r for r in phase_results if 'Proposed' in r.get('Method', '')), None)
        
        if fixed_result and proposed_result:
            dedup_fixed = fixed_result.get('DedupRatio(%)', 0)
            dedup_proposed = proposed_result.get('DedupRatio(%)', 0)
            improvement = dedup_proposed - dedup_fixed
            
            print(f"\n   [{phase}]")
            print(f"   • Fixed-size Chunking 去重率: {dedup_fixed:.2f}%")
            print(f"   • FastCDC (Proposed) 去重率: {dedup_proposed:.2f}%")
            print(f"   • FastCDC 去重率提升: +{improvement:.2f}%")
            
            if improvement > 50:
                print(f"   ✨ FastCDC 在版本迭代场景下显著优于固定分块!")


if __name__ == "__main__":
    main()
