"""
边缘计算核心模块
实现去重检测和混合冗余策略

架构变更：
- 分块逻辑已移至客户端（Client）
- 边缘节点仅负责去重检测和冗余编码
- 输入为客户端加密后的分块列表
"""

import hashlib
from typing import List, Dict, Tuple, Optional, Any
from dataclasses import dataclass, field
from enum import Enum

from reedsolo import RSCodec


class ChunkStatus(Enum):
    """数据块状态"""
    NEW = "NEW"      # 新块，需要存储
    REF = "REF"      # 引用块，已存在


class RedundancyType(Enum):
    """冗余策略类型"""
    REPLICATION = "REPLICATION"    # 多副本
    ERASURE_CODE = "ERASURE_CODE"  # 纠删码


@dataclass
class ChunkInfo:
    """数据块信息"""
    chunk_id: int                          # 块序号
    size: int                              # 原始明文大小（字节）
    fingerprint: str                       # 指纹（用于去重）
    status: ChunkStatus                    # NEW 或 REF
    redundancy_type: Optional[RedundancyType] = None  # 冗余策略
    data: Optional[bytes] = None           # 加密数据（REF 块为 None）
    shards: List[bytes] = field(default_factory=list)  # 冗余分片


class EdgeProcessor:
    """
    边缘处理器：实现去重检测和混合冗余编码
    
    工作流程：
    1. 接收客户端发送的加密分块列表
    2. 使用指纹进行去重检测
    3. 对新块应用混合冗余策略
    
    核心功能：
    - 指纹去重：避免重复存储
    - 混合冗余：小块多副本 + 大块纠删码
    """
    
    # 冗余策略阈值
    EC_THRESHOLD = 8 * 1024  # 8KB，大于此值使用纠删码
    
    # Reed-Solomon 参数 (4数据 + 2校验)
    RS_DATA_SHARDS = 4
    RS_PARITY_SHARDS = 2
    
    def __init__(self, edge_id: str = "edge_01"):
        """
        初始化边缘处理器
        
        Args:
            edge_id: 边缘节点标识
        """
        self.edge_id = edge_id
        
        # 全局指纹表：{fingerprint: encrypted_data}
        self.fingerprint_table: Dict[str, bytes] = {}
        
        # Reed-Solomon 编解码器
        self.rs_codec = RSCodec(self.RS_PARITY_SHARDS)
        
        # 统计信息
        self.stats = {
            "total_chunks": 0,
            "new_chunks": 0,
            "ref_chunks": 0,
            "replication_chunks": 0,
            "erasure_code_chunks": 0,
            "bytes_saved": 0  # 去重节省的字节
        }
    
    def _compute_fingerprint(self, data: bytes) -> str:
        """计算数据的 SHA-256 指纹"""
        return hashlib.sha256(data).hexdigest()
    
    def _deduplicate(self, fingerprint: str, encrypted_data: bytes) -> ChunkStatus:
        """
        去重检测
        
        Args:
            fingerprint: 数据指纹
            encrypted_data: 加密后的数据
            
        Returns:
            ChunkStatus: NEW 或 REF
        """
        if fingerprint in self.fingerprint_table:
            return ChunkStatus.REF
        else:
            # 存入指纹表
            self.fingerprint_table[fingerprint] = encrypted_data
            return ChunkStatus.NEW
    
    def _apply_replication(self, data: bytes, replicas: int = 3) -> List[bytes]:
        """
        多副本冗余策略
        
        Args:
            data: 数据
            replicas: 副本数量
            
        Returns:
            副本列表
        """
        return [data] * replicas
    
    def _apply_erasure_coding(self, data: bytes) -> List[bytes]:
        """
        RS 纠删码冗余策略 (4,2)
        
        将数据编码为 4 个数据分片 + 2 个校验分片
        任意丢失 2 个分片仍可恢复
        
        Args:
            data: 数据
            
        Returns:
            分片列表（4数据 + 2校验 = 6个分片）
        """
        encoded = self.rs_codec.encode(data)
        
        total_shards = self.RS_DATA_SHARDS + self.RS_PARITY_SHARDS
        shard_size = (len(encoded) + total_shards - 1) // total_shards
        
        shards = []
        for i in range(total_shards):
            start = i * shard_size
            end = min(start + shard_size, len(encoded))
            shard = encoded[start:end]
            if len(shard) < shard_size:
                shard = shard + bytes(shard_size - len(shard))
            shards.append(shard)
        
        return shards
    
    def _select_redundancy(self, chunk_size: int) -> RedundancyType:
        """
        选择冗余策略
        
        策略：
        - 小块 (< 8KB)：多副本，减少编码开销
        - 大块 (>= 8KB)：纠删码，提高存储效率
        """
        if chunk_size < self.EC_THRESHOLD:
            return RedundancyType.REPLICATION
        else:
            return RedundancyType.ERASURE_CODE
    
    def process(self, encrypted_chunks: List[Dict[str, Any]]) -> List[ChunkInfo]:
        """
        处理客户端发送的加密分块列表
        
        工作流程：
        1. 遍历每个加密块
        2. 使用指纹进行去重检测
        3. 对新块应用冗余策略
        
        Args:
            encrypted_chunks: 客户端加密后的分块列表，每个元素包含：
                - 'data': 加密数据（bytes）
                - 'size': 原始明文大小（int）
                - 'fingerprint': 明文指纹（str）
                - 'key': MLE 密钥（bytes，可选）
            
        Returns:
            List[ChunkInfo]: 处理后的数据块信息列表
        """
        results = []
        
        for chunk_id, chunk in enumerate(encrypted_chunks):
            encrypted_data = chunk['data']
            original_size = chunk['size']
            fingerprint = chunk['fingerprint']
            
            self.stats["total_chunks"] += 1
            
            # Step 1: 去重检测
            status = self._deduplicate(fingerprint, encrypted_data)
            
            chunk_info = ChunkInfo(
                chunk_id=chunk_id,
                size=original_size,
                fingerprint=fingerprint,
                status=status
            )
            
            if status == ChunkStatus.REF:
                # 引用块：已存在，无需重复存储
                self.stats["ref_chunks"] += 1
                self.stats["bytes_saved"] += len(encrypted_data)
            else:
                # 新块：应用冗余策略
                self.stats["new_chunks"] += 1
                chunk_info.data = encrypted_data
                
                # Step 2: 选择并应用冗余策略（基于原始大小）
                redundancy_type = self._select_redundancy(original_size)
                chunk_info.redundancy_type = redundancy_type
                
                if redundancy_type == RedundancyType.REPLICATION:
                    chunk_info.shards = self._apply_replication(encrypted_data)
                    self.stats["replication_chunks"] += 1
                else:
                    chunk_info.shards = self._apply_erasure_coding(encrypted_data)
                    self.stats["erasure_code_chunks"] += 1
            
            results.append(chunk_info)
        
        return results
    
    def process_data(self, encrypted_chunks: List[Dict[str, Any]]) -> Tuple[List[bytes], int]:
        """
        处理加密分块并返回需要上传的分片列表
        
        Args:
            encrypted_chunks: 客户端加密后的分块列表
            
        Returns:
            Tuple[processed_shards, total_size]:
                - processed_shards: 所有需要上传的分片列表
                - total_size: 分片总大小（字节）
        """
        chunk_infos = self.process(encrypted_chunks)
        
        processed_shards = []
        total_size = 0
        
        for chunk_info in chunk_infos:
            if chunk_info.status == ChunkStatus.NEW:
                for shard in chunk_info.shards:
                    processed_shards.append(shard)
                    total_size += len(shard)
        
        return processed_shards, total_size
    
    def decode_erasure_shards(self, shards: List[bytes], original_size: int) -> bytes:
        """
        从纠删码分片恢复原始数据
        
        Args:
            shards: 分片列表
            original_size: 原始数据大小
            
        Returns:
            恢复的原始数据
        """
        encoded = b''.join(shards)
        decoded = self.rs_codec.decode(encoded)
        return bytes(decoded[:original_size])
    
    def get_chunk_by_fingerprint(self, fingerprint: str) -> Optional[bytes]:
        """通过指纹获取数据块"""
        return self.fingerprint_table.get(fingerprint)
    
    def get_stats(self) -> Dict:
        """获取处理统计信息"""
        stats = self.stats.copy()
        total_stored = sum(len(v) for v in self.fingerprint_table.values())
        stats["dedup_ratio"] = (
            stats["bytes_saved"] / (stats["bytes_saved"] + total_stored)
            if (stats["bytes_saved"] + total_stored) > 0 else 0
        )
        return stats
    
    def reset_stats(self) -> None:
        """重置统计信息（保留指纹表）"""
        self.stats = {
            "total_chunks": 0,
            "new_chunks": 0,
            "ref_chunks": 0,
            "replication_chunks": 0,
            "erasure_code_chunks": 0,
            "bytes_saved": 0
        }
    
    def clear_fingerprint_table(self) -> None:
        """清空指纹表"""
        self.fingerprint_table.clear()
    
    def __repr__(self):
        return (f"EdgeProcessor(id={self.edge_id}, "
                f"chunks={self.stats['total_chunks']}, "
                f"dedup_saved={self.stats['bytes_saved']}B)")


# 默认边缘处理器实例
edge_processor = EdgeProcessor()


if __name__ == "__main__":
    import os
    
    print("=== EdgeProcessor 测试（新架构）===")
    print("模拟客户端发送加密分块列表")
    
    # 模拟客户端发送的加密分块
    # 实际场景由 Client.encrypt_data() 生成
    def mock_encrypted_chunk(data: bytes) -> Dict[str, Any]:
        """模拟加密块"""
        fingerprint = hashlib.sha256(data).hexdigest()
        # 模拟加密（实际为 nonce + ciphertext）
        encrypted = b'nonce_12b' + data + b'tag_16bytes_____'
        return {
            'data': encrypted,
            'size': len(data),
            'fingerprint': fingerprint,
            'key': b'key_32bytes_____________________'
        }
    
    # 创建测试数据：包含重复块
    block1 = os.urandom(8 * 1024)  # 8KB
    block2 = os.urandom(4 * 1024)  # 4KB
    block3 = block1  # 重复块
    
    encrypted_chunks = [
        mock_encrypted_chunk(block1),
        mock_encrypted_chunk(block2),
        mock_encrypted_chunk(block3),  # 与 block1 相同
    ]
    
    print(f"\n输入加密块数: {len(encrypted_chunks)}")
    
    # 处理
    processor = EdgeProcessor()
    results = processor.process(encrypted_chunks)
    
    print(f"\n--- 处理结果 ---")
    for chunk in results:
        status_icon = "🔗" if chunk.status == ChunkStatus.REF else "🆕"
        redundancy = chunk.redundancy_type.value if chunk.redundancy_type else "N/A"
        shards_info = f"{len(chunk.shards)} shards" if chunk.shards else "N/A"
        print(f"  Chunk {chunk.chunk_id}: 原始{chunk.size}B, "
              f"{status_icon} {chunk.status.value}, "
              f"冗余={redundancy}, {shards_info}")
    
    print(f"\n--- 统计信息 ---")
    stats = processor.get_stats()
    print(f"总块数: {stats['total_chunks']}")
    print(f"新块: {stats['new_chunks']}, 引用块: {stats['ref_chunks']}")
    print(f"多副本: {stats['replication_chunks']}, 纠删码: {stats['erasure_code_chunks']}")
    print(f"去重节省: {stats['bytes_saved']} bytes")
    print(f"去重率: {stats['dedup_ratio']*100:.2f}%")
