"""
客户端模块
实现 Message-Locked Encryption (MLE) / 收敛加密 + 端侧分块

架构设计：端侧分块 + 块级 MLE
- 端侧（Client）负责分块和加密
- 每个块独立进行 MLE 加密
- 相同明文块 → 相同密文块 → 支持密文级别去重

MLE 特点：
- 密钥由数据内容本身派生（确定性加密）
- 相同的明文产生相同的密文
- 支持密文级别的去重（加密后仍可去重）
"""

import hashlib
import io
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple, Dict, Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

# FastCDC 可选导入
try:
    from fastcdc import fastcdc
    FASTCDC_AVAILABLE = True
except ImportError:
    FASTCDC_AVAILABLE = False


@dataclass
class EncryptedChunk:
    """加密后的数据块"""
    data: bytes          # 加密后的数据（nonce + ciphertext）
    original_size: int   # 原始明文大小
    fingerprint: str     # 明文指纹（用于去重索引）
    key: bytes           # MLE 密钥（用于解密）


class Client:
    """
    客户端类：实现端侧分块 + 块级 MLE
    
    工作流程：
    1. 对明文数据进行分块（FastCDC 或固定大小）
    2. 对每个块独立进行 MLE 加密
    3. 返回加密块列表，支持后续去重
    
    收敛加密原理：
    - 使用数据内容的哈希值派生加密密钥
    - 相同数据 → 相同密钥 → 相同密文
    - 支持在加密后进行数据去重
    
    安全特性：
    - AES-256-GCM 认证加密
    - 提供机密性和完整性保护
    
    注意：MLE 对于可预测内容存在字典攻击风险，
    适用于文件备份等场景，不适用于低熵数据加密。
    """
    
    # AES-256 密钥长度（字节）
    KEY_SIZE = 32
    # GCM 推荐 Nonce 长度（字节）
    NONCE_SIZE = 12
    
    # FastCDC 分块参数
    CHUNK_MIN = 4096      # 4 KB
    CHUNK_AVG = 8192      # 8 KB
    CHUNK_MAX = 16384     # 16 KB
    
    # 固定分块大小（用于消融实验）
    FIXED_CHUNK_SIZE = 8192  # 8 KB
    
    def __init__(self, client_id: str = "default_client"):
        """
        初始化客户端
        
        Args:
            client_id: 客户端唯一标识
        """
        self.client_id = client_id
    
    def _derive_key_and_nonce(self, data: bytes) -> Tuple[bytes, bytes]:
        """
        从数据内容派生密钥和 Nonce（MLE 核心）
        
        使用 SHA-512 计算数据哈希，然后：
        - Key: 取前 32 字节（256 bits）
        - Nonce: 取第 32-44 字节（96 bits）
        
        这确保相同的数据总是产生相同的 Key 和 Nonce。
        
        Args:
            data: 原始数据
            
        Returns:
            Tuple[key, nonce]: 派生的密钥和 Nonce
        """
        # 使用 SHA-512 获取足够长的哈希值（64 字节）
        # SHA-256 只有 32 字节，不足以同时提供 Key 和 Nonce
        hash_digest = hashlib.sha512(data).digest()
        
        # Key: 前 32 字节
        key = hash_digest[:self.KEY_SIZE]
        
        # Nonce: 第 32-44 字节（共 12 字节）
        nonce = hash_digest[self.KEY_SIZE:self.KEY_SIZE + self.NONCE_SIZE]
        
        return key, nonce
    
    def _chunk_data(self, data: bytes, mode: str = 'fastcdc') -> List[bytes]:
        """
        对数据进行分块
        
        支持两种分块模式：
        - fastcdc: 内容定义分块，边界由内容决定
        - fixed: 固定大小分块，用于消融实验对照
        
        Args:
            data: 要分块的原始数据
            mode: 分块模式，'fastcdc' 或 'fixed'
            
        Returns:
            List[bytes]: 分块后的数据列表
        """
        if mode == 'fastcdc':
            return self._chunk_fastcdc(data)
        elif mode == 'fixed':
            return self._chunk_fixed(data)
        else:
            raise ValueError(f"Unknown chunk mode: {mode}. Use 'fastcdc' or 'fixed'.")
    
    def _chunk_fastcdc(self, data: bytes) -> List[bytes]:
        """
        使用 FastCDC 进行内容定义分块
        
        Args:
            data: 原始数据
            
        Returns:
            List[bytes]: 分块列表
        """
        if not FASTCDC_AVAILABLE:
            print("Warning: fastcdc not available, falling back to fixed chunking")
            return self._chunk_fixed(data)
        
        chunks = []
        for chunk in fastcdc(
            data,
            min_size=self.CHUNK_MIN,
            avg_size=self.CHUNK_AVG,
            max_size=self.CHUNK_MAX
        ):
            chunks.append(data[chunk.offset:chunk.offset + chunk.length])
        
        return chunks
    
    def _chunk_fixed(self, data: bytes) -> List[bytes]:
        """
        使用固定大小分块（消融实验对照）
        
        Args:
            data: 原始数据
            
        Returns:
            List[bytes]: 分块列表
        """
        chunks = []
        offset = 0
        while offset < len(data):
            chunk = data[offset:offset + self.FIXED_CHUNK_SIZE]
            chunks.append(chunk)
            offset += self.FIXED_CHUNK_SIZE
        
        return chunks
    
    def _encrypt_chunk(self, chunk_data: bytes) -> EncryptedChunk:
        """
        对单个块进行 MLE 加密
        
        Args:
            chunk_data: 明文块数据
            
        Returns:
            EncryptedChunk: 加密后的块信息
        """
        # 从块内容派生 Key 和 Nonce
        key, nonce = self._derive_key_and_nonce(chunk_data)
        
        # 计算明文指纹（用于去重索引）
        fingerprint = hashlib.sha256(chunk_data).hexdigest()
        
        # AES-GCM 加密
        aesgcm = AESGCM(key)
        ciphertext = aesgcm.encrypt(nonce, chunk_data, None)
        
        # nonce + ciphertext
        encrypted_data = nonce + ciphertext
        
        return EncryptedChunk(
            data=encrypted_data,
            original_size=len(chunk_data),
            fingerprint=fingerprint,
            key=key
        )
    
    def encrypt_data(self, data: bytes, chunk_mode: str = 'fastcdc') -> List[Dict[str, Any]]:
        """
        端侧分块 + 块级 MLE 加密
        
        工作流程：
        1. 使用指定模式对数据分块
        2. 对每个块独立进行 MLE 加密
        3. 返回加密块列表
        
        Args:
            data: 要加密的原始数据
            chunk_mode: 分块模式，'fastcdc' 或 'fixed'
            
        Returns:
            List[Dict]: 加密块列表，每个元素包含：
                - 'data': 加密后的字节数据（nonce + ciphertext）
                - 'size': 原始明文大小
                - 'fingerprint': 明文指纹（SHA-256）
                - 'key': MLE 密钥
        """
        # Step 1: 分块
        chunks = self._chunk_data(data, mode=chunk_mode)
        
        # Step 2: 逐块 MLE 加密
        encrypted_chunks = []
        for chunk_data in chunks:
            enc_chunk = self._encrypt_chunk(chunk_data)
            encrypted_chunks.append({
                'data': enc_chunk.data,
                'size': enc_chunk.original_size,
                'fingerprint': enc_chunk.fingerprint,
                'key': enc_chunk.key
            })
        
        return encrypted_chunks
    
    def encrypt_data_raw(self, data: bytes) -> Tuple[bytes, bytes]:
        """
        对整个数据块进行 MLE 加密（不分块，兼容旧接口）
        
        Args:
            data: 要加密的原始数据
            
        Returns:
            Tuple[encrypted_data, key]:
                - encrypted_data: nonce + ciphertext
                - key: 派生的 AES-256 密钥
        """
        key, nonce = self._derive_key_and_nonce(data)
        aesgcm = AESGCM(key)
        ciphertext = aesgcm.encrypt(nonce, data, None)
        encrypted_data = nonce + ciphertext
        return encrypted_data, key
    
    def encrypt_file(self, file_path: str, chunk_mode: str = 'fastcdc') -> List[Dict[str, Any]]:
        """
        读取并加密文件
        
        Args:
            file_path: 要加密的文件路径
            chunk_mode: 分块模式
            
        Returns:
            List[Dict]: 加密块列表
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        with open(file_path, 'rb') as f:
            plaintext = f.read()
        
        return self.encrypt_data(plaintext, chunk_mode=chunk_mode)
    
    def decrypt_chunk(self, encrypted_data: bytes, key: bytes) -> bytes:
        """
        解密单个加密块
        
        Args:
            encrypted_data: nonce + ciphertext
            key: MLE 密钥
            
        Returns:
            解密后的明文
        """
        nonce = encrypted_data[:self.NONCE_SIZE]
        ciphertext = encrypted_data[self.NONCE_SIZE:]
        
        aesgcm = AESGCM(key)
        plaintext = aesgcm.decrypt(nonce, ciphertext, None)
        
        return plaintext
    
    def decrypt_chunks(self, encrypted_chunks: List[Dict[str, Any]]) -> bytes:
        """
        解密块列表并重组数据
        
        Args:
            encrypted_chunks: encrypt_data 返回的加密块列表
            
        Returns:
            解密并重组后的完整数据
        """
        plaintext_parts = []
        for chunk in encrypted_chunks:
            decrypted = self.decrypt_chunk(chunk['data'], chunk['key'])
            plaintext_parts.append(decrypted)
        
        return b''.join(plaintext_parts)
    
    def verify_mle_property(self, data: bytes, chunk_mode: str = 'fastcdc') -> bool:
        """
        验证 MLE 属性：相同数据产生相同密文
        
        Args:
            data: 测试数据
            chunk_mode: 分块模式
            
        Returns:
            True 如果 MLE 属性成立
        """
        chunks1 = self.encrypt_data(data, chunk_mode=chunk_mode)
        chunks2 = self.encrypt_data(data, chunk_mode=chunk_mode)
        
        if len(chunks1) != len(chunks2):
            return False
        
        for c1, c2 in zip(chunks1, chunks2):
            if c1['data'] != c2['data'] or c1['fingerprint'] != c2['fingerprint']:
                return False
        
        return True
    
    def get_content_hash(self, data: bytes) -> str:
        """
        获取数据的内容哈希（用于去重索引）
        
        Args:
            data: 数据
            
        Returns:
            SHA-256 十六进制哈希字符串
        """
        return hashlib.sha256(data).hexdigest()
    
    def get_chunk_stats(self, data: bytes, chunk_mode: str = 'fastcdc') -> Dict[str, Any]:
        """
        获取分块统计信息
        
        Args:
            data: 数据
            chunk_mode: 分块模式
            
        Returns:
            分块统计信息
        """
        chunks = self._chunk_data(data, mode=chunk_mode)
        sizes = [len(c) for c in chunks]
        
        return {
            'mode': chunk_mode,
            'total_chunks': len(chunks),
            'total_size': len(data),
            'min_chunk_size': min(sizes) if sizes else 0,
            'max_chunk_size': max(sizes) if sizes else 0,
            'avg_chunk_size': sum(sizes) / len(sizes) if sizes else 0
        }
    
    def __repr__(self):
        return f"Client(id={self.client_id}, mode=MLE, fastcdc={FASTCDC_AVAILABLE})"


# 默认客户端实例
client = Client()


if __name__ == "__main__":
    # 测试代码
    print("=== 端侧分块 + 块级 MLE 测试 ===")
    print(f"FastCDC 可用: {FASTCDC_AVAILABLE}")
    
    # 生成测试数据（100 KB）
    import os
    test_data = os.urandom(100 * 1024)
    print(f"\n测试数据大小: {len(test_data)} bytes")
    
    # 分块统计
    print("\n--- 分块统计 ---")
    for mode in ['fastcdc', 'fixed']:
        stats = client.get_chunk_stats(test_data, chunk_mode=mode)
        print(f"\n{mode.upper()} 模式:")
        print(f"  总块数: {stats['total_chunks']}")
        print(f"  块大小: {stats['min_chunk_size']}-{stats['max_chunk_size']} bytes")
        print(f"  平均块大小: {stats['avg_chunk_size']:.0f} bytes")
    
    # 加密测试
    print("\n--- 块级 MLE 加密 ---")
    encrypted_chunks = client.encrypt_data(test_data, chunk_mode='fastcdc')
    print(f"加密块数: {len(encrypted_chunks)}")
    print(f"第一块:")
    print(f"  原始大小: {encrypted_chunks[0]['size']} bytes")
    print(f"  密文大小: {len(encrypted_chunks[0]['data'])} bytes")
    print(f"  指纹: {encrypted_chunks[0]['fingerprint'][:16]}...")
    
    # MLE 属性验证
    print("\n--- MLE 属性验证 ---")
    encrypted_chunks2 = client.encrypt_data(test_data, chunk_mode='fastcdc')
    all_match = all(
        c1['data'] == c2['data'] and c1['fingerprint'] == c2['fingerprint']
        for c1, c2 in zip(encrypted_chunks, encrypted_chunks2)
    )
    print(f"相同数据两次加密:")
    print(f"  块数相同: {len(encrypted_chunks) == len(encrypted_chunks2)}")
    print(f"  所有密文相同: {all_match}")
    print(f"  MLE 属性: {'✓ 成立' if client.verify_mle_property(test_data) else '✗ 失败'}")
    
    # 解密验证
    print("\n--- 解密验证 ---")
    decrypted = client.decrypt_chunks(encrypted_chunks)
    print(f"解密后大小: {len(decrypted)} bytes")
    print(f"验证结果: {'✓ 成功' if decrypted == test_data else '✗ 失败'}")
    
    # 去重演示
    print("\n--- 去重演示 ---")
    # 创建包含重复块的数据
    repeat_block = os.urandom(8192)
    data_with_dups = repeat_block * 10  # 10 个相同的块
    chunks = client.encrypt_data(data_with_dups, chunk_mode='fixed')
    fingerprints = [c['fingerprint'] for c in chunks]
    unique_fps = set(fingerprints)
    print(f"总块数: {len(chunks)}")
    print(f"唯一指纹数: {len(unique_fps)}")
    print(f"去重率: {(1 - len(unique_fps) / len(chunks)) * 100:.1f}%")
