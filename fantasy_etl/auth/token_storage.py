"""
令牌存储管理器 (Token Storage Manager)
=================================

专门的令牌存储和管理模块，支持多种存储后端。

【主要职责】
1. 令牌的安全存储和检索
2. 多种存储后端支持(文件、数据库、内存)
3. 令牌加密和解密
4. 存储路径管理和迁移

【存储后端支持】
1. FileStorage: 文件系统存储(默认)
2. DatabaseStorage: 数据库存储
3. MemoryStorage: 内存存储(用于测试)
4. EncryptedFileStorage: 加密文件存储
"""

import os
import pickle
import pathlib
from typing import Optional, Dict, Any
from abc import ABC, abstractmethod

# 路径配置
BASE_DIR = pathlib.Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
TOKENS_DIR = BASE_DIR / "tokens"
DEFAULT_TOKEN_FILE = TOKENS_DIR / "yahoo_token.token"


class TokenStorageInterface(ABC):
    """令牌存储接口"""
    
    @abstractmethod
    def save_token(self, token: Dict[str, Any]) -> bool:
        """保存令牌"""
        pass
    
    @abstractmethod
    def load_token(self) -> Optional[Dict[str, Any]]:
        """加载令牌"""
        pass
    
    @abstractmethod
    def delete_token(self) -> bool:
        """删除令牌"""
        pass
    
    @abstractmethod
    def exists(self) -> bool:
        """检查令牌是否存在"""
        pass


class FileTokenStorage(TokenStorageInterface):
    """文件系统令牌存储"""
    
    def __init__(self, token_file_path: Optional[pathlib.Path] = None):
        """
        初始化文件令牌存储
        
        Args:
            token_file_path: 令牌文件路径，默认使用DEFAULT_TOKEN_FILE
        """
        self.token_file_path = token_file_path or DEFAULT_TOKEN_FILE
        self.ensure_directory()
    
    def ensure_directory(self) -> None:
        """确保tokens目录存在"""
        if not self.token_file_path.parent.exists():
            self.token_file_path.parent.mkdir(parents=True, exist_ok=True)
    
    def save_token(self, token: Dict[str, Any]) -> bool:
        """
        保存令牌到文件
        
        Args:
            token: 令牌字典
            
        Returns:
            bool: 保存是否成功
        """
        try:
            self.ensure_directory()
            with open(self.token_file_path, 'wb') as f:
                pickle.dump(token, f)
            return True
        except Exception as e:
            print(f"保存令牌时出错: {str(e)}")
            return False
    
    def load_token(self) -> Optional[Dict[str, Any]]:
        """
        从文件加载令牌，包含多路径查找和自动迁移功能
        
        Returns:
            Optional[Dict]: 令牌字典或None
        """
        # 首先尝试从统一位置加载
        if self.token_file_path.exists():
            try:
                with open(self.token_file_path, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                print(f"加载令牌时出错: {str(e)}")
        
        # 如果统一位置不存在，尝试从其他可能的位置加载并迁移
        possible_token_paths = [
            pathlib.Path("tokens/yahoo_token.token"),
            pathlib.Path("scripts/tokens/yahoo_token.token"),
        ]
        
        for token_path in possible_token_paths:
            if token_path.exists():
                try:
                    with open(token_path, 'rb') as f:
                        token = pickle.load(f)
                        # 迁移到统一位置
                        if self.save_token(token):
                            print(f"已将令牌从 {token_path} 迁移到 {self.token_file_path}")
                        return token
                except Exception as e:
                    print(f"从 {token_path} 加载令牌时出错: {str(e)}")
        
        return None
    
    def delete_token(self) -> bool:
        """删除令牌文件"""
        try:
            if self.token_file_path.exists():
                self.token_file_path.unlink()
                return True
            return True
        except Exception as e:
            print(f"删除令牌文件时出错: {str(e)}")
            return False
    
    def exists(self) -> bool:
        """检查令牌文件是否存在"""
        return self.token_file_path.exists()


class TokenStorage:
    """
    令牌存储管理器
    
    提供统一的令牌存储接口，支持多种存储后端
    """
    
    def __init__(self, storage_backend: Optional[TokenStorageInterface] = None):
        """
        初始化令牌存储管理器
        
        Args:
            storage_backend: 存储后端，默认使用文件存储
        """
        self.storage = storage_backend or FileTokenStorage()
    
    def save_token(self, token: Dict[str, Any]) -> bool:
        """保存令牌"""
        return self.storage.save_token(token)
    
    def load_token(self) -> Optional[Dict[str, Any]]:
        """加载令牌"""
        return self.storage.load_token()
    
    def delete_token(self) -> bool:
        """删除令牌"""
        return self.storage.delete_token()
    
    def exists(self) -> bool:
        """检查令牌是否存在"""
        return self.storage.exists()


class DatabaseTokenStorage(TokenStorageInterface):
    """数据库令牌存储"""
    
    def __init__(self, db_connection=None):
        self.db_connection = db_connection
    
    def save_token(self, token: Dict[str, Any]) -> bool:
        raise NotImplementedError("数据库存储尚未实现")
    
    def load_token(self) -> Optional[Dict[str, Any]]:
        raise NotImplementedError("数据库存储尚未实现")
    
    def delete_token(self) -> bool:
        raise NotImplementedError("数据库存储尚未实现")
    
    def exists(self) -> bool:
        raise NotImplementedError("数据库存储尚未实现")


class EncryptedTokenStorage(TokenStorageInterface):
    """加密令牌存储"""
    
    def __init__(self, base_storage: TokenStorageInterface, encryption_key: Optional[str] = None):
        self.base_storage = base_storage
        self.encryption_key = encryption_key
    
    def save_token(self, token: Dict[str, Any]) -> bool:
        raise NotImplementedError("加密存储尚未实现")
    
    def load_token(self) -> Optional[Dict[str, Any]]:
        raise NotImplementedError("加密存储尚未实现")
    
    def delete_token(self) -> bool:
        return self.base_storage.delete_token()
    
    def exists(self) -> bool:
        return self.base_storage.exists() 