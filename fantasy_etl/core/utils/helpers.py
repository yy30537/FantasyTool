"""
工具函数模块
提供通用的辅助函数
"""
import time
from datetime import datetime, date
from typing import Optional, Any, Dict, List
from functools import wraps

def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff_factor: float = 2.0):
    """重试装饰器"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        wait_time = delay * (backoff_factor ** attempt)
                        print(f"⚠️ 操作失败，{wait_time:.1f}秒后重试... (尝试 {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                    else:
                        print(f"❌ 所有重试失败，最后错误: {e}")
            
            raise last_exception
        return wrapper
    return decorator

def safe_get(data: Dict, *keys, default: Any = None) -> Any:
    """安全获取嵌套字典值"""
    try:
        result = data
        for key in keys:
            if isinstance(result, dict):
                result = result.get(key)
            elif isinstance(result, list) and isinstance(key, int):
                result = result[key] if 0 <= key < len(result) else None
            else:
                return default
            
            if result is None:
                return default
        
        return result
    except (KeyError, IndexError, TypeError):
        return default

def parse_yahoo_date(date_str: str) -> Optional[datetime]:
    """解析Yahoo日期格式"""
    if not date_str:
        return None
    
    formats = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y"]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    return None

def format_number(value: Any) -> Optional[int]:
    """格式化数字"""
    if value is None or value == "":
        return None
    
    try:
        if isinstance(value, str):
            # 处理 "12/34" 这样的格式
            if "/" in value:
                parts = value.split("/")
                return int(parts[0].strip()) if parts[0].strip() else None
            # 处理百分比
            if "%" in value:
                return float(value.replace("%", "").strip())
        
        return int(float(value))
    except (ValueError, TypeError):
        return None

def format_percentage(value: Any) -> Optional[float]:
    """格式化百分比"""
    if value is None or value == "":
        return None
    
    try:
        if isinstance(value, str):
            if "%" in value:
                return float(value.replace("%", "").strip()) / 100
        
        return float(value)
    except (ValueError, TypeError):
        return None

def chunk_list(data: List[Any], chunk_size: int) -> List[List[Any]]:
    """将列表分块"""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

def print_progress(current: int, total: int, prefix: str = "进度"):
    """打印进度条"""
    if total == 0:
        return
    
    percent = (current / total) * 100
    bar_length = 30
    filled_length = int(bar_length * current // total)
    bar = "█" * filled_length + "░" * (bar_length - filled_length)
    
    print(f"\\r{prefix}: [{bar}] {percent:.1f}% ({current}/{total})", end="", flush=True)
    
    if current == total:
        print()  # 换行

def measure_time(func):
    """测量函数执行时间的装饰器"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        
        execution_time = end_time - start_time
        print(f"⏱️ {func.__name__} 执行时间: {execution_time:.2f}秒")
        
        return result
    return wrapper

def validate_league_key(league_key: str) -> bool:
    """验证联盟key格式"""
    if not league_key:
        return False
    
    # Yahoo联盟key格式通常是: game_key.l.league_id
    parts = league_key.split(".")
    return len(parts) >= 3 and parts[1] == "l"

def validate_player_key(player_key: str) -> bool:
    """验证球员key格式"""
    if not player_key:
        return False
    
    # Yahoo球员key格式通常是: game_key.p.player_id
    parts = player_key.split(".")
    return len(parts) >= 3 and parts[1] == "p"

def clean_team_name(name: str) -> str:
    """清理团队名称"""
    if not name:
        return ""
    
    # 移除常见的HTML实体和特殊字符
    replacements = {
        "&amp;": "&",
        "&lt;": "<",
        "&gt;": ">",
        "&quot;": '"',
        "&#39;": "'",
    }
    
    cleaned = name
    for old, new in replacements.items():
        cleaned = cleaned.replace(old, new)
    
    return cleaned.strip()