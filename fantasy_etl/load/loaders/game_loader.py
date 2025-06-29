"""
游戏数据加载器 (Game Loader)
===========================

【主要职责】
1. Yahoo Fantasy游戏数据加载
2. 游戏信息验证和处理
3. 游戏数据去重和更新

【数据来源】
- Yahoo Fantasy API游戏端点
- ETL Extract层处理后的游戏数据

【迁移来源】
- scripts/database_writer.py: write_games_data()
"""

from typing import Dict, Any, Optional, Union
from sqlalchemy.orm import Session

from fantasy_etl.load.database.models import Game
from fantasy_etl.load.loaders.base_loader import BaseLoader, ValidationError


class GameLoader(BaseLoader):
    """游戏数据加载器"""
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """验证游戏记录"""
        required_fields = ['game_key', 'game_id', 'name', 'code', 'season']
        
        for field in required_fields:
            if not record.get(field):
                raise ValidationError(f"Missing required field: {field}")
        
        # 验证数据类型
        if not isinstance(record.get('game_id'), (str, int)):
            raise ValidationError("game_id must be string or integer")
        
        if not isinstance(record.get('season'), (str, int)):
            raise ValidationError("season must be string or integer")
        
        # 验证布尔字段
        boolean_fields = [
            'is_registration_over', 'is_game_over', 'is_offseason', 'scenario_generator'
        ]
        for field in boolean_fields:
            if field in record and record[field] is not None:
                if not isinstance(record[field], bool):
                    # 尝试转换
                    try:
                        record[field] = bool(int(record[field])) if str(record[field]).isdigit() else bool(record[field])
                    except (ValueError, TypeError):
                        raise ValidationError(f"Invalid boolean value for {field}: {record[field]}")
        
        return True
    
    def _create_model_instance(self, record: Dict[str, Any]) -> Game:
        """创建游戏模型实例"""
        return Game(
            game_key=record['game_key'],
            game_id=str(record['game_id']),
            name=record['name'],
            code=record['code'],
            type=record.get('type'),
            url=record.get('url'),
            season=str(record['season']),
            is_registration_over=record.get('is_registration_over', False),
            is_game_over=record.get('is_game_over', False),
            is_offseason=record.get('is_offseason', False),
            editorial_season=record.get('editorial_season'),
            picks_status=record.get('picks_status'),
            contest_group_id=record.get('contest_group_id'),
            scenario_generator=record.get('scenario_generator', False)
        )
    
    def _get_unique_key(self, record: Dict[str, Any]) -> str:
        """获取游戏的唯一键"""
        return record['game_key']
    
    def _find_existing_record(self, session: Session, record: Dict[str, Any]) -> Optional[Game]:
        """查找现有的游戏记录"""
        return session.query(Game).filter_by(game_key=record['game_key']).first()
    
    def _should_update_existing(self, existing: Game, new_record: Dict[str, Any]) -> bool:
        """判断是否需要更新现有游戏记录"""
        # 检查关键字段是否有变化
        fields_to_check = [
            'name', 'type', 'url', 'is_registration_over', 
            'is_game_over', 'is_offseason', 'editorial_season',
            'picks_status', 'contest_group_id', 'scenario_generator'
        ]
        
        for field in fields_to_check:
            if field in new_record:
                existing_value = getattr(existing, field, None)
                new_value = new_record[field]
                
                # 处理布尔值比较
                if isinstance(new_value, bool) and existing_value != new_value:
                    return True
                elif existing_value != new_value:
                    return True
        
        return False
    
    def _preprocess_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """预处理游戏记录"""
        processed = record.copy()
        
        # 确保必要字段为字符串
        if 'game_id' in processed:
            processed['game_id'] = str(processed['game_id'])
        if 'season' in processed:
            processed['season'] = str(processed['season'])
        
        # 处理布尔字段
        boolean_fields = [
            'is_registration_over', 'is_game_over', 
            'is_offseason', 'scenario_generator'
        ]
        
        for field in boolean_fields:
            if field in processed and processed[field] is not None:
                value = processed[field]
                if isinstance(value, str):
                    processed[field] = value.lower() in ('true', '1', 'yes')
                elif isinstance(value, (int, float)):
                    processed[field] = bool(value)
        
        return processed 