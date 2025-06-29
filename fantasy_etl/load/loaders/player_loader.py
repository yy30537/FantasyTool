"""
球员数据加载器 (Player Loader)
=============================

【主要职责】
1. Yahoo Fantasy球员数据加载
2. 球员信息验证和处理
3. 球员位置信息关联处理
4. 球员数据去重和更新

【数据来源】
- Yahoo Fantasy API球员端点
- ETL Extract层处理后的球员数据

【迁移来源】
- scripts/database_writer.py: write_players_batch()
- scripts/database_writer.py: write_player_eligible_positions()
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session

from fantasy_etl.load.database.models import Player, PlayerEligiblePosition
from fantasy_etl.load.loaders.base_loader import BaseLoader, ValidationError


class PlayerLoader(BaseLoader):
    """球员数据加载器"""
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """验证球员记录"""
        required_fields = ['player_key', 'player_id', 'editorial_player_key', 'league_key']
        
        for field in required_fields:
            if not record.get(field):
                raise ValidationError(f"Missing required field: {field}")
        
        # 验证数据类型
        if not isinstance(record.get('player_id'), (str, int)):
            raise ValidationError("player_id must be string or integer")
        
        # 验证布尔字段
        if 'is_undroppable' in record and record['is_undroppable'] is not None:
            if not isinstance(record['is_undroppable'], bool):
                try:
                    # 尝试转换字符串到布尔值
                    if isinstance(record['is_undroppable'], str):
                        if record['is_undroppable'].isdigit():
                            record['is_undroppable'] = bool(int(record['is_undroppable']))
                        else:
                            record['is_undroppable'] = record['is_undroppable'].lower() in ('true', 'yes')
                    else:
                        record['is_undroppable'] = bool(record['is_undroppable'])
                except (ValueError, TypeError):
                    raise ValidationError(f"Invalid boolean value for is_undroppable: {record['is_undroppable']}")
        
        return True
    
    def _create_model_instance(self, record: Dict[str, Any]) -> Player:
        """创建球员模型实例"""
        return Player(
            player_key=record['player_key'],
            player_id=str(record['player_id']),
            editorial_player_key=record['editorial_player_key'],
            league_key=record['league_key'],
            full_name=record.get('full_name', ''),
            first_name=record.get('first_name'),
            last_name=record.get('last_name'),
            current_team_key=record.get('current_team_key'),
            current_team_name=record.get('current_team_name'),
            current_team_abbr=record.get('current_team_abbr'),
            display_position=record.get('display_position'),
            primary_position=record.get('primary_position'),
            position_type=record.get('position_type'),
            uniform_number=record.get('uniform_number'),
            status=record.get('status'),
            image_url=record.get('image_url'),
            headshot_url=record.get('headshot_url'),
            is_undroppable=record.get('is_undroppable', False),
            season=record.get('season', '2024'),
            last_updated=datetime.utcnow()
        )
    
    def _get_unique_key(self, record: Dict[str, Any]) -> str:
        """获取球员的唯一键"""
        return record['player_key']
    
    def _find_existing_record(self, session: Session, record: Dict[str, Any]) -> Optional[Player]:
        """查找现有的球员记录"""
        return session.query(Player).filter_by(player_key=record['player_key']).first()
    
    def _should_update_existing(self, existing: Player, new_record: Dict[str, Any]) -> bool:
        """判断是否需要更新现有球员记录"""
        # 动态信息字段总是更新
        dynamic_fields = [
            'current_team_key', 'current_team_name', 'current_team_abbr',
            'display_position', 'primary_position', 'position_type',
            'uniform_number', 'status', 'image_url', 'headshot_url',
            'is_undroppable'
        ]
        
        for field in dynamic_fields:
            if field in new_record:
                existing_value = getattr(existing, field, None)
                new_value = new_record[field]
                
                if existing_value != new_value:
                    return True
        
        return False
    
    def _preprocess_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """预处理球员记录"""
        processed = record.copy()
        
        # 确保player_id为字符串
        if 'player_id' in processed:
            processed['player_id'] = str(processed['player_id'])
        
        # 处理姓名信息
        if 'name' in processed and isinstance(processed['name'], dict):
            name_info = processed['name']
            processed['full_name'] = name_info.get('full', '')
            processed['first_name'] = name_info.get('first')
            processed['last_name'] = name_info.get('last')
            # 移除原始name字段
            del processed['name']
        
        # 处理团队信息映射
        if 'editorial_team_key' in processed:
            processed['current_team_key'] = processed['editorial_team_key']
        if 'editorial_team_full_name' in processed:
            processed['current_team_name'] = processed['editorial_team_full_name']
        if 'editorial_team_abbr' in processed:
            processed['current_team_abbr'] = processed['editorial_team_abbr']
        
        # 处理头像信息
        if 'headshot' in processed and isinstance(processed['headshot'], dict):
            headshot_info = processed['headshot']
            if 'url' in headshot_info:
                processed['headshot_url'] = headshot_info['url']
            del processed['headshot']
        
        # 处理布尔字段
        if 'is_undroppable' in processed and processed['is_undroppable'] is not None:
            value = processed['is_undroppable']
            if isinstance(value, str):
                if value.isdigit():
                    processed['is_undroppable'] = bool(int(value))
                else:
                    processed['is_undroppable'] = value.lower() in ('true', '1', 'yes')
            elif isinstance(value, (int, float)):
                processed['is_undroppable'] = bool(value)
        
        # 确保有season信息
        if 'season' not in processed or not processed['season']:
            processed['season'] = '2024'  # 默认值
        
        return processed
    
    def _postprocess_record(self, instance: Player, record: Dict[str, Any]) -> Player:
        """后处理球员记录 - 处理位置信息"""
        # 处理eligible_positions
        eligible_positions = record.get('eligible_positions', [])
        if eligible_positions:
            # 注意：这里我们只是存储位置信息，实际的位置记录创建由单独的方法处理
            # 因为涉及到关联表的操作
            instance._eligible_positions_data = eligible_positions
        
        return instance
    
    def load_batch_records(self, records: List[Dict[str, Any]], 
                          session: Optional[Session] = None,
                          skip_duplicates: bool = True) -> 'LoadResult':
        """重写批量加载以处理位置信息"""
        from fantasy_etl.load.loaders.base_loader import LoadResult
        
        # 首先进行基本的球员记录加载
        result = super().load_batch_records(records, session, skip_duplicates)
        
        # 然后处理位置信息
        if session:
            self._process_player_positions(session, records, result)
        else:
            with self.session_manager.transaction() as session:
                self._process_player_positions(session, records, result)
        
        return result
    
    def _process_player_positions(self, session: Session, records: List[Dict[str, Any]], 
                                result: 'LoadResult') -> None:
        """处理球员位置信息"""
        for record in records:
            try:
                player_key = record.get('player_key')
                eligible_positions = record.get('eligible_positions', [])
                
                if not player_key or not eligible_positions:
                    continue
                
                # 删除现有位置记录
                session.query(PlayerEligiblePosition).filter_by(
                    player_key=player_key
                ).delete()
                
                # 添加新的位置记录
                for position in eligible_positions:
                    if isinstance(position, dict):
                        position_str = position.get('position', '')
                    else:
                        position_str = str(position)
                    
                    if position_str:
                        eligible_pos = PlayerEligiblePosition(
                            player_key=player_key,
                            position=position_str
                        )
                        session.add(eligible_pos)
                
                session.flush()
                
            except Exception as e:
                result.add_error(f"Error processing positions for player {player_key}: {e}", record)
                self.logger.error(f"Error processing positions for player {player_key}: {e}")


class PlayerPositionLoader(BaseLoader):
    """球员位置数据加载器 - 独立处理位置信息"""
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """验证位置记录"""
        if not record.get('player_key'):
            raise ValidationError("Missing player_key")
        if not record.get('position'):
            raise ValidationError("Missing position")
        return True
    
    def _create_model_instance(self, record: Dict[str, Any]) -> PlayerEligiblePosition:
        """创建位置模型实例"""
        return PlayerEligiblePosition(
            player_key=record['player_key'],
            position=record['position']
        )
    
    def _get_unique_key(self, record: Dict[str, Any]) -> tuple:
        """获取位置的唯一键"""
        return (record['player_key'], record['position'])
    
    def _find_existing_record(self, session: Session, record: Dict[str, Any]) -> Optional[PlayerEligiblePosition]:
        """查找现有的位置记录"""
        return session.query(PlayerEligiblePosition).filter_by(
            player_key=record['player_key'],
            position=record['position']
        ).first()
    
    def clear_player_positions(self, session: Session, player_key: str) -> int:
        """清除球员的所有位置记录"""
        count = session.query(PlayerEligiblePosition).filter_by(
            player_key=player_key
        ).count()
        
        session.query(PlayerEligiblePosition).filter_by(
            player_key=player_key
        ).delete()
        
        return count 