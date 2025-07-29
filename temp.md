# archive/main.py Functions Documentation

Yahoo Fantasy Sports ETL 工具 - 统一主入口文件的所有函数及其功能说明。

## 主要函数列表

### 1. `run_web_app()` (第18行)
**功能**: 启动Web应用进行OAuth认证
- 检查现有令牌状态（存在性和有效性）
- 根据令牌状态提供不同的操作选项
- 启动Flask应用提供OAuth认证服务
- 支持HTTPS和HTTP模式
- 处理认证流程的用户交互

### 2. `check_token_status_silent()` (第122行)
**功能**: 静默检查OAuth令牌状态（不打印信息）
- 检查令牌文件是否存在
- 验证令牌格式和必要字段
- 检查令牌是否过期
- 返回布尔值表示令牌有效性

### 3. `run_data_fetcher()` (第149行)
**功能**: 运行数据获取器交互式菜单
- 初始化YahooFantasyDataFetcher实例
- 启动交互式数据获取菜单
- 处理资源清理和异常处理

### 4. `run_sample_fetcher()` (第171行)
**功能**: 运行样本数据获取器
- 初始化YahooFantasySampleFetcher实例
- 启动样本数据获取的交互式菜单
- 用于测试和调试目的

### 5. `setup_database()` (第188行)
**功能**: 初始化数据库
- 创建数据库引擎连接
- 创建所有必要的数据库表
- 处理数据库初始化过程中的异常

### 6. `check_token_status()` (第207行)
**功能**: 检查OAuth令牌状态（带详细输出）
- 检查令牌文件存在性
- 验证令牌的完整性和格式
- 显示令牌过期状态和剩余时间
- 提供详细的状态报告和建议

### 7. `show_menu()` (第251行)
**功能**: 显示主菜单
- 展示程序的主要功能选项
- 提供用户友好的交互界面
- 列出所有可用的操作选项

### 8. `show_help()` (第265行)
**功能**: 显示帮助信息
- 提供详细的使用指南
- 展示命令行参数说明
- 包含快速开始指南和项目结构说明

### 9. `main()` (第297行)
**功能**: 主函数 - 统一入口点
- 解析命令行参数
- 根据参数执行对应功能
- 提供交互式菜单模式
- 协调所有子功能的调用

## 程序流程

1. **命令行模式**: 直接通过参数调用特定功能
2. **交互式模式**: 无参数时提供菜单界面
3. **令牌管理**: 自动检查和管理OAuth令牌状态
4. **错误处理**: 全面的异常处理和用户提示

## 支持的命令行参数

- `--web`: 启动Web界面进行OAuth认证
- `--data`: 启动数据获取工具
- `--sample`: 启动样本数据获取工具
- `--db-init`: 初始化数据库
- `--check-token`: 检查OAuth令牌状态
- `--help-detailed`: 显示详细帮助信息

---

# archive/yahoo_api_utils.py Functions Documentation

Yahoo Fantasy API 通用工具模块 - 专注于API请求和令牌管理的所有函数及其功能说明。

## 主要函数列表

### 1. `ensure_tokens_directory()` (第27行)
**功能**: 确保tokens目录存在
- 检查tokens目录是否存在
- 如果不存在则创建目录结构
- 打印目录创建信息

### 2. `save_token(token)` (第39行)
**功能**: 保存令牌到文件
- 确保目录结构存在
- 使用pickle格式将令牌序列化保存
- 保存到默认令牌文件路径
- 返回操作成功状态

### 3. `load_token()` (第52行)
**功能**: 从文件加载令牌
- 优先从统一位置加载令牌文件
- 支持从多个可能位置搜索令牌文件
- 自动迁移旧位置的令牌到新位置
- 使用pickle格式反序列化令牌
- 处理文件不存在和加载错误情况

### 4. `refresh_token_if_needed(token)` (第84行)
**功能**: 检查并刷新令牌（如果已过期）
- 检查令牌过期状态（提前60秒刷新）
- 使用refresh_token向Yahoo API请求新的访问令牌
- 计算并设置新令牌的过期时间
- 保留原有的refresh_token
- 自动保存更新后的令牌
- 处理刷新失败的情况

### 5. `get_api_data(url, max_retries=3)` (第131行)
**功能**: 通用函数：获取Yahoo Fantasy API数据，带重试机制
- 加载并验证OAuth令牌
- 自动刷新过期令牌
- 设置API请求的Authorization头
- 实现指数退避重试机制
- 处理401授权错误（自动重新获取令牌）
- 处理各种HTTP错误状态码
- 返回JSON格式的API响应数据

### 6. `parse_yahoo_date(date_str)` (第189行)
**功能**: 解析Yahoo日期格式
- 将Yahoo API返回的日期字符串转换为datetime对象
- 使用标准的"YYYY-MM-DD"格式
- 处理空值和无效日期格式
- 返回datetime对象或None

### 7. `print_league_selection_info(leagues_data)` (第198行)
**功能**: 打印联盟选择信息
- 格式化显示所有可用的Fantasy联盟
- 展示联盟详细信息（名称、ID、赛季、状态等）
- 为每个联盟分配序号便于选择
- 显示联盟状态（进行中/已结束）
- 显示运动类型、球队数量、计分方式等信息
- 返回格式化的联盟列表

### 8. `select_league_interactively(leagues_data)` (第234行)
**功能**: 交互式选择联盟
- 调用打印函数显示所有联盟信息
- 提供用户友好的交互式选择界面
- 验证用户输入的有效性
- 处理无效输入和用户取消操作
- 返回用户选择的联盟信息对象
- 支持键盘中断退出

## 模块配置和常量

- **CLIENT_ID/CLIENT_SECRET**: Yahoo OAuth应用凭据（从环境变量加载）
- **TOKEN_URL**: Yahoo OAuth令牌刷新API端点
- **DEFAULT_TOKEN_FILE**: 默认令牌文件存储路径
- **TOKENS_DIR**: 令牌文件存储目录

## 核心功能特性

1. **令牌管理**: 自动保存、加载、刷新OAuth令牌
2. **API请求**: 统一的API请求接口，带认证和重试
3. **错误处理**: 完善的错误处理和异常恢复机制
4. **用户交互**: 友好的联盟选择和信息显示界面
5. **文件迁移**: 自动迁移旧版本的令牌文件位置

---

# archive/model.py Functions Documentation

Yahoo Fantasy Sports 数据库模型定义文件 - 所有数据库表结构和管理函数的功能说明。

## 主要函数列表

### 1. `get_database_url()` (第707行)
**功能**: 获取数据库连接URL
- 从环境变量读取数据库连接参数
- 构建PostgreSQL连接字符串
- 提供默认值（localhost、fantasy_db等）
- 返回完整的数据库连接URL

### 2. `create_database_engine()` (第717行)
**功能**: 创建数据库引擎
- 调用get_database_url()获取连接字符串
- 使用SQLAlchemy创建数据库引擎
- 配置echo=False关闭详细日志
- 返回可用的数据库引擎对象

### 3. `create_tables(engine)` (第723行)
**功能**: 创建所有表
- 使用SQLAlchemy的Base.metadata创建所有定义的表
- 基于模型类自动生成表结构
- 处理表之间的外键关系
- 一次性创建完整的数据库schema

### 4. `recreate_tables(engine)` (第727行)
**功能**: 重新创建所有表（先删除再创建）
- 查询数据库中现有的所有表
- 使用CASCADE删除所有现有表和依赖关系
- 清理可能遗留的旧表结构
- 重新创建完整的数据库schema
- 提供详细的操作进度反馈
- 包含错误处理和回滚机制

### 5. `get_session(engine)` (第796行)
**功能**: 获取数据库会话
- 创建SQLAlchemy Session工厂
- 绑定到指定的数据库引擎
- 返回可用的数据库会话对象
- 用于执行数据库操作和事务管理

## 数据库表结构（SQLAlchemy模型类）

### 核心业务表
1. **Game** (第13行): 游戏基本信息表 - 存储NBA、NFL等游戏类型信息
2. **League** (第40行): 联盟信息表 - 存储Fantasy联盟的基本配置
3. **Team** (第169行): 团队信息表 - 存储Fantasy团队基本信息
4. **Player** (第221行): 球员信息表 - 存储球员基本资料和状态

### 配置和设置表
5. **LeagueSettings** (第95行): 联盟设置表 - 存储选秀、交易、waiver等配置
6. **StatCategory** (第136行): 统计类别定义表 - 标记核心统计项
7. **LeagueRosterPosition** (第686行): 联盟阵容位置表 - 存储位置配置

### 用户和管理表
8. **Manager** (第201行): 团队管理员表 - 存储团队经理信息

### 球员位置和名单表
9. **PlayerEligiblePosition** (第265行): 球员合适位置表 - 存储球员可打位置
10. **RosterDaily** (第283行): 每日名单表 - 记录每天的球员分配情况

### 统计数据表
11. **PlayerDailyStats** (第406行): 球员日统计表 - 存储11项核心统计数据
12. **PlayerSeasonStats** (第466行): 球员赛季统计表 - 存储赛季累计统计
13. **TeamStatsWeekly** (第523行): 团队周统计表 - 存储每周团队统计

### 排名和对战表
14. **LeagueStandings** (第579行): 联盟排名表 - 存储联盟排名和胜负记录
15. **TeamMatchups** (第619行): 团队对战表 - 存储每周对战信息和结果

### 交易记录表
16. **Transaction** (第331行): 交易记录表 - 存储add/drop、trade等交易
17. **TransactionPlayer** (第363行): 交易球员详情表 - 存储交易涉及的球员详情

### 辅助表
18. **DateDimension** (第391行): 日期维度表 - 用于管理赛季日程

## 核心特性

### 统计数据标准化
- 基于Yahoo的11个核心篮球统计项设计
- FGM/A, FG%, FTM/A, FT%, 3PTM, PTS, REB, AST, ST, BLK, TO
- 统一的stat_id映射（如stat_id=5对应FG%）

### 索引优化
- 每个表都配置了优化的数据库索引
- 支持高效的时间范围查询、联盟查询、球员查询
- 唯一性约束防止数据重复

### 关系映射
- 完整的SQLAlchemy关系定义
- 支持back_populates双向关系
- 外键约束确保数据完整性

### 环境配置
- 支持从环境变量加载数据库连接参数
- 提供合理的默认配置值
- 支持不同环境的灵活部署

---

# archive/yahoo_api_data.py Functions Documentation

Yahoo Fantasy统一数据获取工具 - 核心数据提取模块的所有函数及其功能说明。

## YahooFantasyDataFetcher 类的主要函数列表 (共75个函数)

### 初始化和设置函数

### 1. `__init__(self, delay: int = 2, batch_size: int = 100)` (第25行)
**功能**: 初始化Yahoo Fantasy数据获取器
- 设置API请求延迟和批处理大小
- 初始化数据库写入器
- 设置缓存属性和联盟选择状态

### 2. `wait(self, message: Optional[str] = None)` (第35行)
**功能**: 实现API速率限制等待机制
- 根据设置的延迟时间暂停执行
- 避免触发Yahoo API的速率限制

### 3. `close(self)` (第39行)
**功能**: 关闭资源连接
- 关闭数据库写入器连接
- 清理占用的系统资源

## 联盟选择和基础数据函数

### 4. `fetch_and_select_league(self, use_existing_data: bool = False)` (第46行)
**功能**: 获取基础数据并选择联盟
- 优先从数据库获取联盟数据
- 可选择从API重新获取数据
- 提供交互式联盟选择界面

### 5. `_get_leagues_from_database(self)` (第71行)
**功能**: 从数据库获取联盟数据
- 查询数据库中的联盟记录
- 格式化为选择界面需要的数据结构
- 按game_key分组返回联盟数据

### 6. `_fetch_all_leagues_data(self)` (第128行)
**功能**: 获取所有联盟数据并写入数据库
- 先获取games数据，再获取各game的联盟数据
- 直接写入数据库存储
- 返回格式化的联盟数据用于选择

### 7. `_fetch_games_data(self)` (第161行)
**功能**: 获取用户的games数据
- 调用Yahoo API获取用户参与的游戏
- 返回games API响应数据

### 8. `_fetch_leagues_data(self, game_key: str)` (第170行)
**功能**: 获取指定游戏的联盟数据
- 根据game_key获取用户参与的联盟
- 调用对应的Yahoo API端点

### 9. `_extract_game_keys(self, games_data: Dict)` (第175行)
**功能**: 从游戏数据中提取游戏键
- 只提取type='full'的完整游戏
- 过滤掉测试或不完整的游戏类型
- 返回有效的game_key列表

### 10. `_extract_leagues_from_data(self, data: Dict, game_key: str)` (第206行)
**功能**: 从API返回数据中提取联盟信息
- 解析复杂的嵌套API响应结构
- 提取每个联盟的详细配置信息
- 添加game_key关联信息

## 完整联盟数据获取函数

### 11. `_ensure_league_exists_in_db(self)` (第263行)
**功能**: 确保当前选择的联盟存在于数据库中
- 验证联盟基本信息是否已存储
- 检查数据库中联盟记录的完整性

### 12. `fetch_complete_league_data(self)` (第287行)
**功能**: 获取完整的联盟数据（主要协调器）
- 协调6个主要步骤获取完整联盟数据
- 包括联盟详情、赛季日程、球员数据、团队数据、交易记录、统计数据
- 提供进度反馈和成功统计

### 13. `fetch_league_details(self)` (第331行)
**功能**: 获取联盟详细信息并写入数据库
- 获取联盟设置和配置信息
- 直接写入数据库的league_settings表

### 14. `_fetch_league_settings(self, league_key: str)` (第347行)
**功能**: 获取联盟设置
- 调用联盟设置API端点
- 返回联盟的详细配置信息

## 团队数据函数

### 15. `fetch_teams_data(self)` (第352行)
**功能**: 获取团队数据并写入数据库
- 获取联盟中所有团队的基本信息
- 批量写入团队数据到数据库
- 返回团队数据供后续处理使用

### 16. `_write_teams_to_db(self, teams_data: Dict, league_key: str)` (第366行)
**功能**: 将团队数据写入数据库
- 解析API返回的复杂团队数据结构
- 提取每个团队的详细信息
- 批量写入数据库

### 17. `_extract_team_data_from_api(self, team_data: List)` (第407行)
**功能**: 从API团队数据中提取团队信息
- 处理复杂的嵌套数据结构
- 提取团队logo、管理员、交易次数等信息
- 规范化团队数据格式

### 18. `fetch_team_rosters(self, teams_data: Dict)` (第461行)
**功能**: 获取所有团队的阵容数据
- 使用赛季结束日期而非当前日期
- 获取每个团队的最终阵容配置
- 批量处理所有团队的阵容信息

### 19. `_fetch_team_roster(self, team_key: str)` (第507行)
**功能**: 获取单个团队的阵容
- 调用单个团队的阵容API
- 返回团队当前阵容数据

### 20. `transform_roster_data(self, roster_data: Dict, team_key: str)` (第519行)
**功能**: 从原始roster数据转换为标准化格式 (纯转换，不写入数据库)
- 解析阵容API返回的球员信息
- 提取位置、状态、伤病信息
- 返回转换后的roster数据列表

### 20.1. `load_roster_data(self, roster_list: List[Dict])` (第611行)
**功能**: 加载roster数据到数据库 (纯加载，不转换数据)
- 接收转换后的roster数据列表
- 执行数据库写入操作
- 处理日期解析和位置判断

### 20.2. `_process_roster_data_to_db(self, roster_data: Dict, team_key: str)` (第661行)
**功能**: 处理阵容数据并写入数据库 (向后兼容的包装方法)
- 调用transform_roster_data进行数据转换
- 调用load_roster_data进行数据加载
- 保持向后兼容性

### 21. `transform_team_keys_from_data(self, teams_data: Dict)` (第668行)
**功能**: 从团队数据中转换提取团队键
- 解析复杂的团队数据结构
- 提取所有团队的team_key标识符
- 用于后续的团队相关API调用

### 22. `fetch_team_rosters_for_date_range(self, teams_data: Dict, start_date: date, end_date: date)` (第688行)
**功能**: 获取指定日期范围内的团队阵容数据
- 遍历日期范围内的每一天
- 为每个团队获取每日阵容配置
- 支持历史阵容数据的时间序列分析

### 23. `_fetch_team_roster_for_date(self, team_key: str, date_str: str)` (第725行)
**功能**: 获取指定日期的团队阵容
- 调用带日期参数的阵容API
- 获取特定日期的阵容快照

## 球员数据函数

### 24. `fetch_complete_players_data(self)` (第730行)
**功能**: 获取完整的球员数据并写入数据库
- 获取联盟中所有球员的基础信息
- 批量写入球员数据到数据库
- 为后续统计数据获取提供基础

### 25. `_fetch_all_league_players(self, league_key: str)` (第744行)
**功能**: 使用分页逻辑获取所有球员
- 每页25个球员，自动处理分页
- 最多迭代100次防止无限循环
- 合并所有页面的球员数据

### 26. `_extract_player_info_from_league_data(self, players_data: Dict)` (第779行)
**功能**: 从联盟球员数据中提取球员信息
- 解析复杂的球员API响应结构
- 提取球员基本信息和属性
- 规范化球员数据格式

### 27. `_normalize_player_info(self, player_info: Dict)` (第832行)
**功能**: 标准化球员信息
- 处理姓名、团队、头像信息
- 统一字段命名和数据格式
- 添加时间戳和赛季信息

## 交易数据函数

### 28. `fetch_complete_transactions_data(self, teams_data: Optional[Dict] = None)` (第860行)
**功能**: 获取完整的交易数据并写入数据库
- 获取联盟中所有的交易记录
- 包括球员交易、waiver claim等操作
- 批量写入交易数据

### 29. `_fetch_all_league_transactions(self, league_key: str, max_count: int = None)` (第877行)
**功能**: 获取联盟所有交易记录（分页处理）
- 每页25个交易，最多200次迭代
- 自动处理分页和数据合并
- 支持最大数量限制

### 30. `_extract_transactions_from_data(self, transactions_data: Dict)` (第917行)
**功能**: 从API返回数据中提取交易信息
- 解析交易API的复杂响应结构
- 提取每个交易的详细信息
- 合并交易相关的所有数据字段

### 31. `_write_transactions_to_db(self, transactions: List[Dict], league_key: str)` (第965行)
**功能**: 将交易数据写入数据库
- 使用批处理方式提高写入效率
- 调用数据库写入器的交易写入方法

## 团队统计函数

### 32. `fetch_team_stats_data(self, teams_data: Optional[Dict] = None)` (第972行)
**功能**: 获取团队统计数据
- 获取联盟排名信息
- 获取每个团队的对战记录
- 生成团队统计数据

### 33. `_fetch_and_process_league_standings(self, league_key: str, season: str)` (第1013行)
**功能**: 获取并处理联盟排名数据
- 调用联盟排名API
- 解析排名和胜负记录信息
- 写入league_standings表

### 34. `_extract_team_standings_info(self, team_data)` (第1081行)
**功能**: 从团队数据中提取排名信息
- 递归解析复杂的嵌套数据结构
- 提取排名、积分、胜负记录
- 处理各种数据格式的异常情况

### 35. `_write_league_standings_to_db(self, team_info: Dict, league_key: str, season: str)` (第1122行)
**功能**: 将联盟排名数据写入数据库
- 提取结构化的排名字段
- 包括胜负记录、胜率、分区记录
- 写入规范化的数据库字段

## 错误处理和工具函数

### 36. `handle_database_error(self)` (第1177行)
**功能**: 处理数据库错误，必要时重新创建表结构
- 检测数据库表结构问题
- 自动重新创建损坏的表
- 重新初始化数据库连接

### 37. `get_season_date_info(self)` (第1201行)
**功能**: 获取赛季日期信息和状态
- 从数据库获取赛季开始和结束日期
- 判断赛季状态（进行中/已结束/未开始）
- 计算最新有效日期

### 38. `calculate_date_range(self, mode: str, days_back: int = None, target_date: str = None)` (第1257行)
**功能**: 计算日期范围
- 支持3种模式：指定日期/天数回溯/完整赛季
- 自动限制在赛季有效范围内
- 返回计算好的开始和结束日期

## 交互式菜单函数

### 39. `run_interactive_menu(self)` (第1318行)
**功能**: 运行交互式菜单
- 提供完整的用户交互界面
- 支持9个主要功能选项
- 处理用户输入和菜单导航

### 40. `_ensure_league_selected(self)` (第1377行)
**功能**: 确保已选择联盟
- 验证联盟选择状态
- 在执行需要联盟的操作前进行检查

### 41. `select_league_interactive(self)` (第1384行)
**功能**: 交互式选择联盟
- 调用联盟获取和选择功能
- 优先使用现有数据库数据

### 42. `run_complete_league_fetch(self)` (第1388行)
**功能**: 运行完整联盟数据获取
- 用户界面包装的完整数据获取
- 提供用户友好的进度反馈

### 43. `run_roster_history_fetch(self)` (第1397行)
**功能**: 运行阵容历史数据获取
- 提供交互式时间范围选择
- 获取指定时间段的阵容历史

### 44. `run_player_stats_fetch(self)` (第1409行)
**功能**: 运行球员日统计数据获取
- 交互式选择日期范围
- 只获取日统计，不包含赛季统计

### 45. `run_player_season_stats_fetch(self)` (第1421行)
**功能**: 运行球员赛季统计数据获取
- 从数据库获取球员列表
- 批量获取所有球员的赛季统计

### 46. `show_database_summary(self)` (第1458行)
**功能**: 显示数据库摘要
- 统计所有18个核心表的记录数量
- 提供完整的数据库状态概览
- 处理查询错误并显示友好信息

### 47. `clear_database(self, confirm: bool = False)` (第1503行)
**功能**: 清空数据库
- 需要明确确认才能执行
- 调用数据库写入器的清空方法

## 数据处理辅助函数

### 48. `_extract_position_string(self, position_data)` (第1513行)
**功能**: 从位置数据中提取位置字符串
- 处理多种位置数据格式
- 统一返回位置字符串格式

### 49. `_fetch_team_matchups(self, team_key: str)` (第1532行)
**功能**: 获取团队对战数据
- 调用团队对战API
- 返回团队的所有对战记录

### 50. `_process_team_matchups_to_db(self, matchups_data: Dict, team_key: str, league_key: str, season: str)` (第1537行)
**功能**: 处理团队对战数据并写入数据库
- 解析对战API响应数据
- 同时写入对战记录和周统计数据
- 使用结构化字段存储

### 51. `_extract_and_write_team_weekly_stats(self, matchup_info: Dict, team_key: str, league_key: str, season: str)` (第1582行)
**功能**: 从对战数据中提取并写入团队周统计
- 从对战记录中提取统计数据
- 写入team_stats_weekly表

### 52. `_extract_team_stats_from_matchup_data(self, teams_data: Dict, target_team_key: str)` (第1609行)
**功能**: 从对战数据中提取目标团队的统计数据
- 在对战的两个团队中找到目标团队
- 提取该团队的统计表现

### 53. `_extract_matchup_info(self, matchup_info, team_key: str)` (第1647行)
**功能**: 从对战数据中提取对战信息
- 提取周次、日期、状态信息
- 判断胜负、平局、季后赛状态
- 处理各种布尔值格式转换

### 54. `_extract_team_matchup_details(self, teams_data, target_team_key: str)` (第1750行)
**功能**: 从teams数据中提取当前团队的对战详情
- 识别对战中的两个团队
- 提取对手信息和积分对比
- 基于积分判断胜负关系

## 球员统计函数

### 55. `_fetch_player_season_stats_direct(self)` (第1843行)
**功能**: 直接获取球员赛季统计数据
- 不依赖日期维度的简化方法
- 从数据库获取球员列表后直接获取统计

### 56. `_fetch_player_season_stats(self, players: List, league_key: str, season: str)` (第1866行)
**功能**: 获取球员赛季统计数据
- 分批处理球员（每批25个）
- 使用批量API提高效率
- 提供详细的进度反馈

### 57. `_process_player_season_stats_data(self, stats_data: Dict, league_key: str, season: str)` (第1911行)
**功能**: 处理球员赛季统计数据
- 解析批量统计API响应
- 提取每个球员的11项核心统计
- 写入player_season_stats表

### 58. `_process_player_daily_stats_data(self, stats_data: Dict, league_key: str, season: str, date_obj: date)` (第2022行)
**功能**: 处理球员日统计数据
- 解析日统计API响应
- 提取指定日期的球员表现
- 写入player_daily_stats表

## 赛季日程和时间序列函数

### 59. `fetch_season_schedule_data(self)` (第2120行)
**功能**: 生成并写入赛季日程数据
- 基于联盟开始和结束日期生成日程
- 写入date_dimension表
- 支持历史数据分析

### 60. `fetch_roster_history_data(self, start_date: date, end_date: date)` (第2185行)
**功能**: 获取指定日期范围的阵容历史数据
- 调用日期范围的阵容获取方法
- 支持阵容变化的时间序列分析

### 61. `_extract_position_string(self, position_data)` (第2207行)
**功能**: 提取位置字符串（重复函数）
- 与第1513行的函数功能相同
- 处理各种位置数据格式

### 62. `_fetch_player_season_stats(self, players: List, league_key: str, season: str)` (第2226行)
**功能**: 获取球员赛季统计（简化版本）
- 功能与第1866行类似但更简化
- 基本的赛季统计获取功能

### 63. `_process_player_season_stats_data(self, stats_data: Dict, league_key: str, season: str)` (第2260行)
**功能**: 处理球员赛季统计数据（简化版本）
- 功能与第1911行类似但更简化
- 基本的统计数据处理

## 时间选择函数

### 64. `get_time_selection_interactive(self, data_type: str)` (第2356行)
**功能**: 交互式时间选择界面
- 提供多种时间选择模式
- 支持指定日期、日期范围、天数回溯
- 根据数据类型提供相应选项

## 球员统计数据获取

### 65. `fetch_player_stats_data(self, start_date: date, end_date: date, include_season_stats: bool = True)` (第2415行)
**功能**: 获取球员统计数据（主要函数）
- 可选择获取赛季统计或日统计
- 支持日期范围的统计数据获取
- 提供进度反馈和错误处理

### 66. `_fetch_player_daily_stats_for_range(self, players: List, league_key: str, season: str, start_date: date, end_date: date)` (第2463行)
**功能**: 获取指定范围的球员日统计
- 遍历日期范围内的每一天
- 为每天获取所有球员的统计数据
- 分批处理提高API效率

## 团队周统计函数

### 67. `run_team_weekly_stats_fetch(self)` (第2515行)
**功能**: 团队周统计获取界面
- 从现有对战数据生成周统计
- 不需要额外的API调用

### 68. `fetch_team_weekly_stats_from_matchups(self)` (第2522行)
**功能**: 从对战数据生成团队周统计
- 查询数据库中的对战记录
- 为每个对战生成对应的周统计记录
- 避免重复的API请求

### 69. `_process_matchup_to_weekly_stats(self, team_key: str, week: int, opponent_team_key: str, is_playoffs: bool, is_winner: Optional[bool], team_points: int, matchup_data: Dict, league_key: str, season: str)` (第2574行)
**功能**: 将对战记录处理为周统计数据
- 提取对战中的统计表现
- 计算获胜的统计类别数量
- 写入团队周统计表

### 70. `_extract_team_stats_from_matchup(self, matchup_data: Dict, target_team_key: str)` (第2598行)
**功能**: 从对战数据中提取团队统计
- 在对战数据中找到目标团队
- 提取该团队的统计数据
- 返回标准化的统计字典

### 71. `_count_categories_won(self, matchup_data: Dict, team_key: str)` (第2646行)
**功能**: 计算团队在对战中获胜的统计类别数量
- 比较两个团队在各统计类别的表现
- 统计目标团队获胜的类别数量
- 用于计算对战胜负

## 团队赛季统计函数

### 72. `run_team_season_stats_fetch(self)` (第2666行)
**功能**: 团队赛季统计获取界面
- 从联盟排名数据生成赛季统计
- 提供用户界面包装

### 73. `fetch_team_season_stats_from_standings(self)` (第2672行)
**功能**: 从联盟排名生成团队赛季统计
- 查询数据库中的排名数据
- 为每个团队生成赛季统计记录
- 基于现有数据避免API调用

### 74. `_process_standing_to_season_stats(self, standing, league_key: str, season: str)` (第2709行)
**功能**: 将排名记录处理为赛季统计数据
- 从排名数据提取赛季表现
- 计算胜率和排名信息
- 写入团队赛季统计

## 数据库辅助函数

### 75. `_get_teams_data_from_db(self, league_key: str)` (第2721行)
**功能**: 从数据库获取团队数据并转换为API格式
- 查询数据库中的团队记录
- 转换为与API响应兼容的格式
- 支持离线数据处理

## 核心特性总结

### 数据获取能力
- **完整的Yahoo Fantasy API集成**: 支持所有主要的数据端点
- **智能分页处理**: 自动处理API的分页限制
- **批量数据获取**: 优化API调用效率
- **时间序列支持**: 支持历史数据和日期范围查询

### 用户交互界面
- **交互式菜单系统**: 完整的命令行用户界面
- **灵活的时间选择**: 多种日期选择模式
- **进度反馈**: 详细的操作进度和状态信息
- **错误处理**: 友好的错误信息和恢复建议

### 数据处理能力
- **复杂数据解析**: 处理Yahoo API的复杂嵌套结构
- **数据标准化**: 统一的数据格式和字段命名
- **增量更新**: 支持数据的增量获取和更新
- **数据验证**: 完整的数据完整性检查

### 性能优化
- **API速率限制**: 智能的请求延迟机制
- **缓存系统**: 减少重复的API调用
- **批处理**: 高效的数据库写入
- **资源管理**: 正确的连接和资源清理

这个2779行的核心模块是整个Yahoo Fantasy ETL系统的心脏，提供了从数据获取到存储的完整解决方案。

---

# archive/database_writer.py Functions Documentation

Yahoo Fantasy数据库直接写入器 - 将Yahoo API数据直接写入数据库的所有函数及其功能说明。

## FantasyDatabaseWriter 类的主要函数列表 (共42个函数)

### 初始化和设置函数

### 1. `__init__(self, batch_size: int = 100)` (第28行)
**功能**: 初始化数据库写入器
- 设置批量写入大小，默认100条记录
- 创建数据库引擎和会话连接
- 检查并修复表结构问题
- 初始化各表统计计数器

### 2. `close(self)` (第69行)
**功能**: 关闭数据库连接
- 安全关闭数据库会话
- 释放占用的数据库资源

### 3. `_check_table_structure_issues(self)` (第74行)
**功能**: 检查数据库表结构是否存在问题
- 测试新的统计值表结构
- 检查模型定义与实际表的匹配性
- 识别列不存在或表结构相关错误
- 返回是否需要重新创建表的布尔值

### 4. `get_stats_summary(self)` (第128行)
**功能**: 获取统计摘要
- 返回所有表的写入统计计数
- 提供数据写入操作的总体概览

## 兼容性方法

### 5. `write_player_season_stats(self, player_key: str, editorial_player_key: str, league_key: str, stats_data: Dict, season: str)` (第139行)
**功能**: 写入球员赛季统计（旧接口兼容）
- 包装新的写入方法保持接口兼容性
- 调用write_player_season_stat_values方法
- 返回操作成功状态

### 6. `write_player_daily_stats(self, player_key: str, editorial_player_key: str, league_key: str, stats_data: Dict, season: str, stats_date: date, week: Optional[int] = None)` (第151行)
**功能**: 写入球员日期统计（旧接口兼容）
- 包装新的写入方法保持接口兼容性
- 调用write_player_daily_stat_values方法
- 返回操作成功状态

## 基础数据写入方法

### 7. `write_games_data(self, games_data: Dict)` (第168行)
**功能**: 写入游戏数据
- 解析Yahoo API返回的games数据结构
- 提取游戏基本信息（ID、名称、类型、赛季等）
- 检查重复记录并批量插入数据库
- 返回成功写入的记录数量

### 8. `write_leagues_data(self, leagues_data: Dict)` (第213行)
**功能**: 写入联盟数据
- 处理多个game下的联盟数据
- 提取联盟详细配置信息
- 包括联盟设置、状态、日期范围等
- 避免重复插入并提交事务

### 9. `write_league_settings(self, league_key: str, settings_data: Dict)` (第266行)
**功能**: 写入联盟设置
- 解析复杂的联盟设置API响应
- 提取选秀、waiver、交易、季后赛等配置
- 同时提取并写入统计类别定义
- 解析并写入roster_positions配置

## 统计类别管理方法

### 10. `write_stat_categories(self, league_key: str, stat_categories_data: Dict)` (第337行)
**功能**: 写入统计类别定义到数据库
- 解析stat_categories的嵌套结构
- 提取每个统计项的详细信息
- 支持更新现有记录或创建新记录
- 标准化统计项的名称和配置

### 11. `get_stat_category_info(self, league_key: str, stat_id: int)` (第400行)
**功能**: 获取统计类别信息
- 根据联盟和统计ID查询统计类别
- 返回统计项的显示名称、缩写等信息
- 用于数据展示和报告生成

## 混合存储统计值写入方法

### 12. `write_player_season_stat_values(self, player_key: str, editorial_player_key: str, league_key: str, season: str, stats_data: Dict)` (第423行)
**功能**: 写入球员赛季统计值（只存储核心统计列）
- 提取并存储11个核心篮球统计项
- 支持更新现有记录或创建新记录
- 使用结构化列存储而非JSON
- 处理FGM/A、FTM/A等复合统计项

### 13. `_extract_core_player_season_stats(self, stats_data: Dict)` (第487行)
**功能**: 从球员赛季统计数据中提取完整的11个统计项
- 基于Yahoo stat_categories的标准映射
- 处理百分比格式转换
- 安全解析复合统计项（FGM/A格式）
- 返回标准化的统计字典

### 14. `write_player_daily_stat_values(self, player_key: str, editorial_player_key: str, league_key: str, season: str, date_obj: date, stats_data: Dict, week: Optional[int] = None)` (第562行)
**功能**: 写入球员日期统计值（只存储核心统计列）
- 与赛季统计类似但针对特定日期
- 支持周次信息的存储
- 处理日统计的特殊字段命名
- 提供增量更新能力

### 15. `_extract_core_daily_stats(self, stats_data: Dict)` (第630行)
**功能**: 从统计数据中提取完整的11个日期统计项
- 与赛季统计提取逻辑类似
- 处理日统计的特殊格式
- 统一的百分比和数值转换

### 16. `write_team_stat_values(self, team_key: str, league_key: str, season: str, coverage_type: str, stats_data: Dict, week: Optional[int] = None, date_obj: Optional[date] = None, opponent_team_key: Optional[str] = None, is_playoff: bool = False, win: Optional[bool] = None, categories_won: int = 0)` (第706行)
**功能**: 写入团队周统计值（只处理week数据）
- 专门处理团队的周统计数据
- 提取11个核心统计项
- 支持对战相关信息存储
- 避免重复数据插入

### 17. `_extract_core_team_weekly_stats(self, categories_won: int, win: Optional[bool] = None)` (第778行)
**功能**: 从matchup数据中提取核心统计项
- 简化的统计提取方法
- 主要处理获胜类别数量
- 用于对战结果分析

### 18. `write_league_standings(self, league_key: str, team_key: str, season: str, rank: Optional[int] = None, playoff_seed: Optional[str] = None, wins: int = 0, losses: int = 0, ties: int = 0, win_percentage: float = 0.0, games_back: str = "-", divisional_wins: int = 0, divisional_losses: int = 0, divisional_ties: int = 0)` (第791行)
**功能**: 写入联盟排名数据
- 存储团队在联盟中的排名信息
- 包括胜负记录、胜率、季后赛种子
- 支持分区战绩统计
- 处理排名变化的增量更新

### 19. `write_team_matchup(self, league_key: str, team_key: str, season: str, week: int, ...)` (第847行)
**功能**: 写入团队对战数据（使用结构化字段替代JSON）
- 存储每周的团队对战详情
- 包括对手信息、胜负结果、积分对比
- 详细的统计类别获胜情况（9个核心类别）
- 比赛场次信息（已完成/剩余/进行中）

### 20. `write_team_matchup_from_data(self, matchup_data: Dict, team_key: str, league_key: str, season: str)` (第960行)
**功能**: 从API返回的matchup数据中解析并写入团队对战记录
- 解析复杂的Yahoo API对战数据结构
- 自动提取对战基本信息和统计获胜情况
- 调用结构化写入方法存储数据
- 处理对战中的多种状态

### 21. `_parse_stat_winners(self, stat_winners: List, team_key: str)` (第1028行)
**功能**: 解析stat_winners，返回该团队在各统计类别中的获胜情况
- 遍历所有统计类别的获胜者
- 判断当前团队是否在该类别获胜
- 返回按stat_id组织的获胜情况字典

### 22. `_parse_teams_matchup_data(self, teams_data: Dict, target_team_key: str)` (第1047行)
**功能**: 解析teams数据，提取对战详情
- 在对战的两个团队中识别目标团队和对手
- 提取积分对比和比赛场次信息
- 基于积分自动判断胜负关系
- 处理复杂的嵌套API响应结构

### 23. `_safe_bool(self, value)` (第1150行)
**功能**: 安全转换为布尔值
- 处理多种类型的布尔值表示
- 支持字符串、数值、布尔值的统一转换
- 提供默认值处理

### 24. `write_player_eligible_positions(self, player_key: str, positions: List)` (第1162行)
**功能**: 写入球员合适位置
- 先删除现有位置记录再插入新记录
- 处理多种位置数据格式
- 确保位置信息的完整性和准确性

### 25. `write_roster_daily(self, team_key: str, player_key: str, league_key: str, roster_date: date, season: str, ...)` (第1196行)
**功能**: 写入每日名单数据
- 记录球员在特定日期的名单状态
- 包括首发/替补/伤病储备等状态
- 支持球员状态和伤病信息
- 处理守门员和可编辑状态

### 26. `write_date_dimension(self, date_obj: date, league_key: str, season: str)` (第1266行)
**功能**: 写入日期维度数据
- 为数据分析创建日期维度表
- 支持按联盟和赛季的日期管理
- 避免重复插入相同日期记录

### 27. `write_date_dimensions_batch(self, dates_data: List[Dict])` (第1295行)
**功能**: 批量写入赛季日期维度数据
- 高效的批量日期插入处理
- 自动跳过已存在的日期记录
- 提供详细的处理进度反馈
- 支持大量日期数据的快速入库

## 批量写入方法

### 28. `write_players_batch(self, players_data: List[Dict], league_key: str)` (第1359行)
**功能**: 批量写入球员数据
- 高效处理大量球员记录
- 自动处理球员的合适位置信息
- 智能跳过重复记录
- 分批提交优化内存使用

### 29. `write_teams_batch(self, teams_data: List[Dict], league_key: str)` (第1442行)
**功能**: 批量写入团队数据
- 批量处理团队和管理员信息
- 同时处理团队关联的管理员数据
- 优化的布尔值转换处理
- 详细的处理进度反馈

### 30. `write_transactions_batch(self, transactions_data: List[Dict], league_key: str)` (第1560行)
**功能**: 批量写入交易数据
- 处理复杂的交易API响应结构
- 同时写入交易记录和涉及的球员信息
- 支持多种交易类型（trade/add/drop等）
- 解析交易涉及的球队和球员详情

## 便捷方法

### 31. `parse_coverage_date(self, date_str: Union[str, None])` (第1732行)
**功能**: 解析日期字符串为date对象
- 统一的日期格式转换
- 安全的错误处理
- 支持多种日期格式输入

### 32. `parse_week(self, week_str: Union[str, int, None])` (第1743行)
**功能**: 解析周数
- 安全的周数转换
- 处理字符串和数值类型
- 提供默认值处理

## 数据库管理方法

### 33. `clear_database(self, confirm: bool = False)` (第1752行)
**功能**: 清空数据库 - 重新创建所有表
- 需要明确确认的安全机制
- 完全重建数据库表结构
- 重新初始化数据库连接
- 清除所有历史数据

### 34. `get_database_summary(self)` (第1784行)
**功能**: 获取数据库摘要信息
- 统计所有18个核心表的记录数量
- 提供完整的数据库状态概览
- 识别查询失败的表并标记
- 用于系统状态监控

### 35. `recreate_database_tables(self)` (第1868行)
**功能**: 强制重新创建数据库表结构
- 不删除数据的表结构更新
- 重新初始化数据库会话
- 处理表结构升级场景

## 工具方法

### 36. `_safe_int(self, value)` (第1822行)
**功能**: 安全转换为整数
- 处理多种数值格式输入
- 支持字符串数值转换
- 安全的错误处理和默认值

### 37. `_safe_float(self, value)` (第1831行)
**功能**: 安全转换为浮点数
- 处理数值和字符串输入
- 提供空值和错误处理
- 统一的数值转换逻辑

### 38. `_parse_percentage(self, pct_str)` (第1840行)
**功能**: 解析百分比字符串，返回百分比值（0-100）
- 处理多种百分比格式（.500、50%、0.500）
- 自动识别小数和百分比格式
- 统一转换为百分比数值
- 精确的数值精度控制

## 团队统计辅助方法

### 39. `_extract_team_season_stats(self, stats_data: Dict)` (第1894行)
**功能**: 从团队赛季统计数据中提取完整统计项
- 从team_standings提取排名和战绩信息
- 处理outcome_totals中的胜负记录
- 提取分区战绩和总积分信息
- 安全的数据提取和类型转换

### 40. `_extract_team_weekly_stats(self, stats_data: Dict)` (第1931行)
**功能**: 从团队周统计数据中提取完整的11个统计项
- 解析stats数组构建stat_id映射
- 提取完整的11个核心篮球统计项
- 处理FGM/A、FTM/A等复合统计格式
- 统一的百分比和数值处理

### 41. `write_team_weekly_stats_from_matchup(self, team_key: str, league_key: str, season: str, week: int, team_stats_data: Dict)` (第2021行)
**功能**: 从matchup数据写入团队周统计（专门用于从team_matchups生成数据）
- 专门处理从对战数据生成的周统计
- 使用现有的统计提取逻辑
- 避免重复的API调用
- 基于已有对战数据生成统计记录

### 42. `write_league_roster_positions(self, league_key: str, roster_positions_data)` (第2087行)
**功能**: 写入联盟roster_positions到新表
- 解析联盟的阵容位置配置
- 支持JSON字符串和对象格式输入
- 先删除旧记录再插入新配置
- 提取位置类型、数量、首发位置等信息

## 核心特性总结

### 数据存储架构
- **混合存储设计**: 结构化列存储核心统计 + JSON存储复杂数据
- **批量处理优化**: 默认100条记录批量提交，提高写入效率
- **增量更新支持**: 智能检测重复记录，支持数据更新
- **事务完整性**: 完整的事务管理和错误回滚机制

### 数据完整性保障
- **重复数据检测**: 基于唯一键自动跳过重复记录
- **数据类型转换**: 安全的类型转换和默认值处理
- **关系完整性**: 正确处理表间关系和外键约束
- **数据验证**: 完整的输入数据验证和清理

### 性能优化特性
- **批量写入**: 高效的批处理写入机制
- **连接管理**: 正确的数据库连接生命周期管理
- **内存优化**: 分批处理避免大量数据占用内存
- **索引友好**: 按数据库索引优化的写入顺序

### 错误处理和恢复
- **表结构自动修复**: 检测并自动修复表结构问题
- **事务回滚**: 出错时自动回滚保持数据一致性
- **详细错误信息**: 完整的错误日志和调试信息
- **资源清理**: 异常情况下的资源正确释放

### 统计数据标准化
- **11项核心统计**: 基于Yahoo的标准篮球统计项
- **统一数据格式**: 标准化的统计项命名和数值格式
- **复合统计处理**: 智能解析FGM/A、FTM/A等复合统计
- **百分比标准化**: 统一的百分比格式和精度处理

这个2135行的数据库写入模块是整个ETL系统的数据存储核心，提供了高性能、可靠的数据持久化解决方案，支持从简单的基础数据到复杂的统计分析数据的完整存储需求。



# oauth_authenticator.py Functions Documentation

Yahoo Fantasy Sports OAuth 认证器 - 专门负责OAuth认证流程的独立模块的所有函数及其功能说明。

## YahooOAuthAuthenticator 类的主要函数列表 (共7个函数)

### 类初始化和配置函数

### 1. `__init__(self)` (第25行)
**功能**: 初始化OAuth认证器
- 创建Flask应用实例并配置密钥
- 设置Yahoo OAuth配置参数
- 配置授权URL和token端点
- 注册Flask路由处理器
- 验证OAuth配置完整性

### 2. `_validate_config(self)` (第44行)
**功能**: 验证OAuth配置
- 检查CLIENT_ID和CLIENT_SECRET是否设置
- 显示配置状态的详细信息
- 打印REDIRECT_URI和TOKEN文件路径
- 提供配置错误的友好提示信息
- 引导用户查看配置指南

### 3. `get_oauth_session(self)` (第56行)
**功能**: 创建OAuth2Session
- 使用client_id创建OAuth2Session实例
- 配置redirect_uri为"oob"模式
- 设置scope为Fantasy Sports读写权限
- 返回可用的OAuth2Session对象

## 令牌管理函数

### 4. `refresh_token_if_expired(self)` (第60行)
**功能**: 检查并刷新令牌（如果已过期）
- 从session或文件加载现有令牌
- 检查令牌过期状态（提前60秒刷新）
- 使用refresh_token向Yahoo API请求新令牌
- 计算并设置新令牌的过期时间
- 更新session和文件中的令牌
- 处理令牌刷新失败的情况
- 返回刷新操作成功状态

## 路由注册和Web界面函数

### 5. `_register_routes(self)` (第114行)
**功能**: 注册Flask路由
- 定义所有Web应用的路由处理器
- 包含7个路由：主页、配置检查、授权码处理、成功页、数据获取、登出
- 每个路由都有完整的HTML界面和错误处理
- 提供用户友好的OAuth授权流程

#### 内嵌路由处理器：

**5.1 index() 路由 - `/`**
- 检查现有令牌状态并显示相应界面
- 有令牌时显示已授权界面和可用操作
- 无令牌时显示OAuth授权界面
- 生成Yahoo授权URL并提供授权步骤指导
- 提供授权码输入表单

**5.2 config_check() 路由 - `/config_check`**
- 显示OAuth配置检查页面
- 展示CLIENT_ID、CLIENT_SECRET、REDIRECT_URI等配置信息
- 提供配置状态的详细概览
- 隐藏敏感信息（如CLIENT_SECRET）

**5.3 auth_code() 路由 - `/auth_code` (POST)**
- 处理用户提交的授权码
- 验证授权码有效性
- 使用授权码向Yahoo API获取访问令牌
- 计算并设置令牌过期时间
- 保存令牌到session和文件
- 处理授权失败情况
- 重定向到成功页面

**5.4 success() 路由 - `/success`**
- 显示OAuth授权成功页面
- 展示令牌获取时间和有效期信息
- 提供下一步操作指导
- 引导用户停止服务器并使用数据获取功能

**5.5 fetch() 路由 - `/fetch`**
- 测试API调用功能
- 自动加载和刷新令牌
- 调用Yahoo Fantasy API获取用户games数据
- 显示API响应数据
- 处理API请求错误和异常

**5.6 logout() 路由 - `/logout`**
- 清除session中的所有认证信息
- 重定向到主页重新开始授权流程

## 服务器启动函数

### 6. `start_server(self, host='localhost', port=8000, debug=True, use_https=True)` (第347行)
**功能**: 启动OAuth认证服务器
- 配置服务器启动参数（主机、端口、调试模式）
- 优先尝试HTTPS模式，失败时自动切换到HTTP
- 显示详细的服务器信息和访问地址
- 提供完整的OAuth认证流程指导
- 处理SSL证书相关错误
- 禁用Flask的自动重载器避免重启问题

## 便捷函数（模块级别）

### 7. `create_oauth_app()` (第383行)
**功能**: 创建OAuth认证应用
- 向后兼容的便捷函数
- 创建YahooOAuthAuthenticator实例
- 返回Flask应用对象
- 用于需要直接访问Flask app的场景

### 8. `start_oauth_server(host='localhost', port=8000, debug=True, use_https=True)` (第389行)
**功能**: 启动OAuth认证服务器
- 模块级别的便捷启动函数
- 创建认证器实例并启动服务器
- 处理键盘中断（Ctrl+C）优雅关闭
- 捕获和显示启动错误信息
- 提供用户友好的错误处理和提示

## 核心特性总结

### OAuth认证流程
- **简化的2步授权**: 点击链接获取授权码，输入授权码完成认证
- **自动令牌管理**: 自动刷新过期令牌，无需手动重新认证
- **会话状态管理**: 在Flask session中维护认证状态
- **文件持久化**: 令牌保存到文件供其他模块使用

### Web界面设计
- **响应式HTML界面**: 完整的Bootstrap风格界面
- **状态感知显示**: 根据认证状态显示不同界面
- **详细操作指导**: 每个步骤都有清晰的说明
- **错误处理友好**: 提供明确的错误信息和解决建议

### 安全特性
- **配置验证**: 启动时验证必要的OAuth配置
- **令牌安全存储**: 使用pickle格式安全存储令牌
- **会话密钥**: 使用随机密钥保护Flask会话
- **HTTPS支持**: 优先使用HTTPS保护传输安全

### 开发友好特性
- **调试模式**: 支持Flask调试模式便于开发
- **详细日志**: 完整的操作日志和状态信息
- **配置检查**: 专门的配置检查页面
- **API测试**: 内置API测试功能验证认证状态

### 与系统集成
- **统一令牌管理**: 复用yahoo_api_utils的令牌管理功能
- **环境变量支持**: 从.env文件加载OAuth配置
- **模块化设计**: 可独立运行或作为模块导入使用
- **向后兼容**: 提供便捷函数保持与旧代码的兼容性

这个404行的OAuth认证模块提供了完整的Yahoo Fantasy Sports OAuth认证解决方案，通过用户友好的Web界面简化了复杂的OAuth流程，是整个ETL系统的认证基础设施。

---

# 🔧 函数重构总结 (2024年7月29日更新)

本次重构基于清晰的函数命名约定，改进了代码的可读性和模块化程度。

## 📋 函数命名约定

建立了以下命名约定：
- **`fetch_*`**: Yahoo API调用
- **`get_*`**: 数据库查询
- **`transform_*`**: 数据转换
- **`load_*`**: 数据库写入
- **`verify_*`**: 验证和检查

## 🔄 已完成的重构更改

### 1. 混合职责函数拆分 (yahoo_api_data.py)

#### `_process_roster_data_to_db` → 拆分为3个函数
- ✅ **`transform_roster_data()`** (第519行): 纯数据转换
- ✅ **`load_roster_data()`** (第611行): 纯数据库加载
- ✅ **`_process_roster_data_to_db()`** (第661行): 向后兼容包装

#### `_process_team_matchups_to_db` → 拆分为3个函数
- ✅ **`transform_team_matchups()`** (第1557行): 纯数据转换
- ✅ **`load_team_matchups()`** (第1601行): 纯数据库加载
- ✅ **`_process_team_matchups_to_db()`** (第1622行): 向后兼容包装

### 2. 转换函数重命名 (yahoo_api_data.py)

- ✅ `_extract_team_keys_from_data` → **`transform_team_keys_from_data`**
- ✅ `_extract_team_standings_info` → **`transform_team_standings_info`**
- ✅ `_extract_team_stats_from_matchup_data` → **`transform_team_stats_from_matchup_data`**
- ✅ `_extract_matchup_info` → **`transform_matchup_info`**
- ✅ `_extract_team_matchup_details` → **`transform_team_matchup_details`**
- ✅ `_extract_team_stats_from_matchup` → **`transform_team_stats_from_matchup`**
- ✅ `_extract_position_string` → **`transform_position_string`**

### 3. 写入函数重命名 (yahoo_api_data.py)

- ✅ `_write_teams_to_db` → **`load_teams_to_db`**
- ✅ `_write_transactions_to_db` → **`load_transactions_to_db`**
- ✅ `_write_league_standings_to_db` → **`load_league_standings_to_db`**

### 4. 验证函数重命名 (yahoo_api_data.py)

- ✅ `_ensure_league_exists_in_db` → **`verify_league_exists_in_db`**
- ✅ `_ensure_league_selected` → **`verify_league_selected`**

### 5. 转换函数重命名 (database_writer.py)

- ✅ `_extract_core_team_weekly_stats` → **`transform_core_team_weekly_stats`**
- ✅ `_extract_team_season_stats` → **`transform_team_season_stats`**
- ✅ `_extract_team_weekly_stats` → **`transform_team_weekly_stats_from_stats_data`**

## 📊 重构统计

- **总计处理函数**: 17个
- **拆分的混合职责函数**: 2个 → 6个
- **重命名的转换函数**: 10个
- **重命名的写入函数**: 3个
- **重命名的验证函数**: 2个

## ✅ 重构优势

### 1. **职责分离清晰**
- 数据转换与数据库操作完全分离
- 每个函数单一职责，便于测试和维护

### 2. **命名约定一致**
- 函数名称直接反映功能类型
- 代码可读性大幅提升

### 3. **向后兼容性**
- 保留原有函数作为包装方法
- 现有代码无需修改即可继续工作

### 4. **可测试性增强**
- 转换逻辑可独立测试
- 数据库操作可独立测试

### 5. **重用性提升**
- 转换函数可被多个地方重用
- 减少代码重复

## 🔮 未来改进方向

1. **进一步模块化**: 考虑将转换函数分组到独立模块
2. **类型注解完善**: 为所有重构函数添加完整类型注解
3. **单元测试**: 为拆分的函数编写专门的单元测试
4. **文档化**: 为新函数添加详细的docstring文档

---


