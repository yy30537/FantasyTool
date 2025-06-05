
## 关于Yahoo Fantasy API时间序列数据支持的分析

根据你对`Yahoo-Fantasy-Sports-API-Guide.md`的详细查看，以下是支持时间选择的数据类型：

### 1. **Roster数据 - 支持按时间获取**
- **NFL**: 按周获取 - `roster;week=10`
- **MLB/NBA/NHL**: 按日期获取 - `roster;date=2011-05-01`
- **示例URL**: 
  - `https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster;week=10`
  - `https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/roster;date=2011-05-01`

### 2. **球员统计数据 - 支持多种时间维度**
- **Season统计**: `stats;type=season` (整赛季数据)
- **Weekly统计** (NFL): `stats;type=week;week=10`  
- **Daily统计** (MLB/NBA/NHL): `stats;type=date;date=2011-07-06`
- **示例URL**:
  - `https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/stats;type=season`
  - `https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/stats;type=date;date=2011-07-06`

### 3. **团队统计数据 - 支持时间维度**
- 与球员统计类似，支持season、week、date等维度

### 4. **Matchups数据 - 支持按周获取**
- **NFL**: `matchups;weeks=1,5` (可以指定多个周)
- **示例URL**: `https://fantasysports.yahooapis.com/fantasy/v2/team/{team_key}/matchups;weeks=1,5`

### 5. **Players Collection过滤器 - 支持时间排序**
- `sort_type`: season, date, week, lastweek, lastmonth
- `sort_season`: 年份
- `sort_date`: YYYY-MM-DD (棒球、篮球、冰球)
- `sort_week`: 周数 (橄榄球)



## 时间序列数据处理建议

### 数据库设计方面：
1. **Roster表**：已经有`coverage_date`字段，可以存储不同日期的roster快照
2. **PlayerStats表**：可以增加时间维度字段（`coverage_type`, `week`, `date`, `season`）
3. **新增时间维度表**：考虑增加专门的时间序列统计表

### 实现策略：
1. **获取历史数据**：使用API的时间参数循环获取不同时期的数据
2. **增量更新**：定期更新最新数据，避免重复获取
3. **数据完整性**：确保时间序列数据的连续性和一致性

这样我们就可以构建一个完整的时间序列数据库，支持历史分析、趋势追踪等高级分析功能！


# table

执行 `python3 model.py`，创建数据库模型，表包括：
  - games
  - leagues
  - league_settings
  - teams
  - managers
  - players
  - player_stats_history
  - team_stats
  - rosters
  - roster_history
  - transactions
  - transaction_players

  # 现在我想你帮助我申请一个职位，首先请阅读以下关于这个职位的信息

  ## About the job

  ### Let’s talk talent

  You have talent. Ready to make impact. We know. We also know that making impact starts with that first contact between likeminded people who have the same drive to shape their future. We see you, with your skills, personality and dreams. Now let’s take it to even greater heights by means of one of our talent programs for master’s graduates.

  We are Mploy Associates, a talent focused consultancy firm ready to make not only our clients’ teams successful but also you. Your talent, our focus.



  ### More specifically?

  We are a consultancy firm specialized in the areas of data, finance, and analytics and work on a variety of projects like investment analyses, data engineering, quant modelling and much more. Our clients are banks, insurance companies, and asset managers and we are still expanding.

  We help our clients build successful teams by providing the right skills, talents and know-how at the right place and time and developing ourselves continuously to stay successful. We believe that investing in talent through proven methods always pays out. Today we are proud to say that we are an international-driven company with more than 100 people and many top-tier clients who share the same vision.



  ### How do you fit into this vision?

  You will work as a Data Engineer for one of our clients with a special focus on your own development through one of our Talent Programs. Our Talent Programs are designed to accelerate your career from entry level to medior+ level within 24 months. By means of certified training courses, coaching and intervision sessions our employees follow an above-average steep career curve. Successful graduation in our Talent Programs usually results in a permanent medior+ or senior position at one of our clients or where it becomes your responsibility to keep the team successful.



  ### Your activities as a Data Engineer

  You are mainly involved in projects to enhance data storage solutions in cloud platforms as AWS, Azure, Google Cloud or Oracle Cloud. In these platforms you develop data pipelines and make data available for the business teams of our clients. Your day-to-day responsibilities include:

  Supervising the technical implementation of data elements, from raw to cleansed data;
  Making an inventory of the relevant databases within the organisation and gaining access to them;
  Developing and maintaining scripts, tools and applications based on different languages and structures (e.g., Python, PySpark, Java);
  Migrating projects between different environments (e.g. from Hadoop to Azure);
  Documenting and presenting the implemented dataflows to relevant stakeholders.

  ### What are we looking for?

  We are looking for graduates with different data and IT-related backgrounds, experiences, and passions. It speaks for itself that you are ambitious, self-reflective and dare to be confident in an authentic way.

  Furthermore, you are energized by stakeholder management and are well adept at gathering reliable information. You use this in combination with a consultive mindset to make our clients more successful. Last but not least, you are eager to develop yourself on personal, professional and technical level with our personalized in-house Talent Programs. To make it more specific:

  You have 0-3 years of work experience and want to learn everything about data engineering;
  You want to put the skills into practice that you have acquired during your master’s or PhD in Computer Science, Data Science or another related study;
  You are familiar with one or more operating systems (e.g. Linux, Unix, Windows), cloud platforms (e.g., Azure, AWS, Google Cloud) and common programming languages (e.g., Java, SQL, Python, C#);
  You do not hesitate to use your analytical skills to solve complex problems creatively and logically.


  ### Interested?

  For more information about our different positions and Talent Programs you can visit our website www.mployassociates.com. Want to know more about the vacancy? Use the apply button above or email us at careers@mployassociates.com from the moment you start your master’s so we can help you prepare for your dream career. We will be in touch.

  Please note that unfortunately there is no point in applying when you do not live in the Netherlands or if you have more than 3 years of work experience. These applications will not be processed.

  Are you interested in multiple positions at Mploy Associates? Apply for one position and mention the other position(s) in your motivation message. Together we will find the best match for you.

  # 现在，你也知道我们在做一个yahoo nba fantasy的工具，我想通过这个project练习一些data engineer/data scientist/data analyst 会设计的技能，例如ETL，用pyspark处理大数据，还有更多。

  # 我envision这个project会：

  - 用户用yahoo账号登录这个工具，
  - 工具会用 yahoo fantasy api 获取玩家的games和leagues，包括league_settings。
  - 用户选择一个league使用这个工具。
  - 工具用yahoo api：
    1. 获取历史数据 （如果是第一次登录使用）
    2. 更新最新数据 （如果数据库已经被更新过）
    这些数据包括：
    - teams
    - managers
    - players
    - player_stats
    - player_stats_history
    - team_stats
    - rosters
    - roster_history
    - transactions
    - transaction_players
  - 工具包括 (还未implement):
    - team analysis 
    - trade analysis
    - add/drop suggestion based on current matchup
    - streaming plan

