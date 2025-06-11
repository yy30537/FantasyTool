


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


# hist


请看终端输出，现在运行脚本后的数据库情况。

让我们一步一步来修正现在的问题。

我们已经获得的api数据样本：

(venv) yy@yy-Swift-SFA16-41:~/Documents/GitHub/FantasyTool$ ls sample_data/
league_info_454_l_53472.json                  

player_season_stats_454_l_53472.json

league_players_454_l_53472.json               

team_matchups_454_l_53472_t_1.json

league_settings_454_l_53472.json              

team_matchups_454_l_53472_t_2.json

league_standings_454_l_53472.json             

team_roster_454_l_53472_t_1.json

league_teams_454_l_53472.json                 

team_roster_454_l_53472_t_2.json

league_transactions_454_l_53472.json          

user_games.json

player_daily_stats_20240601_454_l_53472.json  

user_leagues_364.json

player_daily_stats_20240615_454_l_53472.json  

user_leagues_375.json

player_daily_stats_20240701_454_l_53472.json

可供查询api返回的数据的结构；

---

首先，输出的数据库摘要不全，没有涵盖所有表格。然后，清理main，我们现在只focus在nba的fantasy。
---

我们现在定义的数据库表 @model.py ：


现在运行脚本 @yahoo_api_data.py 后以下表格没有数据：

- date_dimension
- player_daily_stats
- player_season_stats
- roster_daily
- team_stats_season
- team_stats_weekly

其余表格的数据正确的加载了。让我们一项一项来。首先，修复获取赛季日程数据的逻辑，我们之前有写的。



相关脚本：
- @model.py
- @yahoo_api_data.py
- @database_writer.py 
- @yahoo_api_utils.py 



这样我们能获得date_dimension，然后才能enable获得：

- 每日阵容 (roster_daily)
- 球员日统计 (player_daily_stats)

在没有date_dimension我们也应该可以直接获取以下表：

- 球员赛季统计 (player_season_stats)
- 团队周统计 (team_stats_weekly)
- 团队赛季统计 (team_stats_season)
