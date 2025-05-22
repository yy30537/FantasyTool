import express from 'express';
import path from 'path';
import { PrismaClient } from '@prisma/client';

const app = express();
const port = process.env.PORT || 3000;
const prisma = new PrismaClient();

// 设置视图引擎
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(express.static(path.join(__dirname, 'public')));

// 主页
app.get('/', async (req, res) => {
  try {
    const games = await prisma.game.findMany({
      orderBy: { season: 'desc' }
    });
    
    const users = await prisma.user.findMany();
    
    res.render('index', {
      games,
      users,
      title: 'Yahoo Fantasy工具'
    });
  } catch (error) {
    console.error('主页加载错误:', error);
    res.status(500).send('服务器错误');
  }
});

// 游戏详情页
app.get('/games/:id', async (req, res) => {
  try {
    const gameId = parseInt(req.params.id);
    
    const game = await prisma.game.findUnique({
      where: { id: gameId },
      include: {
        leagues: true
      }
    });
    
    if (!game) {
      return res.status(404).send('游戏未找到');
    }
    
    res.render('game', {
      game,
      title: `${game.name} ${game.season}`
    });
  } catch (error) {
    console.error('游戏详情页加载错误:', error);
    res.status(500).send('服务器错误');
  }
});

// 联盟详情页
app.get('/leagues/:id', async (req, res) => {
  try {
    const leagueId = parseInt(req.params.id);
    
    const league = await prisma.league.findUnique({
      where: { id: leagueId },
      include: {
        game: true,
        teams: {
          include: {
            manager: true,
            standings: true
          }
        },
        settings: true
      }
    });
    
    if (!league) {
      return res.status(404).send('联盟未找到');
    }
    
    res.render('league', {
      league,
      title: league.name
    });
  } catch (error) {
    console.error('联盟详情页加载错误:', error);
    res.status(500).send('服务器错误');
  }
});

// 队伍详情页
app.get('/teams/:id', async (req, res) => {
  try {
    const teamId = parseInt(req.params.id);
    
    const team = await prisma.team.findUnique({
      where: { id: teamId },
      include: {
        league: true,
        manager: true,
        standings: true,
        roster: {
          include: {
            player: true
          }
        }
      }
    });
    
    if (!team) {
      return res.status(404).send('队伍未找到');
    }
    
    res.render('team', {
      team,
      title: team.name
    });
  } catch (error) {
    console.error('队伍详情页加载错误:', error);
    res.status(500).send('服务器错误');
  }
});

// API路由
app.get('/api/games', async (req, res) => {
  try {
    const games = await prisma.game.findMany({
      orderBy: { season: 'desc' }
    });
    res.json(games);
  } catch (error) {
    res.status(500).json({ error: '获取游戏数据失败' });
  }
});

app.get('/api/leagues', async (req, res) => {
  try {
    const leagues = await prisma.league.findMany({
      include: {
        game: true
      }
    });
    res.json(leagues);
  } catch (error) {
    res.status(500).json({ error: '获取联盟数据失败' });
  }
});

app.get('/api/teams', async (req, res) => {
  try {
    const teams = await prisma.team.findMany({
      include: {
        league: true,
        manager: true
      }
    });
    res.json(teams);
  } catch (error) {
    res.status(500).json({ error: '获取队伍数据失败' });
  }
});

// 启动服务器
app.listen(port, () => {
  console.log(`服务器运行在 http://localhost:${port}`);
}); 