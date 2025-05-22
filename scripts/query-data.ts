import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

async function main() {
  try {
    // 查询所有游戏
    console.log('===== 游戏数据 =====');
    const games = await prisma.game.findMany({
      select: {
        id: true,
        game_key: true,
        name: true,
        code: true,
        season: true,
        type: true
      }
    });
    console.log(`找到 ${games.length} 个游戏：`);
    console.log(games);

    // 查询用户
    console.log('\n===== 用户数据 =====');
    const users = await prisma.user.findMany();
    console.log(`找到 ${users.length} 个用户：`);
    console.log(users);

    // 查询联盟
    // ...

  } catch (error) {
    console.error('查询数据时出错:', error);
  } finally {
    await prisma.$disconnect();
  }
}

main(); 