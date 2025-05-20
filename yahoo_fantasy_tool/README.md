# 雅虎NBA Fantasy工具

这是一个用于获取和分析雅虎NBA Fantasy数据的简单工具。它使用OAuth 2.0认证流程获取用户授权，然后通过Yahoo Fantasy Sports API获取用户的历年NBA Fantasy队伍数据。

## 功能

- 使用雅虎账号登录授权
- 查看当前赛季的队伍数据
- 查看历年所有参与过的NBA Fantasy赛季数据

## 安装

1. 确保你已经安装了Python 3.7+
2. 克隆或下载这个仓库
3. 建议使用虚拟环境:

```bash
# 创建虚拟环境
python3 -m venv venv

# 激活虚拟环境 (Linux/macOS)
source venv/bin/activate

# 激活虚拟环境 (Windows)
# venv\Scripts\activate
```

4. 安装所需依赖：

```bash
pip install -r requirements.txt

# 确保安装cryptography库（用于HTTPS支持）
pip install cryptography
```

## 使用方法

1. 进入项目目录（确保已激活虚拟环境）：

```bash
cd yahoo_fantasy_tool
```

2. 运行Flask应用：

```bash
python app.py
```

或使用提供的脚本（会自动安装依赖）：

```bash
./run.sh
```

3. 在浏览器中访问 `https://localhost:8000`

4. 点击"使用雅虎账号登录"按钮，授权应用访问你的Fantasy数据

5. 授权成功后，你将能够查看你的Fantasy队伍数据

## HTTPS重要说明

雅虎OAuth 2.0要求重定向URL使用HTTPS协议。这个应用程序使用Flask的`ssl_context='adhoc'`选项来创建自签名证书，这需要`cryptography`库。如果你遇到以下错误：

```
TypeError: Using ad-hoc certificates requires the cryptography library.
```

请确保安装了cryptography库：

```bash
pip install cryptography
```

如果无法安装cryptography库，应用程序会尝试回退到HTTP模式，但雅虎OAuth可能会因为不安全的重定向URL而拒绝认证请求。

## 授权流程说明

本应用使用"服务器端应用的授权码流程"（Authorization Code Flow for Server-side Apps）来实现OAuth 2.0认证：

1. 用户点击登录按钮，应用生成授权URL并重定向到雅虎
2. 用户在雅虎登录并授权应用
3. 雅虎重定向回应用的回调URL，附带授权码
4. 应用使用授权码交换访问令牌和刷新令牌
5. 应用使用访问令牌请求Fantasy数据
6. 当访问令牌过期时，应用自动使用刷新令牌获取新的访问令牌

## 安全说明

- 这个工具使用Flask的`secret_key`来保护会话，确保每次启动应用时都会生成新的密钥
- 你的OAuth令牌只会存储在服务器会话中，不会持久化到磁盘
- 应用默认使用HTTPS（自签名证书）来保护数据传输安全

## 技术说明

- **Flask**: Web应用框架
- **Requests**: HTTP客户端库
- **Requests-OAuthlib**: OAuth认证库
- **Python-dotenv**: 环境变量管理
- **Cryptography**: 自签名SSL证书生成

## 问题排查

如果遇到问题，请尝试以下操作：

1. **SSL/HTTPS错误**：确保已安装`cryptography`库
2. **授权失败**：检查`redirect_uri`是否与雅虎应用设置中的回调URL匹配
3. **令牌过期**：应用会自动刷新令牌，如果仍出现问题，请重新登录
4. **API调用错误**：检查雅虎API响应中的错误信息，可能是权限或请求格式问题

## 注意事项

- 访问HTTPS时，由于使用的是自签名证书，浏览器可能会显示警告，这是正常的，可以继续访问
- 如果长时间未使用，令牌可能会过期，需要重新登录授权
- 雅虎API可能会有访问频率限制，请避免短时间内发起大量请求 