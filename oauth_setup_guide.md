# Yahoo OAuth 设置指南

## 问题诊断

您的 `.env` 文件缺少Yahoo OAuth必需的环境变量，这导致OAuth认证失败。

## 解决步骤

### 1. 配置Yahoo Developer Console

1. 访问 [Yahoo Developer Console](https://developer.yahoo.com/apps/)
2. 登录您的Yahoo账号
3. 点击 "Create an Application"

#### 关键配置选项：

**应用类型选择**：
- ✅ **选择 "Installed Application"** (推荐用于独立数据管理)
- ❌ 不要选择 "Web Application" (除非您需要管理其他用户的数据)

**回调URL配置**：
- **方案1 (推荐)**: 留空或设置为 `oob`
- **方案2**: 设置为 `https://localhost:8000/auth/callback`

**权限范围**：
- 确保勾选 Fantasy Sports 相关权限

### 2. 更新 .env 文件

将以下内容添加到您的 `.env` 文件中：

```bash
# 现有数据库配置
DB_USER=fantasy_user
DB_PASSWORD=fantasyPassword
DB_HOST=localhost
DB_PORT=5432
DB_NAME=fantasy_db

# Yahoo OAuth 配置 (请替换为您的实际值)
CLIENT_ID=your_yahoo_client_id_here
CLIENT_SECRET=your_yahoo_client_secret_here
REDIRECT_URI=https://localhost:8000/auth/callback
```

### 3. 可能需要的app.py修改

如果问题持续，可能需要以下修改：

#### 修改1: 处理"Installed Application"类型
```python
# 在app.py中，如果使用oob回调
redirect_uri = "oob"  # 或者从环境变量获取
```

#### 修改2: 添加错误处理
```python
@app.route('/auth/callback')
def callback():
    try:
        # 现有代码...
    except Exception as e:
        app.logger.error(f"OAuth callback error: {str(e)}")
        return f"OAuth认证失败: {str(e)}", 400
```

## 常见问题排查

### 1. "Invalid Grant" 错误
- 确保Yahoo Developer Console中的callback区域为空
- 尝试创建新应用并选择"Installed Application"

### 2. "Forbidden" 错误  
- 确保请求来自代码而不是浏览器直接访问
- 检查HTTPS配置

### 3. Redirect URI 不匹配
- 确保 `.env` 中的 `REDIRECT_URI` 与Yahoo Console中的配置完全一致
- 注意拼写：使用 `redirect_uri` 而不是 `redirect_url`

### 4. HTTPS证书问题
如果遇到SSL证书问题，可以临时使用HTTP模式：
```python
# 在app.py的最后，修改为：
if __name__ == '__main__':
    app.run(host='localhost', port=8000, debug=True)  # 移除ssl_context
```

但注意：Yahoo OAuth通常要求HTTPS。

## 测试步骤

1. 更新 `.env` 文件
2. 重启 `python app.py`
3. 访问 `https://localhost:8000/auth`
4. 完成Yahoo登录流程

## 替代方案：使用命令行工具

如果Web界面持续有问题，可以直接使用我们的时间序列数据获取器：

```bash
# 确保已配置Yahoo API令牌
python time_series_fetcher.py --historical --weeks-back=3
```

这需要您已经有有效的Yahoo API访问令牌。 