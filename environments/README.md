# Environment Configuration Files

这个目录包含不同环境的配置文件模板。

## 使用方法

1. 复制模板文件：
   ```bash
   cp environments/development.env .env
   ```

2. 编辑 `.env` 文件，填入实际的配置值

3. 应用会自动加载根目录下的 `.env` 文件

## 文件说明

- `development.env` - 开发环境配置模板
- `production.env` - 生产环境配置模板 (计划中)
- `testing.env` - 测试环境配置模板 (计划中)

## 配置项

详细的配置项说明请参考各个模板文件中的注释。