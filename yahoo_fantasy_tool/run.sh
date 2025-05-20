#!/bin/bash

# 检查Python是否已安装
if ! [ -x "$(command -v python3)" ]; then
  echo '错误: 未安装Python3' >&2
  exit 1
fi

# 检查pip是否已安装
if ! [ -x "$(command -v pip)" ] && ! [ -x "$(command -v pip3)" ]; then
  echo '错误: 未安装pip' >&2
  exit 1
fi

# 确定使用pip还是pip3
PIP_CMD="pip"
if [ -x "$(command -v pip3)" ]; then
  PIP_CMD="pip3"
fi

# 检查是否在虚拟环境中
if [ -z "$VIRTUAL_ENV" ]; then
  echo "警告: 未检测到虚拟环境。建议在虚拟环境中运行此应用程序。"
  echo "如需创建虚拟环境，请参阅README.md"
fi

# 安装依赖
echo "安装依赖..."
$PIP_CMD install -r requirements.txt

# 确保已安装cryptography库
if ! $PIP_CMD show cryptography &>/dev/null; then
  echo "安装cryptography库..."
  $PIP_CMD install cryptography
fi

# 运行应用
echo "启动雅虎NBA Fantasy工具..."
echo "请在浏览器中访问 https://localhost:8000"
python3 app.py 