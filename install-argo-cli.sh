#!/bin/bash

# Argo Workflows CLI 安装脚本

set -e

# 检测操作系统
OS="linux"
if [[ "$(uname -s)" == "Darwin" ]]; then
    OS="darwin"
fi

# 检测架构
ARCH="amd64"
if [[ "$(uname -m)" == "arm64" ]] || [[ "$(uname -m)" == "aarch64" ]]; then
    ARCH="arm64"
fi

# Argo Workflows 版本
ARGO_VERSION="v3.6.7"

echo "正在为 ${OS}-${ARCH} 安装 Argo Workflows CLI ${ARGO_VERSION}..."

# 下载二进制文件
echo "下载 argo-${OS}-${ARCH}.gz..."
curl -sLO "https://github.com/argoproj/argo-workflows/releases/download/${ARGO_VERSION}/argo-${OS}-${ARCH}.gz"

# 解压
echo "解压文件..."
gunzip "argo-${OS}-${ARCH}.gz"

# 添加执行权限
echo "设置执行权限..."
chmod +x "argo-${OS}-${ARCH}"

# 移动到 PATH
echo "安装到 /usr/local/bin/argo..."
sudo mv "argo-${OS}-${ARCH}" /usr/local/bin/argo

# 验证安装
echo "验证安装..."
argo version

echo "✅ Argo Workflows CLI 安装完成！"
echo ""
echo "常用命令："
echo "  argo list -n argo                    # 列出工作流"
echo "  argo get <workflow-name> -n argo     # 查看工作流详情"
echo "  argo logs <workflow-name> -n argo    # 查看工作流日志"
echo "  argo submit <workflow.yaml>          # 提交工作流" 