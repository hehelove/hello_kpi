#!/bin/bash
# 定位监控服务部署脚本
# 用法: sudo ./scripts/deploy_loc_monitor.sh

set -e

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# 检查 root 权限
check_root() {
    if [ "$EUID" -ne 0 ]; then
        log_error "请使用 sudo 运行此脚本"
        exit 1
    fi
}

# 获取脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="carservice_loc"

# 源文件路径
MONITOR_SH="${SCRIPT_DIR}/monitor_loc.sh"
MONITOR_PY="${SCRIPT_DIR}/monitor.py"
SERVICE_FILE="${SCRIPT_DIR}/carservice_loc.service"

# 目标路径
DEPLOY_DIR="/opt/env/tmp/hb_carservice"
SYSTEMD_DIR="/etc/systemd/system"

main() {
    log_info "=========================================="
    log_info "  定位监控服务部署"
    log_info "=========================================="
    
    check_root
    
    # 1. 检查源文件
    log_info "检查源文件..."
    for file in "$MONITOR_SH" "$MONITOR_PY" "$SERVICE_FILE"; do
        if [ ! -f "$file" ]; then
            log_error "找不到文件: $file"
            exit 1
        fi
    done
    log_info "  ✓ 源文件检查通过"
    
    # 2. 创建部署目录
    log_info "创建部署目录: $DEPLOY_DIR"
    mkdir -p "$DEPLOY_DIR"
    
    # 3. 复制脚本文件
    log_info "复制脚本文件..."
    cp "$MONITOR_SH" "$DEPLOY_DIR/"
    cp "$MONITOR_PY" "$DEPLOY_DIR/"
    log_info "  ✓ 已复制到 $DEPLOY_DIR"
    
    # 4. 添加执行权限
    log_info "设置执行权限..."
    chmod +x "$DEPLOY_DIR/monitor_loc.sh"
    chmod +x "$DEPLOY_DIR/monitor.py"
    log_info "  ✓ 执行权限已设置"
    
    # 5. 停止旧服务（如果存在）
    if systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
        log_info "停止旧服务..."
        systemctl stop "$SERVICE_NAME"
        log_info "  ✓ 旧服务已停止"
    fi
    
    # 6. 复制 service 文件
    log_info "安装 systemd 服务..."
    cp "$SERVICE_FILE" "$SYSTEMD_DIR/${SERVICE_NAME}.service"
    log_info "  ✓ 已复制到 $SYSTEMD_DIR"
    
    # 7. 重新加载 systemd
    log_info "重新加载 systemd..."
    systemctl daemon-reload
    log_info "  ✓ systemd 已重新加载"
    
    # 8. 设置开机自启动
    log_info "设置开机自启动..."
    systemctl enable "$SERVICE_NAME"
    log_info "  ✓ 开机自启动已启用"
    
    # 9. 启动服务
    log_info "启动服务..."
    systemctl start "$SERVICE_NAME"
    log_info "  ✓ 服务已启动"
    
    # 10. 显示状态
    echo ""
    log_info "=========================================="
    log_info "  部署完成!"
    log_info "=========================================="
    echo ""
    log_info "服务状态:"
    systemctl status "$SERVICE_NAME" --no-pager -l || true
    echo ""
    log_info "常用命令:"
    echo "  查看状态: systemctl status $SERVICE_NAME"
    echo "  查看日志: journalctl -u $SERVICE_NAME -f"
    echo "  重启服务: systemctl restart $SERVICE_NAME"
    echo "  停止服务: systemctl stop $SERVICE_NAME"
    echo "  禁用自启: systemctl disable $SERVICE_NAME"
}

# 卸载函数
uninstall() {
    log_info "卸载定位监控服务..."
    
    check_root
    
    # 停止服务
    if systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
        log_info "停止服务..."
        systemctl stop "$SERVICE_NAME"
    fi
    
    # 禁用自启动
    if systemctl is-enabled --quiet "$SERVICE_NAME" 2>/dev/null; then
        log_info "禁用自启动..."
        systemctl disable "$SERVICE_NAME"
    fi
    
    # 删除 service 文件
    if [ -f "$SYSTEMD_DIR/${SERVICE_NAME}.service" ]; then
        log_info "删除服务文件..."
        rm -f "$SYSTEMD_DIR/${SERVICE_NAME}.service"
    fi
    
    # 重新加载 systemd
    systemctl daemon-reload
    
    log_info "✓ 卸载完成"
    log_info "注意: $DEPLOY_DIR 目录未删除，如需删除请手动执行:"
    echo "  rm -rf $DEPLOY_DIR"
}

# 参数处理
case "${1:-}" in
    --uninstall|-u)
        uninstall
        ;;
    --help|-h)
        echo "用法: sudo $0 [选项]"
        echo ""
        echo "选项:"
        echo "  (无参数)      安装并启动服务"
        echo "  --uninstall   卸载服务"
        echo "  --help        显示帮助"
        ;;
    *)
        main
        ;;
esac

