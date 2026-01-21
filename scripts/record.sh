#!/usr/bin/env bash
#
# KPI ROS2 Bag 录制脚本
# 功能：录制 KPI 分析所需的 topic 数据
#

# set -euo pipefail

# =========================
# 环境配置 (自动检测运行环境)
# =========================
setup_ros_env() {
    local arch
    arch=$(uname -m)
    
    if [[ "$arch" == "aarch64" ]] && [[ -d "/opt/env/ros2" ]]; then
        # 车载嵌入式环境 (aarch64)
        export AMENT_PREFIX_PATH="/opt/env/hv_interface:/opt/env/ros2"
        export CMAKE_PREFIX_PATH="/opt/env/hv_interface:/opt/env/ros2"
        export COLCON_PREFIX_PATH="/opt/env/hv_interface:/opt/env/ros2"
        export PKG_CONFIG_PATH="/opt/env/ros2/lib/aarch64-linux-gnu/pkgconfig:/opt/env/ros2/lib/pkgconfig"
        export PYTHONPATH="/opt/env/python/lib/python3.8/site-packages:/opt/env/hv_interface/lib/python3.8/site-packages:/opt/env/ros2/lib/python3.8/site-packages"
        export LD_LIBRARY_PATH="/lib/aarch64-linux-gnu:/opt/env/python/lib:/opt/env/hv_interface/lib:/opt/env/ros2/opt/yaml_cpp_vendor/lib:/opt/env/ros2/lib:/app/lib"
        export PATH="/opt/env/ros2/bin:$PATH"
    elif [[ -f "/opt/ros/humble/setup.bash" ]]; then
        # 开发机环境 (ROS2 Humble)
        # ros
        source /opt/ros/humble/setup.bash
    elif [[ -f "/opt/ros/iron/setup.bash" ]]; then
        # 开发机环境 (ROS2 Iron)
        source /opt/ros/iron/setup.bash
    else
        echo "ERROR: 未找到 ROS2 环境，请先安装 ROS2" >&2
        exit 1
    fi
    
    export ROS_LOG_DIR=/tmp
}

setup_ros_env

# =========================
# 配置参数
# =========================
DATA_DISK="/data_collection_disk"
MIN_DISK_SIZE_GB=100           # 最小磁盘容量要求 (1TB = 1000GB)
MIN_FREE_SPACE_GB=50            # 最小剩余空间要求 (50GB)
DATE_STR=$(date +"%Y%m%d")
TIME_STR=$(date +"%H%M%S")
BASE_DIR="${DATA_DISK}/${DATE_STR}/kpi"
BAG_PREFIX="kpi_bag_${TIME_STR}"
LOG_FILE="${BASE_DIR}/record_${TIME_STR}.log"

# 录制参数
SPLIT_DURATION=600              # 分割时长 (秒)，默认 10 分钟
STORAGE=sqlite3
COMPRESSION_MODE=none           # 不压缩

# =========================
# 颜色输出
# =========================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info()  { echo -e "${GREEN}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') $*" >&2; }

# =========================
# 磁盘检查函数
# =========================
check_disk_exists() {
    if [[ ! -d "$DATA_DISK" ]]; then
        log_error "数据盘 $DATA_DISK 不存在！"
        return 1
    fi
    
    if ! mountpoint -q "$DATA_DISK" 2>/dev/null; then
        log_warn "$DATA_DISK 可能未挂载为独立分区"
    fi
    
    return 0
}

check_disk_capacity() {
    # 获取磁盘总容量 (GB)
    local total_kb
    total_kb=$(df -k "$DATA_DISK" | awk 'NR==2 {print $2}')
    local total_gb=$((total_kb / 1024 / 1024))
    
    log_info "磁盘总容量: ${total_gb} GB"
    
    if [[ $total_gb -lt $MIN_DISK_SIZE_GB ]]; then
        log_error "磁盘容量不足！要求 >= ${MIN_DISK_SIZE_GB} GB，实际 ${total_gb} GB"
        return 1
    fi
    
    log_info "✓ 磁盘容量检查通过 (>= ${MIN_DISK_SIZE_GB} GB)"
    return 0
}

check_free_space() {
    # 获取剩余空间 (GB)
    local avail_kb
    avail_kb=$(df -k "$DATA_DISK" | awk 'NR==2 {print $4}')
    local avail_gb=$((avail_kb / 1024 / 1024))
    
    log_info "剩余空间: ${avail_gb} GB"
    
    if [[ $avail_gb -lt $MIN_FREE_SPACE_GB ]]; then
        log_error "剩余空间不足！要求 >= ${MIN_FREE_SPACE_GB} GB，实际 ${avail_gb} GB"
        return 1
    fi
    
    log_info "✓ 剩余空间检查通过 (>= ${MIN_FREE_SPACE_GB} GB)"
    return 0
}

# =========================
# 信号处理
# =========================
RECORD_PID=""
CLEANUP_DONE=false

cleanup() {
    # 防止重复清理
    if [[ "$CLEANUP_DONE" == "true" ]]; then
        return
    fi
    CLEANUP_DONE=true
    
    local exit_code=${1:-0}
    log_info "正在优雅退出 (exit_code=$exit_code)..."
    
    if [[ -n "$RECORD_PID" ]] && kill -0 "$RECORD_PID" 2>/dev/null; then
        log_info "正在停止录制进程 (PID: $RECORD_PID)，等待数据写入..."
        
        # 发送 SIGINT 让 ros2 bag record 优雅退出并保存数据
        kill -INT "$RECORD_PID" 2>/dev/null || true
        
        # 等待进程退出，最多等待 60 秒以确保大文件写入完成
        local count=0
        local max_wait=60
        while kill -0 "$RECORD_PID" 2>/dev/null && [[ $count -lt $max_wait ]]; do
            if [[ $((count % 10)) -eq 0 ]] && [[ $count -gt 0 ]]; then
                log_info "等待录制进程退出... (${count}/${max_wait}s)"
            fi
            sleep 1
            ((count++))
        done
        
        if kill -0 "$RECORD_PID" 2>/dev/null; then
            log_warn "进程未响应 SIGINT，发送 SIGTERM..."
            kill -TERM "$RECORD_PID" 2>/dev/null || true
            sleep 5
            
            if kill -0 "$RECORD_PID" 2>/dev/null; then
                log_error "进程仍未响应，强制终止 (数据可能丢失)..."
                kill -KILL "$RECORD_PID" 2>/dev/null || true
            fi
        fi
        
        wait "$RECORD_PID" 2>/dev/null || true
        log_info "录制进程已停止"
    fi
    
    log_info "录制已停止，数据保存在: ${BASE_DIR}"
}

# 捕获各种退出信号
trap 'cleanup 130' SIGINT      # Ctrl+C
trap 'cleanup 143' SIGTERM     # kill
trap 'cleanup 129' SIGHUP      # 终端断开
trap 'cleanup 131' SIGQUIT     # Ctrl+\

# 捕获脚本错误退出 (set -e 触发)
trap 'cleanup $?' ERR

# 捕获正常退出
trap 'cleanup $?' EXIT

# =========================
# 使用帮助
# =========================
usage() {
    cat << EOF
用法: $(basename "$0") [选项]

KPI ROS2 Bag 录制脚本

选项:
    -d, --duration SEC    分割时长（秒），默认 600
    -o, --output DIR      输出目录，默认 ${DATA_DISK}/<日期>/kpi
    -h, --help            显示帮助信息

示例:
    $(basename "$0")                    # 使用默认配置录制
    $(basename "$0") -d 300             # 每 5 分钟分割一次
    $(basename "$0") -o /custom/path    # 指定输出目录

EOF
    exit 0
}

# =========================
# 参数解析
# =========================
while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--duration)
            SPLIT_DURATION="$2"
            shift 2
            ;;
        -o|--output)
            BASE_DIR="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            log_error "未知参数: $1"
            usage
            ;;
    esac
done

# =========================
# 主流程
# =========================
main() {
    echo "======================================"
    echo "  KPI ROS2 Bag 录制脚本"
    echo "======================================"
    
    # 磁盘检查
    log_info "正在检查数据盘..."
    check_disk_exists || exit 1
    check_disk_capacity || exit 1
    check_free_space || exit 1
    
    # 创建目录
    mkdir -p "${BASE_DIR}"
    
    # 打印配置
    echo ""
    log_info "录制配置:"
    log_info "  日期        : ${DATE_STR}"
    log_info "  输出目录    : ${BASE_DIR}"
    log_info "  Bag 前缀    : ${BAG_PREFIX}"
    log_info "  分割时长    : ${SPLIT_DURATION} 秒"
    log_info "  压缩模式    : ${COMPRESSION_MODE}"
    echo ""
    
    log_info "开始录制... (按 Ctrl+C 停止)"
    echo ""
    
    # 启动录制 (后台运行，使用 process substitution 保持日志)
    ros2 bag record \
        --storage ${STORAGE} \
        --max-bag-duration ${SPLIT_DURATION} \
        --output "${BASE_DIR}/${BAG_PREFIX}" \
        --compression-mode ${COMPRESSION_MODE} \
        \
        /map/map_utm \
        /map/map \
        /planning/debug \
        /vehicle/imu_measure_report \
        /vehicle/chassis_domain_report \
        /vehicle/body_domain_report \
        /vehicle/gnss_measure_report \
        /prediction/prediction \
        /prediction/prediction_utm \
        /perception/fusion/obstacle_list_utm \
        /perception/fusion/obstacle_list \
        /perception/obstacle_list \
        /perception/traffic_light \
        /recorder/scene_filter/sdmap_response \
        /control/control \
        /control/debug \
        /localization/localization \
        /planning/trajectory \
        /map/routing_status \
        /function/body_domain_cmd \
        /function/function_manager \
        /function/tour_routing_request \
        /function/tour_routing_response_for_planning \
        /cloud/tour_routing_cmd \
        /cloud/tour_routing_response \
        /map/routing_response \
        /perception/lane_lines \
        > >(tee -a "${BASE_DIR}/record.log") 2>&1 &
    
    RECORD_PID=$!
    log_info "录制进程已启动 (PID: $RECORD_PID)"
    
    # 等待录制进程
    wait "$RECORD_PID"
    local record_exit_code=$?
    
    if [[ $record_exit_code -eq 0 ]]; then
        log_info "录制正常完成"
    else
        log_warn "录制进程退出 (exit_code=$record_exit_code)"
    fi
}

main "$@"
