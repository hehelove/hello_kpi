#!/bin/bash
# 定位监控启动脚本
# 自动获取车辆信息，生成输出路径

# set -e
export AMENT_PREFIX_PATH="/opt/env/hv_interface:/opt/env/ros2"
export CMAKE_PREFIX_PATH="/opt/env/hv_interface:/opt/env/ros2"
export COLCON_PREFIX_PATH="/opt/env/hv_interface:/opt/env/ros2"
export PKG_CONFIG_PATH="/opt/env/ros2/lib/aarch64-linux-gnu/pkgconfig:/opt/env/ros2/lib/pkgconfig"
export PYTHONPATH="/opt/env/python/lib/python3.8/site-packages:/opt/env/hv_interface/lib/python3.8/site-packages:/opt/env/ros2/lib/python3.8/site-packages"
export LD_LIBRARY_PATH="/lib/aarch64-linux-gnu:/opt/env/python/lib:/opt/env/hv_interface/lib:/opt/env/ros2/opt/yaml_cpp_vendor/lib:/opt/env/ros2/lib:/app/lib"
export PATH="/opt/env/ros2/bin:$PATH"
export ROS_LOG_DIR=/tmp

# 配置
CONFIG_FILE="/opt/update/cal/static_configuration.yaml"
BASE_DIR="/data_collection_disk/bag"
MIN_DISK_SIZE_TB=1           # 最小磁盘容量 (TB)
WAIT_INTERVAL=10             # 等待间隔 (秒)
MAX_WAIT_TIME=600            # 最大等待时间 (秒，10分钟)

# 检查磁盘容量是否大于指定大小
# 参数: $1 = 路径, $2 = 最小容量(TB)
# 返回: 0=满足, 1=不满足
check_disk_size() {
    local path="$1"
    local min_tb="$2"
    local min_bytes=$((min_tb * 1024 * 1024 * 1024 * 1024))
    
    # 获取磁盘总容量 (bytes)
    local total_bytes=$(df -B1 "$path" 2>/dev/null | awk 'NR==2 {print $2}')
    
    if [ -z "$total_bytes" ]; then
        return 1
    fi
    
    if [ "$total_bytes" -ge "$min_bytes" ]; then
        return 0
    else
        return 1
    fi
}

# 等待目录可用且磁盘容量满足要求
wait_for_storage() {
    local base_dir="$1"
    local min_tb="$2"
    local wait_interval="$3"
    local max_wait="$4"
    local elapsed=0
    
    echo "等待存储就绪: $base_dir"
    echo "  要求: 磁盘容量 >= ${min_tb}TB"
    
    while [ $elapsed -lt $max_wait ]; do
        # 检查目录是否存在
        if [ -d "$base_dir" ]; then
            # 检查磁盘容量
            if check_disk_size "$base_dir" "$min_tb"; then
                local size_tb=$(df -B1 "$base_dir" | awk 'NR==2 {printf "%.2f", $2/1024/1024/1024/1024}')
                echo "  ✓ 存储就绪! 磁盘容量: ${size_tb}TB"
                return 0
            else
                local size_tb=$(df -B1 "$base_dir" 2>/dev/null | awk 'NR==2 {printf "%.2f", $2/1024/1024/1024/1024}')
                echo "  等待中... 目录存在但磁盘容量不足 (当前: ${size_tb}TB < ${min_tb}TB)"
            fi
        else
            echo "  等待中... 目录不存在: $base_dir"
        fi
        
        sleep "$wait_interval"
        elapsed=$((elapsed + wait_interval))
    done
    
    echo "  ✗ 超时! 等待 ${max_wait}s 后存储仍未就绪"
    return 1
}

# 获取车辆ID
get_vehicle_id() {
    local config_file="$1"
    if [ ! -f "$config_file" ]; then
        echo ""
        return 1
    fi
    
    local VEHICLE_ID=$(grep -A1 "VEHICLE_ID:" "$config_file" | grep -oE '[0-9]+' | head -1)
    local location=$(grep -A1 "location:" "$config_file" | grep -oE '[0-9]+' | head -1)
    
    if [ -z "$VEHICLE_ID" ] || [ -z "$location" ]; then
        echo ""
        return 1
    fi
    
    echo "${location}_${VEHICLE_ID}"
}

# 获取当前日期
get_date() {
    date +%Y%m%d
}

# 主逻辑
main() {
    echo "=========================================="
    echo "  定位监控启动"
    echo "=========================================="
    
    # 1. 等待存储就绪
    if ! wait_for_storage "$BASE_DIR" "$MIN_DISK_SIZE_TB" "$WAIT_INTERVAL" "$MAX_WAIT_TIME"; then
        echo "错误: 存储未就绪，退出"
        exit 1
    fi
    
    # 2. 获取车辆ID
    VEHICLE_ID=$(get_vehicle_id "$CONFIG_FILE")
    if [ -z "$VEHICLE_ID" ]; then
        echo "警告: 无法从 $CONFIG_FILE 获取车辆ID，使用默认值 'unknown'"
        VEHICLE_ID="unknown"
    fi
    echo "车辆ID: $VEHICLE_ID"
    
    # 3. 获取日期
    DATE_STR=$(get_date)
    echo "日期: $DATE_STR"
    
    # 4. 构建输出目录
    OUTPUT_DIR="${BASE_DIR}/${DATE_STR}/${VEHICLE_ID}/location"
    OUTPUT_FILE="${OUTPUT_DIR}/loc.txt"
    
    echo "输出目录: $OUTPUT_DIR"
    echo "输出文件: $OUTPUT_FILE"
    
    # 5. 创建目录
    mkdir -p "$OUTPUT_DIR"
    
    # 获取脚本目录
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    PYTHON_SCRIPT="${SCRIPT_DIR}/monitor.py"
    
    if [ ! -f "$PYTHON_SCRIPT" ]; then
        echo "错误: 找不到 $PYTHON_SCRIPT"
        exit 1
    fi
    
    # 可选参数
    HZ=${1:-5}              # 默认 5Hz
    
    echo ""
    echo "启动定位监控..."
    echo "  采样频率: ${HZ} Hz"
    echo "  文件命名: loc_{启动时间戳}.txt (重启自动新文件)"
    echo ""
    
    # 启动 Python 脚本
    python3 "$PYTHON_SCRIPT" \
        -o "$OUTPUT_FILE" \
        --hz "$HZ"
}

# 处理信号
trap 'echo "收到停止信号"; exit 0' SIGINT SIGTERM

# 运行
main "$@"

