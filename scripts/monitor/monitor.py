#!/usr/bin/env python3
"""
定位信息实时监控脚本

实时订阅 ROS2 定位和 GNSS topic，将数据写入 txt 文件

订阅 Topics:
    - /localization/localization (定位信息)
    - /vehicle/gnss_measure_report (GNSS 原始测量)

文件命名:
    - 文件名包含启动时间戳，每次重启自动生成新文件
    - 格式: {stem}_{YYYYMMDD_HHMMSS}.txt
    - 例如: loc_20251213_143052.txt

用法:
    # 使用启动脚本（推荐）
    ./scripts/monitor_loc.sh
    
    # 手动指定
    python scripts/monitor.py -o /path/to/loc.txt

输出格式 (CSV):
    timestamp,lat,lon,height,stddev_east,stddev_north,stddev_up,status,
    gnss_position_type,gnss_stddev_x,gnss_stddev_y,gnss_num_sat_used
"""
import argparse
import os
import sys
import time
import signal
from datetime import datetime
from threading import Lock
from collections import deque
from pathlib import Path

try:
    import rclpy
    from rclpy.node import Node
    from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy
except ImportError:
    print("错误: 需要 ROS2 环境，请先 source /opt/ros/humble/setup.bash")
    sys.exit(1)


class LocalizationMonitor(Node):
    """定位信息监控节点"""
    
    def __init__(self, output_path: str, 
                 target_hz: float = 10.0, 
                 buffer_size: int = 100,
                 split_mode: str = 'hour',
                 split_size_mb: int = 0):
        super().__init__('localization_monitor')
        
        self.output_base = output_path
        self.target_hz = target_hz
        self.min_interval = 1.0 / target_hz if target_hz > 0 else 0
        self.buffer_size = buffer_size
        self.split_mode = split_mode
        self.split_size_bytes = split_size_mb * 1024 * 1024 if split_size_mb > 0 else 0
        
        self.last_write_time = 0
        self.record_count = 0
        self.file_count = 0
        self.current_file_size = 0
        self.current_file_hour = None
        self.current_file_day = None
        self.current_3hour_block = None
        self.current_file_path = None
        self.current_file = None
        
        # 启动时间戳，用于文件命名
        self.start_time = datetime.now()
        self.start_timestamp = self.start_time.strftime('%Y%m%d_%H%M%S')
        
        self.buffer = deque(maxlen=buffer_size * 2)
        self.lock = Lock()
        self.running = True
        
        # GNSS 数据缓存（用于合并到定位数据）
        self.latest_gnss = {
            'position_type': 0,
            'stddev_x': 0.0,
            'stddev_y': 0.0,
            'num_sat_used': 0
        }
        self.gnss_lock = Lock()
        
        # 打开第一个文件
        self._open_new_file()
        
        # QoS 配置
        qos = QoSProfile(
            reliability=ReliabilityPolicy.BEST_EFFORT,
            history=HistoryPolicy.KEEP_LAST,
            depth=10
        )
        
        # 订阅定位 topic
        loc_msg_type = self._get_localization_msg_type()
        if loc_msg_type:
            self.loc_subscription = self.create_subscription(
                loc_msg_type,
                '/localization/localization',
                self.localization_callback,
                qos
            )
            self.get_logger().info('订阅 /localization/localization')
        
        # 订阅 GNSS measure topic
        gnss_msg_type = self._get_gnss_msg_type()
        if gnss_msg_type:
            self.gnss_subscription = self.create_subscription(
                gnss_msg_type,
                '/vehicle/gnss_measure_report',
                self.gnss_callback,
                qos
            )
            self.get_logger().info('订阅 /vehicle/gnss_measure_report')
        
        # 定时器
        self.flush_timer = self.create_timer(1.0, self.flush_buffer)
        self.split_timer = self.create_timer(60.0, self.check_file_split)
        
        self.get_logger().info(f'开始监控')
        self.get_logger().info(f'  目标频率: {target_hz} Hz')
        self.get_logger().info(f'  分文件模式: {split_mode}')
    
    def _get_localization_msg_type(self):
        """获取定位消息类型"""
        try:
            from hv_localization_msgs.msg import Localization
            return Localization
        except ImportError:
            self.get_logger().warn('未找到 hv_localization_msgs.msg.Localization')
            return None
    
    def _get_gnss_msg_type(self):
        """获取 GNSS 消息类型"""
        try:
            from hv_sensor_msgs.msg import GnssMeasurement
            return GnssMeasurement
        except ImportError:
            self.get_logger().warn('未找到 hv_sensor_msgs.msg.GnssMeasurement')
            return None
    
    def _get_file_path(self) -> str:
        """根据分割模式生成文件路径
        
        文件名格式: {stem}_{启动时间戳}[_{序号}].txt
        例如: loc_20251213_143052.txt, loc_20251213_143052_0001.txt
        """
        base = Path(self.output_base)
        stem = base.stem
        suffix = base.suffix or '.txt'
        parent = base.parent
        
        # 基础文件名：stem + 启动时间戳
        base_name = f"{stem}_{self.start_timestamp}"
        
        if self.split_mode == 'size':
            # 按大小分割时追加序号
            return str(parent / f"{base_name}_{self.file_count:04d}{suffix}")
        else:
            # 其他模式（none/hour/3hour/day）：不分割，单文件
            return str(parent / f"{base_name}{suffix}")
    
    def _open_new_file(self):
        """打开新文件"""
        if self.current_file:
            self.current_file.close()
        
        self.current_file_path = self._get_file_path()
        self.file_count += 1
        self.current_file_size = 0
        now = datetime.now()
        self.current_file_hour = now.hour
        self.current_file_day = now.day
        self.current_3hour_block = now.hour // 3  # 0, 1, 2, 3, 4, 5, 6, 7
        
        os.makedirs(os.path.dirname(os.path.abspath(self.current_file_path)), exist_ok=True)
        
        # 每次启动都是新文件（文件名包含启动时间戳）
        self.current_file = open(self.current_file_path, 'w', buffering=8192)
        
        # 写入头部
        self.current_file.write("# Localization & GNSS Monitor Log\n")
        self.current_file.write(f"# Start Time: {self.start_time.isoformat()}\n")
        self.current_file.write(f"# Target Rate: {self.target_hz} Hz\n")
        self.current_file.write("#\n")
        self.current_file.write("timestamp,lat,lon,height,"
                                "stddev_east,stddev_north,stddev_up,status,"
                                "gnss_position_type,gnss_stddev_x,gnss_stddev_y,gnss_num_sat_used\n")
        
        self.get_logger().info(f'新文件: {self.current_file_path}')
    
    def check_file_split(self):
        """检查是否需要分割文件（仅 size 模式）"""
        if self.split_mode != 'size' or self.split_size_bytes <= 0:
            return
        
        if self.current_file_size >= self.split_size_bytes:
            with self.lock:
                self._write_buffer()
                self._open_new_file()
    
    def gnss_callback(self, msg):
        """GNSS 消息回调 - 缓存最新数据"""
        if not self.running:
            return
        
        with self.gnss_lock:
            # position_type
            self.latest_gnss['position_type'] = getattr(msg, 'position_type', 0)
            
            # position_stddev.x, y
            if hasattr(msg, 'position_stddev'):
                self.latest_gnss['stddev_x'] = getattr(msg.position_stddev, 'x', 0.0)
                self.latest_gnss['stddev_y'] = getattr(msg.position_stddev, 'y', 0.0)
            
            # num_sat_used
            self.latest_gnss['num_sat_used'] = getattr(msg, 'num_sat_used', 0)
    
    def localization_callback(self, msg):
        """定位消息回调"""
        if not self.running:
            return
        
        current_time = time.time()
        
        # 降采样检查
        if self.min_interval > 0 and (current_time - self.last_write_time) < self.min_interval:
            return
        
        self.last_write_time = current_time
        
        try:
            record = self._extract_data(msg, current_time)
            if record:
                with self.lock:
                    self.buffer.append(record)
                    self.record_count += 1
                    
                    if len(self.buffer) >= self.buffer_size:
                        self._write_buffer()
        except Exception as e:
            self.get_logger().error(f'提取数据失败: {e}')
    
    def _extract_data(self, msg, timestamp: float) -> dict:
        """从消息中提取数据"""
        record = {'timestamp': timestamp}
        
        # 定位信息
        if hasattr(msg, 'global_localization'):
            gl = msg.global_localization
            if hasattr(gl, 'position'):
                record['lat'] = getattr(gl.position, 'latitude', 0)
                record['lon'] = getattr(gl.position, 'longitude', 0)
                record['height'] = getattr(gl.position, 'height', 0)
            if hasattr(gl, 'position_stddev'):
                record['stddev_east'] = getattr(gl.position_stddev, 'east', 0)
                record['stddev_north'] = getattr(gl.position_stddev, 'north', 0)
                record['stddev_up'] = getattr(gl.position_stddev, 'up', 0)
        
        # 定位状态
        if hasattr(msg, 'status'):
            record['status'] = getattr(msg.status, 'common', 0)
        
        # 合并 GNSS 数据
        with self.gnss_lock:
            record['gnss_position_type'] = self.latest_gnss['position_type']
            record['gnss_stddev_x'] = self.latest_gnss['stddev_x']
            record['gnss_stddev_y'] = self.latest_gnss['stddev_y']
            record['gnss_num_sat_used'] = self.latest_gnss['num_sat_used']
        
        # 默认值
        defaults = {
            'lat': 0, 'lon': 0, 'height': 0,
            'stddev_east': 0, 'stddev_north': 0, 'stddev_up': 0,
            'status': 0,
            'gnss_position_type': 0, 'gnss_stddev_x': 0, 'gnss_stddev_y': 0, 'gnss_num_sat_used': 0
        }
        for key, default in defaults.items():
            if key not in record:
                record[key] = default
        
        return record
    
    def _write_buffer(self):
        """写入缓冲区"""
        if not self.buffer or not self.current_file:
            return
        
        lines = []
        while self.buffer:
            r = self.buffer.popleft()
            line = (f"{r['timestamp']:.6f},"
                    f"{r['lat']:.9f},"
                    f"{r['lon']:.9f},"
                    f"{r['height']:.3f},"
                    f"{r['stddev_east']:.6f},"
                    f"{r['stddev_north']:.6f},"
                    f"{r['stddev_up']:.6f},"
                    f"{r['status']},"
                    f"{r['gnss_position_type']},"
                    f"{r['gnss_stddev_x']:.6f},"
                    f"{r['gnss_stddev_y']:.6f},"
                    f"{r['gnss_num_sat_used']}\n")
            lines.append(line)
        
        data = ''.join(lines)
        self.current_file.write(data)
        self.current_file_size += len(data.encode('utf-8'))
    
    def flush_buffer(self):
        """定时刷新"""
        with self.lock:
            self._write_buffer()
            if self.current_file:
                self.current_file.flush()
        
        if int(time.time()) % 10 == 0:
            size_kb = self.current_file_size / 1024
            with self.gnss_lock:
                gnss_type = self.latest_gnss['position_type']
                gnss_sat = self.latest_gnss['num_sat_used']
            self.get_logger().info(
                f'记录 {self.record_count} 条, 文件 {size_kb:.1f}KB, '
                f'GNSS: type={gnss_type}, sat={gnss_sat}'
            )
    
    def stop(self):
        """停止"""
        self.running = False
        with self.lock:
            self._write_buffer()
            if self.current_file:
                self.current_file.close()
        
        self.get_logger().info(f'监控结束, 总记录: {self.record_count}, 文件数: {self.file_count}')


def main():
    parser = argparse.ArgumentParser(
        description="实时监控定位和 GNSS 信息",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    python scripts/monitor.py -o ~/loc.txt
    python scripts/monitor.py -o ~/loc.txt --hz 5
    python scripts/monitor.py -o ~/loc.txt --split size --split-size 50

文件命名: {stem}_{启动时间戳}.txt (重启自动新文件)

Ctrl+C 停止
        """
    )
    
    parser.add_argument('-o', '--output', required=True, help='输出文件路径 (文件名会追加启动时间戳)')
    parser.add_argument('--hz', type=float, default=5.0, help='写入频率，默认 5Hz')
    parser.add_argument('--buffer', type=int, default=100, help='缓冲区大小')
    parser.add_argument('--split', choices=['size', 'none'], 
                        default='none', help='分文件模式: none=单文件, size=按大小分割')
    parser.add_argument('--split-size', type=int, default=100, help='按大小分文件 (MB)，需配合 --split size')
    
    args = parser.parse_args()
    
    rclpy.init()
    
    node = LocalizationMonitor(
        output_path=args.output,
        target_hz=args.hz,
        buffer_size=args.buffer,
        split_mode=args.split,
        split_size_mb=args.split_size if args.split == 'size' else 0
    )
    
    def signal_handler(sig, frame):
        print("\n停止中...")
        node.stop()
        rclpy.shutdown()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
