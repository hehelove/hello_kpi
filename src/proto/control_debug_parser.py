"""
控制调试消息解析器
用于解析/control/debug topic中reserved0字段的protobuf数据
"""
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import os
import sys

# 添加proto目录到路径
PROTO_DIR = os.path.dirname(os.path.abspath(__file__))
if PROTO_DIR not in sys.path:
    sys.path.insert(0, PROTO_DIR)

# 尝试导入proto模块
try:
    from . import mpc_trajectory_debug_pb2
    PROTO_AVAILABLE = True
except ImportError:
    try:
        import mpc_trajectory_debug_pb2
        PROTO_AVAILABLE = True
    except ImportError:
        PROTO_AVAILABLE = False
        mpc_trajectory_debug_pb2 = None


@dataclass
class LatDebugInfo:
    """横向控制调试信息"""
    nearest_lateral_error: float = 0.0
    nearest_heading_error: float = 0.0
    lateral_error: float = 0.0
    heading_error: float = 0.0
    ref_kappa: float = 0.0
    vehicle_speed: float = 0.0


@dataclass
class LonDebugInfo:
    """纵向控制调试信息"""
    current_speed: float = 0.0
    current_acceleration: float = 0.0
    acceleration_command: float = 0.0
    jerk_command: float = 0.0
    current_s_error: float = 0.0
    current_v_error: float = 0.0


@dataclass
class ControlDebugInfo:
    """控制调试信息"""
    timestamp: float
    lat_debug: LatDebugInfo
    lon_debug: LonDebugInfo


class ControlDebugParser:
    """
    控制调试消息解析器
    
    解析 /control/debug.reserved0 中的protobuf数据
    使用 mpc_trajectory_debug_pb2.py
    """
    
    def __init__(self):
        """初始化解析器"""
        self._proto_available = PROTO_AVAILABLE
        if self._proto_available:
            print("  Proto模块加载成功: mpc_trajectory_debug_pb2")
        else:
            print("  Warning: Proto模块未加载，控制调试数据解析将受限")
    
    def parse_control_debug(self, msg: Any) -> Optional[ControlDebugInfo]:
        """
        解析控制调试消息
        
        Args:
            msg: ROS消息对象 (/control/debug)
            
        Returns:
            解析后的调试信息
        """
        if not self._proto_available:
            return None
        
        try:
            # 获取reserved0字段 (protobuf二进制数据)
            reserved0 = getattr(msg, 'reserved0', None)
            
            if reserved0 is None or len(reserved0) == 0:
                return None
            
            # 解析protobuf - MPCTrajectoryDebug
            debug_msg = mpc_trajectory_debug_pb2.MPCTrajectoryDebug()
            debug_msg.ParseFromString(bytes(reserved0))
            
            # 提取横向调试数据
            lat_debug_pb = debug_msg.lat_debug
            lat_debug = LatDebugInfo(
                nearest_lateral_error=getattr(lat_debug_pb, 'nearest_lateral_error', 0.0),
                nearest_heading_error=getattr(lat_debug_pb, 'nearest_heading_error', 0.0),
                lateral_error=getattr(lat_debug_pb, 'lateral_error', 0.0),
                heading_error=getattr(lat_debug_pb, 'heading_error', 0.0),
                ref_kappa=getattr(lat_debug_pb, 'ref_kappa', 0.0),
                vehicle_speed=getattr(lat_debug_pb, 'vehicle_speed', 0.0)
            )
            
            # 提取纵向调试数据
            lon_debug_pb = debug_msg.long_debug
            lon_debug = LonDebugInfo(
                current_speed=getattr(lon_debug_pb, 'current_speed', 0.0),
                current_acceleration=getattr(lon_debug_pb, 'current_acceleration', 0.0),
                acceleration_command=getattr(lon_debug_pb, 'acceleration_command', 0.0),
                jerk_command=getattr(lon_debug_pb, 'jerk_command', 0.0),
                current_s_error=getattr(lon_debug_pb, 'current_s_error', 0.0),
                current_v_error=getattr(lon_debug_pb, 'current_v_error', 0.0)
            )
            
            # 获取时间戳
            timestamp = 0.0
            if hasattr(msg, 'header'):
                timestamp = getattr(msg.header, 'global_timestamp', 0.0)
            
            return ControlDebugInfo(
                timestamp=timestamp,
                lat_debug=lat_debug,
                lon_debug=lon_debug
            )
            
        except Exception as e:
            # 静默处理解析错误
            return None
    
    def batch_parse(self, messages: List[Any], 
                     timestamps: Optional[List[float]] = None) -> Dict[float, Dict]:
        """
        批量解析消息
        
        Args:
            messages: 消息列表
            timestamps: 对应的时间戳列表 (来自bag读取)，如果为None则使用消息内部时间戳
            
        Returns:
            {timestamp: {'lateral_error': value, ...}}
        """
        result = {}
        success_count = 0
        
        for i, msg in enumerate(messages):
            parsed = self.parse_control_debug(msg)
            if parsed:
                # 优先使用外部提供的时间戳
                if timestamps is not None and i < len(timestamps):
                    ts = timestamps[i]
                else:
                    ts = parsed.timestamp
                
                if ts > 0:
                    result[ts] = {
                        'lateral_error': parsed.lat_debug.nearest_lateral_error,
                        'heading_error': parsed.lat_debug.nearest_heading_error,
                        'lateral_error_raw': parsed.lat_debug.lateral_error,
                        'heading_error_raw': parsed.lat_debug.heading_error,
                        'ref_kappa': parsed.lat_debug.ref_kappa,
                        'vehicle_speed': parsed.lat_debug.vehicle_speed,
                        'current_speed': parsed.lon_debug.current_speed,
                        'current_acceleration': parsed.lon_debug.current_acceleration,
                        'acceleration_command': parsed.lon_debug.acceleration_command,
                        'jerk_command': parsed.lon_debug.jerk_command,
                    }
                    success_count += 1
        
        if success_count > 0:
            print(f"    成功解析 {success_count}/{len(messages)} 条控制调试数据")
        
        return result


# 创建解析器实例的工厂函数
def create_parser() -> ControlDebugParser:
    """创建解析器"""
    return ControlDebugParser()
