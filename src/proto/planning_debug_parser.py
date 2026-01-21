"""
Planning调试消息解析器
用于解析 /planning/debug topic 中 data 字段的 protobuf 数据 (ADCTrajectory)
"""
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import os
import sys

# 添加proto目录到路径（用于加载 modules.xxx.proto 依赖）
PROTO_DIR = os.path.dirname(os.path.abspath(__file__))
if PROTO_DIR not in sys.path:
    sys.path.insert(0, PROTO_DIR)
# 确保 modules 目录的父目录在路径中（planning_internal_pb2 使用 from modules.xxx 导入）
MODULES_PARENT = PROTO_DIR  # src/proto 目录
if MODULES_PARENT not in sys.path:
    sys.path.insert(0, MODULES_PARENT)

# 尝试导入proto模块
import os
import sys
PROTO_AVAILABLE = False
planning_pb2 = None
planning_internal_pb2 = None

def _find_loaded_proto_module(suffix: str):
    """从 sys.modules 中查找已加载的 proto 模块"""
    for mod_name, mod in list(sys.modules.items()):
        if mod_name.endswith(suffix) and mod is not None:
            return mod
    return None

# 方法1: 先检查是否已经加载过（避免重复注册）
planning_pb2 = _find_loaded_proto_module('.planning_pb2')
planning_internal_pb2 = _find_loaded_proto_module('.planning_internal_pb2')

if planning_pb2 is not None and planning_internal_pb2 is not None:
    PROTO_AVAILABLE = True
else:
    # 方法2: 尝试绝对路径导入（优先，因为 sys.path 已设置）
    try:
        from modules.planning.proto import planning_pb2 as _planning_pb2
        from modules.planning.proto import planning_internal_pb2 as _planning_internal_pb2
        planning_pb2 = _planning_pb2
        planning_internal_pb2 = _planning_internal_pb2
        PROTO_AVAILABLE = True
    except ImportError:
        # 方法3: 尝试相对路径导入
        try:
            from .modules.planning.proto import planning_pb2 as _planning_pb2
            from .modules.planning.proto import planning_internal_pb2 as _planning_internal_pb2
            planning_pb2 = _planning_pb2
            planning_internal_pb2 = _planning_internal_pb2
            PROTO_AVAILABLE = True
        except ImportError:
            pass
    except Exception as e:
        # 捕获 protobuf descriptor pool 冲突
        error_msg = str(e)
        if 'duplicate file name' in error_msg:
            # 再次尝试从已加载模块中查找
            planning_pb2 = _find_loaded_proto_module('.planning_pb2')
            planning_internal_pb2 = _find_loaded_proto_module('.planning_internal_pb2')
            if planning_pb2 is not None and planning_internal_pb2 is not None:
                PROTO_AVAILABLE = True
            else:
                print(f"  [WARN] Planning proto 冲突且无法找到已加载模块: {error_msg[:80]}...")
        else:
            print(f"  [WARN] Planning proto 加载失败: {error_msg[:80]}...")


@dataclass
class CentralDeciderDebugInfo:
    """CentralDecider 调试信息"""
    present_status: str = ""
    prev_status: str = ""
    has_reset: bool = False
    current_target_line_id: str = ""
    current_back_line_id: str = ""
    global_lane_change_direction: str = ""
    global_is_safe_to_change: bool = False


@dataclass
class PlanningDebugInfo:
    """Planning 调试信息"""
    timestamp: float
    trajectory_type: int = 0
    trajectory_type_name: str = ""
    is_replan: bool = False
    replan_reason: str = ""
    central_decider_debug: Optional[CentralDeciderDebugInfo] = None


class PlanningDebugParser:
    """
    Planning 调试消息解析器
    
    解析 /planning/debug.data 中的 protobuf 数据 (ADCTrajectory)
    """
    
    # TrajectoryType 枚举映射
    TRAJECTORY_TYPE_NAMES = {
        0: "UNKNOWN",
        1: "NORMAL",
        2: "PATH_FALLBACK",
        3: "SPEED_FALLBACK",
        4: "PATH_REUSED",
        5: "ST_BOUNDS_DECISION_FALLBACK",
        6: "ST_BOUNDS_BOUNDARY_FALLBACK",
        7: "DYNAMIC_PROGRAM_FALLBACK",
        8: "PATH_OPTIMIZER_FALLBACK",
        9: "SPEED_OPTIMIZER_FALLBACK",
        10: "SPEED_OPTIMIZER_BOUNDARY_FALLBACK",
        11: "UNKNOWN_FALLBACK",
    }
    
    def __init__(self):
        """初始化解析器"""
        self._proto_available = PROTO_AVAILABLE
        if self._proto_available:
            print("  Proto模块加载成功: planning_pb2, planning_internal_pb2")
        else:
            print("  Warning: Proto模块未加载，Planning调试数据解析将受限")
    
    @property
    def is_available(self) -> bool:
        """检查 proto 是否可用"""
        return self._proto_available
    
    def parse_planning_debug(self, msg: Any, external_timestamp: float = 0.0) -> Optional[PlanningDebugInfo]:
        """
        解析 Planning 调试消息
        
        Args:
            msg: ROS消息对象 (/planning/debug)，需要有 data 字段
            external_timestamp: 外部时间戳（来自 bag 读取）
            
        Returns:
            解析后的调试信息
        """
        if not self._proto_available:
            return None
        
        try:
            # 获取 data 字段 (protobuf 二进制数据)
            data = getattr(msg, 'data', None)
            
            if data is None or len(data) == 0:
                return None
            
            # 解析 protobuf - ADCTrajectory
            trajectory = planning_pb2.ADCTrajectory()
            trajectory.ParseFromString(bytes(data))
            
            # 提取基本信息
            trajectory_type = trajectory.trajectory_type
            trajectory_type_name = self.TRAJECTORY_TYPE_NAMES.get(trajectory_type, f"UNKNOWN({trajectory_type})")
            is_replan = trajectory.is_replan
            replan_reason = trajectory.replan_reason if trajectory.HasField('replan_reason') else ""
            
            # 提取 CentralDeciderDebug
            central_decider_debug = None
            if trajectory.HasField('debug'):
                debug = trajectory.debug
                if debug.HasField('planning_data'):
                    planning_data = debug.planning_data
                    if planning_data.HasField('central_decider_debug'):
                        cdd = planning_data.central_decider_debug
                        central_decider_debug = CentralDeciderDebugInfo(
                            present_status=cdd.present_status,
                            prev_status=cdd.prev_status,
                            has_reset=cdd.has_reset,
                            current_target_line_id=cdd.current_target_line_id,
                            current_back_line_id=cdd.current_back_line_id,
                            global_lane_change_direction=cdd.global_lane_change_direction,
                            global_is_safe_to_change=cdd.global_is_safe_to_change,
                        )
            
            # 获取时间戳
            timestamp = external_timestamp
            if timestamp <= 0 and hasattr(msg, 'header'):
                timestamp = getattr(msg.header, 'global_timestamp', 0.0)
            
            return PlanningDebugInfo(
                timestamp=timestamp,
                trajectory_type=trajectory_type,
                trajectory_type_name=trajectory_type_name,
                is_replan=is_replan,
                replan_reason=replan_reason,
                central_decider_debug=central_decider_debug,
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
            {timestamp: {'present_status': value, ...}}
        """
        result = {}
        success_count = 0
        
        for i, msg in enumerate(messages):
            ts = timestamps[i] if timestamps is not None and i < len(timestamps) else 0.0
            parsed = self.parse_planning_debug(msg, external_timestamp=ts)
            
            if parsed:
                if parsed.timestamp > 0:
                    result[parsed.timestamp] = {
                        'trajectory_type': parsed.trajectory_type,
                        'trajectory_type_name': parsed.trajectory_type_name,
                        'is_replan': parsed.is_replan,
                        'replan_reason': parsed.replan_reason,
                        'present_status': parsed.central_decider_debug.present_status if parsed.central_decider_debug else "",
                        'prev_status': parsed.central_decider_debug.prev_status if parsed.central_decider_debug else "",
                        'has_reset': parsed.central_decider_debug.has_reset if parsed.central_decider_debug else False,
                        'current_target_line_id': parsed.central_decider_debug.current_target_line_id if parsed.central_decider_debug else "",
                        'global_lane_change_direction': parsed.central_decider_debug.global_lane_change_direction if parsed.central_decider_debug else "",
                    }
                    success_count += 1
        
        if success_count > 0:
            print(f"    成功解析 {success_count}/{len(messages)} 条 Planning 调试数据")
        
        return result


# 创建解析器实例的工厂函数
def create_parser() -> PlanningDebugParser:
    """创建解析器"""
    return PlanningDebugParser()
