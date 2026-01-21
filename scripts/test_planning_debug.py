#!/usr/bin/env python3
"""
测试脚本：读取 /planning/debug topic 并解析 ADCTrajectory proto

使用 src/proto/planning_debug_parser.py
"""
import sys
import os

# 添加项目路径
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from rosbags.rosbag2 import Reader

# 尝试导入 ROS2 序列化和自定义消息类型
RCLPY_AVAILABLE = False
HV_MSG_CLASSES = {}

try:
    from rclpy.serialization import deserialize_message
    RCLPY_AVAILABLE = True
except ImportError:
    deserialize_message = None

# 尝试导入 hv_planning_msgs/msg/PlanningDebug
try:
    from hv_planning_msgs.msg import PlanningDebug
    HV_MSG_CLASSES['hv_planning_msgs/msg/PlanningDebug'] = PlanningDebug
except ImportError:
    pass

if HV_MSG_CLASSES:
    print(f"[INFO] 已加载 {len(HV_MSG_CLASSES)} 个自定义HV消息类型")
else:
    print("[WARN] 未能加载 hv_planning_msgs，将使用 rosbags 解析")


def parse_planning_debug(bag_path: str, max_messages: int = 10):
    """
    解析 /planning/debug topic
    
    Args:
        bag_path: ROS2 bag 路径
        max_messages: 最多解析的消息数
    """
    # 导入 planning debug parser
    try:
        from src.proto.planning_debug_parser import PlanningDebugParser
        parser = PlanningDebugParser()
        if not parser.is_available:
            print("[ERROR] Proto 模块不可用")
            print("[HINT] 请先运行: python compile_proto.py src/proto/modules")
            return
    except ImportError as e:
        print(f"[ERROR] 无法导入 PlanningDebugParser: {e}")
        return
    
    topic_name = "/planning/debug"
    msg_type = "hv_planning_msgs/msg/PlanningDebug"
    msg_class = HV_MSG_CLASSES.get(msg_type)
    
    # 检查解析方式
    if RCLPY_AVAILABLE and msg_class:
        print(f"[INFO] 使用 rclpy 反序列化 {msg_type}")
    else:
        print(f"[WARN] rclpy 或消息类型不可用，尝试使用 rosbags")
    
    count = 0
    
    with Reader(bag_path) as reader:
        # 检查 topic 是否存在
        topics = {conn.topic for conn in reader.connections}
        if topic_name not in topics:
            print(f"[ERROR] Topic {topic_name} 不存在")
            print(f"[INFO] 可用的 topics: {sorted(topics)}")
            return
        
        # 获取 topic 连接
        connections = [c for c in reader.connections if c.topic == topic_name]
        if not connections:
            print(f"[ERROR] 无法获取 {topic_name} 的连接信息")
            return
        
        print(f"[INFO] 开始解析 {topic_name}...")
        print("-" * 60)
        
        for connection, timestamp, rawdata in reader.messages(connections):
            if count >= max_messages:
                break
            
            try:
                # 解析 ROS2 消息
                if RCLPY_AVAILABLE and msg_class:
                    msg = deserialize_message(rawdata, msg_class)
                else:
                    from rosbags.typesys import Stores, get_typestore
                    typestore = get_typestore(Stores.ROS2_HUMBLE)
                    msg = typestore.deserialize_cdr(rawdata, connection.msgtype)
                
                # 使用 parser 解析
                ts_sec = timestamp / 1e9
                parsed = parser.parse_planning_debug(msg, external_timestamp=ts_sec)
                
                if parsed:
                    print(f"[{count+1}] timestamp={ts_sec:.3f}s")
                    print(f"    trajectory_type: {parsed.trajectory_type_name}")
                    print(f"    is_replan: {parsed.is_replan}")
                    if parsed.replan_reason:
                        print(f"    replan_reason: '{parsed.replan_reason}'")
                    if parsed.central_decider_debug:
                        cdd = parsed.central_decider_debug
                        print(f"    present_status: '{cdd.present_status}'")
                        print(f"    prev_status: '{cdd.prev_status}'")
                        if cdd.global_lane_change_direction:
                            print(f"    lane_change_dir: '{cdd.global_lane_change_direction}'")
                    print()
                else:
                    print(f"[{count+1}] timestamp={ts_sec:.3f}s, 解析失败或数据为空")
                
            except Exception as e:
                print(f"[{count+1}] 解析失败: {e}")
                import traceback
                traceback.print_exc()
            
            count += 1
    
    print("-" * 60)
    print(f"[INFO] 共解析 {count} 条消息")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="解析 /planning/debug proto 数据")
    parser.add_argument("bag_path", help="ROS2 bag 路径")
    parser.add_argument("-n", "--num", type=int, default=10, help="最多解析消息数 (默认: 10)")
    args = parser.parse_args()
    
    if not os.path.exists(args.bag_path):
        print(f"[ERROR] Bag 路径不存在: {args.bag_path}")
        sys.exit(1)
    
    parse_planning_debug(args.bag_path, args.num)


if __name__ == "__main__":
    main()
