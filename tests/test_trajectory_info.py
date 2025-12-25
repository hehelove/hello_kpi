#!/usr/bin/env python3
"""
测试脚本：读取 /planning/trajectory topic 中的限速和红绿灯信息

Usage:
    source /opt/ros/humble/setup.bash
    source ~/your_ws/install/setup.bash  # 包含 hv_planning_msgs
    python scripts/test_trajectory_info.py /path/to/bag
"""
import sys
import os
from pathlib import Path
from typing import Any, Optional
from dataclasses import dataclass
from collections import defaultdict

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rosbags.rosbag2 import Reader
from rosbags.typesys import Stores, get_typestore

# 尝试导入 ROS2 消息
RCLPY_AVAILABLE = False
HV_MSG_CLASSES = {}

try:
    from rclpy.serialization import deserialize_message
    RCLPY_AVAILABLE = True
except ImportError as e:
    print(f"✗ rclpy 加载失败: {e}")
    deserialize_message = None

# 导入所有 hv_planning_msgs 相关消息类型
try:
    from hv_planning_msgs.msg import (
        Trajectory,
        TrajectoryPoint,
        PathPoint,
        PlanningHeader,
        Estop,
        DecisionResult,
        MainDecision,
        TargetLane,
        ObjectDecisions,
        ObjectDecision,
        ObjectDecisionType,
        VehicleSignal,
        LatencyStats,
        TaskStats,
        EngageAdvice,
        CriticalRegion,
        EgoIntent,
        PlanningSpeedLimit,
        PlanningTrafficLight,
        PlanningTrafficLightDecision,
        PlanningTrafficLightInfo,
        PlanningSubLight,
        PlanningStopLineInfo,
    )
    HV_MSG_CLASSES['hv_planning_msgs/msg/Trajectory'] = Trajectory
    print(f"✓ hv_planning_msgs 加载成功")
except ImportError as e:
    print(f"✗ hv_planning_msgs 加载失败: {e}")
    Trajectory = None

# 导入 hv_common_msgs
try:
    from hv_common_msgs.msg import Header, Polygon, Point3d
    HV_MSG_CLASSES['hv_common_msgs/msg/Header'] = Header
    print(f"✓ hv_common_msgs 加载成功")
except ImportError as e:
    print(f"  hv_common_msgs 加载失败: {e}")

if RCLPY_AVAILABLE and Trajectory is not None:
    print(f"✓ ROS2 消息类型全部加载成功 ({len(HV_MSG_CLASSES)} 个)")
else:
    print("✗ 请先 source ROS2 环境和 hv_planning_msgs 包")


# ========== 颜色定义 ==========
TRAFFIC_LIGHT_COLORS = {
    0: "UNKNOWN",
    1: "RED",
    2: "YELLOW", 
    3: "GREEN",
    4: "BLACK",
}

TRAFFIC_LIGHT_SHAPES = {
    0: "UNKNOWN",
    1: "CIRCLE",
    2: "ARROW_LEFT",
    3: "ARROW_RIGHT",
    4: "ARROW_FORWARD",
    5: "ARROW_LEFT_FORWARD",
    6: "ARROW_RIGHT_FORWARD",
    7: "ARROW_UTURN",
}


def get_color_name(color_code: int) -> str:
    return TRAFFIC_LIGHT_COLORS.get(color_code, f"UNKNOWN({color_code})")


def get_shape_name(shape_code: int) -> str:
    return TRAFFIC_LIGHT_SHAPES.get(shape_code, f"UNKNOWN({shape_code})")


def print_speed_limit_info(msg: Any, timestamp: float):
    """打印限速信息"""
    speed_limit = getattr(msg, 'speed_limit', None)
    if speed_limit is None:
        return
    
    # speed_limit_parsa - 字符串数组
    parsa = getattr(speed_limit, 'speed_limit_parsa', [])
    # speed_limit_graph - PlanningTrafficLight 数组
    graph = getattr(speed_limit, 'speed_limit_graph', [])
    
    if not parsa and not graph:
        return
    
    print(f"\n{'='*60}")
    print(f"[限速信息] timestamp: {timestamp:.3f}")
    print(f"{'='*60}")
    
    if parsa:
        print(f"\n  speed_limit_parsa ({len(parsa)} 项):")
        for i, p in enumerate(parsa[:10]):  # 最多显示10个
            print(f"    [{i}] {p}")
        if len(parsa) > 10:
            print(f"    ... 还有 {len(parsa) - 10} 项")
    
    if graph:
        print(f"\n  speed_limit_graph ({len(graph)} 项):")
        for i, tl in enumerate(graph[:10]):
            tl_id = getattr(tl, 'id', '')
            color = get_color_name(getattr(tl, 'color', 0))
            shape = get_shape_name(getattr(tl, 'shape', 0))
            time_val = getattr(tl, 'time', 0)
            print(f"    [{i}] id={tl_id}, color={color}, shape={shape}, time={time_val}")
        if len(graph) > 10:
            print(f"    ... 还有 {len(graph) - 10} 项")


def print_traffic_light_info(msg: Any, timestamp: float):
    """打印红绿灯决策信息"""
    tl_decision = getattr(msg, 'traffic_light_decision', None)
    if tl_decision is None:
        return
    
    tl_seq = getattr(tl_decision, 'tl_seq_num', 0)
    obs_seq = getattr(tl_decision, 'observe_seq_num', 0)
    traffic_lights = getattr(tl_decision, 'traffic_lights', [])
    stop_lines = getattr(tl_decision, 'stop_lines', [])
    
    if not traffic_lights and not stop_lines:
        return
    
    print(f"\n{'='*60}")
    print(f"[红绿灯决策] timestamp: {timestamp:.3f}")
    print(f"{'='*60}")
    print(f"  tl_seq_num: {tl_seq}, observe_seq_num: {obs_seq}")
    
    if traffic_lights:
        print(f"\n  traffic_lights ({len(traffic_lights)} 个):")
        for i, tl in enumerate(traffic_lights[:10]):
            tl_id = getattr(tl, 'id', '')
            sub_lights = getattr(tl, 'sub_lights', [])
            print(f"    [{i}] id={tl_id}")
            for j, sub in enumerate(sub_lights[:5]):
                sub_type = getattr(sub, 'type', 0)
                sub_color = get_color_name(getattr(sub, 'color', 0))
                sub_blink = getattr(sub, 'blink', 0)
                sub_counter = getattr(sub, 'counter', 0)
                print(f"        sub[{j}]: type={sub_type}, color={sub_color}, blink={sub_blink}, counter={sub_counter}")
        if len(traffic_lights) > 10:
            print(f"    ... 还有 {len(traffic_lights) - 10} 个")
    
    if stop_lines:
        print(f"\n  stop_lines ({len(stop_lines)} 条):")
        for i, sl in enumerate(stop_lines[:10]):
            sl_id = getattr(sl, 'id', '')
            start_s = getattr(sl, 'start_s', 0)
            end_s = getattr(sl, 'end_s', 0)
            is_stop = getattr(sl, 'is_stop', False)
            related_tl = getattr(sl, 'related_tl_id', [])
            print(f"    [{i}] id={sl_id}, s=[{start_s:.2f}, {end_s:.2f}], is_stop={is_stop}, related_tl={related_tl}")
        if len(stop_lines) > 10:
            print(f"    ... 还有 {len(stop_lines) - 10} 条")


def print_target_lane_speed_limit(msg: Any, timestamp: float):
    """打印目标车道限速"""
    decision = getattr(msg, 'decision', None)
    if decision is None:
        return
    
    main_decision = getattr(decision, 'main_decision', None)
    if main_decision is None:
        return
    
    target_lanes = getattr(main_decision, 'target_lane', [])
    if not target_lanes:
        return
    
    print(f"\n{'='*60}")
    print(f"[目标车道限速] timestamp: {timestamp:.3f}")
    print(f"{'='*60}")
    
    for i, lane in enumerate(target_lanes[:10]):
        lane_id = getattr(lane, 'id', '')
        start_s = getattr(lane, 'start_s', 0)
        end_s = getattr(lane, 'end_s', 0)
        speed_limit = getattr(lane, 'speed_limit', 0)
        print(f"  [{i}] lane_id={lane_id}, s=[{start_s:.2f}, {end_s:.2f}], speed_limit={speed_limit:.1f} m/s ({speed_limit*3.6:.1f} km/h)")


def read_trajectory_topic(bag_path: str, max_messages: int = 100, sample_interval: int = 10):
    """
    读取 /planning/trajectory topic
    
    Args:
        bag_path: bag 文件路径
        max_messages: 最大读取消息数
        sample_interval: 采样间隔（每隔多少条消息打印一次）
    """
    if not RCLPY_AVAILABLE:
        print("错误: rclpy 未加载，请 source ROS2 环境")
        return
    
    if Trajectory is None:
        print("错误: hv_planning_msgs 未加载，请 source 包含该包的工作空间")
        return
    
    bag_path = Path(bag_path)
    if not bag_path.exists():
        print(f"错误: 路径不存在 {bag_path}")
        return
    
    # 如果是目录，查找 metadata.yaml
    if bag_path.is_dir():
        metadata_file = bag_path / "metadata.yaml"
        if not metadata_file.exists():
            print(f"错误: 未找到 metadata.yaml in {bag_path}")
            return
    
    topic_name = "/planning/trajectory"
    typestore = get_typestore(Stores.ROS2_HUMBLE)
    
    print(f"\n读取: {bag_path}")
    print(f"Topic: {topic_name}")
    print(f"采样间隔: 每 {sample_interval} 条消息打印一次")
    print("-" * 60)
    
    # 统计信息
    stats = {
        'total_messages': 0,
        'has_speed_limit': 0,
        'has_traffic_light': 0,
        'has_target_lane': 0,
        'speed_limit_parsa_values': set(),
        'traffic_light_ids': set(),
        'stop_line_ids': set(),
    }
    
    with Reader(bag_path) as reader:
        # 获取 topic 类型
        topic_type = None
        for conn in reader.connections:
            if conn.topic == topic_name:
                topic_type = conn.msgtype
                break
        
        if topic_type is None:
            print(f"错误: 未找到 topic {topic_name}")
            return
        
        print(f"消息类型: {topic_type}")
        
        msg_count = 0
        for conn, timestamp, rawdata in reader.messages():
            if conn.topic != topic_name:
                continue
            
            # 反序列化
            try:
                msg = deserialize_message(rawdata, Trajectory)
            except Exception as e:
                print(f"反序列化失败: {e}")
                continue
            
            ts = timestamp / 1e9  # 纳秒转秒
            stats['total_messages'] += 1
            
            # 检查是否有数据
            speed_limit = getattr(msg, 'speed_limit', None)
            if speed_limit:
                parsa = getattr(speed_limit, 'speed_limit_parsa', [])
                graph = getattr(speed_limit, 'speed_limit_graph', [])
                if parsa or graph:
                    stats['has_speed_limit'] += 1
                    for p in parsa:
                        stats['speed_limit_parsa_values'].add(p)
            
            tl_decision = getattr(msg, 'traffic_light_decision', None)
            if tl_decision:
                traffic_lights = getattr(tl_decision, 'traffic_lights', [])
                stop_lines = getattr(tl_decision, 'stop_lines', [])
                if traffic_lights or stop_lines:
                    stats['has_traffic_light'] += 1
                    for tl in traffic_lights:
                        stats['traffic_light_ids'].add(getattr(tl, 'id', ''))
                    for sl in stop_lines:
                        stats['stop_line_ids'].add(getattr(sl, 'id', ''))
            
            decision = getattr(msg, 'decision', None)
            if decision:
                main_decision = getattr(decision, 'main_decision', None)
                if main_decision:
                    target_lanes = getattr(main_decision, 'target_lane', [])
                    if target_lanes:
                        stats['has_target_lane'] += 1
            
            # 采样打印
            if msg_count % sample_interval == 0:
                print_speed_limit_info(msg, ts)
                print_traffic_light_info(msg, ts)
                print_target_lane_speed_limit(msg, ts)
            
            msg_count += 1
            if msg_count >= max_messages:
                print(f"\n已达到最大消息数 {max_messages}，停止读取")
                break
    
    # 打印统计
    print(f"\n{'='*60}")
    print("统计信息")
    print(f"{'='*60}")
    print(f"  总消息数: {stats['total_messages']}")
    print(f"  包含限速信息: {stats['has_speed_limit']} ({stats['has_speed_limit']/max(1,stats['total_messages'])*100:.1f}%)")
    print(f"  包含红绿灯决策: {stats['has_traffic_light']} ({stats['has_traffic_light']/max(1,stats['total_messages'])*100:.1f}%)")
    print(f"  包含目标车道: {stats['has_target_lane']} ({stats['has_target_lane']/max(1,stats['total_messages'])*100:.1f}%)")
    
    if stats['speed_limit_parsa_values']:
        print(f"\n  限速 parsa 值 ({len(stats['speed_limit_parsa_values'])} 种):")
        for v in sorted(stats['speed_limit_parsa_values'])[:20]:
            print(f"    - {v}")
    
    if stats['traffic_light_ids']:
        print(f"\n  红绿灯 ID ({len(stats['traffic_light_ids'])} 个):")
        for v in sorted(stats['traffic_light_ids'])[:20]:
            print(f"    - {v}")
    
    if stats['stop_line_ids']:
        print(f"\n  停止线 ID ({len(stats['stop_line_ids'])} 个):")
        for v in sorted(stats['stop_line_ids'])[:20]:
            print(f"    - {v}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python test_trajectory_info.py <bag_path> [max_messages] [sample_interval]")
        print("\n示例:")
        print("  python test_trajectory_info.py /data/rosbag/test")
        print("  python test_trajectory_info.py /data/rosbag/test 500 20")
        sys.exit(1)
    
    bag_path = sys.argv[1]
    max_messages = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    sample_interval = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    
    read_trajectory_topic(bag_path, max_messages, sample_interval)


if __name__ == "__main__":
    main()
