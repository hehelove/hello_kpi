    from hv_vehicle_io_msgs.msg import ChassisDomain
    from hv_localization_msgs.msg import Localization
    from hv_perception_msgs.msg import ObstacleList
    from hv_function_manager_msgs.msg import FunctionManager
    from hv_control_msgs.msg import ControlDebug
( ros2 interface show hv_function_manager_msgs/msg/FunctionManager 
# Generated from /home/jenkins/agent/workspace/CPP-Build/cpp-hv_interface-oss-x86/hv_interface/interface/function_manager/function_manager.h
# Original struct: FunctionManager

hv_common_msgs/Header header
        float64 global_timestamp  #
        float64 local_timestamp  #
        uint32 frame_sequence  #
        string frame_id
int8 function_status  # default: FunctionStatus::INIT
int8 tour_status  # default: TourStatus::TOUR_INIT
int8 take_over_request  # default: TakeOverRequest::NO_REQUEST
int8 operator_type  # default: OperatorType::UNKNOWN
int8 mrm_status  # default: MrmStatus::NO_MRM
float32 estimated_distance  # default: 0.0
float32 estimated_time  # default: 0.0
uint32 init_bitmask  # default: 0
uint32 inhibit_bitmask1  # default: 0
uint32 inhibit_bitmask2  # default: 0
uint32 inhibit_bitmask3  # default: 0
uint32 inhibit_bitmask4  # default: 0
uint32 fault_bitmask1  # default: 0
uint32 fault_bitmask2  # default: 0
uint32 fault_bitmask3  # default: 0
uint32 fault_bitmask4  # default: 0
uint32 driver_intervention_bitmask  # default: 0)
所有 topic 的时间戳可以以.header.global_timestamp,以 10hz 为基准计算 kpi，如果300ms 都找不到则丢掉，
可以以/function/function_manager.header.global_timestamp为基准，你也可以选择更好的
/function/function_manager.operator_type 值 是2为自动驾驶，非 2 为非自动驾驶；

1、里程统计  ---  自动驾驶里程和非自动驾驶里程，根据定位信息/localization/localization.global_localization.position.latitude   /localization/localization.global_localization.position.longitude 计算总里程和时间，剔除异常数据（为 0 的数据），区分自动驾驶、非自动驾驶的里程和时间
（车道保持、弯道保持、转向平滑、加速、减速相关指标均是自动驾驶状态下统计）
2、人工接管次数 --- 横纵向退出   统计接管次数（按照第一次进入自动后，开始计算）
3、关键点如何区分直道-弯道
3.1、直道保持精度 --- 使用 control 的 lat_error   /control/parsed_debug.latDebug.nearestLateralError(需要单独 proto 解析topic /control/debug.reserved0,已编译文件./proto/mpc_trajectory_debug_pb2.py)
3.2、弯道保持精度  --- 使用 control 的 lat_error   /control/parsed_debug.latDebug.nearestLateralError(需要单独 proto 解析/control/debug，已编译文件./proto/mpc_trajectory_debug_pb2.py) 
4、转向平滑度  --- 全路段|转角加速度|平均值   /vehicle/chassis_domain_report.eps_system.actual_steering_angle 方向盘转角，计算一阶导平均值,直接从/vehicle/chassis_domain_report.eps_system.actual_steering_angle_velocity 获取
5、加速 / 制动舒适性  --- 全路段|jerk|平均值 /vehicle/chassis_domain_report.motion_system.vehicle_longitudinal_acceleration，/vehicle/chassis_domain_report.motion_system.vehicle_lateral_acceleration 计算 jerk
6、急减速次数  --- 减速度<-3，且持续100ms以上的次数   /vehicle/chassis_domain_report.motion_system.vehicle_longitudinal_acceleration  加速度值连续 200ms记录一次，小于后重置并重新判断
7、横向猛打次数 --- 根据自车速度变化调整阈值（转角加速度，°/s2）帮我设计一个时速对应的阈值，每 10KM/h 为一个阶梯    /vehicle/chassis_domain_report.eps_system.actual_steering_angle_velocity 方向盘转角速度
8、顿挫次数  --- |纵向jerk|>2.5m/s³,持续时间200ms以上的次数 /vehicle/chassis_domain_report.motion_system.vehicle_longitudinal_acceleration 加速度值，计算 jerk
9、画龙次数  --- 横向偏移量出现幅度30cm以上的波动， /control/parsed_debug.latDebug.nearestLateralError(需要单独 proto 解析/control/debug) 超过 30cm

10、
ROI15平均每帧目标  计算以自车bbox（详见自车信息，后轴中心为定位点）为中心，前 15m后 3m，左右各 3m 矩形区域内的障碍物，使用 bbox 交集判断（IoU），并且计算障碍物动态障碍物、静态障碍物各一个数量指标； 周围障碍物为utm 坐标/perception/fusion/obstacle_list_utm.obstacles[:].position_boot.x y 长宽/perception/fusion/obstacle_list_utm.obstacles[:].length width, 朝向 /perception/fusion/obstacle_list_utm.obstacles[:].yaw ，自车位置及朝向（/localization/localization.global_localization.position.latitude   /localization/localization.global_localization.position.longitude /localization/localization.global_localization.position.euler_angles.yaw）计算时先将 utm坐标转换到经纬度坐标再进行 roi 的交集计算；障碍物类别：/perception/fusion/obstacle_list_utm.obstacles[:].type_history[0].type，对应关系    0: "未知", 30: "人", 60: "车", 90: "自行车",120: "动物", 150: "静态障碍物", 180: "交通锥"；障碍物是否静止：/perception/fusion/obstacle_list_utm.obstacles[:].motion_status,值为 3 是静止，1 为运动；
ROI5平均每帧目标  计算以自车box（详见自车信息，后轴中心为定位点）为中心，前 5m后 2m，左右各 2m 矩形区域内的障碍物；
11、道路平均曲率 根据/planning/trajectory.path_point[].x y ,这是 refline 点（UTM 坐标）， 自车定位/localization/localization.global_localization.position.latitude   /localization/localization.global_localization.position.longitude需要转换到 UTM 坐标，再进行最近点查找，获取点的/planning/trajectory.path_point[].kappa值，最后进行平均值计算
ros2 interface show hv_planning_msgs/msg/Trajectory
# Generated from /home/jenkins/agent/workspace/CPP-Build/cpp-hv_interface-oss-x86/hv_interface/interface/planning/trajectory.h
# Original struct: Trajectory

hv_common_msgs/Header header
        float64 global_timestamp  #
        float64 local_timestamp  #
        uint32 frame_sequence  #
        string frame_id
PlanningHeader planning_header
        float64 global_timestamp  #
        float64 local_timestamp  #
        string module_name
        uint32 frame_sequence  #
        uint32 version  #
        string frame_id
float64 total_path_length  # default: 0.0
float64 total_path_time  # default: 0.0
TrajectoryPoint[] trajectory_point
        PathPoint path_point
                float64 x  #
                float64 y  #
                float64 z  #
                float64 theta  #
                float64 kappa  #
                float64 s  #
                float64 dkappa  #
                float64 ddkappa  #
                string lane_id
                float64 x_derivative  #
                float64 y_derivative  #
        float64 v  #
        float64 a  #
        float64 relative_time  #
        float64 da  #
        float64 steer  #
Estop estop
        bool is_estop  #
        string reason
PathPoint[] path_point
        float64 x  #
        float64 y  #
        float64 z  #
        float64 theta  #
        float64 kappa  #
        float64 s  #
        float64 dkappa  #
        float64 ddkappa  #
        string lane_id
        float64 x_derivative  #
        float64 y_derivative  #
bool is_replan  # default: false
string replan_reason
bool is_uturn  # default: false
int8 gear  # default: GearPosition::GEAR_NEUTRAL
DecisionResult decision
        MainDecision main_decision
                int8 task  #
                TargetLane[] target_lane
                        string id
                        float64 start_s  #
                        float64 end_s  #
                        float64 speed_limit  #
        ObjectDecisions object_decision
                ObjectDecision[] decision
                        string id
                        int32 perception_id  #
                        ObjectDecisionType[] object_decisions
                                int8 object_tag  #
        VehicleSignal vehicle_signal
                int8 turn_signal  #
                bool high_beam  #
                bool low_beam  #
                bool horn  #
                bool emergency_light  #
LatencyStats latency_stats
        float64 total_time_ms  #
        TaskStats[] task_stats
                string name
                float64 time_ms  #
        float64 init_frame_time_ms  #
PlanningHeader routing_header
        float64 global_timestamp  #
        float64 local_timestamp  #
        string module_name
        uint32 frame_sequence  #
        uint32 version  #
        string frame_id
int8 right_of_way_status  # default: RightOfWayStatus::UNPROTECTED
string[] lane_id
EngageAdvice engage_advice
        int8 advice  #
        string reason
CriticalRegion critical_region
        hv_common_msgs/Polygon[] region
                Point3d[] data
                        float64 x  #
                        float64 y  #
                        float64 z  #
int8 trajectory_type  # default: TrajectoryType::UNKNOWN
string[] target_lane_id
EgoIntent ego_intent
        uint8[] lateral_intents
        uint32[] lateral_index
        uint8[] longitudinal_intens
        uint32[] longitudinal_index
uint8 stop_start_states
PlanningSpeedLimit speed_limit
        string[] speed_limit_parsa
        PlanningTrafficLight[] speed_limit_graph
                string id
                uint8 color  #
                uint8 shape  #
                uint8 time  #
PlanningTrafficLightDecision traffic_light_decision
        uint32 tl_seq_num  #
        uint32 observe_seq_num  #
        PlanningTrafficLightInfo[] traffic_lights
                string id
                PlanningSubLight[] sub_lights
                        uint8 type  #
                        uint8 color  #
                        uint8 blink  #
                        uint8 counter  #
        PlanningStopLineInfo[] stop_lines
                string id
                float64 start_s  #
                float64 end_s  #
                bool is_stop  #
                string[] related_tl_id
12、平均定位置信度  /localization/localization
{switch status.common
  case 2 => 等待初始化
  case 3 => 正常
  case 7 => 
    max_position_stddev = max(global_localization.position_stddev.east, global_localization.position_stddev.north)
    if (max_position_stddev >1 ) => 定位偏移
    if (0.5 < max_position_stddev < 1) => 可能偏移
    else => 正常}
注：/localization/localization，control/debug，/vehicle/chassis_domain_report为 50hz，其余 topic 均为 10hz！
补充自车基本信息，用于自车 bbox 计算：
以后轴中心为原点，到前保最外沿中心点的长度（前悬+轴距） mm	940+2850=3790
以后轴中心为原点，到车尾最外沿中心点的长度 mm	883
以后轴中心为原点，到车身左侧外沿的长度 mm	945
以后轴中心为原点，到车身右侧外沿的长度 mm	945
车长 mm	4658
车宽 mm	1890
车高 mm	1656
