"""
画龙检测KPI
检测自动驾驶横向控制异常（lateral oscillation）

简化判定方法（直观易懂）：
1. 转角速度"过零点"：方向盘从左打到右（或反过来）
2. 在时间窗口内（如3秒），过零点次数 ≥ 4次 = 至少2个完整的左右摆动周期
3. 转角速度幅度 ≥ 阈值（如15°/s），排除微小抖动
4. 持续时间 ≥ 2秒

核心思想：画龙 = 方向盘频繁左右来回打
"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import numpy as np
import os
import json

from .base_kpi import BaseKPI, KPIResult, BagTimeMapper, StreamingData
from ..data_loader.bag_reader import MessageAccessor
from ..utils.signal import SignalProcessor


@dataclass
class WeavingEvent:
    """画龙事件（L4标准：横向控制振荡）"""
    start_time: float
    end_time: float
    max_steering_rate: float      # 最大方向盘角速度 (deg/s)
    max_steering_acc: float       # 最大方向盘角加速度 (deg/s²)
    max_lateral_error_rate: float # 最大横向误差变化率 (m/s)
    max_lateral_acc: float        # 最大横向加速度 (m/s²)
    oscillation_count: int        # 符号翻转次数
    max_rms: float = 0.0          # 最大 RMS(|δ̇|) (deg/s) - L4标准新增
    trajectory: List[Dict] = None  # 轨迹点列表 [{lat, lon, timestamp, steering_vel}]
    
    @property
    def duration(self) -> float:
        return self.end_time - self.start_time
    
    def __post_init__(self):
        if self.trajectory is None:
            self.trajectory = []


class WeavingKPI(BaseKPI):
    """
    画龙检测KPI
    
    基于行业标准的横向控制异常检测
    """
    
    @property
    def name(self) -> str:
        return "画龙检测"
    
    @property
    def required_topics(self) -> List[str]:
        return [
            "/function/function_manager",
            "/vehicle/chassis_domain_report",
            "/control/debug",  # 用于获取横向误差
            "/localization/localization"  # 用于检查定位可信度
        ]
    
    @property
    def dependencies(self) -> List[str]:
        return ["里程统计"]
    
    @property
    def provides(self) -> List[str]:
        return ["画龙次数", "画龙频率"]
    
    @property
    def supports_streaming(self) -> bool:
        """支持流式收集模式"""
        return True
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__(config)
        
        weaving_config = self.config.get('kpi', {}).get('weaving', {})
        
        # ========== 简化版画龙检测参数（直观易懂）==========
        # 转角速度幅度阈值：低于此值认为是微小抖动，不算画龙
        self.steering_rate_threshold = weaving_config.get('steering_rate_threshold', 15.0)  # deg/s
        
        # 检测窗口：在多长时间内统计过零点次数
        self.window_duration = weaving_config.get('window_duration', 5.0)  # 秒
        
        # 最小过零点次数：窗口内至少要有这么多次过零点才算振荡
        # 2次过零 = 1个完整周期（左→右 或 右→左）
        self.min_zero_crossings = weaving_config.get('min_zero_crossings', 2)
        
        # 最小持续时间：画龙要持续多久才算
        self.min_duration = weaving_config.get('min_duration', 2.0)  # 秒
        
        # 方向盘转角振幅阈值：窗口内转角的(最大值-最小值)超过此值时认为是过弯，不检测画龙
        # 画龙特点：转角在某个位置左右小幅振荡，振幅较小
        # 过弯特点：转角有明显的单向偏转，振幅较大
        self.max_steering_amplitude = weaving_config.get('max_steering_amplitude', 60.0)  # deg
        
        # 定位可信度配置
        loc_config = self.config.get('kpi', {}).get('localization', {})
        self.loc_valid_status = [3, 7]  # 有效的定位状态
        self.loc_max_stddev = loc_config.get('medium_stddev_threshold', 0.2)
        
        # 数据缓存
        self._debug_timestamps = None
    
    def _find_nearest_debug_data(self, parsed_data: Dict, target_ts: float, 
                                   key: str, tolerance: float = 0.05) -> Optional[float]:
        """根据时间戳查找最近的调试数据"""
        if not parsed_data:
            return None
        
        if self._debug_timestamps is None:
            self._debug_timestamps = sorted(parsed_data.keys())
        
        timestamps = self._debug_timestamps
        if not timestamps:
            return None
            
        left, right = 0, len(timestamps) - 1
        
        while left < right:
            mid = (left + right) // 2
            if timestamps[mid] < target_ts:
                left = mid + 1
            else:
                right = mid
        
        best_ts = None
        best_diff = float('inf')
        
        for idx in [left - 1, left, left + 1]:
            if 0 <= idx < len(timestamps):
                diff = abs(timestamps[idx] - target_ts)
                if diff < best_diff:
                    best_diff = diff
                    best_ts = timestamps[idx]
        
        if best_ts is not None and best_diff <= tolerance:
            return parsed_data.get(best_ts, {}).get(key)
        
        return None
    
    def compute(self, synced_frames: List, 
                parsed_debug_data: Optional[Dict] = None,
                **kwargs) -> List[KPIResult]:
        """计算画龙检测KPI - 通过流式模式复用逻辑"""
        return self._compute_via_streaming(synced_frames, 
                                           parsed_debug_data=parsed_debug_data,
                                           **kwargs)
    
    def _detect_weaving_events(self,
                                timestamps: np.ndarray,
                                steering_angles: np.ndarray,
                                steering_velocities: np.ndarray,
                                ts_acc: np.ndarray,
                                steering_accs: np.ndarray,
                                lateral_errors: np.ndarray,
                                ts_err_rate: np.ndarray,
                                lateral_error_rates: np.ndarray,
                                lateral_accs: np.ndarray,
                                speeds: np.ndarray,
                                latitudes: np.ndarray = None,
                                longitudes: np.ndarray = None) -> List[WeavingEvent]:
        """
        简化版画龙检测（直观易懂）
        
        核心思想：画龙 = 直线行驶时方向盘频繁左右来回打
        
        判定条件：
        1. 过零点次数：在窗口时间内，转角速度过零点 ≥ 4次（2个完整周期）
        2. 幅度要求：转角速度峰值 ≥ 阈值（排除微小抖动）
        3. 转角要求：方向盘转角绝对值 < 阈值（排除转弯场景）
        4. 持续时间：满足条件的持续时间 ≥ 最小时长
        
        过零点示意图：
            转角速度
               ↑
           +   │    ╱╲      ╱╲
               │   ╱  ╲    ╱  ╲
           ────┼──╱────╲──╱────╲──→ 时间
               │ ╱      ╲╱      ╲
           -   │╱                ╲
               
           过零点: ↑    ↑  ↑    ↑   = 4次过零 = 2个完整周期
        """
        events = []
        
        if len(timestamps) < 10:
            return events
        
        # 计算采样间隔
        dt = np.median(np.diff(timestamps))
        if dt <= 0:
            dt = 0.1
        
        # ========== 参数 ==========
        window_samples = max(int(self.window_duration / dt), 10)  # 窗口大小（采样点数）
        max_gap_sec = 0.5  # 最大允许时间间隔（超过则认为数据中断）
        
        # ========== 第一步：检测过零点 ==========
        # 过零点：转角速度从正变负或从负变正
        signs = np.sign(steering_velocities)
        # 处理零值：继承前一个符号
        for i in range(1, len(signs)):
            if signs[i] == 0:
                signs[i] = signs[i-1]
        
        # 标记过零点位置（符号发生变化的点）
        zero_crossings = np.zeros(len(signs), dtype=bool)
        zero_crossings[1:] = (signs[1:] != signs[:-1])
        
        # ========== 第二步：标记满足条件的点 ==========
        # 转角速度绝对值 >= 阈值（排除微小抖动）
        amplitude_ok = np.abs(steering_velocities) >= self.steering_rate_threshold
        
        # ========== 第三步：滑动窗口检测 ==========
        is_weaving = np.zeros(len(timestamps), dtype=bool)
        
        for i in range(window_samples, len(timestamps)):
            window_start = i - window_samples
            window_end = i
            
            # 检查窗口内是否有数据中断
            window_ts = timestamps[window_start:window_end]
            ts_gaps = np.diff(window_ts)
            if np.any(ts_gaps > max_gap_sec):
                continue  # 有中断，跳过
            
            # 统计窗口内的过零点次数
            window_zero_crossings = np.sum(zero_crossings[window_start:window_end])
            
            # 统计窗口内满足幅度条件的点比例
            window_amplitude_ok_ratio = np.mean(amplitude_ok[window_start:window_end])
            
            # 计算窗口内转角的振幅（最大值 - 最小值）
            # 画龙特点：转角在某个位置小幅振荡，振幅小
            # 过弯特点：转角有单向偏转趋势，振幅大
            window_angles = steering_angles[window_start:window_end]
            steering_amplitude = np.max(window_angles) - np.min(window_angles)
            
            # 判定条件：
            # 1. 过零点次数 >= 阈值（频繁来回打方向）
            # 2. 至少50%的点满足幅度条件（不是微小抖动）
            # 3. 转角振幅 < 阈值（不是过弯场景）
            if (window_zero_crossings >= self.min_zero_crossings and 
                window_amplitude_ok_ratio >= 0.5 and
                steering_amplitude < self.max_steering_amplitude):
                is_weaving[i] = True
        
        # ========== 第四步：提取连续的画龙区间 ==========
        in_event = False
        event_start_idx = 0
        
        for i in range(len(is_weaving)):
            # 检查时间连续性
            if i > 0:
                time_gap = timestamps[i] - timestamps[i-1]
                if time_gap > max_gap_sec:
                    # 数据中断，结束当前事件
                    if in_event:
                        self._finalize_simple_event(
                            events, timestamps, steering_velocities, lateral_accs,
                            event_start_idx, i - 1, latitudes, longitudes
                        )
                        in_event = False
                    continue
            
            if is_weaving[i]:
                if not in_event:
                    in_event = True
                    event_start_idx = i
            else:
                if in_event:
                    # 事件结束
                    self._finalize_simple_event(
                        events, timestamps, steering_velocities, lateral_accs,
                        event_start_idx, i - 1, latitudes, longitudes
                    )
                    in_event = False
        
        # 处理最后一个事件
        if in_event:
            self._finalize_simple_event(
                events, timestamps, steering_velocities, lateral_accs,
                event_start_idx, len(timestamps) - 1, latitudes, longitudes
            )
        
        return events
    
    def _finalize_simple_event(self, events: List[WeavingEvent],
                                timestamps: np.ndarray,
                                steering_velocities: np.ndarray,
                                lateral_accs: np.ndarray,
                                start_idx: int,
                                end_idx: int,
                                latitudes: np.ndarray = None,
                                longitudes: np.ndarray = None):
        """完成简化版画龙事件的记录"""
        if end_idx <= start_idx:
            return
        
        start_time = timestamps[start_idx]
        end_time = timestamps[end_idx]
        duration = end_time - start_time
        
        # 检查持续时间
        if duration < self.min_duration:
            return
        
        # 计算统计信息
        segment_vel = steering_velocities[start_idx:end_idx+1]
        segment_lat_acc = lateral_accs[start_idx:end_idx+1]
        
        max_steering_rate = float(np.max(np.abs(segment_vel)))
        max_lateral_acc = float(np.max(np.abs(segment_lat_acc)))
        
        # 计算RMS
        rms = float(np.sqrt(np.mean(segment_vel ** 2)))
        
        # 计算过零点次数（振荡次数）
        signs = np.sign(segment_vel)
        for i in range(1, len(signs)):
            if signs[i] == 0:
                signs[i] = signs[i-1]
        oscillation_count = int(np.sum(signs[1:] != signs[:-1]))
        
        # 提取轨迹数据（用于可视化）
        trajectory = []
        if latitudes is not None and longitudes is not None:
            segment_ts = timestamps[start_idx:end_idx+1]
            segment_lat = latitudes[start_idx:end_idx+1]
            segment_lon = longitudes[start_idx:end_idx+1]
            
            for i in range(len(segment_ts)):
                # 只保留有效的位置点
                if segment_lat[i] != 0 and segment_lon[i] != 0:
                    trajectory.append({
                        'lat': float(segment_lat[i]),
                        'lon': float(segment_lon[i]),
                        'timestamp': float(segment_ts[i]),
                        'steering_vel': float(segment_vel[i])
                    })
        
        events.append(WeavingEvent(
            start_time=start_time,
            end_time=end_time,
            max_steering_rate=max_steering_rate,
            max_steering_acc=0.0,  # 简化版不计算
            max_lateral_error_rate=0.0,  # 简化版不计算
            max_lateral_acc=max_lateral_acc,
            oscillation_count=oscillation_count,
            max_rms=rms,
            trajectory=trajectory
        ))
    
    def _generate_weaving_map(self, weaving_events: List[WeavingEvent], output_dir: str):
        """
        生成画龙事件的轨迹地图可视化
        
        为每个画龙事件生成一个 HTML 地图文件，显示事件期间的车辆轨迹
        轨迹颜色根据转角速度大小渐变
        """
        # 创建输出目录
        weaving_viz_dir = os.path.join(output_dir, 'weaving_viz')
        os.makedirs(weaving_viz_dir, exist_ok=True)
        
        # 检查是否有有效轨迹的事件
        events_with_trajectory = [e for e in weaving_events if e.trajectory and len(e.trajectory) >= 2]
        
        if not events_with_trajectory:
            print(f"    [画龙可视化] 没有包含有效轨迹的画龙事件")
            return
        
        # 为每个事件生成地图
        for i, event in enumerate(events_with_trajectory):
            output_path = os.path.join(weaving_viz_dir, f'weaving_event_{i+1:02d}.html')
            self._generate_single_weaving_map(event, i + 1, output_path)
        
        # 生成汇总地图（所有画龙事件）
        all_trajectory_map_path = os.path.join(weaving_viz_dir, 'weaving_all_events.html')
        self._generate_all_weaving_map(events_with_trajectory, all_trajectory_map_path)
        
        print(f"    [画龙可视化] 已生成 {len(events_with_trajectory)} 个画龙事件地图到 {weaving_viz_dir}")
    
    def _generate_single_weaving_map(self, event: WeavingEvent, event_idx: int, output_path: str):
        """生成单个画龙事件的地图"""
        trajectory = event.trajectory
        if not trajectory or len(trajectory) < 2:
            return
        
        # 中心点
        center_lat = trajectory[len(trajectory) // 2]['lat']
        center_lon = trajectory[len(trajectory) // 2]['lon']
        
        # 转换为 GeoJSON
        # 轨迹线
        line_coords = [[p['lon'], p['lat']] for p in trajectory]
        line_feature = {
            "type": "Feature",
            "properties": {},
            "geometry": {
                "type": "LineString",
                "coordinates": line_coords
            }
        }
        
        # 轨迹点（带转角速度信息）
        point_features = []
        max_steering_vel = max(abs(p['steering_vel']) for p in trajectory)
        
        for j, p in enumerate(trajectory):
            # 归一化转角速度用于颜色渐变 (0-1)
            normalized_vel = abs(p['steering_vel']) / max_steering_vel if max_steering_vel > 0 else 0
            
            point_features.append({
                "type": "Feature",
                "properties": {
                    "index": j,
                    "steering_vel": round(p['steering_vel'], 1),
                    "normalized": round(normalized_vel, 3),
                    "timestamp": p['timestamp']
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [p['lon'], p['lat']]
                }
            })
        
        line_collection = {"type": "FeatureCollection", "features": [line_feature]}
        points_collection = {"type": "FeatureCollection", "features": point_features}
        
        # 获取 Mapbox token
        mapbox_token = self.config.get('kpi', {}).get('curvature', {}).get(
            'map_viz', {}).get('mapbox_token', 'YOUR_MAPBOX_TOKEN_HERE')
        
        html_content = self._generate_weaving_html(
            center_lat=center_lat,
            center_lon=center_lon,
            line_json=json.dumps(line_collection),
            points_json=json.dumps(points_collection),
            event_idx=event_idx,
            event=event,
            mapbox_token=mapbox_token
        )
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
    
    def _generate_all_weaving_map(self, events: List[WeavingEvent], output_path: str):
        """生成所有画龙事件的汇总地图"""
        if not events:
            return
        
        # 收集所有轨迹点
        all_features = []
        all_line_features = []
        
        # 颜色列表（为不同事件分配不同颜色）
        colors = ['#e74c3c', '#3498db', '#2ecc71', '#f39c12', '#9b59b6', 
                  '#1abc9c', '#e67e22', '#34495e', '#16a085', '#d35400']
        
        for i, event in enumerate(events):
            if not event.trajectory or len(event.trajectory) < 2:
                continue
            
            color = colors[i % len(colors)]
            
            # 轨迹线
            line_coords = [[p['lon'], p['lat']] for p in event.trajectory]
            all_line_features.append({
                "type": "Feature",
                "properties": {
                    "event_idx": i + 1,
                    "color": color,
                    "duration": round(event.duration, 2),
                    "max_steering_rate": round(event.max_steering_rate, 1)
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": line_coords
                }
            })
            
            # 起点和终点标记
            all_features.append({
                "type": "Feature",
                "properties": {
                    "type": "start",
                    "event_idx": i + 1,
                    "color": color
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [event.trajectory[0]['lon'], event.trajectory[0]['lat']]
                }
            })
            all_features.append({
                "type": "Feature",
                "properties": {
                    "type": "end",
                    "event_idx": i + 1,
                    "color": color
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [event.trajectory[-1]['lon'], event.trajectory[-1]['lat']]
                }
            })
        
        if not all_line_features:
            return
        
        # 计算中心点（所有轨迹的中心）
        all_coords = []
        for event in events:
            for p in event.trajectory:
                all_coords.append((p['lat'], p['lon']))
        
        center_lat = sum(c[0] for c in all_coords) / len(all_coords)
        center_lon = sum(c[1] for c in all_coords) / len(all_coords)
        
        lines_collection = {"type": "FeatureCollection", "features": all_line_features}
        points_collection = {"type": "FeatureCollection", "features": all_features}
        
        mapbox_token = self.config.get('kpi', {}).get('curvature', {}).get(
            'map_viz', {}).get('mapbox_token', 'YOUR_MAPBOX_TOKEN_HERE')
        
        html_content = self._generate_all_weaving_html(
            center_lat=center_lat,
            center_lon=center_lon,
            lines_json=json.dumps(lines_collection),
            points_json=json.dumps(points_collection),
            event_count=len(events),
            mapbox_token=mapbox_token
        )
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
    
    def _generate_weaving_html(self, center_lat: float, center_lon: float,
                                line_json: str, points_json: str,
                                event_idx: int, event: WeavingEvent,
                                mapbox_token: str) -> str:
        """生成单个画龙事件的 HTML 页面"""
        
        return f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>画龙事件 #{event_idx} 轨迹</title>
    <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no">
    <link href="https://api.mapbox.com/mapbox-gl-js/v3.0.1/mapbox-gl.css" rel="stylesheet">
    <script src="https://api.mapbox.com/mapbox-gl-js/v3.0.1/mapbox-gl.js"></script>
    <style>
        body {{ margin: 0; padding: 0; }}
        #map {{ position: absolute; top: 0; bottom: 0; width: 100%; }}
        .info-panel {{
            position: absolute;
            top: 10px;
            left: 10px;
            background: rgba(255, 255, 255, 0.95);
            padding: 15px;
            border-radius: 8px;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            font-size: 13px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
            max-width: 280px;
        }}
        .info-title {{
            font-size: 16px;
            font-weight: 600;
            margin-bottom: 10px;
            color: #e74c3c;
        }}
        .info-row {{
            display: flex;
            justify-content: space-between;
            margin: 6px 0;
        }}
        .info-label {{ color: #666; }}
        .info-value {{ font-weight: 500; }}
        .legend {{
            position: absolute;
            bottom: 30px;
            left: 10px;
            background: rgba(255, 255, 255, 0.95);
            padding: 12px 15px;
            border-radius: 8px;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            font-size: 12px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
        }}
        .legend-title {{ font-weight: 600; margin-bottom: 8px; }}
        .gradient-bar {{
            width: 150px;
            height: 15px;
            background: linear-gradient(to right, #3498db, #f39c12, #e74c3c);
            border-radius: 3px;
            margin: 5px 0;
        }}
        .gradient-labels {{
            display: flex;
            justify-content: space-between;
            font-size: 11px;
            color: #666;
        }}
    </style>
</head>
<body>
<div id="map"></div>

<div class="info-panel">
    <div class="info-title">🐉 画龙事件 #{event_idx}</div>
    <div class="info-row">
        <span class="info-label">持续时间:</span>
        <span class="info-value">{event.duration:.2f} 秒</span>
    </div>
    <div class="info-row">
        <span class="info-label">峰值转角速度:</span>
        <span class="info-value">{event.max_steering_rate:.1f} °/s</span>
    </div>
    <div class="info-row">
        <span class="info-label">RMS转角速度:</span>
        <span class="info-value">{event.max_rms:.1f} °/s</span>
    </div>
    <div class="info-row">
        <span class="info-label">振荡次数:</span>
        <span class="info-value">{event.oscillation_count} 次</span>
    </div>
    <div class="info-row">
        <span class="info-label">轨迹点数:</span>
        <span class="info-value">{len(event.trajectory)}</span>
    </div>
</div>

<div class="legend">
    <div class="legend-title">转角速度幅度</div>
    <div class="gradient-bar"></div>
    <div class="gradient-labels">
        <span>0</span>
        <span>{event.max_steering_rate:.0f}°/s</span>
    </div>
</div>

<script>
    mapboxgl.accessToken = '{mapbox_token}';
    
    const lineData = {line_json};
    const pointsData = {points_json};
    
    const map = new mapboxgl.Map({{
        container: 'map',
        style: 'mapbox://styles/mapbox/dark-v11',
        center: [{center_lon}, {center_lat}],
        zoom: 18
    }});
    
    map.addControl(new mapboxgl.NavigationControl());
    map.addControl(new mapboxgl.ScaleControl());
    
    map.on('load', () => {{
        // 添加轨迹线
        map.addSource('line', {{ 'type': 'geojson', 'data': lineData }});
        map.addLayer({{
            'id': 'trajectory-line',
            'type': 'line',
            'source': 'line',
            'paint': {{
                'line-color': '#e74c3c',
                'line-width': 4,
                'line-opacity': 0.7
            }}
        }});
        
        // 添加轨迹点（颜色根据转角速度渐变）
        map.addSource('points', {{ 'type': 'geojson', 'data': pointsData }});
        map.addLayer({{
            'id': 'trajectory-points',
            'type': 'circle',
            'source': 'points',
            'paint': {{
                'circle-radius': 6,
                'circle-color': [
                    'interpolate',
                    ['linear'],
                    ['get', 'normalized'],
                    0, '#3498db',
                    0.5, '#f39c12',
                    1, '#e74c3c'
                ],
                'circle-opacity': 0.9,
                'circle-stroke-width': 1,
                'circle-stroke-color': '#fff'
            }}
        }});
        
        // 自动适配视野
        const bounds = new mapboxgl.LngLatBounds();
        pointsData.features.forEach(f => bounds.extend(f.geometry.coordinates));
        map.fitBounds(bounds, {{ padding: 60 }});
        
        // 添加起点终点标记
        if (pointsData.features.length > 0) {{
            const startCoord = pointsData.features[0].geometry.coordinates;
            const endCoord = pointsData.features[pointsData.features.length - 1].geometry.coordinates;
            
            new mapboxgl.Marker({{ color: '#2ecc71' }})
                .setLngLat(startCoord)
                .setPopup(new mapboxgl.Popup().setHTML('<b>起点</b>'))
                .addTo(map);
            
            new mapboxgl.Marker({{ color: '#e74c3c' }})
                .setLngLat(endCoord)
                .setPopup(new mapboxgl.Popup().setHTML('<b>终点</b>'))
                .addTo(map);
        }}
        
        // 点击弹窗
        map.on('click', 'trajectory-points', (e) => {{
            const props = e.features[0].properties;
            new mapboxgl.Popup()
                .setLngLat(e.features[0].geometry.coordinates)
                .setHTML(`<b>轨迹点 #${{props.index}}</b><br>转角速度: ${{props.steering_vel}}°/s`)
                .addTo(map);
        }});
        
        map.on('mouseenter', 'trajectory-points', () => {{ map.getCanvas().style.cursor = 'pointer'; }});
        map.on('mouseleave', 'trajectory-points', () => {{ map.getCanvas().style.cursor = ''; }});
    }});
</script>
</body>
</html>
'''
    
    def _generate_all_weaving_html(self, center_lat: float, center_lon: float,
                                    lines_json: str, points_json: str,
                                    event_count: int, mapbox_token: str) -> str:
        """生成所有画龙事件汇总的 HTML 页面"""
        
        return f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>画龙事件汇总 ({event_count}个事件)</title>
    <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no">
    <link href="https://api.mapbox.com/mapbox-gl-js/v3.0.1/mapbox-gl.css" rel="stylesheet">
    <script src="https://api.mapbox.com/mapbox-gl-js/v3.0.1/mapbox-gl.js"></script>
    <style>
        body {{ margin: 0; padding: 0; }}
        #map {{ position: absolute; top: 0; bottom: 0; width: 100%; }}
        .info-panel {{
            position: absolute;
            top: 10px;
            left: 10px;
            background: rgba(255, 255, 255, 0.95);
            padding: 15px;
            border-radius: 8px;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            font-size: 14px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
        }}
        .info-title {{
            font-size: 16px;
            font-weight: 600;
            margin-bottom: 10px;
            color: #e74c3c;
        }}
        .legend {{
            position: absolute;
            bottom: 30px;
            left: 10px;
            background: rgba(255, 255, 255, 0.95);
            padding: 12px 15px;
            border-radius: 8px;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            font-size: 12px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
        }}
        .legend-title {{ font-weight: 600; margin-bottom: 8px; }}
        .legend-item {{
            display: flex;
            align-items: center;
            margin: 5px 0;
        }}
        .legend-line {{
            width: 25px;
            height: 4px;
            margin-right: 8px;
            border-radius: 2px;
        }}
    </style>
</head>
<body>
<div id="map"></div>

<div class="info-panel">
    <div class="info-title">🐉 画龙事件汇总</div>
    <div>共检测到 <b>{event_count}</b> 个画龙事件</div>
    <div style="margin-top: 8px; font-size: 12px; color: #666;">
        点击轨迹线查看详情<br>
        🟢 起点 &nbsp; 🔴 终点
    </div>
</div>

<div class="legend">
    <div class="legend-title">事件图例</div>
    <div id="legend-items"></div>
</div>

<script>
    mapboxgl.accessToken = '{mapbox_token}';
    
    const linesData = {lines_json};
    const pointsData = {points_json};
    
    const map = new mapboxgl.Map({{
        container: 'map',
        style: 'mapbox://styles/mapbox/dark-v11',
        center: [{center_lon}, {center_lat}],
        zoom: 15
    }});
    
    map.addControl(new mapboxgl.NavigationControl());
    map.addControl(new mapboxgl.ScaleControl());
    
    map.on('load', () => {{
        // 添加所有轨迹线
        map.addSource('lines', {{ 'type': 'geojson', 'data': linesData }});
        map.addLayer({{
            'id': 'trajectory-lines',
            'type': 'line',
            'source': 'lines',
            'paint': {{
                'line-color': ['get', 'color'],
                'line-width': 4,
                'line-opacity': 0.8
            }}
        }});
        
        // 自动适配视野
        const bounds = new mapboxgl.LngLatBounds();
        linesData.features.forEach(f => {{
            f.geometry.coordinates.forEach(coord => bounds.extend(coord));
        }});
        map.fitBounds(bounds, {{ padding: 60 }});
        
        // 为每个事件添加起点终点标记
        pointsData.features.forEach(f => {{
            const props = f.properties;
            const color = props.type === 'start' ? '#2ecc71' : '#e74c3c';
            const label = props.type === 'start' ? `事件#${{props.event_idx}} 起点` : `事件#${{props.event_idx}} 终点`;
            
            new mapboxgl.Marker({{ color: color, scale: 0.7 }})
                .setLngLat(f.geometry.coordinates)
                .setPopup(new mapboxgl.Popup().setHTML(`<b>${{label}}</b>`))
                .addTo(map);
        }});
        
        // 生成图例
        const legendItems = document.getElementById('legend-items');
        const seenEvents = new Set();
        linesData.features.forEach(f => {{
            const idx = f.properties.event_idx;
            if (!seenEvents.has(idx)) {{
                seenEvents.add(idx);
                const item = document.createElement('div');
                item.className = 'legend-item';
                item.innerHTML = `<div class="legend-line" style="background: ${{f.properties.color}};"></div>` +
                                 `<span>事件 #${{idx}} (${{f.properties.duration}}s)</span>`;
                legendItems.appendChild(item);
            }}
        }});
        
        // 点击弹窗
        map.on('click', 'trajectory-lines', (e) => {{
            const props = e.features[0].properties;
            new mapboxgl.Popup()
                .setLngLat(e.lngLat)
                .setHTML(`<b>画龙事件 #${{props.event_idx}}</b><br>` +
                         `持续: ${{props.duration}}s<br>` +
                         `峰值: ${{props.max_steering_rate}}°/s`)
                .addTo(map);
        }});
        
        map.on('mouseenter', 'trajectory-lines', () => {{ map.getCanvas().style.cursor = 'pointer'; }});
        map.on('mouseleave', 'trajectory-lines', () => {{ map.getCanvas().style.cursor = ''; }});
    }});
</script>
</body>
</html>
'''
    
    def _finalize_weaving_event(self, events: List[WeavingEvent],
                                 timestamps: np.ndarray,
                                 steering_velocities: np.ndarray,
                                 lateral_accs: np.ndarray,
                                 ts_acc: np.ndarray,
                                 steering_accs: np.ndarray,
                                 ts_err_rate: np.ndarray,
                                 lateral_error_rates: np.ndarray,
                                 rms_timestamps: np.ndarray,
                                 rms_values: np.ndarray,
                                 event_start_idx: int,
                                 event_end_idx: int,
                                 max_gap_sec: float = 0.5):
        """完成画龙事件的记录"""
        if event_end_idx <= event_start_idx:
            return
        
        start_time = rms_timestamps[event_start_idx]
        end_time = rms_timestamps[event_end_idx]
        
        # 计算实际有效持续时间（排除大间隔，如接管期间）
        orig_mask = (timestamps >= start_time) & (timestamps <= end_time)
        event_timestamps = timestamps[orig_mask]
        if len(event_timestamps) > 1:
            ts_diffs = np.diff(event_timestamps)
            # 只累加正常间隔，排除大间隔
            valid_diffs = ts_diffs[ts_diffs <= max_gap_sec]
            duration = float(np.sum(valid_diffs))
        else:
            duration = 0.0
        
        if duration < self.min_duration:
            return
        
        # 在原始数据中找到对应的时间范围
        orig_mask = (timestamps >= start_time) & (timestamps <= end_time)
        
        # 计算峰值
        max_steering_rate = float(np.max(np.abs(steering_velocities[orig_mask]))) if np.any(orig_mask) else 0
        max_lateral_acc = float(np.max(np.abs(lateral_accs[orig_mask]))) if np.any(orig_mask) else 0
        max_rms = float(np.max(rms_values[event_start_idx:event_end_idx+1]))
        
        # 加速度峰值
        acc_mask = (ts_acc >= start_time) & (ts_acc <= end_time)
        max_steering_acc = float(np.max(np.abs(steering_accs[acc_mask]))) if np.any(acc_mask) else 0
        
        # 横向误差变化率峰值
        err_mask = (ts_err_rate >= start_time) & (ts_err_rate <= end_time)
        max_lateral_error_rate = float(np.max(np.abs(lateral_error_rates[err_mask]))) if np.any(err_mask) else 0
        
        # 计算振荡次数（符号翻转）
        if np.any(orig_mask):
            vel_segment = steering_velocities[orig_mask]
            signs = np.sign(vel_segment)
            oscillation_count = int(np.sum(np.diff(signs) != 0))
        else:
            oscillation_count = 0
        
        events.append(WeavingEvent(
            start_time=start_time,
            end_time=end_time,
            max_steering_rate=max_steering_rate,
            max_steering_acc=max_steering_acc,
            max_lateral_error_rate=max_lateral_error_rate,
            max_lateral_acc=max_lateral_acc,
            oscillation_count=oscillation_count,
            max_rms=max_rms  # 新增：最大 RMS 值
        ))
    
    def _detect_weaving_events_legacy(self,
                                timestamps: np.ndarray,
                                steering_velocities: np.ndarray,
                                ts_acc: np.ndarray,
                                steering_accs: np.ndarray,
                                lateral_errors: np.ndarray,
                                ts_err_rate: np.ndarray,
                                lateral_error_rates: np.ndarray,
                                lateral_accs: np.ndarray,
                                speeds: np.ndarray) -> List[WeavingEvent]:
        """
        旧版画龙检测（保留作为备用）
        
        综合判断条件：
        1. 方向盘角速度超阈值 |δ̇| > 25 deg/s
        2. 方向盘角速度符号频繁翻转（振荡）
        3. 持续时间 > 0.5s
        """
        events = []
        
        if len(timestamps) < 10:
            return events
        
        # 计算采样间隔
        dt = np.median(np.diff(timestamps))
        if dt <= 0:
            dt = 0.1
        
        # 符号翻转检测的窗口大小
        window_samples = max(int(self.sign_flip_window / dt), 5)
        
        # 预计算符号翻转前缀，O(n) 获取窗口内翻转次数
        signs = np.sign(steering_velocities)
        for i in range(1, len(signs)):
            if signs[i] == 0:
                signs[i] = signs[i-1]
        sign_changes = np.concatenate([[0], (np.diff(signs) != 0).astype(int)])
        prefix_flips = np.cumsum(sign_changes)  # prefix_flips[i] 表示 0..i-1 的翻转次数
        
        # 逐点检测
        in_event = False
        event_start_idx = 0
        event_data = {
            'max_steering_rate': 0,
            'max_steering_acc': 0,
            'max_lateral_error_rate': 0,
            'max_lateral_acc': 0,
            'sign_flips': 0
        }
        
        for i in range(len(timestamps)):
            # 计算当前点的指标
            steering_rate = abs(steering_velocities[i])
            lat_acc = abs(lateral_accs[i])
            
            # 计算窗口内的符号翻转次数
            window_start = max(0, i - window_samples)
            # 翻转次数 = prefix_flips[i] - prefix_flips[window_start]
            sign_flips = int(prefix_flips[i] - prefix_flips[window_start])
            
            # 判断是否满足画龙条件
            is_weaving = (
                steering_rate > self.steering_rate_threshold and
                sign_flips >= self.min_sign_flips
            )
            
            if is_weaving:
                if not in_event:
                    # 开始新事件
                    in_event = True
                    event_start_idx = i
                    event_data = {
                        'max_steering_rate': steering_rate,
                        'max_steering_acc': 0,
                        'max_lateral_error_rate': 0,
                        'max_lateral_acc': lat_acc,
                        'sign_flips': sign_flips
                    }
                else:
                    # 更新事件数据
                    event_data['max_steering_rate'] = max(event_data['max_steering_rate'], steering_rate)
                    event_data['max_lateral_acc'] = max(event_data['max_lateral_acc'], lat_acc)
                    event_data['sign_flips'] = max(event_data['sign_flips'], sign_flips)
            else:
                if in_event:
                    # 事件结束，检查持续时间
                    duration = timestamps[i-1] - timestamps[event_start_idx]
                    
                    if duration >= self.min_duration:
                        # 查找对应的加速度峰值
                        acc_mask = (ts_acc >= timestamps[event_start_idx]) & (ts_acc <= timestamps[i-1])
                        if np.any(acc_mask):
                            event_data['max_steering_acc'] = float(np.max(np.abs(steering_accs[acc_mask])))
                        
                        # 查找对应的横向误差变化率峰值
                        err_mask = (ts_err_rate >= timestamps[event_start_idx]) & (ts_err_rate <= timestamps[i-1])
                        if np.any(err_mask):
                            event_data['max_lateral_error_rate'] = float(np.max(np.abs(lateral_error_rates[err_mask])))
                        
                        events.append(WeavingEvent(
                            start_time=timestamps[event_start_idx],
                            end_time=timestamps[i-1],
                            max_steering_rate=event_data['max_steering_rate'],
                            max_steering_acc=event_data['max_steering_acc'],
                            max_lateral_error_rate=event_data['max_lateral_error_rate'],
                            max_lateral_acc=event_data['max_lateral_acc'],
                            oscillation_count=event_data['sign_flips']
                        ))
                    
                    in_event = False
        
        # 处理最后一个事件
        if in_event:
            duration = timestamps[-1] - timestamps[event_start_idx]
            if duration >= self.min_duration:
                acc_mask = (ts_acc >= timestamps[event_start_idx])
                if np.any(acc_mask):
                    event_data['max_steering_acc'] = float(np.max(np.abs(steering_accs[acc_mask])))
                
                err_mask = (ts_err_rate >= timestamps[event_start_idx])
                if np.any(err_mask):
                    event_data['max_lateral_error_rate'] = float(np.max(np.abs(lateral_error_rates[err_mask])))
                
                events.append(WeavingEvent(
                    start_time=timestamps[event_start_idx],
                    end_time=timestamps[-1],
                    max_steering_rate=event_data['max_steering_rate'],
                    max_steering_acc=event_data['max_steering_acc'],
                    max_lateral_error_rate=event_data['max_lateral_error_rate'],
                    max_lateral_acc=event_data['max_lateral_acc'],
                    oscillation_count=event_data['sign_flips']
                ))
        
        return events
    
    def _count_sign_flips(self, values: np.ndarray) -> int:
        """计算符号翻转次数"""
        if len(values) < 2:
            return 0
        
        signs = np.sign(values)
        # 过滤掉0值（取前一个符号）
        for i in range(1, len(signs)):
            if signs[i] == 0:
                signs[i] = signs[i-1]
        
        # 计算符号变化次数
        sign_changes = np.sum(np.diff(signs) != 0)
        return int(sign_changes)
    
    def _add_empty_results(self):
        """添加空结果"""
        self.add_result(KPIResult(
            name="画龙次数",
            value=0,
            unit="次",
            description="数据不足，无法检测画龙"
        ))
        self.add_result(KPIResult(
            name="画龙频率",
            value=0.0,
            unit="次/百公里",
            description="数据不足"
        ))
        self.add_result(KPIResult(
            name="平均画龙里程",
            value="∞",
            unit="km/次",
            description="数据不足"
        ))
    
    # ========== 流式模式支持 ==========
    
    def collect(self, synced_frames: List, streaming_data: StreamingData,
                parsed_debug_data: Optional[Dict] = None, **kwargs):
        """
        收集画龙检测数据（流式模式）
        """
        self._debug_timestamps = None  # 重置缓存
        
        for frame in synced_frames:
            fm_msg = frame.messages.get("/function/function_manager")
            chassis_msg = frame.messages.get("/vehicle/chassis_domain_report")
            loc_msg = frame.messages.get("/localization/localization")
            
            if fm_msg is None or chassis_msg is None:
                continue
            
            operator_type = MessageAccessor.get_field(fm_msg, "operator_type")
            is_auto = operator_type == self.auto_operator_type
            
            if not is_auto:
                continue
            
            # 获取方向盘数据
            steering_angle = MessageAccessor.get_field(
                chassis_msg, "eps_system.actual_steering_angle", None)
            steering_vel = MessageAccessor.get_field(
                chassis_msg, "eps_system.actual_steering_angle_velocity", None)
            lat_acc = MessageAccessor.get_field(
                chassis_msg, "motion_system.vehicle_lateral_acceleration", None)
            speed = MessageAccessor.get_field(
                chassis_msg, "motion_system.vehicle_speed", None)
            
            # 获取位置信息（用于轨迹可视化）
            ego_lat = None
            ego_lon = None
            if loc_msg is not None:
                ego_lat = MessageAccessor.get_field(
                    loc_msg, "global_localization.position.latitude", None)
                ego_lon = MessageAccessor.get_field(
                    loc_msg, "global_localization.position.longitude", None)
            
            # 检查定位可信度
            loc_reliable = True
            if loc_msg is not None:
                loc_status = MessageAccessor.get_field(loc_msg, "status.common", None)
                pos_stddev_east = MessageAccessor.get_field(
                    loc_msg, "global_localization.position_stddev.east", None)
                pos_stddev_north = MessageAccessor.get_field(
                    loc_msg, "global_localization.position_stddev.north", None)
                
                if loc_status is not None and loc_status not in self.loc_valid_status:
                    loc_reliable = False
                
                if pos_stddev_east is not None and pos_stddev_north is not None:
                    if max(pos_stddev_east, pos_stddev_north) > self.loc_max_stddev:
                        loc_reliable = False
            
            # 获取横向误差
            lat_error = None
            if loc_reliable and parsed_debug_data is not None and len(parsed_debug_data) > 0:
                lat_error = self._find_nearest_debug_data(
                    parsed_debug_data, frame.timestamp, 'lateral_error', tolerance=0.05)
            
            if steering_vel is None:
                continue
            
            # 存储: (steering_vel, lat_acc, lat_error, loc_reliable, speed, timestamp, steering_angle, lat, lon)
            streaming_data.weaving_data.append((
                steering_vel,
                lat_acc if lat_acc is not None else 0.0,
                lat_error if lat_error is not None else 0.0,
                loc_reliable,
                speed if speed is not None else 0.0,
                frame.timestamp,
                steering_angle if steering_angle is not None else 0.0,
                ego_lat if ego_lat is not None else 0.0,
                ego_lon if ego_lon is not None else 0.0
            ))
    
    def compute_from_collected(self, streaming_data: StreamingData, **kwargs) -> List[KPIResult]:
        """
        从收集的数据计算画龙检测KPI（流式模式）
        """
        self.clear_results()
        
        if len(streaming_data.weaving_data) < 20:
            self._add_empty_results()
            return self.get_results()
        
        # 解构数据 (steering_vel, lat_acc, lat_error, loc_reliable, speed, timestamp, steering_angle, lat, lon)
        timestamps = np.array([d[5] for d in streaming_data.weaving_data])
        steering_velocities = np.array([d[0] for d in streaming_data.weaving_data])
        lateral_accs = np.array([d[1] for d in streaming_data.weaving_data])
        lateral_errors = np.array([d[2] for d in streaming_data.weaving_data])
        loc_reliable_flags = np.array([d[3] for d in streaming_data.weaving_data])
        speeds = np.array([d[4] for d in streaming_data.weaving_data])
        steering_angles = np.array([d[6] for d in streaming_data.weaving_data])
        latitudes = np.array([d[7] for d in streaming_data.weaving_data])
        longitudes = np.array([d[8] for d in streaming_data.weaving_data])
        
        # 计算方向盘角加速度
        ts_acc, steering_accs = SignalProcessor.compute_derivative(timestamps, steering_velocities)
        
        # 计算横向误差变化率
        ts_err_rate, lateral_error_rates = SignalProcessor.compute_derivative(timestamps, lateral_errors)
        
        # 检测画龙事件
        weaving_events = self._detect_weaving_events(
            timestamps, steering_angles, steering_velocities, ts_acc, steering_accs,
            lateral_errors, ts_err_rate, lateral_error_rates,
            lateral_accs, speeds, latitudes, longitudes
        )
        
        # 计算统计信息
        reliable_lateral_errors = lateral_errors[loc_reliable_flags]
        max_lateral_error = np.max(np.abs(reliable_lateral_errors)) if len(reliable_lateral_errors) > 0 else 0
        
        bag_mapper = BagTimeMapper(streaming_data.bag_infos)
        
        # 添加结果
        weaving_result = KPIResult(
            name="画龙次数",
            value=len(weaving_events),
            unit="次",
            description=f"方向盘频繁左右摆动（{self.window_duration}s内过零≥{self.min_zero_crossings}次，幅度≥{self.steering_rate_threshold}°/s，转角振幅<{self.max_steering_amplitude}°）",
            details={
                'steering_rate_threshold': self.steering_rate_threshold,
                'max_steering_amplitude': self.max_steering_amplitude,
                'window_duration': self.window_duration,
                'min_zero_crossings': self.min_zero_crossings,
                'min_duration': self.min_duration
            }
        )
        
        for i, e in enumerate(weaving_events):
            weaving_result.add_anomaly(
                timestamp=e.start_time,
                bag_name=bag_mapper.get_bag_name(e.start_time),
                description=(f"画龙 #{i+1}：持续 {e.duration:.2f}s，"
                           f"RMS {e.max_rms:.1f}°/s，"
                           f"峰值 {e.max_steering_rate:.1f}°/s，"
                           f"振荡 {e.oscillation_count} 次"),
                value=e.max_rms,
                threshold=self.steering_rate_threshold
            )
        self.add_result(weaving_result)
        
        # 获取自动驾驶里程
        auto_mileage_km = kwargs.get('auto_mileage_km', 0)
        
        weaving_count = len(weaving_events)
        weaving_per_100km = (weaving_count / auto_mileage_km * 100) if auto_mileage_km > 0 else 0
        avg_weaving_mileage = (auto_mileage_km / weaving_count) if weaving_count > 0 else float('inf')
        
        self.add_result(KPIResult(
            name="画龙频率",
            value=round(weaving_per_100km, 2),
            unit="次/百公里",
            description="每百公里自动驾驶里程的画龙次数"
        ))
        
        self.add_result(KPIResult(
            name="平均画龙里程",
            value="∞" if weaving_count == 0 else round(avg_weaving_mileage, 3),
            unit="km/次",
            description="平均每多少公里自动驾驶里程画龙一次"
        ))
        
        # 生成画龙轨迹可视化
        output_dir = kwargs.get('output_dir')
        if output_dir and weaving_events:
            self._generate_weaving_map(weaving_events, output_dir)
        
        return self.get_results()
