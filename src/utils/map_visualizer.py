"""
地图可视化工具
使用 Mapbox 在地图上显示 refline 轨迹点
"""
import os
import json
from typing import List, Dict, Any
from dataclasses import dataclass


@dataclass
class TrajectoryPoint:
    """轨迹点"""
    lat: float
    lon: float
    kappa: float = 0.0
    timestamp: float = 0.0


@dataclass
class MatchedPairViz:
    """可视化用匹配点对"""
    ego_lat: float
    ego_lon: float
    refline_lat: float
    refline_lon: float
    distance: float
    kappa: float
    timestamp: float = 0.0


class MapVisualizer:
    """地图可视化器"""
    
    # Mapbox token 占位符，用户需要替换为自己的 token
    MAPBOX_TOKEN_PLACEHOLDER = "YOUR_MAPBOX_TOKEN_HERE"
    
    def __init__(self, mapbox_token: str = None):
        """
        初始化地图可视化器
        
        Args:
            mapbox_token: Mapbox access token，如果不提供则使用占位符
        """
        self.mapbox_token = mapbox_token or self.MAPBOX_TOKEN_PLACEHOLDER
    
    def generate_refline_map(self, 
                             points: List[TrajectoryPoint],
                             output_path: str,
                             title: str = "Refline 轨迹可视化",
                             curvature_threshold: float = 0.002) -> str:
        """
        生成包含 refline 匹配点的 HTML 地图文件
        
        Args:
            points: 轨迹点列表 (需要是 WGS84 坐标)
            output_path: 输出 HTML 文件路径
            title: 地图标题
            curvature_threshold: 曲率阈值，用于区分直道和弯道
            
        Returns:
            生成的 HTML 文件路径
        """
        if not points:
            return None
        
        # 以第一个点为中心
        center_lat = points[0].lat
        center_lon = points[0].lon
        
        # 根据 kappa 值分段生成 GeoJSON（区分直道和弯道）
        straight_segments, curve_segments = self._segment_by_curvature(points, curvature_threshold)
        
        # 生成 HTML
        html_content = self._generate_html(
            center_lat=center_lat,
            center_lon=center_lon,
            straight_segments=straight_segments,
            curve_segments=curve_segments,
            title=title,
            curvature_threshold=curvature_threshold
        )
        
        # 确保输出目录存在
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return output_path
    
    def _points_to_geojson_points(self, points: List[TrajectoryPoint], point_type: str) -> List[Dict]:
        """将轨迹点转换为 GeoJSON Point 列表"""
        features = []
        for p in points:
            features.append({
            "type": "Feature",
            "properties": {
                    "type": point_type,
                    "kappa": p.kappa
            },
            "geometry": {
                    "type": "Point",
                    "coordinates": [p.lon, p.lat]
            }
            })
        return features
    
    def generate_matched_pairs_map(self,
                                   matched_pairs: List[Any],
                                   output_path: str,
                                   title: str = "Refline 匹配点可视化",
                                   curvature_threshold: float = 0.002) -> str:
        """
        生成包含自车位置和匹配 refline 点的 HTML 地图文件
        
        Args:
            matched_pairs: 匹配点对列表 (需要有 ego_lat, ego_lon, refline_lat, refline_lon, kappa, distance)
            output_path: 输出 HTML 文件路径
            title: 地图标题
            curvature_threshold: 曲率阈值，用于区分直道和弯道
            
        Returns:
            生成的 HTML 文件路径
        """
        if not matched_pairs:
            return None
        
        # 以第一个自车位置为中心
        center_lat = matched_pairs[0].ego_lat
        center_lon = matched_pairs[0].ego_lon
        
        # 构建 GeoJSON 数据
        ego_features = []  # 自车位置点
        refline_straight_features = []  # 直道 refline 点
        refline_curve_features = []  # 弯道 refline 点
        connection_features = []  # 连接线（自车到最近点）
        
        for i, pair in enumerate(matched_pairs):
            # 自车位置点 (蓝色)
            ego_features.append({
                "type": "Feature",
                "properties": {
                    "type": "ego",
                    "index": i,
                    "distance": round(pair.distance, 3),
                    "kappa": round(pair.kappa, 6)
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [pair.ego_lon, pair.ego_lat]
                }
            })
            
            # refline 最近点 (根据曲率分类颜色)
            is_straight = abs(pair.kappa) < curvature_threshold
            refline_feature = {
                "type": "Feature",
                "properties": {
                    "type": "straight" if is_straight else "curve",
                    "index": i,
                    "kappa": round(pair.kappa, 6),
                    "distance": round(pair.distance, 3)
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [pair.refline_lon, pair.refline_lat]
                }
            }
            
            if is_straight:
                refline_straight_features.append(refline_feature)
            else:
                refline_curve_features.append(refline_feature)
            
            # 连接线 (自车到最近点) - 只在距离较大时显示
            if pair.distance > 0.1:  # 距离大于 0.1m 才显示连接线
                connection_features.append({
                    "type": "Feature",
                    "properties": {
                        "distance": round(pair.distance, 3)
                    },
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [
                            [pair.ego_lon, pair.ego_lat],
                            [pair.refline_lon, pair.refline_lat]
                        ]
                    }
                })
        
        # 创建 GeoJSON FeatureCollections
        ego_collection = {"type": "FeatureCollection", "features": ego_features}
        straight_collection = {"type": "FeatureCollection", "features": refline_straight_features}
        curve_collection = {"type": "FeatureCollection", "features": refline_curve_features}
        connection_collection = {"type": "FeatureCollection", "features": connection_features}
        
        # 生成 HTML
        html_content = self._generate_matched_pairs_html(
            center_lat=center_lat,
            center_lon=center_lon,
            ego_json=json.dumps(ego_collection),
            straight_json=json.dumps(straight_collection),
            curve_json=json.dumps(curve_collection),
            connection_json=json.dumps(connection_collection),
            title=title,
            curvature_threshold=curvature_threshold,
            total_pairs=len(matched_pairs),
            straight_count=len(refline_straight_features),
            curve_count=len(refline_curve_features)
        )
        
        # 确保输出目录存在
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return output_path
    
    def _generate_matched_pairs_html(self,
                                     center_lat: float,
                                     center_lon: float,
                                     ego_json: str,
                                     straight_json: str,
                                     curve_json: str,
                                     connection_json: str,
                                     title: str,
                                     curvature_threshold: float,
                                     total_pairs: int,
                                     straight_count: int,
                                     curve_count: int) -> str:
        """生成匹配点对的 HTML 页面"""
        
        html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>{title}</title>
    <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no">
    <link href="https://api.mapbox.com/mapbox-gl-js/v3.0.1/mapbox-gl.css" rel="stylesheet">
    <script src="https://api.mapbox.com/mapbox-gl-js/v3.0.1/mapbox-gl.js"></script>
    <style>
        body {{ margin: 0; padding: 0; }}
        #map {{ position: absolute; top: 0; bottom: 0; width: 100%; }}
        .legend {{
            position: absolute;
            bottom: 30px;
            left: 10px;
            background: rgba(255, 255, 255, 0.95);
            padding: 12px 15px;
            border-radius: 8px;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            font-size: 13px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
        }}
        .legend-title {{
            font-weight: 600;
            margin-bottom: 8px;
            padding-bottom: 6px;
            border-bottom: 1px solid #eee;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            margin: 6px 0;
        }}
        .legend-dot {{
            width: 12px;
            height: 12px;
            margin-right: 10px;
            border-radius: 50%;
            border: 2px solid rgba(0,0,0,0.3);
        }}
        .legend-line {{
            width: 20px;
            height: 2px;
            margin-right: 10px;
            background: #888;
        }}
        .info-panel {{
            position: absolute;
            top: 10px;
            left: 10px;
            background: rgba(255, 255, 255, 0.95);
            padding: 12px 15px;
            border-radius: 8px;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            font-size: 13px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
            max-width: 300px;
        }}
        .info-panel strong {{
            display: block;
            margin-bottom: 8px;
            font-size: 14px;
        }}
        .info-row {{
            display: flex;
            justify-content: space-between;
            margin: 4px 0;
        }}
        .info-label {{ color: #666; }}
        .info-value {{ font-weight: 500; }}
        .layer-toggle {{
            position: absolute;
            top: 10px;
            right: 10px;
            background: rgba(255, 255, 255, 0.95);
            padding: 12px 15px;
            border-radius: 8px;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            font-size: 13px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
        }}
        .layer-toggle label {{
            display: flex;
            align-items: center;
            margin: 6px 0;
            cursor: pointer;
        }}
        .layer-toggle input {{
            margin-right: 8px;
        }}
        .mapboxgl-popup-content {{
            padding: 10px 15px;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            font-size: 13px;
        }}
    </style>
</head>
<body>
<div id="map"></div>

<div class="info-panel">
    <strong>{title}</strong>
    <div class="info-row">
        <span class="info-label">总采样点:</span>
        <span class="info-value">{total_pairs}</span>
    </div>
    <div class="info-row">
        <span class="info-label">直道点:</span>
        <span class="info-value">{straight_count} ({straight_count*100//total_pairs if total_pairs > 0 else 0}%)</span>
    </div>
    <div class="info-row">
        <span class="info-label">弯道点:</span>
        <span class="info-value">{curve_count} ({curve_count*100//total_pairs if total_pairs > 0 else 0}%)</span>
    </div>
    <div class="info-row">
        <span class="info-label">曲率阈值:</span>
        <span class="info-value">{curvature_threshold}</span>
    </div>
</div>

<div class="layer-toggle">
    <strong style="margin-bottom: 8px; display: block;">图层控制</strong>
    <label><input type="checkbox" id="toggle-ego" checked> 自车位置 (蓝)</label>
    <label><input type="checkbox" id="toggle-refline" checked> Refline 点</label>
    <label><input type="checkbox" id="toggle-connection" checked> 连接线</label>
</div>

<div class="legend">
    <div class="legend-title">图例说明</div>
    <div class="legend-item">
        <div class="legend-dot" style="background: #3b82f6;"></div>
        <span>自车位置</span>
    </div>
    <div class="legend-item">
        <div class="legend-dot" style="background: #22c55e;"></div>
        <span>Refline 直道 (κ &lt; {curvature_threshold})</span>
    </div>
    <div class="legend-item">
        <div class="legend-dot" style="background: #ef4444;"></div>
        <span>Refline 弯道 (κ ≥ {curvature_threshold})</span>
    </div>
    <div class="legend-item">
        <div class="legend-line" style="background: #888; border-style: dashed;"></div>
        <span>匹配连接 (距离 &gt; 0.1m)</span>
    </div>
</div>

<script>
    mapboxgl.accessToken = '{self.mapbox_token}';
    
    const egoData = {ego_json};
    const straightData = {straight_json};
    const curveData = {curve_json};
    const connectionData = {connection_json};
    
    const map = new mapboxgl.Map({{
        container: 'map',
        style: 'mapbox://styles/mapbox/dark-v11',
        center: [{center_lon}, {center_lat}],
        zoom: 17
    }});
    
    map.addControl(new mapboxgl.NavigationControl());
    map.addControl(new mapboxgl.ScaleControl());
    
    map.on('load', () => {{
        // 添加连接线
        map.addSource('connections', {{ 'type': 'geojson', 'data': connectionData }});
        map.addLayer({{
            'id': 'connection-lines',
            'type': 'line',
            'source': 'connections',
            'paint': {{
                'line-color': '#888',
                'line-width': 1,
                'line-opacity': 0.5,
                'line-dasharray': [2, 2]
            }}
        }});
        
        // 添加直道 refline 点 (绿色)
        map.addSource('straight', {{ 'type': 'geojson', 'data': straightData }});
        map.addLayer({{
            'id': 'straight-points',
            'type': 'circle',
            'source': 'straight',
            'paint': {{
                'circle-radius': 5,
                'circle-color': '#22c55e',
                'circle-opacity': 0.9,
                'circle-stroke-width': 1,
                'circle-stroke-color': '#166534'
            }}
        }});
        
        // 添加弯道 refline 点 (红色)
        map.addSource('curve', {{ 'type': 'geojson', 'data': curveData }});
        map.addLayer({{
            'id': 'curve-points',
            'type': 'circle',
            'source': 'curve',
            'paint': {{
                'circle-radius': 5,
                'circle-color': '#ef4444',
                'circle-opacity': 0.9,
                'circle-stroke-width': 1,
                'circle-stroke-color': '#991b1b'
            }}
        }});
        
        // 添加自车位置点 (蓝色)
        map.addSource('ego', {{ 'type': 'geojson', 'data': egoData }});
        map.addLayer({{
            'id': 'ego-points',
            'type': 'circle',
            'source': 'ego',
            'paint': {{
                'circle-radius': 4,
                'circle-color': '#3b82f6',
                'circle-opacity': 0.9,
                'circle-stroke-width': 1,
                'circle-stroke-color': '#1d4ed8'
            }}
        }});
        
        // 自动适配视野
        const bounds = new mapboxgl.LngLatBounds();
        egoData.features.forEach(f => bounds.extend(f.geometry.coordinates));
        straightData.features.forEach(f => bounds.extend(f.geometry.coordinates));
        curveData.features.forEach(f => bounds.extend(f.geometry.coordinates));
        map.fitBounds(bounds, {{ padding: 50 }});
        
        // 添加起点/终点标记
        if (egoData.features.length > 0) {{
            new mapboxgl.Marker({{ color: '#22c55e' }})
                .setLngLat(egoData.features[0].geometry.coordinates)
                .setPopup(new mapboxgl.Popup().setHTML('<b>起点</b>'))
                .addTo(map);
            
            new mapboxgl.Marker({{ color: '#ef4444' }})
                .setLngLat(egoData.features[egoData.features.length - 1].geometry.coordinates)
                .setPopup(new mapboxgl.Popup().setHTML('<b>终点</b>'))
                .addTo(map);
        }}
        
        // 点击弹窗
        ['ego-points', 'straight-points', 'curve-points'].forEach(layerId => {{
            map.on('click', layerId, (e) => {{
                const props = e.features[0].properties;
                const coords = e.features[0].geometry.coordinates;
                let content = '';
                
                if (layerId === 'ego-points') {{
                    content = `<b>自车位置 #${{props.index}}</b><br>
                               距离最近点: ${{props.distance}}m<br>
                               匹配点曲率: ${{props.kappa}}`;
                }} else {{
                    const type = layerId === 'straight-points' ? '直道' : '弯道';
                    content = `<b>Refline ${{type}}点 #${{props.index}}</b><br>
                               曲率: ${{props.kappa}}<br>
                               距自车: ${{props.distance}}m`;
                }}
                
                new mapboxgl.Popup()
                    .setLngLat(coords)
                    .setHTML(content)
                    .addTo(map);
            }});
            
            map.on('mouseenter', layerId, () => {{ map.getCanvas().style.cursor = 'pointer'; }});
            map.on('mouseleave', layerId, () => {{ map.getCanvas().style.cursor = ''; }});
        }});
    }});
    
    // 图层切换
    document.getElementById('toggle-ego').addEventListener('change', (e) => {{
        map.setLayoutProperty('ego-points', 'visibility', e.target.checked ? 'visible' : 'none');
    }});
    document.getElementById('toggle-refline').addEventListener('change', (e) => {{
        const vis = e.target.checked ? 'visible' : 'none';
        map.setLayoutProperty('straight-points', 'visibility', vis);
        map.setLayoutProperty('curve-points', 'visibility', vis);
    }});
    document.getElementById('toggle-connection').addEventListener('change', (e) => {{
        map.setLayoutProperty('connection-lines', 'visibility', e.target.checked ? 'visible' : 'none');
    }});
</script>
</body>
</html>
'''
        return html
    
    def _segment_by_curvature(self, points: List[TrajectoryPoint], 
                              threshold: float) -> tuple:
        """
        根据曲率阈值将轨迹点分段为直道和弯道
        
        Args:
            points: 轨迹点列表
            threshold: 曲率阈值
            
        Returns:
            (straight_segments, curve_segments) - 直道和弯道的线段列表
        """
        straight_segments = []
        curve_segments = []
        
        current_straight = []
        current_curve = []
        
        for point in points:
            is_straight = abs(point.kappa) < threshold
            
            if is_straight:
                # 直道点
                current_straight.append(point)
                # 如果之前是弯道，结束弯道段
                if current_curve:
                    if len(current_curve) > 1:
                        curve_segments.append(current_curve)
                    current_curve = []
            else:
                # 弯道点
                current_curve.append(point)
                # 如果之前是直道，结束直道段
                if current_straight:
                    if len(current_straight) > 1:
                        straight_segments.append(current_straight)
                    current_straight = []
        
        # 处理最后一段
        if current_straight and len(current_straight) > 1:
            straight_segments.append(current_straight)
        if current_curve and len(current_curve) > 1:
            curve_segments.append(current_curve)
        
        return straight_segments, curve_segments
    
    def _generate_html(self, 
                       center_lat: float, 
                       center_lon: float,
                       straight_segments: List[List[TrajectoryPoint]],
                       curve_segments: List[List[TrajectoryPoint]],
                       title: str,
                       curvature_threshold: float) -> str:
        """生成 Mapbox HTML 页面"""
        
        # 将直道和弯道点转换为 GeoJSON Point（不再使用 LineString）
        straight_features = []
        for segment in straight_segments:
            straight_features.extend(self._points_to_geojson_points(segment, "straight"))
        
        curve_features = []
        for segment in curve_segments:
            curve_features.extend(self._points_to_geojson_points(segment, "curve"))
        
        # 创建 FeatureCollection
        straight_collection = {
            "type": "FeatureCollection",
            "features": straight_features
        }
        curve_collection = {
            "type": "FeatureCollection",
            "features": curve_features
        }
        
        straight_json = json.dumps(straight_collection)
        curve_json = json.dumps(curve_collection)
        
        html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>{title}</title>
    <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no">
    <link href="https://api.mapbox.com/mapbox-gl-js/v3.0.1/mapbox-gl.css" rel="stylesheet">
    <script src="https://api.mapbox.com/mapbox-gl-js/v3.0.1/mapbox-gl.js"></script>
    <style>
        body {{ margin: 0; padding: 0; }}
        #map {{ position: absolute; top: 0; bottom: 0; width: 100%; }}
        .legend {{
            position: absolute;
            bottom: 30px;
            left: 10px;
            background: rgba(255, 255, 255, 0.9);
            padding: 10px 15px;
            border-radius: 5px;
            font-family: Arial, sans-serif;
            font-size: 14px;
            box-shadow: 0 1px 5px rgba(0,0,0,0.3);
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            margin: 5px 0;
        }}
        .legend-dot {{
            width: 12px;
            height: 12px;
            margin-right: 10px;
            border-radius: 50%;
            border: 1px solid #333;
        }}
        .info-panel {{
            position: absolute;
            top: 10px;
            left: 10px;
            background: rgba(255, 255, 255, 0.9);
            padding: 10px 15px;
            border-radius: 5px;
            font-family: Arial, sans-serif;
            font-size: 14px;
            box-shadow: 0 1px 5px rgba(0,0,0,0.3);
        }}
    </style>
</head>
<body>
<div id="map"></div>
<div class="info-panel">
    <strong>{title}</strong><br>
    <span id="point-count">加载中...</span>
</div>
<div class="legend">
    <div class="legend-item">
        <div class="legend-dot" style="background: #00ff00;"></div>
        <span>直道 (κ &lt; {curvature_threshold})</span>
    </div>
    <div class="legend-item">
        <div class="legend-dot" style="background: #ff0000;"></div>
        <span>弯道 (κ ≥ {curvature_threshold})</span>
    </div>
</div>

<script>
    mapboxgl.accessToken = '{self.mapbox_token}';
    
    const straightData = {straight_json};
    const curveData = {curve_json};
    
    const map = new mapboxgl.Map({{
        container: 'map',
        style: 'mapbox://styles/mapbox/streets-v12',
        center: [{center_lon}, {center_lat}],
        zoom: 16
    }});
    
    map.addControl(new mapboxgl.NavigationControl());
    map.addControl(new mapboxgl.ScaleControl());
    
    map.on('load', () => {{
        // 添加直道点
        if (straightData.features.length > 0) {{
            map.addSource('straight', {{
                'type': 'geojson',
                'data': straightData
            }});
            
            map.addLayer({{
                'id': 'straight-points',
                'type': 'circle',
                'source': 'straight',
                'paint': {{
                    'circle-radius': 4,
                    'circle-color': '#00ff00',
                    'circle-opacity': 0.8,
                    'circle-stroke-width': 1,
                    'circle-stroke-color': '#006600'
                }}
            }});
        }}
        
        // 添加弯道点
        if (curveData.features.length > 0) {{
            map.addSource('curve', {{
                'type': 'geojson',
                'data': curveData
            }});
            
            map.addLayer({{
                'id': 'curve-points',
                'type': 'circle',
                'source': 'curve',
                'paint': {{
                    'circle-radius': 4,
                    'circle-color': '#ff0000',
                    'circle-opacity': 0.8,
                    'circle-stroke-width': 1,
                    'circle-stroke-color': '#660000'
                }}
            }});
        }}
        
        // 收集所有坐标点用于标记和视野适配
        const allCoords = [];
        straightData.features.forEach(feature => {{
            allCoords.push(feature.geometry.coordinates);
        }});
        curveData.features.forEach(feature => {{
            allCoords.push(feature.geometry.coordinates);
        }});
        
        if (allCoords.length > 0) {{
            // 添加起点标记
            const startCoord = allCoords[0];
            new mapboxgl.Marker({{ color: '#00ff00' }})
                .setLngLat(startCoord)
                .setPopup(new mapboxgl.Popup().setHTML('<b>起点</b>'))
                .addTo(map);
            
            // 添加终点标记
            const endCoord = allCoords[allCoords.length - 1];
            new mapboxgl.Marker({{ color: '#ff0000' }})
                .setLngLat(endCoord)
                .setPopup(new mapboxgl.Popup().setHTML('<b>终点</b>'))
                .addTo(map);
            
            // 更新点数信息
            const straightCount = straightData.features.length;
            const curveCount = curveData.features.length;
            document.getElementById('point-count').textContent = 
                `总点数: ${{allCoords.length}} (直道: ${{straightCount}}, 弯道: ${{curveCount}})`;
            
            // 自动适配视野
            const bounds = new mapboxgl.LngLatBounds();
            allCoords.forEach(coord => {{
                bounds.extend(coord);
            }});
            map.fitBounds(bounds, {{ padding: 50 }});
        }}
    }});
</script>
</body>
</html>
'''
        return html
