"""
KPI计算模块测试
"""
import pytest
import numpy as np
import sys
from pathlib import Path

# 添加项目根目录
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.geo import haversine_distance, GeoConverter
from src.utils.geometry import BoundingBox, Rectangle, compute_iou, create_ego_roi
from src.utils.signal import SignalProcessor, CurvatureEstimator


class TestGeoUtils:
    """地理工具测试"""
    
    def test_haversine_distance(self):
        """测试Haversine距离计算"""
        # 北京到上海约1000km
        lat1, lon1 = 39.9042, 116.4074  # 北京
        lat2, lon2 = 31.2304, 121.4737  # 上海
        
        distance = haversine_distance(lat1, lon1, lat2, lon2)
        
        # 距离应该在1000-1100km之间
        assert 1000000 < distance < 1100000
    
    def test_geo_converter_utm(self):
        """测试UTM转换"""
        converter = GeoConverter(utm_zone=50, hemisphere='N')
        
        lat, lon = 39.9042, 116.4074
        
        # 转换到UTM
        easting, northing = converter.latlon_to_utm(lat, lon)
        
        # 反向转换
        lat2, lon2 = converter.utm_to_latlon(easting, northing)
        
        # 误差应该很小
        assert abs(lat - lat2) < 0.0001
        assert abs(lon - lon2) < 0.0001


class TestGeometry:
    """几何计算测试"""
    
    def test_rectangle_intersection(self):
        """测试矩形相交"""
        rect1 = Rectangle(0, 0, 10, 10)
        rect2 = Rectangle(5, 5, 15, 15)
        rect3 = Rectangle(20, 20, 30, 30)
        
        assert rect1.intersects(rect2)
        assert not rect1.intersects(rect3)
        
        # 交集面积
        area = rect1.intersection_area(rect2)
        assert area == 25  # 5x5
    
    def test_bounding_box_corners(self):
        """测试边界框角点"""
        box = BoundingBox(
            center_x=0, center_y=0,
            length=4, width=2,
            yaw=0
        )
        
        corners = box.get_corners()
        assert len(corners) == 4
        
        # 检查角点坐标
        x_coords = sorted([c.x for c in corners])
        y_coords = sorted([c.y for c in corners])
        
        assert x_coords == [-2, -2, 2, 2]
        assert y_coords == [-1, -1, 1, 1]
    
    def test_iou_calculation(self):
        """测试IoU计算"""
        box1 = BoundingBox(0, 0, 10, 10, 0)
        box2 = BoundingBox(5, 0, 10, 10, 0)
        
        iou = compute_iou(box1, box2, use_approximate=True)
        
        # IoU应该是0.5左右 (一半重叠)
        assert 0.3 < iou < 0.7
    
    def test_ego_roi_creation(self):
        """测试ROI创建"""
        roi = create_ego_roi(front=15, rear=3, left=3, right=3)
        
        assert roi.x_min == -3
        assert roi.x_max == 15
        assert roi.y_min == -3
        assert roi.y_max == 3


class TestSignalProcessing:
    """信号处理测试"""
    
    def test_derivative(self):
        """测试导数计算"""
        timestamps = np.array([0, 1, 2, 3, 4])
        values = np.array([0, 1, 4, 9, 16])  # v = t^2
        
        ts, derivs = SignalProcessor.compute_derivative(timestamps, values)
        
        # 导数应该约等于 2t
        assert len(derivs) == 4
        # 检查中间点的导数 (t=1.5时应该约等于3)
        assert abs(derivs[1] - 3) < 1
    
    def test_jerk_calculation(self):
        """测试Jerk计算"""
        timestamps = np.array([0, 0.1, 0.2, 0.3, 0.4])
        accelerations = np.array([0, 1, 2, 3, 4])  # 线性加速度
        
        ts, jerks = SignalProcessor.compute_jerk(timestamps, accelerations)
        
        # Jerk应该是常数10
        assert len(jerks) > 0
        assert all(abs(j - 10) < 1 for j in jerks)
    
    def test_threshold_event_detection(self):
        """测试阈值事件检测"""
        timestamps = np.array([0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6])
        values = np.array([0, 1, 5, 6, 5, 1, 0])
        
        events = SignalProcessor.detect_threshold_events(
            timestamps, values,
            threshold=4,
            min_duration=0.1,
            compare='greater'
        )
        
        assert len(events) == 1
        assert events[0].peak_value == 6
    
    def test_statistics(self):
        """测试统计计算"""
        values = np.array([1, 2, 3, 4, 5])
        
        stats = SignalProcessor.compute_statistics(values)
        
        assert stats['mean'] == 3
        assert stats['min'] == 1
        assert stats['max'] == 5
        assert abs(stats['std'] - np.std(values)) < 0.001


class TestCurvatureEstimator:
    """曲率估计测试"""
    
    def test_curvature_estimation(self):
        """测试曲率估计"""
        estimator = CurvatureEstimator()
        
        # 直行 (转角为0)
        curvature = estimator.estimate_road_curvature(0)
        assert abs(curvature) < 0.001
        
        # 转弯 (转角为180度)
        curvature = estimator.estimate_road_curvature(180)
        assert abs(curvature) > 0.01
    
    def test_road_type_classification(self):
        """测试道路类型分类"""
        estimator = CurvatureEstimator()
        
        # 小曲率 = 直道
        assert estimator.classify_road_type(0.001) == 'straight'
        
        # 大曲率 = 弯道
        assert estimator.classify_road_type(0.01) == 'curve'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

