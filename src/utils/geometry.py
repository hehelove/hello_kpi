"""
几何计算工具
用于边界框(BBox)计算、IoU计算等
"""
import math
from typing import Tuple, List, Optional
from dataclasses import dataclass
import numpy as np


@dataclass
class Point:
    """二维点"""
    x: float
    y: float
    
    def to_array(self) -> np.ndarray:
        return np.array([self.x, self.y])


@dataclass
class Rectangle:
    """轴对齐矩形 (用于简化计算)"""
    x_min: float
    y_min: float
    x_max: float
    y_max: float
    
    @property
    def width(self) -> float:
        return self.x_max - self.x_min
    
    @property
    def height(self) -> float:
        return self.y_max - self.y_min
    
    @property
    def area(self) -> float:
        return self.width * self.height
    
    @property
    def center(self) -> Point:
        return Point((self.x_min + self.x_max) / 2, 
                     (self.y_min + self.y_max) / 2)
    
    def contains_point(self, x: float, y: float) -> bool:
        """判断点是否在矩形内"""
        return (self.x_min <= x <= self.x_max and 
                self.y_min <= y <= self.y_max)
    
    def intersects(self, other: 'Rectangle') -> bool:
        """判断是否与另一个矩形相交"""
        return not (self.x_max < other.x_min or 
                    self.x_min > other.x_max or
                    self.y_max < other.y_min or 
                    self.y_min > other.y_max)
    
    def intersection_area(self, other: 'Rectangle') -> float:
        """计算与另一个矩形的交集面积"""
        if not self.intersects(other):
            return 0.0
        
        x_overlap = max(0, min(self.x_max, other.x_max) - max(self.x_min, other.x_min))
        y_overlap = max(0, min(self.y_max, other.y_max) - max(self.y_min, other.y_min))
        
        return x_overlap * y_overlap


@dataclass
class BoundingBox:
    """
    有向边界框 (Oriented Bounding Box)
    """
    center_x: float   # 中心点X
    center_y: float   # 中心点Y
    length: float     # 长度 (沿航向方向)
    width: float      # 宽度 (垂直航向方向)
    yaw: float        # 航向角 (弧度)
    
    def get_corners(self) -> List[Point]:
        """获取四个角点"""
        cos_yaw = math.cos(self.yaw)
        sin_yaw = math.sin(self.yaw)
        
        # 半长和半宽
        half_l = self.length / 2
        half_w = self.width / 2
        
        # 局部坐标系中的四个角
        local_corners = [
            (half_l, half_w),    # 右前
            (half_l, -half_w),   # 左前
            (-half_l, -half_w),  # 左后
            (-half_l, half_w),   # 右后
        ]
        
        # 转换到全局坐标系
        corners = []
        for lx, ly in local_corners:
            gx = self.center_x + lx * cos_yaw - ly * sin_yaw
            gy = self.center_y + lx * sin_yaw + ly * cos_yaw
            corners.append(Point(gx, gy))
        
        return corners
    
    def get_axis_aligned_bounds(self) -> Rectangle:
        """获取轴对齐的外接矩形"""
        corners = self.get_corners()
        x_coords = [c.x for c in corners]
        y_coords = [c.y for c in corners]
        
        return Rectangle(
            x_min=min(x_coords),
            y_min=min(y_coords),
            x_max=max(x_coords),
            y_max=max(y_coords)
        )
    
    def contains_point(self, x: float, y: float) -> bool:
        """判断点是否在边界框内"""
        # 将点转换到边界框的局部坐标系
        dx = x - self.center_x
        dy = y - self.center_y
        
        cos_yaw = math.cos(-self.yaw)
        sin_yaw = math.sin(-self.yaw)
        
        local_x = dx * cos_yaw - dy * sin_yaw
        local_y = dx * sin_yaw + dy * cos_yaw
        
        return (abs(local_x) <= self.length / 2 and 
                abs(local_y) <= self.width / 2)


def compute_iou(box1: BoundingBox, box2: BoundingBox, 
                use_approximate: bool = True) -> float:
    """
    计算两个边界框的IoU (Intersection over Union)
    
    Args:
        box1, box2: 两个边界框
        use_approximate: 是否使用近似计算 (轴对齐外接矩形)
        
    Returns:
        IoU值 [0, 1]
    """
    if use_approximate:
        # 使用轴对齐外接矩形近似计算
        rect1 = box1.get_axis_aligned_bounds()
        rect2 = box2.get_axis_aligned_bounds()
        
        intersection = rect1.intersection_area(rect2)
        union = rect1.area + rect2.area - intersection
        
        return intersection / union if union > 0 else 0.0
    else:
        # 精确计算 (使用多边形交集)
        return _compute_polygon_iou(box1.get_corners(), box2.get_corners())


def _compute_polygon_iou(poly1: List[Point], poly2: List[Point]) -> float:
    """计算两个多边形的IoU (精确计算)"""
    try:
        # 使用Sutherland-Hodgman算法计算多边形交集
        intersection_poly = _polygon_intersection(poly1, poly2)
        
        if not intersection_poly:
            return 0.0
        
        intersection_area = _polygon_area(intersection_poly)
        area1 = _polygon_area(poly1)
        area2 = _polygon_area(poly2)
        union_area = area1 + area2 - intersection_area
        
        return intersection_area / union_area if union_area > 0 else 0.0
    except:
        return 0.0


def _polygon_area(vertices: List[Point]) -> float:
    """计算多边形面积 (Shoelace公式)"""
    n = len(vertices)
    if n < 3:
        return 0.0
    
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += vertices[i].x * vertices[j].y
        area -= vertices[j].x * vertices[i].y
    
    return abs(area) / 2.0


def _polygon_intersection(subject: List[Point], clip: List[Point]) -> List[Point]:
    """
    Sutherland-Hodgman多边形裁剪算法
    计算两个凸多边形的交集
    """
    def inside(p: Point, edge_start: Point, edge_end: Point) -> bool:
        return ((edge_end.x - edge_start.x) * (p.y - edge_start.y) - 
                (edge_end.y - edge_start.y) * (p.x - edge_start.x)) >= 0
    
    def line_intersection(p1: Point, p2: Point, p3: Point, p4: Point) -> Optional[Point]:
        x1, y1 = p1.x, p1.y
        x2, y2 = p2.x, p2.y
        x3, y3 = p3.x, p3.y
        x4, y4 = p4.x, p4.y
        
        denom = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4)
        if abs(denom) < 1e-10:
            return None
        
        t = ((x1 - x3) * (y3 - y4) - (y1 - y3) * (x3 - x4)) / denom
        
        return Point(x1 + t * (x2 - x1), y1 + t * (y2 - y1))
    
    output = list(subject)
    
    for i in range(len(clip)):
        if len(output) == 0:
            return []
        
        input_list = output
        output = []
        
        edge_start = clip[i]
        edge_end = clip[(i + 1) % len(clip)]
        
        for j in range(len(input_list)):
            current = input_list[j]
            previous = input_list[(j - 1) % len(input_list)]
            
            if inside(current, edge_start, edge_end):
                if not inside(previous, edge_start, edge_end):
                    intersection = line_intersection(previous, current, edge_start, edge_end)
                    if intersection:
                        output.append(intersection)
                output.append(current)
            elif inside(previous, edge_start, edge_end):
                intersection = line_intersection(previous, current, edge_start, edge_end)
                if intersection:
                    output.append(intersection)
    
    return output


def check_box_in_roi(box: BoundingBox, roi: Rectangle) -> bool:
    """
    检查边界框是否与ROI相交
    
    Args:
        box: 障碍物边界框
        roi: ROI矩形 (自车局部坐标系)
        
    Returns:
        是否相交
    """
    # 获取box的轴对齐外接矩形
    aabb = box.get_axis_aligned_bounds()
    
    # 检查是否与ROI相交
    return roi.intersects(aabb)


def create_ego_roi(front: float, rear: float, 
                   left: float, right: float) -> Rectangle:
    """
    创建以自车后轴中心为原点的ROI
    
    Args:
        front: 前方距离 (米)
        rear: 后方距离 (米)
        left: 左侧距离 (米)
        right: 右侧距离 (米)
        
    Returns:
        ROI矩形 (自车局部坐标系)
    """
    return Rectangle(
        x_min=-rear,
        y_min=-left,
        x_max=front,
        y_max=right
    )


def create_ego_bbox(front_length: float, rear_length: float,
                    left_width: float, right_width: float) -> BoundingBox:
    """
    创建自车边界框
    
    Args:
        front_length: 前悬+轴距 (米)
        rear_length: 到车尾距离 (米)
        left_width: 到左侧距离 (米)
        right_width: 到右侧距离 (米)
        
    Returns:
        自车边界框 (后轴中心为原点)
    """
    total_length = front_length + rear_length
    total_width = left_width + right_width
    
    # 中心点相对于后轴中心的偏移
    center_x = (front_length - rear_length) / 2
    center_y = (right_width - left_width) / 2  # 如果左右对称则为0
    
    return BoundingBox(
        center_x=center_x,
        center_y=center_y,
        length=total_length,
        width=total_width,
        yaw=0.0
    )

