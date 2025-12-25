"""
地理坐标转换工具
支持UTM坐标和经纬度坐标的转换
"""
import math
from typing import Tuple
from dataclasses import dataclass

try:
    from pyproj import Transformer, CRS
    HAS_PYPROJ = True
except ImportError:
    HAS_PYPROJ = False


@dataclass
class GeoPoint:
    """地理坐标点"""
    latitude: float   # 纬度 (度)
    longitude: float  # 经度 (度)
    altitude: float = 0.0


@dataclass 
class UTMPoint:
    """UTM坐标点"""
    easting: float    # 东向坐标 (米)
    northing: float   # 北向坐标 (米)
    zone: int = 51    # UTM区域 (上海区域为51)
    hemisphere: str = 'N'  # 北半球N, 南半球S


def haversine_distance(lat1: float, lon1: float, 
                       lat2: float, lon2: float) -> float:
    """
    使用Haversine公式计算两个经纬度点之间的距离
    
    Args:
        lat1, lon1: 第一个点的经纬度 (度)
        lat2, lon2: 第二个点的经纬度 (度)
        
    Returns:
        距离 (米)
    """
    R = 6371000  # 地球半径 (米)
    
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    
    a = (math.sin(delta_phi / 2) ** 2 + 
         math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c


class GeoConverter:
    """
    地理坐标转换器
    支持经纬度和UTM坐标的相互转换
    """
    
    def __init__(self, utm_zone: int = 51, hemisphere: str = 'N'):
        """
        初始化转换器
        
        Args:
            utm_zone: UTM区域号 (1-60)，上海为51
            hemisphere: 'N' 北半球, 'S' 南半球
        """
        self.utm_zone = utm_zone
        self.hemisphere = hemisphere
        
        if HAS_PYPROJ:
            # 使用pyproj进行高精度转换
            self.wgs84 = CRS.from_epsg(4326)  # WGS84
            # UTM zone
            if hemisphere == 'N':
                self.utm = CRS.from_epsg(32600 + utm_zone)
            else:
                self.utm = CRS.from_epsg(32700 + utm_zone)
            
            self._to_utm = Transformer.from_crs(self.wgs84, self.utm, always_xy=True)
            self._to_wgs84 = Transformer.from_crs(self.utm, self.wgs84, always_xy=True)
        else:
            print("Warning: pyproj not installed. Geo conversions unavailable.")
            self._to_utm = None
            self._to_wgs84 = None
    
    def latlon_to_utm(self, lat: float, lon: float) -> Tuple[float, float]:
        """
        经纬度转UTM坐标
        
        Args:
            lat: 纬度 (度)
            lon: 经度 (度)
            
        Returns:
            (easting, northing) UTM坐标 (米)
        """
        if self._to_utm is not None:
            easting, northing = self._to_utm.transform(lon, lat)
            return easting, northing
        else:
            raise RuntimeError("pyproj not installed, cannot convert coordinates")
    
    def utm_to_latlon(self, easting: float, northing: float) -> Tuple[float, float]:
        """
        UTM坐标转经纬度
        
        Args:
            easting: 东向坐标 (米)
            northing: 北向坐标 (米)
            
        Returns:
            (latitude, longitude) 经纬度 (度)
        """
        if self._to_wgs84 is not None:
            lon, lat = self._to_wgs84.transform(easting, northing)
            return lat, lon
        else:
            raise RuntimeError("pyproj not installed, cannot convert coordinates")
    
    @staticmethod
    def calculate_distance_meters(lat1: float, lon1: float,
                                   lat2: float, lon2: float) -> float:
        """计算两点间的距离(米)"""
        return haversine_distance(lat1, lon1, lat2, lon2)
