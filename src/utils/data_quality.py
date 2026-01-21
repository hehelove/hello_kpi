"""
数据质量监控工具

提供以下功能：
1. Topic 完整性检查
2. 时间同步质量监控
3. Bag 时间间隔检测
4. 采样率检查与告警
5. 配置参数校验
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
import numpy as np
import yaml
import hashlib
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# 数据类
# ============================================================================

@dataclass
class TopicCoverage:
    """Topic 覆盖率统计"""
    topic_name: str
    total_frames: int = 0
    matched_frames: int = 0
    coverage_ratio: float = 0.0  # 0.0 ~ 1.0
    
    def to_dict(self) -> Dict:
        return {
            'topic': self.topic_name,
            'total_frames': self.total_frames,
            'matched_frames': self.matched_frames,
            'coverage_ratio': round(self.coverage_ratio * 100, 2)  # 百分比
        }


@dataclass
class BagTimeGap:
    """Bag 时间间隔"""
    before_bag: str
    after_bag: str
    gap_seconds: float
    before_end_time: float
    after_start_time: float
    
    def to_dict(self) -> Dict:
        return {
            'before_bag': self.before_bag,
            'after_bag': self.after_bag,
            'gap_seconds': round(self.gap_seconds, 3),
            'before_end_time': self.before_end_time,
            'after_start_time': self.after_start_time
        }


@dataclass
class SamplingRateInfo:
    """采样率信息"""
    topic_name: str
    expected_rate: float  # 期望采样率 (Hz)
    actual_rate: float    # 实际采样率 (Hz)
    is_sufficient: bool   # 是否足够
    warning: Optional[str] = None
    
    def to_dict(self) -> Dict:
        result = {
            'topic': self.topic_name,
            'expected_rate_hz': self.expected_rate,
            'actual_rate_hz': round(self.actual_rate, 2),
            'is_sufficient': self.is_sufficient
        }
        if self.warning:
            result['warning'] = self.warning
        return result


@dataclass
class DataQualityReport:
    """数据质量报告"""
    # Topic 完整性
    missing_topics: List[str] = field(default_factory=list)
    available_topics: List[str] = field(default_factory=list)
    
    # 时间同步覆盖率
    topic_coverage: List[TopicCoverage] = field(default_factory=list)
    low_coverage_topics: List[str] = field(default_factory=list)  # 覆盖率 < 80%
    
    # Bag 时间间隔
    time_gaps: List[BagTimeGap] = field(default_factory=list)
    has_significant_gaps: bool = False  # 是否有 > 1s 的间隔
    
    # 采样率
    sampling_rates: List[SamplingRateInfo] = field(default_factory=list)
    low_sampling_warnings: List[str] = field(default_factory=list)
    
    # 配置校验
    config_warnings: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return {
            'missing_topics': self.missing_topics,
            'available_topics': self.available_topics,
            'topic_coverage': [tc.to_dict() for tc in self.topic_coverage],
            'low_coverage_topics': self.low_coverage_topics,
            'time_gaps': [tg.to_dict() for tg in self.time_gaps],
            'has_significant_gaps': self.has_significant_gaps,
            'sampling_rates': [sr.to_dict() for sr in self.sampling_rates],
            'low_sampling_warnings': self.low_sampling_warnings,
            'config_warnings': self.config_warnings
        }
    
    def print_summary(self, verbose: bool = False):
        """打印数据质量摘要"""
        print(f"\n{'='*60}")
        print("数据质量报告")
        print(f"{'='*60}")
        
        # Topic 完整性
        if self.missing_topics:
            print(f"\n[⚠️ 缺失 Topic] ({len(self.missing_topics)} 个)")
            for topic in self.missing_topics:
                print(f"    - {topic}")
        else:
            print(f"\n[✓ Topic 完整] 所有必需 Topic 均存在")
        
        # 覆盖率
        if self.low_coverage_topics:
            print(f"\n[⚠️ 低覆盖率 Topic] ({len(self.low_coverage_topics)} 个)")
            for tc in self.topic_coverage:
                if tc.topic_name in self.low_coverage_topics:
                    print(f"    - {tc.topic_name}: {tc.coverage_ratio*100:.1f}%")
        elif verbose:
            print(f"\n[✓ 同步覆盖率正常]")
            for tc in self.topic_coverage:
                print(f"    - {tc.topic_name}: {tc.coverage_ratio*100:.1f}%")
        
        # 时间间隔
        if self.has_significant_gaps:
            print(f"\n[⚠️ Bag 时间间隔] ({len(self.time_gaps)} 处)")
            for gap in self.time_gaps:
                if gap.gap_seconds > 1.0:
                    print(f"    - {gap.before_bag} → {gap.after_bag}: {gap.gap_seconds:.2f}s")
        elif verbose and self.time_gaps:
            print(f"\n[✓ Bag 时间连续] (最大间隔 {max(g.gap_seconds for g in self.time_gaps):.2f}s)")
        
        # 采样率
        if self.low_sampling_warnings:
            print(f"\n[⚠️ 采样率不足] ({len(self.low_sampling_warnings)} 个)")
            for warning in self.low_sampling_warnings:
                print(f"    - {warning}")
        elif verbose:
            print(f"\n[✓ 采样率正常]")
        
        # 配置警告
        if self.config_warnings:
            print(f"\n[⚠️ 配置警告] ({len(self.config_warnings)} 个)")
            for warning in self.config_warnings:
                print(f"    - {warning}")
        
        print(f"\n{'='*60}\n")


# ============================================================================
# 数据质量检查器
# ============================================================================

class DataQualityChecker:
    """数据质量检查器"""
    
    # Topic 期望采样率 (Hz)
    EXPECTED_RATES = {
        '/function/function_manager': 10.0,
        '/localization/localization': 50.0,
        '/vehicle/chassis_domain_report': 50.0,
        '/control/control': 50.0,
        '/control/debug': 50.0,
        '/perception/fusion/obstacle_list_utm': 10.0,
        '/planning/trajectory': 10.0,
    }
    
    # 高频 KPI 最低采样率要求 (Hz)
    MIN_HIGHFREQ_RATE = 20.0
    
    # 覆盖率告警阈值
    LOW_COVERAGE_THRESHOLD = 0.8  # 80%
    
    # 时间间隔告警阈值 (秒)
    SIGNIFICANT_GAP_THRESHOLD = 1.0
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.report = DataQualityReport()
    
    def check_topic_completeness(
        self, 
        required_topics: List[str], 
        available_topics: List[str]
    ) -> Tuple[List[str], List[str]]:
        """
        检查 Topic 完整性
        
        Args:
            required_topics: 必需的 Topic 列表
            available_topics: 可用的 Topic 列表
            
        Returns:
            (missing_topics, matched_topics)
        """
        available_set = set(available_topics)
        missing = [t for t in required_topics if t not in available_set]
        matched = [t for t in required_topics if t in available_set]
        
        self.report.missing_topics = missing
        self.report.available_topics = matched
        
        if missing:
            logger.warning(f"缺失 Topic: {missing}")
        
        return missing, matched
    
    def check_sync_coverage(
        self, 
        synced_frames: List,
        topics: List[str]
    ) -> List[TopicCoverage]:
        """
        检查时间同步覆盖率
        
        Args:
            synced_frames: 同步帧列表
            topics: 要检查的 Topic 列表
            
        Returns:
            各 Topic 的覆盖率统计
        """
        total_frames = len(synced_frames)
        if total_frames == 0:
            return []
        
        coverage_list = []
        low_coverage = []
        
        for topic in topics:
            matched = sum(1 for f in synced_frames if f.messages.get(topic) is not None)
            ratio = matched / total_frames
            
            coverage = TopicCoverage(
                topic_name=topic,
                total_frames=total_frames,
                matched_frames=matched,
                coverage_ratio=ratio
            )
            coverage_list.append(coverage)
            
            if ratio < self.LOW_COVERAGE_THRESHOLD:
                low_coverage.append(topic)
                logger.warning(f"Topic {topic} 覆盖率低: {ratio*100:.1f}%")
        
        self.report.topic_coverage = coverage_list
        self.report.low_coverage_topics = low_coverage
        
        return coverage_list
    
    def check_bag_time_gaps(self, bag_infos: List) -> List[BagTimeGap]:
        """
        检查 Bag 时间间隔
        
        Args:
            bag_infos: BagInfo 列表，每个包含 path, start_time, end_time
            
        Returns:
            时间间隔列表
        """
        if len(bag_infos) < 2:
            return []
        
        # 按开始时间排序
        sorted_infos = sorted(bag_infos, key=lambda x: x.start_time)
        
        gaps = []
        has_significant = False
        
        for i in range(len(sorted_infos) - 1):
            current = sorted_infos[i]
            next_bag = sorted_infos[i + 1]
            
            gap_seconds = next_bag.start_time - current.end_time
            
            # 只记录正间隔（负间隔表示重叠）
            if gap_seconds > 0:
                gap = BagTimeGap(
                    before_bag=current.path.name if hasattr(current.path, 'name') else str(current.path),
                    after_bag=next_bag.path.name if hasattr(next_bag.path, 'name') else str(next_bag.path),
                    gap_seconds=gap_seconds,
                    before_end_time=current.end_time,
                    after_start_time=next_bag.start_time
                )
                gaps.append(gap)
                
                if gap_seconds > self.SIGNIFICANT_GAP_THRESHOLD:
                    has_significant = True
                    logger.warning(
                        f"Bag 时间间隔过大: {gap.before_bag} → {gap.after_bag}: {gap_seconds:.2f}s"
                    )
        
        self.report.time_gaps = gaps
        self.report.has_significant_gaps = has_significant
        
        return gaps
    
    def check_sampling_rate(
        self, 
        topic_data: Dict,
        high_freq_topics: Optional[List[str]] = None
    ) -> List[SamplingRateInfo]:
        """
        检查采样率
        
        Args:
            topic_data: topic 数据字典，{topic_name: TopicData}
            high_freq_topics: 高频 KPI 依赖的 topic 列表
            
        Returns:
            采样率信息列表
        """
        if high_freq_topics is None:
            high_freq_topics = [
                '/vehicle/chassis_domain_report',
                '/control/control'
            ]
        
        sampling_rates = []
        warnings = []
        
        for topic_name, topic_data_obj in topic_data.items():
            timestamps = topic_data_obj.timestamps
            if len(timestamps) < 2:
                continue
            
            # 计算实际采样率
            diffs = np.diff(timestamps)
            valid_diffs = diffs[(diffs > 0) & (diffs < 1.0)]  # 过滤异常
            
            if len(valid_diffs) > 0:
                actual_rate = 1.0 / np.median(valid_diffs)
            else:
                actual_rate = 0.0
            
            expected_rate = self.EXPECTED_RATES.get(topic_name, 10.0)
            
            # 高频 Topic 特殊检查
            is_highfreq = topic_name in high_freq_topics
            min_required = self.MIN_HIGHFREQ_RATE if is_highfreq else expected_rate * 0.5
            is_sufficient = actual_rate >= min_required
            
            warning = None
            if not is_sufficient:
                warning = (
                    f"{topic_name}: 实际 {actual_rate:.1f}Hz < 要求 {min_required:.1f}Hz"
                )
                warnings.append(warning)
                logger.warning(f"采样率不足: {warning}")
            
            info = SamplingRateInfo(
                topic_name=topic_name,
                expected_rate=expected_rate,
                actual_rate=actual_rate,
                is_sufficient=is_sufficient,
                warning=warning
            )
            sampling_rates.append(info)
        
        self.report.sampling_rates = sampling_rates
        self.report.low_sampling_warnings = warnings
        
        return sampling_rates
    
    def get_report(self) -> DataQualityReport:
        """获取完整报告"""
        return self.report


# ============================================================================
# 配置校验器
# ============================================================================

class ConfigValidator:
    """配置参数校验器"""
    
    # 参数范围定义: {参数路径: (min, max, default)}
    PARAM_RANGES = {
        'base.sync_tolerance': (0.01, 1.0, 0.3),
        'kpi.steering.min_duration': (0.01, 1.0, 0.1),
        'kpi.steering.merge_gap': (0.1, 2.0, 0.3),
        'kpi.steering.angle_filter_cutoff': (1.0, 25.0, 10.0),
        'kpi.steering.rate_filter_cutoff': (1.0, 25.0, 15.0),
        'kpi.jerk_event.jerk_threshold': (0.5, 10.0, 2.5),
        'kpi.jerk_event.min_duration': (0.05, 1.0, 0.2),
        'kpi.jerk_event.merge_gap': (0.1, 2.0, 0.4),
        'kpi.weaving.min_speed': (1.0, 20.0, 5.0),
        'kpi.weaving.min_steering_amplitude': (10.0, 90.0, 40.0),
        'kpi.weaving.min_steering_vel_rms': (1.0, 50.0, 10.0),
        'kpi.weaving.min_zero_crossings': (2, 10, 4),
        'kpi.weaving.event_merge_gap': (0.1, 2.0, 0.5),
        'kpi.comfort.cutoff_hz': (1.0, 25.0, 10.0),
    }
    
    def __init__(self, config: Dict):
        self.config = config
        self.warnings: List[str] = []
    
    def _get_nested_value(self, path: str) -> Optional[Any]:
        """获取嵌套配置值"""
        keys = path.split('.')
        value = self.config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
        return value
    
    def validate(self) -> List[str]:
        """
        校验所有配置参数
        
        Returns:
            警告消息列表
        """
        self.warnings = []
        
        for param_path, (min_val, max_val, default) in self.PARAM_RANGES.items():
            value = self._get_nested_value(param_path)
            
            if value is None:
                # 参数不存在，使用默认值（不报警）
                continue
            
            if not isinstance(value, (int, float)):
                self.warnings.append(
                    f"配置 {param_path} 类型错误: 期望数值，实际 {type(value).__name__}"
                )
                continue
            
            if value < min_val:
                self.warnings.append(
                    f"配置 {param_path}={value} 过小 (建议范围: {min_val}~{max_val})"
                )
            elif value > max_val:
                self.warnings.append(
                    f"配置 {param_path}={value} 过大 (建议范围: {min_val}~{max_val})"
                )
        
        # 特殊校验：滤波截止频率 < 采样率/2 (Nyquist)
        # 这个在 KPI 计算时动态检查
        
        if self.warnings:
            for warning in self.warnings:
                logger.warning(f"配置校验: {warning}")
        
        return self.warnings


# ============================================================================
# 缓存版本化
# ============================================================================

def compute_config_fingerprint(config: Dict) -> str:
    """
    计算配置指纹（用于缓存版本化）
    
    只包含影响计算结果的关键参数
    
    Args:
        config: 配置字典
        
    Returns:
        16字符的哈希指纹
    """
    # 提取关键配置
    key_params = {}
    
    # 基础参数
    base = config.get('base', {})
    key_params['sync_tolerance'] = base.get('sync_tolerance', 0.3)
    
    # KPI 阈值参数
    kpi = config.get('kpi', {})
    
    # 转向
    steering = kpi.get('steering', {})
    key_params['steering_min_duration'] = steering.get('min_duration', 0.1)
    key_params['steering_angle_filter'] = steering.get('angle_filter_cutoff', 10.0)
    
    # 顿挫
    jerk = kpi.get('jerk_event', {})
    key_params['jerk_threshold'] = jerk.get('jerk_threshold', 2.5)
    key_params['jerk_min_duration'] = jerk.get('min_duration', 0.2)
    
    # 画龙
    weaving = kpi.get('weaving', {})
    key_params['weaving_min_speed'] = weaving.get('min_speed', 5.0)
    key_params['weaving_min_amplitude'] = weaving.get('min_steering_amplitude', 40.0)
    
    # 序列化并哈希
    param_str = str(sorted(key_params.items()))
    return hashlib.md5(param_str.encode()).hexdigest()[:16]


def get_code_version() -> str:
    """
    获取代码版本（Git commit 或时间戳）
    
    Returns:
        版本字符串
    """
    import subprocess
    from datetime import datetime
    
    try:
        # 尝试获取 Git commit hash
        result = subprocess.run(
            ['git', 'rev-parse', '--short', 'HEAD'],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    
    # 回退到时间戳
    return datetime.now().strftime('%Y%m%d')
