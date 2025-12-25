"""
定位置信度KPI
统计定位状态和置信度
"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import numpy as np
import os

from .base_kpi import BaseKPI, KPIResult, BagTimeMapper, AnomalyRecord, StreamingData
from ..data_loader.bag_reader import MessageAccessor


@dataclass
class LocalizationStatus:
    """定位状态"""
    timestamp: float
    status: int  # 2=等待初始化, 3=正常, 7=需要检查stddev
    max_position_stddev: float
    status_name: str


class LocalizationKPI(BaseKPI):
    """定位置信度KPI"""
    
    @property
    def name(self) -> str:
        return "定位置信度"
    
    @property
    def required_topics(self) -> List[str]:
        return ["/localization/localization"]
    
    @property
    def supports_streaming(self) -> bool:
        """支持流式收集模式"""
        return True
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__(config)
        
        loc_config = self.config.get('kpi', {}).get('localization', {})
        self.status_normal = loc_config.get('status_normal', 3)
        self.status_waiting = loc_config.get('status_waiting', 2)
        self.high_stddev_threshold = loc_config.get('high_stddev_threshold', 1.0)
        self.medium_stddev_threshold = loc_config.get('medium_stddev_threshold', 0.2)
    
    def compute(self, synced_frames: List, **kwargs) -> List[KPIResult]:
        """计算定位置信度KPI - 通过流式模式复用逻辑"""
        return self._compute_via_streaming(synced_frames, **kwargs)
    
    def _merge_abnormal_events(self, statuses: List[LocalizationStatus], 
                                bag_mapper: BagTimeMapper) -> List[AnomalyRecord]:
        """
        合并连续帧的定位异常事件
        
        连续的相同异常状态合并为一个事件
        """
        events = []
        if not statuses:
            return events
        
        # 找出异常状态的帧
        abnormal_indices = []
        for i, s in enumerate(statuses):
            if s.status_name != '正常':
                abnormal_indices.append(i)
        
        if not abnormal_indices:
            return events
        
        # 分组连续的异常帧（相同状态名且连续，同时检查时间间隔防止接管导致的错误累计）
        max_gap_frames = 3
        max_gap_sec = 0.5  # 最大允许时间间隔
        
        groups = []
        current_group = [abnormal_indices[0]]
        current_status = statuses[abnormal_indices[0]].status_name
        
        for i in range(1, len(abnormal_indices)):
            idx = abnormal_indices[i]
            prev_idx = abnormal_indices[i-1]
            status_name = statuses[idx].status_name
            
            # 检查帧间隔和时间间隔
            frame_gap_ok = (idx - prev_idx) <= max_gap_frames
            time_gap = statuses[idx].timestamp - statuses[prev_idx].timestamp
            time_gap_ok = time_gap <= max_gap_sec
            
            # 连续帧（间隔不超过阈值）且相同状态
            if frame_gap_ok and time_gap_ok and status_name == current_status:
                current_group.append(idx)
            else:
                groups.append((current_status, current_group))
                current_group = [idx]
                current_status = status_name
        groups.append((current_status, current_group))
        
        # 为每组创建事件
        for status_name, group in groups:
            start_s = statuses[group[0]]
            end_s = statuses[group[-1]]
            
            # 计算实际有效持续时间（排除大间隔）
            if len(group) > 1:
                group_timestamps = [statuses[idx].timestamp for idx in group]
                ts_diffs = [group_timestamps[j+1] - group_timestamps[j] for j in range(len(group_timestamps)-1)]
                valid_diffs = [d for d in ts_diffs if d <= max_gap_sec]
                duration = sum(valid_diffs)
            else:
                duration = 0.0
            
            # 计算这组帧中的最大 stddev
            max_stddev = max(statuses[idx].max_position_stddev for idx in group)
            avg_stddev = sum(statuses[idx].max_position_stddev for idx in group) / len(group)
            
            desc = f"定位{status_name}：峰值stddev={max_stddev:.4f}，均值={avg_stddev:.4f}"
            if len(group) > 1:
                desc += f"，持续{len(group)}帧/{duration:.2f}s"
            
            events.append(AnomalyRecord(
                timestamp=start_s.timestamp,
                bag_name=bag_mapper.get_bag_name(start_s.timestamp),
                description=desc,
                value=avg_stddev,
                peak_value=max_stddev,
                end_timestamp=end_s.timestamp if duration > 0 else 0,
                frame_count=len(group)
            ))
        
        return events
    
    def _determine_status_name(self, status: int, max_stddev: float) -> str:
        """
        根据状态码和stddev确定状态名称
        
        规则：
        - status == 2: 等待初始化
        - status == 3 或 7: 根据 stddev 判断
            - stddev > 1.0: 定位偏移
            - stddev > 0.2: 可能偏移
            - else: 正常
        - 其他 status: 未知状态（数据不可信）
        """
        if status == self.status_waiting:  # 2
            return "等待初始化"
        elif status in [3, 7]:  # 有效状态，需要看 stddev
            if max_stddev > self.high_stddev_threshold:
                return "定位偏移"
            elif max_stddev > self.medium_stddev_threshold:
                return "可能偏移"
            else:
                return "正常"
        else:
            return f"未知状态({status})"
    
    def _generate_visualizations(self, statuses: List['LocalizationStatus'],
                                  timestamps: np.ndarray,
                                  max_stddevs: np.ndarray,
                                  status_rates: Dict[str, float],
                                  output_dir: str):
        """
        生成定位置信度可视化 HTML 报告（使用 Chart.js，避免字体问题）
        
        包括：
        1. 状态分布饼图
        2. 标准差时间序列图
        3. 标准差分布直方图
        4. 统计摘要
        """
        # 创建输出目录
        viz_dir = os.path.join(output_dir, "localization_viz")
        os.makedirs(viz_dir, exist_ok=True)
        
        # 降采样用于绘图（最多 5000 点）
        max_points = 5000
        if len(timestamps) > max_points:
            step = len(timestamps) // max_points
            ts_plot = timestamps[::step]
            stddev_plot = max_stddevs[::step]
        else:
            ts_plot = timestamps
            stddev_plot = max_stddevs
        
        # 转换为相对时间（秒）
        relative_time = (ts_plot - ts_plot[0]).tolist()
        stddev_list = stddev_plot.tolist()
        
        # 计算直方图数据
        hist_bins = 50
        display_max = min(float(np.percentile(max_stddevs, 99)), 2.0)
        hist_data = max_stddevs[max_stddevs <= display_max]
        hist_counts, hist_edges = np.histogram(hist_data, bins=hist_bins)
        hist_labels = [f"{hist_edges[i]:.3f}" for i in range(len(hist_counts))]
        hist_colors = []
        for i in range(len(hist_counts)):
            bin_center = (hist_edges[i] + hist_edges[i+1]) / 2
            if bin_center > self.high_stddev_threshold:
                hist_colors.append('#F44336')  # 红色
            elif bin_center > self.medium_stddev_threshold:
                hist_colors.append('#FFC107')  # 黄色
            else:
                hist_colors.append('#4CAF50')  # 绿色
        
        # 统计数据
        normal_rate = status_rates.get('正常', 0)
        possible_drift_rate = status_rates.get('可能偏移', 0)
        drift_rate = status_rates.get('定位偏移', 0)
        waiting_rate = status_rates.get('等待初始化', 0)
        
        mean_stddev = float(np.mean(max_stddevs))
        max_stddev_val = float(np.max(max_stddevs))
        min_stddev_val = float(np.min(max_stddevs))
        p50_stddev = float(np.percentile(max_stddevs, 50))
        p95_stddev = float(np.percentile(max_stddevs, 95))
        p99_stddev = float(np.percentile(max_stddevs, 99))
        
        # 生成 HTML
        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>定位置信度分析报告</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        h1 {{
            text-align: center;
            color: #333;
            margin-bottom: 30px;
        }}
        .summary {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .summary h2 {{
            margin-top: 0;
            color: #333;
            border-bottom: 2px solid #4CAF50;
            padding-bottom: 10px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-top: 15px;
        }}
        .stat-item {{
            background: #f8f9fa;
            padding: 15px;
            border-radius: 6px;
            text-align: center;
        }}
        .stat-value {{
            font-size: 24px;
            font-weight: bold;
            color: #333;
        }}
        .stat-label {{
            font-size: 14px;
            color: #666;
            margin-top: 5px;
        }}
        .stat-normal {{ border-left: 4px solid #4CAF50; }}
        .stat-warning {{ border-left: 4px solid #FFC107; }}
        .stat-danger {{ border-left: 4px solid #F44336; }}
        .charts-row {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-bottom: 20px;
        }}
        .chart-container {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .chart-container h3 {{
            margin-top: 0;
            color: #333;
        }}
        .full-width {{
            grid-column: 1 / -1;
        }}
        @media (max-width: 768px) {{
            .charts-row {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📍 定位置信度分析报告</h1>
        
        <div class="summary">
            <h2>📊 统计摘要</h2>
            <div class="stats-grid">
                <div class="stat-item stat-normal">
                    <div class="stat-value">{normal_rate:.1f}%</div>
                    <div class="stat-label">定位正常率</div>
                </div>
                <div class="stat-item stat-warning">
                    <div class="stat-value">{possible_drift_rate:.1f}%</div>
                    <div class="stat-label">可能偏移率</div>
                </div>
                <div class="stat-item stat-danger">
                    <div class="stat-value">{drift_rate:.1f}%</div>
                    <div class="stat-label">定位偏移率</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{mean_stddev:.4f}m</div>
                    <div class="stat-label">平均标准差</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{p95_stddev:.4f}m</div>
                    <div class="stat-label">P95 标准差</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{len(max_stddevs):,}</div>
                    <div class="stat-label">总帧数</div>
                </div>
            </div>
        </div>
        
        <div class="charts-row">
            <div class="chart-container">
                <h3>🥧 状态分布</h3>
                <canvas id="pieChart"></canvas>
            </div>
            <div class="chart-container">
                <h3>📈 标准差分布直方图</h3>
                <canvas id="histChart"></canvas>
            </div>
        </div>
        
        <div class="charts-row">
            <div class="chart-container full-width">
                <h3>📉 标准差时间序列</h3>
                <canvas id="timeChart"></canvas>
            </div>
        </div>
        
        <div class="summary">
            <h2>📋 详细统计</h2>
            <table style="width:100%; border-collapse: collapse;">
                <tr style="background:#f0f0f0;">
                    <th style="padding:10px; text-align:left; border-bottom:2px solid #ddd;">指标</th>
                    <th style="padding:10px; text-align:right; border-bottom:2px solid #ddd;">值</th>
                </tr>
                <tr><td style="padding:8px;">最小标准差</td><td style="padding:8px; text-align:right;">{min_stddev_val:.4f} m</td></tr>
                <tr style="background:#f9f9f9;"><td style="padding:8px;">最大标准差</td><td style="padding:8px; text-align:right;">{max_stddev_val:.4f} m</td></tr>
                <tr><td style="padding:8px;">平均标准差</td><td style="padding:8px; text-align:right;">{mean_stddev:.4f} m</td></tr>
                <tr style="background:#f9f9f9;"><td style="padding:8px;">中位数 (P50)</td><td style="padding:8px; text-align:right;">{p50_stddev:.4f} m</td></tr>
                <tr><td style="padding:8px;">P95</td><td style="padding:8px; text-align:right;">{p95_stddev:.4f} m</td></tr>
                <tr style="background:#f9f9f9;"><td style="padding:8px;">P99</td><td style="padding:8px; text-align:right;">{p99_stddev:.4f} m</td></tr>
                <tr><td style="padding:8px;">高阈值</td><td style="padding:8px; text-align:right;">{self.high_stddev_threshold} m</td></tr>
                <tr style="background:#f9f9f9;"><td style="padding:8px;">中阈值</td><td style="padding:8px; text-align:right;">{self.medium_stddev_threshold} m</td></tr>
            </table>
        </div>
    </div>
    
    <script>
        // 饼图
        new Chart(document.getElementById('pieChart'), {{
            type: 'doughnut',
            data: {{
                labels: ['正常', '可能偏移', '定位偏移', '等待初始化'],
                datasets: [{{
                    data: [{normal_rate:.2f}, {possible_drift_rate:.2f}, {drift_rate:.2f}, {waiting_rate:.2f}],
                    backgroundColor: ['#4CAF50', '#FFC107', '#F44336', '#9E9E9E'],
                    borderWidth: 2,
                    borderColor: '#fff'
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{
                        position: 'bottom'
                    }}
                }}
            }}
        }});
        
        // 直方图
        new Chart(document.getElementById('histChart'), {{
            type: 'bar',
            data: {{
                labels: {hist_labels},
                datasets: [{{
                    label: '帧数',
                    data: {hist_counts.tolist()},
                    backgroundColor: {hist_colors},
                    borderWidth: 0
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{
                        display: false
                    }}
                }},
                scales: {{
                    x: {{
                        title: {{
                            display: true,
                            text: '标准差 (m)'
                        }},
                        ticks: {{
                            maxTicksLimit: 10
                        }}
                    }},
                    y: {{
                        title: {{
                            display: true,
                            text: '帧数'
                        }}
                    }}
                }}
            }}
        }});
        
        // 时间序列图
        new Chart(document.getElementById('timeChart'), {{
            type: 'line',
            data: {{
                labels: {relative_time},
                datasets: [{{
                    label: '位置标准差 (m)',
                    data: {stddev_list},
                    borderColor: '#2196F3',
                    backgroundColor: 'rgba(33, 150, 243, 0.1)',
                    borderWidth: 1,
                    pointRadius: 0,
                    fill: true
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{
                        display: true
                    }},
                    annotation: {{
                        annotations: {{
                            highLine: {{
                                type: 'line',
                                yMin: {self.high_stddev_threshold},
                                yMax: {self.high_stddev_threshold},
                                borderColor: '#F44336',
                                borderWidth: 2,
                                borderDash: [5, 5]
                            }},
                            mediumLine: {{
                                type: 'line',
                                yMin: {self.medium_stddev_threshold},
                                yMax: {self.medium_stddev_threshold},
                                borderColor: '#FFC107',
                                borderWidth: 2,
                                borderDash: [5, 5]
                            }}
                        }}
                    }}
                }},
                scales: {{
                    x: {{
                        title: {{
                            display: true,
                            text: '时间 (s)'
                        }},
                        ticks: {{
                            maxTicksLimit: 20
                        }}
                    }},
                    y: {{
                        title: {{
                            display: true,
                            text: '标准差 (m)'
                        }},
                        min: 0
                    }}
                }},
                interaction: {{
                    intersect: false,
                    mode: 'index'
                }}
            }}
        }});
    </script>
</body>
</html>"""
        
        output_path = os.path.join(viz_dir, "localization_report.html")
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html)
        
        print(f"    [定位可视化] 已生成 HTML 报告: {output_path}")
    
    def _add_empty_results(self):
        """添加空结果"""
        self.add_result(KPIResult(
            name="定位正常率",
            value=0.0,
            unit="%",
            description="数据不足，无法统计定位置信度"
        ))
    
    # ========== 流式模式支持 ==========
    
    def collect(self, synced_frames: List, streaming_data: StreamingData, **kwargs):
        """
        收集定位状态数据（流式模式）
        """
        for frame in synced_frames:
            loc_msg = frame.messages.get("/localization/localization")
            
            if loc_msg is None:
                continue
            
            status = MessageAccessor.get_field(loc_msg, "status.common", None)
            stddev_east = MessageAccessor.get_field(
                loc_msg, "global_localization.position_stddev.east", 0.0)
            stddev_north = MessageAccessor.get_field(
                loc_msg, "global_localization.position_stddev.north", 0.0)
            
            if status is None:
                continue
            
            # 存储: (status, stddev_east, stddev_north, timestamp)
            streaming_data.localization_data.append((
                status,
                stddev_east,
                stddev_north,
                frame.timestamp
            ))
    
    def compute_from_collected(self, streaming_data: StreamingData, **kwargs) -> List[KPIResult]:
        """
        从收集的数据计算定位置信度KPI（流式模式）
        """
        self.clear_results()
        
        if len(streaming_data.localization_data) == 0:
            self._add_empty_results()
            return self.get_results()
        
        # 解构数据
        statuses = []
        stddevs_east = []
        stddevs_north = []
        timestamps = []
        
        for data in streaming_data.localization_data:
            status, stddev_east, stddev_north, ts = data
            max_stddev = max(stddev_east, stddev_north)
            status_name = self._determine_status_name(status, max_stddev)
            
            statuses.append(LocalizationStatus(
                timestamp=ts,
                status=status,
                max_position_stddev=max_stddev,
                status_name=status_name
            ))
            stddevs_east.append(stddev_east)
            stddevs_north.append(stddev_north)
            timestamps.append(ts)
        
        # 统计各状态的占比
        status_counts = {}
        for s in statuses:
            status_counts[s.status_name] = status_counts.get(s.status_name, 0) + 1
        
        total_count = len(statuses)
        status_rates = {k: v / total_count * 100 for k, v in status_counts.items()}
        
        # 计算统计值
        stddevs_east = np.array(stddevs_east)
        stddevs_north = np.array(stddevs_north)
        max_stddevs = np.maximum(stddevs_east, stddevs_north)
        
        bag_mapper = BagTimeMapper(streaming_data.bag_infos)
        
        # 添加结果
        normal_rate = status_rates.get('正常', 0)
        loc_result = KPIResult(
            name="定位正常率",
            value=round(normal_rate, 2),
            unit="%",
            description="定位状态为正常的时间占比",
            details={
                'status_distribution': status_rates,
                'status_counts': status_counts,
                'total_frames': total_count
            }
        )
        
        merged_events = self._merge_abnormal_events(statuses, bag_mapper)
        for event in merged_events[:50]:
            loc_result.anomalies.append(event)
        loc_result.details['abnormal_event_count'] = len(merged_events)
        
        self.add_result(loc_result)
        
        self.add_result(KPIResult(
            name="定位偏移率",
            value=round(status_rates.get('定位偏移', 0), 2),
            unit="%",
            description="定位偏移的时间占比"
        ))
        
        self.add_result(KPIResult(
            name="可能偏移率",
            value=round(status_rates.get('可能偏移', 0), 2),
            unit="%",
            description="可能偏移的时间占比"
        ))
        
        self.add_result(KPIResult(
            name="平均位置标准差",
            value=round(np.mean(max_stddevs), 4),
            unit="",
            description="位置标准差(east, north)最大值的平均",
            details={
                'mean_east': round(np.mean(stddevs_east), 4),
                'mean_north': round(np.mean(stddevs_north), 4),
                'max': round(np.max(max_stddevs), 4)
            }
        ))
        
        # 超限统计
        high_stddev_count = np.sum(max_stddevs > self.high_stddev_threshold)
        medium_stddev_count = np.sum(
            (max_stddevs > self.medium_stddev_threshold) & 
            (max_stddevs <= self.high_stddev_threshold)
        )
        
        self.add_result(KPIResult(
            name="高标准差帧数",
            value=int(high_stddev_count),
            unit="帧",
            description=f"位置标准差>{self.high_stddev_threshold}m的帧数"
        ))
        
        self.add_result(KPIResult(
            name="中等标准差帧数",
            value=int(medium_stddev_count),
            unit="帧",
            description=f"位置标准差在{self.medium_stddev_threshold}-{self.high_stddev_threshold}m之间的帧数"
        ))
        
        # 生成可视化
        output_dir = kwargs.get('output_dir')
        if output_dir:
            self._generate_visualizations(
                statuses=statuses,
                timestamps=np.array(timestamps),
                max_stddevs=max_stddevs,
                status_rates=status_rates,
                output_dir=output_dir
            )
        
        return self.get_results()

