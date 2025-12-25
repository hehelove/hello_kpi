#!/usr/bin/env python3
"""
定位日志分析脚本

分析 monitor.py 产生的定位日志文件，统计 stddev 等指标

功能:
    - stddev_east/north/up 统计: 最大值、平均值、P95、P99
    - gnss_stddev_x/y 统计
    - 趋势图生成 (HTML 交互式)

用法:
    python scripts/monitor/analyze_loc.py /path/to/loc_xxx.txt
    python scripts/monitor/analyze_loc.py /path/to/loc_xxx.txt -o report.html
    python scripts/monitor/analyze_loc.py /path/to/dir/  # 分析目录下所有 txt
"""

import argparse
import os
import sys
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Optional
import numpy as np
from datetime import datetime


@dataclass
class StddevStats:
    """Stddev 统计结果"""
    name: str
    count: int = 0
    min_val: float = 0.0
    max_val: float = 0.0
    mean_val: float = 0.0
    std_val: float = 0.0
    p50: float = 0.0
    p90: float = 0.0
    p95: float = 0.0
    p99: float = 0.0
    
    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'count': self.count,
            'min': self.min_val,
            'max': self.max_val,
            'mean': self.mean_val,
            'std': self.std_val,
            'p50': self.p50,
            'p90': self.p90,
            'p95': self.p95,
            'p99': self.p99
        }


@dataclass
class AnalysisResult:
    """分析结果"""
    file_path: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    total_records: int = 0
    
    # 时间序列数据
    timestamps: np.ndarray = field(default_factory=lambda: np.array([]))
    stddev_east: np.ndarray = field(default_factory=lambda: np.array([]))
    stddev_north: np.ndarray = field(default_factory=lambda: np.array([]))
    stddev_up: np.ndarray = field(default_factory=lambda: np.array([]))
    gnss_stddev_x: np.ndarray = field(default_factory=lambda: np.array([]))
    gnss_stddev_y: np.ndarray = field(default_factory=lambda: np.array([]))
    status: np.ndarray = field(default_factory=lambda: np.array([]))
    gnss_position_type: np.ndarray = field(default_factory=lambda: np.array([]))
    
    # 统计结果
    stats: Dict[str, StddevStats] = field(default_factory=dict)


def parse_log_file(file_path: str) -> AnalysisResult:
    """解析日志文件"""
    result = AnalysisResult(file_path=file_path)
    
    timestamps = []
    stddev_east = []
    stddev_north = []
    stddev_up = []
    gnss_stddev_x = []
    gnss_stddev_y = []
    status = []
    gnss_position_type = []
    
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            
            # 跳过注释和空行
            if not line or line.startswith('#'):
                continue
            
            # 跳过 header
            if line.startswith('timestamp,'):
                continue
            
            parts = line.split(',')
            if len(parts) < 12:
                continue
            
            try:
                timestamps.append(float(parts[0]))
                stddev_east.append(float(parts[4]))
                stddev_north.append(float(parts[5]))
                stddev_up.append(float(parts[6]))
                status.append(int(parts[7]))
                gnss_position_type.append(int(parts[8]))
                gnss_stddev_x.append(float(parts[9]))
                gnss_stddev_y.append(float(parts[10]))
            except (ValueError, IndexError):
                continue
    
    if not timestamps:
        return result
    
    # 转换为 numpy 数组
    result.timestamps = np.array(timestamps)
    result.stddev_east = np.array(stddev_east)
    result.stddev_north = np.array(stddev_north)
    result.stddev_up = np.array(stddev_up)
    result.gnss_stddev_x = np.array(gnss_stddev_x)
    result.gnss_stddev_y = np.array(gnss_stddev_y)
    result.status = np.array(status)
    result.gnss_position_type = np.array(gnss_position_type)
    
    result.total_records = len(timestamps)
    result.start_time = datetime.fromtimestamp(timestamps[0])
    result.end_time = datetime.fromtimestamp(timestamps[-1])
    result.duration_seconds = timestamps[-1] - timestamps[0]
    
    return result


def compute_stats(data: np.ndarray, name: str) -> StddevStats:
    """计算统计指标"""
    if len(data) == 0:
        return StddevStats(name=name)
    
    # 过滤掉异常值 (0 或负数)
    valid_data = data[data > 0]
    if len(valid_data) == 0:
        valid_data = data
    
    return StddevStats(
        name=name,
        count=len(valid_data),
        min_val=float(np.min(valid_data)),
        max_val=float(np.max(valid_data)),
        mean_val=float(np.mean(valid_data)),
        std_val=float(np.std(valid_data)),
        p50=float(np.percentile(valid_data, 50)),
        p90=float(np.percentile(valid_data, 90)),
        p95=float(np.percentile(valid_data, 95)),
        p99=float(np.percentile(valid_data, 99))
    )


def analyze(result: AnalysisResult) -> AnalysisResult:
    """执行分析"""
    result.stats['stddev_east'] = compute_stats(result.stddev_east, 'stddev_east')
    result.stats['stddev_north'] = compute_stats(result.stddev_north, 'stddev_north')
    result.stats['stddev_up'] = compute_stats(result.stddev_up, 'stddev_up')
    result.stats['gnss_stddev_x'] = compute_stats(result.gnss_stddev_x, 'gnss_stddev_x')
    result.stats['gnss_stddev_y'] = compute_stats(result.gnss_stddev_y, 'gnss_stddev_y')
    
    # 计算水平误差 (sqrt(east^2 + north^2))
    horizontal = np.sqrt(result.stddev_east**2 + result.stddev_north**2)
    result.stats['stddev_horizontal'] = compute_stats(horizontal, 'stddev_horizontal')
    
    return result


def print_report(result: AnalysisResult):
    """打印报告"""
    print(f"\n{'='*70}")
    print(f"定位日志分析报告")
    print(f"{'='*70}")
    print(f"文件: {result.file_path}")
    print(f"记录数: {result.total_records:,}")
    if result.start_time:
        print(f"开始时间: {result.start_time}")
        print(f"结束时间: {result.end_time}")
        print(f"持续时间: {result.duration_seconds:.1f}s ({result.duration_seconds/60:.1f}min)")
    
    print(f"\n{'─'*70}")
    print(f"{'指标':<20} {'最小值':>10} {'最大值':>10} {'平均值':>10} {'P95':>10} {'P99':>10}")
    print(f"{'─'*70}")
    
    for name in ['stddev_east', 'stddev_north', 'stddev_up', 'stddev_horizontal', 
                 'gnss_stddev_x', 'gnss_stddev_y']:
        if name in result.stats:
            s = result.stats[name]
            print(f"{s.name:<20} {s.min_val:>10.4f} {s.max_val:>10.4f} "
                  f"{s.mean_val:>10.4f} {s.p95:>10.4f} {s.p99:>10.4f}")
    
    print(f"{'─'*70}")
    
    # 状态分布
    if len(result.status) > 0:
        print(f"\n定位状态分布:")
        unique, counts = np.unique(result.status, return_counts=True)
        for u, c in zip(unique, counts):
            pct = c / len(result.status) * 100
            print(f"  状态 {u}: {c:,} ({pct:.1f}%)")
    
    # GNSS 类型分布
    if len(result.gnss_position_type) > 0:
        print(f"\nGNSS Position Type 分布:")
        unique, counts = np.unique(result.gnss_position_type, return_counts=True)
        for u, c in zip(unique, counts):
            pct = c / len(result.gnss_position_type) * 100
            print(f"  Type {u}: {c:,} ({pct:.1f}%)")


def generate_html_report(result: AnalysisResult, output_path: str):
    """生成 HTML 报告 (带趋势图)"""
    
    # 降采样用于绘图 (最多 5000 点)
    max_points = 5000
    if len(result.timestamps) > max_points:
        step = len(result.timestamps) // max_points
        indices = np.arange(0, len(result.timestamps), step)
    else:
        indices = np.arange(len(result.timestamps))
    
    # 转换时间戳为相对时间 (秒)
    t0 = result.timestamps[0] if len(result.timestamps) > 0 else 0
    rel_time = (result.timestamps[indices] - t0).tolist()
    
    # 数据
    east = result.stddev_east[indices].tolist()
    north = result.stddev_north[indices].tolist()
    up = result.stddev_up[indices].tolist()
    gnss_x = result.gnss_stddev_x[indices].tolist()
    gnss_y = result.gnss_stddev_y[indices].tolist()
    
    # 统计表格 HTML
    stats_rows = ""
    for name in ['stddev_east', 'stddev_north', 'stddev_up', 'stddev_horizontal',
                 'gnss_stddev_x', 'gnss_stddev_y']:
        if name in result.stats:
            s = result.stats[name]
            stats_rows += f"""
            <tr>
                <td>{s.name}</td>
                <td>{s.min_val:.4f}</td>
                <td>{s.max_val:.4f}</td>
                <td>{s.mean_val:.4f}</td>
                <td>{s.p50:.4f}</td>
                <td>{s.p90:.4f}</td>
                <td>{s.p95:.4f}</td>
                <td>{s.p99:.4f}</td>
            </tr>"""
    
    # 状态分布
    status_info = ""
    if len(result.status) > 0:
        unique, counts = np.unique(result.status, return_counts=True)
        for u, c in zip(unique, counts):
            pct = c / len(result.status) * 100
            status_info += f"<li>状态 {u}: {c:,} ({pct:.1f}%)</li>"
    
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>定位日志分析报告</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }}
        .card {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{ color: #333; }}
        h2 {{ color: #555; margin-top: 0; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
        }}
        th, td {{
            padding: 10px;
            text-align: right;
            border-bottom: 1px solid #eee;
        }}
        th {{ background: #f8f9fa; text-align: right; }}
        td:first-child, th:first-child {{ text-align: left; font-weight: 500; }}
        .chart-container {{
            position: relative;
            height: 300px;
            margin-bottom: 20px;
        }}
        .meta {{ color: #666; font-size: 14px; }}
        .meta span {{ margin-right: 20px; }}
        ul {{ padding-left: 20px; }}
    </style>
</head>
<body>
    <h1>📍 定位日志分析报告</h1>
    
    <div class="card">
        <h2>概览</h2>
        <p class="meta">
            <span>📁 文件: {Path(result.file_path).name}</span>
            <span>📊 记录数: {result.total_records:,}</span>
            <span>⏱️ 时长: {result.duration_seconds:.1f}s</span>
        </p>
        <p class="meta">
            <span>🕐 开始: {result.start_time}</span>
            <span>🕐 结束: {result.end_time}</span>
        </p>
    </div>
    
    <div class="card">
        <h2>统计指标 (单位: 米)</h2>
        <table>
            <tr>
                <th>指标</th>
                <th>最小值</th>
                <th>最大值</th>
                <th>平均值</th>
                <th>P50</th>
                <th>P90</th>
                <th>P95</th>
                <th>P99</th>
            </tr>
            {stats_rows}
        </table>
    </div>
    
    <div class="card">
        <h2>定位 Stddev 趋势</h2>
        <div class="chart-container">
            <canvas id="locChart"></canvas>
        </div>
    </div>
    
    <div class="card">
        <h2>GNSS Stddev 趋势</h2>
        <div class="chart-container">
            <canvas id="gnssChart"></canvas>
        </div>
    </div>
    
    <div class="card">
        <h2>状态分布</h2>
        <ul>{status_info}</ul>
    </div>
    
    <script>
        const relTime = {rel_time};
        const eastData = {east};
        const northData = {north};
        const upData = {up};
        const gnssX = {gnss_x};
        const gnssY = {gnss_y};
        
        // 定位 Stddev 图
        new Chart(document.getElementById('locChart'), {{
            type: 'line',
            data: {{
                labels: relTime.map(t => t.toFixed(0) + 's'),
                datasets: [
                    {{
                        label: 'stddev_east',
                        data: eastData,
                        borderColor: '#FF6384',
                        backgroundColor: 'rgba(255,99,132,0.1)',
                        borderWidth: 1.5,
                        pointRadius: 0,
                        fill: false
                    }},
                    {{
                        label: 'stddev_north',
                        data: northData,
                        borderColor: '#36A2EB',
                        backgroundColor: 'rgba(54,162,235,0.1)',
                        borderWidth: 1.5,
                        pointRadius: 0,
                        fill: false
                    }},
                    {{
                        label: 'stddev_up',
                        data: upData,
                        borderColor: '#4BC0C0',
                        backgroundColor: 'rgba(75,192,192,0.1)',
                        borderWidth: 1.5,
                        pointRadius: 0,
                        fill: false
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                interaction: {{ mode: 'index', intersect: false }},
                scales: {{
                    x: {{ 
                        title: {{ display: true, text: '时间' }},
                        ticks: {{ maxTicksLimit: 20 }}
                    }},
                    y: {{ 
                        title: {{ display: true, text: 'Stddev (m)' }},
                        beginAtZero: true
                    }}
                }},
                plugins: {{
                    legend: {{ position: 'top' }}
                }}
            }}
        }});
        
        // GNSS Stddev 图
        new Chart(document.getElementById('gnssChart'), {{
            type: 'line',
            data: {{
                labels: relTime.map(t => t.toFixed(0) + 's'),
                datasets: [
                    {{
                        label: 'gnss_stddev_x',
                        data: gnssX,
                        borderColor: '#FF9F40',
                        borderWidth: 1.5,
                        pointRadius: 0,
                        fill: false
                    }},
                    {{
                        label: 'gnss_stddev_y',
                        data: gnssY,
                        borderColor: '#9966FF',
                        borderWidth: 1.5,
                        pointRadius: 0,
                        fill: false
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                interaction: {{ mode: 'index', intersect: false }},
                scales: {{
                    x: {{ 
                        title: {{ display: true, text: '时间' }},
                        ticks: {{ maxTicksLimit: 20 }}
                    }},
                    y: {{ 
                        title: {{ display: true, text: 'Stddev (m)' }},
                        beginAtZero: true
                    }}
                }},
                plugins: {{
                    legend: {{ position: 'top' }}
                }}
            }}
        }});
    </script>
</body>
</html>"""
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"\n✓ HTML 报告已生成: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="分析定位日志文件",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('input', help='输入文件或目录')
    parser.add_argument('-o', '--output', help='输出 HTML 报告路径')
    parser.add_argument('--no-html', action='store_true', help='不生成 HTML 报告')
    
    args = parser.parse_args()
    
    input_path = Path(args.input)
    
    # 查找文件
    if input_path.is_dir():
        files = sorted(input_path.glob('*.txt'))
        if not files:
            print(f"错误: 目录中未找到 .txt 文件: {input_path}")
            sys.exit(1)
        print(f"找到 {len(files)} 个文件")
    else:
        if not input_path.exists():
            print(f"错误: 文件不存在: {input_path}")
            sys.exit(1)
        files = [input_path]
    
    # 分析每个文件
    for file_path in files:
        print(f"\n处理: {file_path.name}")
        
        # 解析
        result = parse_log_file(str(file_path))
        if result.total_records == 0:
            print(f"  跳过: 无有效数据")
            continue
        
        # 分析
        result = analyze(result)
        
        # 打印报告
        print_report(result)
        
        # 生成 HTML
        if not args.no_html:
            if args.output:
                html_path = args.output
            else:
                html_path = str(file_path.with_suffix('.html'))
            
            generate_html_report(result, html_path)


if __name__ == '__main__':
    main()

