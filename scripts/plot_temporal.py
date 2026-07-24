# 任务三：Top5 影响力随季度变化折线图（全量数据版）
# 用法：先把 HDFS 上的 tracked 结果下载到本地，再跑这个脚本

import matplotlib
matplotlib.use('Agg')   # 无界面环境必须加这行（服务器/notebook 后台出图）
import matplotlib.pyplot as plt
from collections import defaultdict

# ============ 1. 读数据 ============
# tracked 文件格式：quarter \t email \t pagerank（带表头）
SRC = "final_project_task3_timeseries_tracked"        
OUT = "task3_lines.png"

data = defaultdict(dict)   # data[email][quarter] = pagerank
quarters = []
with open(SRC, encoding="utf-8") as f:
    next(f)                # 跳过表头那一行
    for line in f:
        parts = line.strip().split("\t")
        if len(parts) < 3:
            continue
        q, email, pr = parts[0], parts[1], float(parts[2])
        if q not in quarters:
            quarters.append(q)
        data[email][q] = pr
quarters = sorted(quarters)   # 季度排序（字典序=时间序）

# 去掉邮箱的 @enron.com 后缀，图例更简洁
def short(e):
    return e.split("@")[0]

# 5 个人各用一种颜色
colors = ['#C0504D', '#4F81BD', '#9BBB59', '#8064A2', '#F79646']

# ============ 2. 画双子图 ============
# 因为 klay 在 2002-Q1 有个巨大尖峰，直接画会把别人压扁，
# 所以画两个子图：上图看全貌(含尖峰)，下图放大(限制y轴)看其他人细节。
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 9))

# ----- 上图：完整视图 -----
for i, (email, series) in enumerate(data.items()):
    ys = [series.get(q, 0.0) for q in quarters]   # 某季度没出现就记 0
    ax1.plot(quarters, ys, marker='o', linewidth=2, markersize=4,
             label=short(email), color=colors[i % len(colors)])
ax1.set_title('Top5 Influence Over Time - Full View', fontsize=12, fontweight='bold')
ax1.set_ylabel('PageRank')
ax1.legend(fontsize=9, loc='upper left')
ax1.grid(True, alpha=0.3)
ax1.tick_params(axis='x', rotation=45, labelsize=8)
# 标注尖峰
if '2002-Q1' in quarters:
    xi = quarters.index('2002-Q1')
    ax1.annotate('peak', xy=(xi, 0.045),
                 xytext=(xi - 2.5, 0.043), fontsize=9, color='#C0504D',
                 arrowprops=dict(arrowstyle='->', color='#C0504D'))

# ----- 下图：放大视图（y轴封顶，看清其他人） -----
for i, (email, series) in enumerate(data.items()):
    ys = [series.get(q, 0.0) for q in quarters]
    ax2.plot(quarters, ys, marker='o', linewidth=2, markersize=4,
             label=short(email), color=colors[i % len(colors)])
ax2.set_ylim(0, 0.013)   # 封顶，放大细节（klay 尖峰会冲出顶部，但能看清其他线）
ax2.set_title('Zoomed View (y-axis capped to show detail)', fontsize=12, fontweight='bold')
ax2.set_ylabel('PageRank')
ax2.set_xlabel('Quarter')
ax2.legend(fontsize=9, loc='upper right')
ax2.grid(True, alpha=0.3)
ax2.tick_params(axis='x', rotation=45, labelsize=8)

plt.tight_layout()
plt.savefig(OUT, dpi=120)
print("图已保存:", OUT)