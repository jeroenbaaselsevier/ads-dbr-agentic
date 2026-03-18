import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import os, textwrap

os.makedirs('/mnt/git/ads-dbr-agentic/output', exist_ok=True)

BASE = '/mnt/git/ads-dbr-agentic/tmp/aum_csvs'
NOTE = 'Bubble size = AUM paper count 2020–2024  |  Colour = prominence trend pace  |  Data: Scopus ANI 20260301 + SciVal burst 2024'
CMAP = 'RdYlGn'
VMIN, VMAX = -10, 10


# ── Helpers ───────────────────────────────────────────────────────────────────
def load_csv(name):
    df = pd.read_csv(f'{BASE}/{name}', sep=',', encoding='cp1252', quotechar='"')
    df.columns = df.columns.str.strip().str.strip('"')
    for col in ['aum_papers','global_papers','aum_share_pct','ProminenceP',
                'Rank','burst_prominence','burst_output','Prominence']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def scale_bubbles(sizes, s_min=150, s_max=4000):
    lo, hi = sizes.min(), sizes.max()
    if hi == lo:
        return np.full(len(sizes), (s_min + s_max) / 2)
    return s_min + (s_max - s_min) * (sizes - lo) / (hi - lo)

def wrap(txt, width=24):
    return '\n'.join(textwrap.wrap(str(txt), width))

def make_chart(data, id_col, label_col, title, out_path):
    data = data.dropna(subset=['burst_prominence','ProminenceP','aum_papers','aum_share_pct']).copy()

    fig, ax = plt.subplots(figsize=(16, 11))
    fig.patch.set_facecolor('#f9f9f9')
    ax.set_facecolor('#f9f9f9')

    s_scaled = scale_bubbles(data['aum_papers'].values)
    sc = ax.scatter(
        data['burst_prominence'], data['aum_share_pct'],
        s=s_scaled, c=data['burst_prominence'],
        cmap=CMAP, vmin=VMIN, vmax=VMAX,
        alpha=0.88, edgecolors='#555555', linewidths=0.4, zorder=3,
    )

    for _, row in data.iterrows():
        lbl = f"{int(row[id_col])} - {wrap(row.get(label_col, ''), 24)}"
        ax.annotate(lbl, xy=(row['burst_prominence'], row['aum_share_pct']),
                    xytext=(6, 5), textcoords='offset points',
                    fontsize=7, color='#222222',
                    arrowprops=dict(arrowstyle='-', color='#cccccc', lw=0.4))

    cbar = plt.colorbar(sc, ax=ax, shrink=0.55, pad=0.02, aspect=25)
    cbar.set_label('Prominence score trend pace', fontsize=9)
    cbar.ax.text(0.5, 1.04, 'Fastest\ngrowing ►', transform=cbar.ax.transAxes,
                 ha='center', fontsize=7, color='#1a7a1a')
    cbar.ax.text(0.5, -0.10, '◄ Fastest\ndeclining', transform=cbar.ax.transAxes,
                 ha='center', fontsize=7, color='#cc0000')

    ax.axvline(x=0, color='#999999', linestyle='--', linewidth=0.8, alpha=0.5)

    xlim, ylim = ax.get_xlim(), ax.get_ylim()
    xr, yr = xlim[1]-xlim[0], ylim[1]-ylim[0]
    ax.text(xlim[0]+xr*0.02, ylim[1]-yr*0.04, 'Monitor /\nReview',
            fontsize=10, color='#996600', style='italic', alpha=0.8, va='top')
    ax.text(xlim[1]-xr*0.02, ylim[1]-yr*0.04, 'Opportunity',
            fontsize=10, color='#1a7a1a', style='italic', alpha=0.8, va='top', ha='right')

    # Bubble size legend
    refs = sorted({1, max(1, int(data['aum_papers'].max()//2)), int(data['aum_papers'].max())})
    s_ref = scale_bubbles(np.array(refs, dtype=float))
    handles = [plt.scatter([],[],s=s,c='#aaaaaa',alpha=0.6,edgecolors='grey',
                           linewidths=0.5, label=f'{r} papers')
               for s, r in zip(s_ref, refs)]
    ax.legend(handles=handles, title='AUM papers\n(bubble size)',
              loc='lower right', fontsize=8, title_fontsize=8,
              framealpha=0.7, scatterpoints=1)

    ax.set_xlabel('Prominence Trending Score', fontsize=12)
    ax.set_ylabel('AUM share of global output (%)', fontsize=12)
    ax.set_title(title, fontsize=13, fontweight='bold', pad=14)
    ax.grid(axis='y', alpha=0.2, linestyle=':')
    ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(decimals=2))
    fig.text(0.01, 0.005, NOTE, fontsize=6.5, color='#888888')

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {out_path}  ({len(data)} bubbles)")


# ═══════════════════════════════════════════════════════════════════════════════
# TOPIC CLUSTER charts (3 charts)
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── TOPIC CLUSTER LEVEL ──")

# Chart C1: Top 20 by global prominence rank
make_chart(
    load_csv('aum_burst_top100_prominent.csv').nlargest(20, 'ProminenceP'),
    'Topic_Cluster', 'cluster_label',
    'Top 20 highest ranked prominent topic clusters globally\nAUM share of output · 2020–2024',
    '/mnt/git/ads-dbr-agentic/output/aum_clusters_C1_top20_prominent.png',
)

# Chart C2: Top 20 by AUM highest share (within prominent clusters)
make_chart(
    load_csv('aum_burst_top100_highshare.csv').nlargest(20, 'aum_share_pct'),
    'Topic_Cluster', 'cluster_label',
    'Top 20 prominent topic clusters where AUM has highest share\nAUM share of output · 2020–2024',
    '/mnt/git/ads-dbr-agentic/output/aum_clusters_C2_top20_highshare.png',
)

# Chart C3: Top 20 fastest trending
make_chart(
    load_csv('aum_burst_top100_trending.csv').nlargest(20, 'burst_prominence'),
    'Topic_Cluster', 'cluster_label',
    'Top 20 fastest trending prominent topic clusters globally\nAUM share of output · 2020–2024',
    '/mnt/git/ads-dbr-agentic/output/aum_clusters_C3_top20_trending.png',
)

# ═══════════════════════════════════════════════════════════════════════════════
# TOPIC charts (3 charts — finer-grained, ~1386 topics)
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── TOPIC LEVEL ──")

# Chart T1: Top 20 by global prominence rank
make_chart(
    load_csv('aum_burst_top100_topics_prominent.csv').nlargest(20, 'ProminenceP'),
    'TopicId', 'topic_label',
    'Top 20 highest ranked prominent topics globally\nAUM share of output · 2020–2024',
    '/mnt/git/ads-dbr-agentic/output/aum_topics_T1_top20_prominent.png',
)

# Chart T2: Top 20 by AUM highest share (within prominent topics)
make_chart(
    load_csv('aum_burst_top100_topics_highshare.csv').nlargest(20, 'aum_share_pct'),
    'TopicId', 'topic_label',
    'Top 20 prominent topics where AUM has highest share\nAUM share of output · 2020–2024',
    '/mnt/git/ads-dbr-agentic/output/aum_topics_T2_top20_highshare.png',
)

# Chart T3: Top 20 fastest trending
make_chart(
    load_csv('aum_burst_top100_topics_trending.csv').nlargest(20, 'burst_prominence'),
    'TopicId', 'topic_label',
    'Top 20 fastest trending prominent topics globally\nAUM share of output · 2020–2024',
    '/mnt/git/ads-dbr-agentic/output/aum_topics_T3_top20_trending.png',
)

print("\nAll 6 charts done.")
print("\n=== KEY STATS — TOPIC CLUSTERS ===")
df_c = load_csv('aum_burst_all_clusters.csv').dropna(subset=['burst_prominence'])
cols = ['Topic_Cluster','cluster_label','aum_papers','aum_share_pct','ProminenceP','burst_prominence']
print(df_c.nlargest(10,'aum_papers')[cols].to_string(index=False))
print("\n=== KEY STATS — TOPICS ===")
df_t = load_csv('aum_burst_all_topics.csv').dropna(subset=['burst_prominence'])
tcols = ['TopicId','topic_label','aum_papers','aum_share_pct','ProminenceP','burst_prominence']
print(df_t.nlargest(10,'aum_papers')[tcols].to_string(index=False))
