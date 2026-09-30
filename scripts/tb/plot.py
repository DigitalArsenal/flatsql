#!/usr/bin/env python3
"""Plots and slopes for the terabyte harness CSVs (standard library only).

    scripts/tb/plot.py <csv> [--x records] [--metrics a,b,...] [--out plots.html]
                       [--title "..."] [--slopes]

<csv> is any harness CSV: <prefix>.samples.csv or .steps.csv (engine and
count-scaled tiers), meta.csv (metadata tier, x = tier_tb), or
sds-tb-gen.samples.csv. Writes one self-contained HTML page of small
multiples, one metric per chart against the count axis --x (never two
scales on one chart), each with a hover crosshair, its table, and its
least-squares slope. --slopes prints the slopes as a markdown table.
"""
import argparse
import csv
import html
import json
import math
import sys

DEFAULT_METRICS = {
    'engine': ['rec_s', 'frame_mb_s', 'disk_bytes', 'write_amp', 'at_rest_ratio', 'heap_bytes', 'rss_bytes',
                'committed_bytes', 'accel_bytes', 'descriptor_bytes', 'handles_hw', 'fds', 'segs_max', 'segs_total',
                'seals_s', 'merges_s', 'fetch_p50_ms', 'fetch_p99_ms', 'dominant_fetch_ms',
                'maint_p99_ms', 'commit_p99_ms', 'maint_ms_per_merge', 'maint_per_merge_ms', 'label_lag_rows_max', 'q_hit_p99_ms',
                'q_miss_p99_ms', 'q_epoch_p99_ms', 'q_tag_p99_ms', 'q_arrivals_p99_ms', 'q_parts_p99_ms',
                'q_lanes_p99_ms', 'ack_timeouts'],
    'meta': ['records_total', 'segments_total', 's_max', 'cat_runs_omm', 'files', 'allocated_bytes', 'open_ms',
                'open_read_bytes', 'open_heap_store', 'cat_accel_est_omm', 'cat_warm_heap_delta_OMM', 'dom_accel_est',
                'dom_warm_heap_delta', 'dom_rec_s', 'dom_commit_p99_ms', 'dom_maint_p99_ms', 'dom_maint_per_merge_ms',
                'handles_after_dom_warm', 'handles_hw',
                'reopen_ms'],
    'sdn': ['rec_s', 'call_p50_ms', 'call_p99_ms', 'inflight', 'stalled', 'partitions', 'go_heap_bytes',
            'rss_bytes', 'get_hit_ms', 'get_miss_ms', 'disk_usage_ms', 'peer_bytes_ms', 'data_summary_ms'],
}


def load(path):
    with open(path, newline='') as f:
        rows = list(csv.DictReader(f))
    out = []
    for r in rows:
        d = {}
        for k, v in r.items():
            try:
                d[k] = float(v)
            except (TypeError, ValueError):
                d[k] = None
        out.append(d)
    return out


def slope(xs, ys):
    pts = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
    if len(pts) < 2:
        return None, None
    n = len(pts)
    mx = sum(p[0] for p in pts) / n
    my = sum(p[1] for p in pts) / n
    sxx = sum((p[0] - mx) ** 2 for p in pts)
    if sxx == 0:
        return None, None
    b = sum((p[0] - mx) * (p[1] - my) for p in pts) / sxx
    return b, my - b * mx


def human(v):
    if v is None:
        return 'n/a'
    a = abs(v)
    for unit, div in (('T', 1e12), ('G', 1e9), ('M', 1e6), ('k', 1e3)):
        if a >= div:
            return f'{v / div:.3g}{unit}'
    if a >= 100 or v == int(v):
        return f'{v:.0f}'
    return f'{v:.3g}'


def nice_ticks(lo, hi, n=4):
    if hi <= lo:
        hi = lo + 1
    raw = (hi - lo) / n
    mag = 10 ** math.floor(math.log10(raw))
    step = min((s * mag for s in (1, 2, 2.5, 5, 10) if s * mag >= raw), default=raw)
    start = math.floor(lo / step) * step
    ticks = []
    t = start
    while t <= hi + step * 1e-9:
        ticks.append(t)
        t += step
    return ticks


def chart(xname, yname, xs, ys, idx):
    W, H, L, R, T, B = 340, 190, 52, 12, 14, 30
    pts = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
    if not pts:
        return ''
    xlo, xhi = min(p[0] for p in pts), max(p[0] for p in pts)
    ylo, yhi = min(0.0, min(p[1] for p in pts)), max(p[1] for p in pts)
    xt, yt = nice_ticks(xlo, xhi), nice_ticks(ylo, yhi)
    xlo, xhi = min(xlo, xt[0]), max(xhi, xt[-1])
    ylo, yhi = min(ylo, yt[0]), max(yhi, yt[-1])
    sx = lambda x: L + (x - xlo) / ((xhi - xlo) or 1) * (W - L - R)
    sy = lambda y: H - B - (y - ylo) / ((yhi - ylo) or 1) * (H - T - B)
    g = []
    for t in yt:
        g.append(f'<line class="grid" x1="{L}" x2="{W - R}" y1="{sy(t):.1f}" y2="{sy(t):.1f}"/>'
                 f'<text class="tick" x="{L - 6}" y="{sy(t) + 4:.1f}" text-anchor="end">{human(t)}</text>')
    for t in xt:
        g.append(f'<text class="tick" x="{sx(t):.1f}" y="{H - B + 16}" text-anchor="middle">{human(t)}</text>')
    path = ' '.join(f'{"M" if i == 0 else "L"}{sx(x):.1f},{sy(y):.1f}' for i, (x, y) in enumerate(pts))
    last = pts[-1]
    b, a = slope([p[0] for p in pts], [p[1] for p in pts])
    sub = f'slope {human(b)} per {xname}' if b is not None else ''
    data = json.dumps([[round(sx(x), 1), round(sy(y), 1), human(x), human(y)] for x, y in pts])
    rows = ''.join(f'<tr><td>{human(x)}</td><td>{human(y)}</td></tr>' for x, y in pts)
    return f'''<figure class="card">
<figcaption><b>{html.escape(yname)}</b><span>{html.escape(sub)}</span></figcaption>
<svg viewBox="0 0 {W} {H}" data-pts='{data}' data-x="{html.escape(xname)}" data-y="{html.escape(yname)}" role="img"
 aria-label="{html.escape(yname)} against {html.escape(xname)}">
{''.join(g)}
<line class="axis" x1="{L}" x2="{W - R}" y1="{H - B}" y2="{H - B}"/>
<path class="line" d="{path}"/>
<circle class="dot" cx="{sx(last[0]):.1f}" cy="{sy(last[1]):.1f}" r="4"/>
<line class="xhair" x1="0" x2="0" y1="{T}" y2="{H - B}" visibility="hidden"/>
<circle class="hov" r="4" visibility="hidden"/>
</svg>
<details><summary>Table</summary><table><tr><th>{html.escape(xname)}</th><th>{html.escape(yname)}</th></tr>{rows}</table></details>
</figure>'''


PAGE = '''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title><style>
.viz-root{{color-scheme:light;--surface-1:#fcfcfb;--surface-2:#f1f0ec;--text-primary:#0b0b0b;--text-secondary:#52514e;--grid:#e3e2dd;--series-1:#2a78d6}}
@media (prefers-color-scheme:dark){{:root:where(:not([data-theme="light"])) .viz-root{{color-scheme:dark;--surface-1:#1a1a19;--surface-2:#252524;--text-primary:#fff;--text-secondary:#c3c2b7;--grid:#3a3a38;--series-1:#3987e5}}}}
:root[data-theme="dark"] .viz-root{{color-scheme:dark;--surface-1:#1a1a19;--surface-2:#252524;--text-primary:#fff;--text-secondary:#c3c2b7;--grid:#3a3a38;--series-1:#3987e5}}
body{{margin:0;background:var(--surface-1)}}
.viz-root{{background:var(--surface-1);color:var(--text-primary);font:13px/1.4 system-ui,sans-serif;padding:16px}}
h1{{font-size:17px;margin:0 0 4px}} p{{color:var(--text-secondary);margin:0 0 12px}}
.grid-wrap{{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:12px}}
.card{{margin:0;background:var(--surface-2);border-radius:8px;padding:10px;position:relative}}
figcaption{{display:flex;justify-content:space-between;gap:8px;font-size:12px}} figcaption span{{color:var(--text-secondary)}}
svg{{width:100%;height:auto;display:block}} .grid{{stroke:var(--grid);stroke-width:1}} .axis{{stroke:var(--text-secondary);stroke-width:1}}
.tick{{fill:var(--text-secondary);font-size:10px}} .line{{fill:none;stroke:var(--series-1);stroke-width:2;stroke-linejoin:round;stroke-linecap:round}}
.dot,.hov{{fill:var(--series-1);stroke:var(--surface-2);stroke-width:2}} .xhair{{stroke:var(--text-secondary);stroke-width:1}}
details{{font-size:11px;color:var(--text-secondary)}} table{{border-collapse:collapse}} td,th{{padding:1px 8px;text-align:right}}
.tip{{position:absolute;pointer-events:none;background:var(--surface-1);color:var(--text-primary);border:1px solid var(--grid);border-radius:6px;padding:3px 6px;font-size:11px;white-space:nowrap}}
</style></head><body><div class="viz-root"><h1>{title}</h1><p>{sub}</p><div class="grid-wrap">{charts}</div></div>
<script>
document.querySelectorAll('svg[data-pts]').forEach(function(svg){{
  var pts=JSON.parse(svg.dataset.pts),card=svg.parentNode,tip=document.createElement('div');tip.className='tip';tip.hidden=true;card.appendChild(tip);
  var xh=svg.querySelector('.xhair'),hv=svg.querySelector('.hov');
  svg.addEventListener('mousemove',function(e){{var r=svg.getBoundingClientRect(),vb=svg.viewBox.baseVal,x=(e.clientX-r.left)/r.width*vb.width,b=pts[0];
    pts.forEach(function(p){{if(Math.abs(p[0]-x)<Math.abs(b[0]-x))b=p;}});
    xh.setAttribute('x1',b[0]);xh.setAttribute('x2',b[0]);xh.setAttribute('visibility','visible');hv.setAttribute('cx',b[0]);hv.setAttribute('cy',b[1]);hv.setAttribute('visibility','visible');
    tip.textContent=svg.dataset.x+' '+b[2]+' · '+svg.dataset.y+' '+b[3];tip.hidden=false;tip.style.left=(b[0]/vb.width*r.width+12)+'px';tip.style.top=(b[1]/vb.height*r.height+20)+'px';}});
  svg.addEventListener('mouseleave',function(){{xh.setAttribute('visibility','hidden');hv.setAttribute('visibility','hidden');tip.hidden=true;}});
}});
</script></body></html>'''


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('csv')
    ap.add_argument('--x', default='')
    ap.add_argument('--metrics', default='')
    ap.add_argument('--out', default='')
    ap.add_argument('--title', default='')
    ap.add_argument('--slopes', action='store_true')
    a = ap.parse_args()
    rows = load(a.csv)
    if not rows:
        sys.exit('no rows')
    cols = list(rows[0].keys())
    # Sentinels are not values: the metadata tier writes -1 for a size it did
    # not warm (and 0 for the dominant partition's measurements then), and a
    # negative latency or count never happens.
    for r in rows:
        for k, v in r.items():
            if v is not None and v < 0:
                r[k] = None
        if 'dom_ok' in r and not r['dom_ok']:
            for k in r:
                if k.startswith('dom_') and k not in ('dom_ok', 'dom_accel_est'):
                    r[k] = None
        if 'handles_after_dom_warm' in r and not r.get('dom_ok'):
            r['handles_after_dom_warm'] = None
    kind = 'meta' if 'tier_tb' in cols else 'sdn' if 'call_p50_ms' in cols else 'engine'
    x = a.x or ('tier_tb' if kind == 'meta' else 'records')
    metrics = [m for m in (a.metrics.split(',') if a.metrics else DEFAULT_METRICS[kind]) if m in cols and m != x]
    xs = [r[x] for r in rows]
    if a.slopes:
        print(f'| metric | slope per {x} | first | last |')
        print('|---|---|---|---|')
        for m in metrics:
            ys = [r[m] for r in rows]
            b, _ = slope(xs, ys)
            vals = [v for v in ys if v is not None]
            print(f'| {m} | {human(b)} | {human(vals[0]) if vals else "n/a"} | {human(vals[-1]) if vals else "n/a"} |')
    out = a.out or a.csv.rsplit('.', 1)[0] + '.html'
    charts = ''.join(chart(x, m, xs, [r[m] for r in rows], i) for i, m in enumerate(metrics))
    title = a.title or a.csv.rsplit('/', 1)[-1]
    with open(out, 'w') as f:
        f.write(PAGE.format(title=html.escape(title), charts=charts,
                            sub=html.escape(f'{len(rows)} samples; x = {x}; slope = least squares over every sample')))
    print(f'wrote {out}', file=sys.stderr)


if __name__ == '__main__':
    main()
