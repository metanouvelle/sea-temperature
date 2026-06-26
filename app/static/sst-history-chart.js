/**
 * sst-history-chart.js
 * Interactive 1-year SST history + 7-day forecast chart.
 * Drop this file into app/static/ and load it in sea-temp-map template.
 *
 * Usage:
 *   const chart = new SSTHistoryChart(containerEl);
 *   await chart.load(lat, lon);
 *   chart.destroy(); // cleanup
 */

const BAND_COLORS = [
  { max: 17,   color: "#2563eb", label: "Cold"      },
  { max: 20,   color: "#0ea5e9", label: "Brave"     },
  { max: 23,   color: "#22c55e", label: "Nice"      },
  { max: 26,   color: "#f59e0b", label: "Perfect"   },
  { max: Infinity, color: "#ef4444", label: "Warm"  },
];

function bandColor(sst) {
  return (BAND_COLORS.find(b => sst < b.max) || BAND_COLORS.at(-1)).color;
}

export class SSTHistoryChart {
  constructor(container) {
    this.container = container;
    this.canvas    = null;
    this.tooltip   = null;
    this._onMove   = this._onMouseMove.bind(this);
    this._onLeave  = this._onMouseLeave.bind(this);
    this._onResize = this._onResize.bind(this);
    this.data      = null;   // { history, forecast }
    this.layout    = null;   // computed on each draw
  }

  // ── Public API ────────────────────────────────────────────────────────────

  async load(lat, lon) {
    this._showSkeleton();

    const url = `/api/sst-history?lat=${lat}&lon=${lon}`;
    let json;
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      json = await res.json();
    } catch (err) {
      this._showError(err.message);
      return;
    }

    this.data = json;
    this._build();
    this._draw();
    window.addEventListener("resize", this._onResize);
  }

  destroy() {
    window.removeEventListener("resize", this._onResize);
    if (this.canvas) {
      this.canvas.removeEventListener("mousemove", this._onMove);
      this.canvas.removeEventListener("mouseleave", this._onLeave);
    }
    this.container.innerHTML = "";
  }

  // ── Build DOM ─────────────────────────────────────────────────────────────

  _build() {
    this.container.innerHTML = "";

    // Canvas
    const canvas = document.createElement("canvas");
    canvas.style.cssText = "width:100%;height:200px;cursor:crosshair;display:block";
    canvas.addEventListener("mousemove", this._onMove);
    canvas.addEventListener("mouseleave", this._onLeave);
    this.canvas = canvas;
    this.container.appendChild(canvas);

    // Tooltip (absolute inside container)
    this.container.style.position = "relative";
    const tip = document.createElement("div");
    tip.style.cssText = `
      position:absolute;pointer-events:none;display:none;
      background:rgba(6,15,26,0.92);color:#e2e8f0;
      font-family:'DM Mono',monospace;font-size:11px;line-height:1.5;
      padding:6px 10px;border-radius:6px;white-space:nowrap;
      border:1px solid rgba(56,189,248,0.3);box-shadow:0 4px 12px rgba(0,0,0,0.4);
      z-index:10;
    `;
    this.tooltip = tip;
    this.container.appendChild(tip);

    this._sizeCanvas();
  }

  _sizeCanvas() {
    const dpr = window.devicePixelRatio || 1;
    const w   = this.canvas.offsetWidth  || this.container.clientWidth  || 600;
    const h   = this.canvas.offsetHeight || 200;
    this.canvas.width  = w * dpr;
    this.canvas.height = h * dpr;
    this.canvas.getContext("2d").scale(dpr, dpr);
    this._computeLayout(w, h);
  }

  _computeLayout(w, h) {
    const pad = { top: 16, right: 20, bottom: 40, left: 44 };
    const allSST = [
      ...this.data.history.map(p => p.sst),
      ...this.data.forecast.map(p => p.sst),
    ];
    const minSST = Math.floor(Math.min(...allSST)) - 1;
    const maxSST = Math.ceil(Math.max(...allSST))  + 1;

    const allDates = [
      ...this.data.history.map(p => p.date),
      ...this.data.forecast.map(p => p.date),
    ].sort();

    const dateMin = allDates.at(0);
    const dateMax = allDates.at(-1);
    const totalMs = new Date(dateMax) - new Date(dateMin) || 1;

    const plotW = w - pad.left - pad.right;
    const plotH = h - pad.top  - pad.bottom;

    this.layout = { w, h, pad, minSST, maxSST, dateMin, dateMax, totalMs, plotW, plotH };
  }

  // ── Draw ──────────────────────────────────────────────────────────────────

  _draw() {
    const ctx = this.canvas.getContext("2d");
    const { w, h, pad, minSST, maxSST, dateMin, dateMax, totalMs, plotW, plotH } = this.layout;

    ctx.clearRect(0, 0, w, h);

    const xOf = date => pad.left + (new Date(date) - new Date(dateMin)) / totalMs * plotW;
    const yOf = sst  => pad.top  + (1 - (sst - minSST) / (maxSST - minSST)) * plotH;

    // ── Grid lines + Y labels ─────────────────────────────────────────────
    ctx.strokeStyle = "rgba(255,255,255,0.07)";
    ctx.lineWidth   = 1;
    ctx.fillStyle   = "rgba(255,255,255,0.35)";
    ctx.font        = "10px 'DM Mono', monospace";
    ctx.textAlign   = "right";

    const step = (maxSST - minSST) <= 8 ? 1 : 2;
    for (let t = Math.ceil(minSST); t <= maxSST; t += step) {
      const y = yOf(t);
      ctx.beginPath();
      ctx.moveTo(pad.left, y);
      ctx.lineTo(pad.left + plotW, y);
      ctx.stroke();
      ctx.fillText(`${t}°`, pad.left - 6, y + 3.5);
    }

    // ── Today line ────────────────────────────────────────────────────────
    const todayStr = new Date().toISOString().slice(0, 10);
    const todayX   = xOf(todayStr);
    if (todayX >= pad.left && todayX <= pad.left + plotW) {
      ctx.strokeStyle = "#38bdf8";
      ctx.lineWidth   = 1.5;
      ctx.setLineDash([4, 3]);
      ctx.beginPath();
      ctx.moveTo(todayX, pad.top);
      ctx.lineTo(todayX, pad.top + plotH);
      ctx.stroke();
      ctx.setLineDash([]);

      ctx.fillStyle   = "#38bdf8";
      ctx.font        = "9px 'DM Mono', monospace";
      ctx.textAlign   = "center";
      ctx.fillText("today", todayX, pad.top + plotH + 14);
    }

    // ── X-axis month ticks ────────────────────────────────────────────────
    ctx.fillStyle   = "rgba(255,255,255,0.3)";
    ctx.font        = "10px 'DM Mono', monospace";
    ctx.textAlign   = "center";

    const monthsFmt = new Intl.DateTimeFormat("en", { month: "short" });
    let cur = new Date(dateMin);
    cur.setUTCDate(1);
    while (cur <= new Date(dateMax)) {
      const label = monthsFmt.format(cur);
      const x = xOf(cur.toISOString().slice(0, 10));
      if (x >= pad.left && x <= pad.left + plotW) {
        ctx.fillText(label, x, pad.top + plotH + 28);
      }
      cur.setUTCMonth(cur.getUTCMonth() + 1);
    }

    // ── Colour-banded fill under history line ─────────────────────────────
    const hist = this.data.history;
    if (hist.length > 1) {
      for (let i = 0; i < hist.length - 1; i++) {
        const x0 = xOf(hist[i].date),   y0 = yOf(hist[i].sst);
        const x1 = xOf(hist[i+1].date), y1 = yOf(hist[i+1].sst);
        const base = pad.top + plotH;

        const grad = ctx.createLinearGradient(0, y0, 0, base);
        const col  = bandColor((hist[i].sst + hist[i+1].sst) / 2);
        grad.addColorStop(0,   col + "55");
        grad.addColorStop(1,   col + "00");

        ctx.beginPath();
        ctx.moveTo(x0, base);
        ctx.lineTo(x0, y0);
        ctx.lineTo(x1, y1);
        ctx.lineTo(x1, base);
        ctx.closePath();
        ctx.fillStyle = grad;
        ctx.fill();
      }
    }

    // ── History line (solid, colour-segmented) ────────────────────────────
    if (hist.length > 1) {
      for (let i = 0; i < hist.length - 1; i++) {
        const x0 = xOf(hist[i].date),   y0 = yOf(hist[i].sst);
        const x1 = xOf(hist[i+1].date), y1 = yOf(hist[i+1].sst);
        ctx.beginPath();
        ctx.moveTo(x0, y0);
        ctx.lineTo(x1, y1);
        ctx.strokeStyle = bandColor((hist[i].sst + hist[i+1].sst) / 2);
        ctx.lineWidth   = 2;
        ctx.stroke();
      }
    }

    // ── Forecast line (dashed, accent colour) ─────────────────────────────
    const fc = this.data.forecast;
    if (fc.length > 1) {
      // Bridge: connect last history point to first forecast point
      const bridge = [hist.at(-1), ...fc].filter(Boolean);
      ctx.beginPath();
      ctx.setLineDash([5, 4]);
      ctx.strokeStyle = "#38bdf8";
      ctx.lineWidth   = 2;
      bridge.forEach((pt, i) => {
        const x = xOf(pt.date), y = yOf(pt.sst);
        i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
      });
      ctx.stroke();
      ctx.setLineDash([]);
    }

    // ── Legend ────────────────────────────────────────────────────────────
    const legendX = pad.left + plotW - 2;
    ctx.textAlign = "right";
    ctx.font      = "9px 'DM Mono', monospace";

    // Solid line legend
    ctx.strokeStyle = "#94a3b8";
    ctx.lineWidth   = 2;
    ctx.setLineDash([]);
    ctx.beginPath();
    ctx.moveTo(legendX - 54, pad.top + 6);
    ctx.lineTo(legendX - 40, pad.top + 6);
    ctx.stroke();
    ctx.fillStyle = "rgba(255,255,255,0.45)";
    ctx.fillText("observed", legendX, pad.top + 10);

    // Dashed line legend
    ctx.strokeStyle = "#38bdf8";
    ctx.setLineDash([4, 3]);
    ctx.beginPath();
    ctx.moveTo(legendX - 54, pad.top + 20);
    ctx.lineTo(legendX - 40, pad.top + 20);
    ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = "#38bdf8";
    ctx.fillText("7-day fcst", legendX, pad.top + 24);

    // Store for hit-testing
    this._xOf = xOf;
    this._yOf = yOf;
  }

  // ── Interaction ───────────────────────────────────────────────────────────

  _onMouseMove(e) {
    if (!this.layout || !this.data) return;
    const rect     = this.canvas.getBoundingClientRect();
    const mouseX   = e.clientX - rect.left;
    const { pad, plotW } = this.layout;

    // Find closest data point across history + forecast
    const all = [
      ...this.data.history.map(p => ({ ...p, type: "observed" })),
      ...this.data.forecast.map(p => ({ ...p, type: "forecast" })),
    ];

    let best = null, bestDist = Infinity;
    for (const pt of all) {
      const x = this._xOf(pt.date);
      const d = Math.abs(x - mouseX);
      if (d < bestDist) { bestDist = d; best = pt; }
    }

    if (!best || bestDist > plotW / all.length * 3) {
      this.tooltip.style.display = "none";
      return;
    }

    // Redraw with crosshair dot
    this._draw();
    const ctx = this.canvas.getContext("2d");
    const x   = this._xOf(best.date);
    const y   = this._yOf(best.sst);
    ctx.beginPath();
    ctx.arc(x, y, 4, 0, Math.PI * 2);
    ctx.fillStyle   = best.type === "forecast" ? "#38bdf8" : bandColor(best.sst);
    ctx.strokeStyle = "#fff";
    ctx.lineWidth   = 1.5;
    ctx.fill();
    ctx.stroke();

    // Position tooltip
    const tip = this.tooltip;
    const label = new Date(best.date + "T00:00:00Z").toLocaleDateString("en", {
      month: "short", day: "numeric", year: "numeric", timeZone: "UTC"
    });
    const flag  = best.type === "forecast" ? " (forecast)" : "";
    tip.innerHTML = `<span style="color:#94a3b8">${label}${flag}</span><br>
      <span style="color:${best.type === "forecast" ? "#38bdf8" : bandColor(best.sst)};font-size:13px;font-weight:700">${best.sst.toFixed(1)} °C</span>`;
    tip.style.display = "block";

    // Keep tooltip inside container
    const tipW = 130;
    const left = Math.min(x + 12, (this.layout.w - tipW - 4));
    tip.style.left = `${left}px`;
    tip.style.top  = `${y - 10}px`;
  }

  _onMouseLeave() {
    this.tooltip.style.display = "none";
    this._draw();
  }

  _onResize() {
    if (!this.data) return;
    this._sizeCanvas();
    this._draw();
  }

  // ── Loading / error states ────────────────────────────────────────────────

  _showSkeleton() {
    this.container.innerHTML = `
      <div style="height:200px;display:flex;align-items:center;justify-content:center;
                  background:rgba(255,255,255,0.03);border-radius:8px;border:1px solid rgba(255,255,255,0.08)">
        <span style="color:#38bdf8;font-family:'DM Mono',monospace;font-size:12px;
                     opacity:0.7;animation:pulse 1.5s ease-in-out infinite">
          Loading temperature history…
        </span>
      </div>
      <style>
        @keyframes pulse { 0%,100%{opacity:.4} 50%{opacity:1} }
      </style>`;
  }

  _showError(msg) {
    this.container.innerHTML = `
      <div style="height:200px;display:flex;align-items:center;justify-content:center;
                  background:rgba(255,255,255,0.03);border-radius:8px;border:1px solid rgba(239,68,68,0.2)">
        <span style="color:#ef4444;font-family:'DM Mono',monospace;font-size:11px">
          Could not load history: ${msg}
        </span>
      </div>`;
  }
}
