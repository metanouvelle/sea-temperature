/**
 * Responsive 1-year SST history + 7-day forecast chart.
 */

const BAND_COLORS = [
  { max: 16, color: "#2563eb" },
  { max: 20, color: "#0ea5e9" },
  { max: 25, color: "#22a866" },
  { max: Infinity, color: "#d97706" },
];

function bandColor(sst) {
  return (BAND_COLORS.find(b => sst < b.max) || BAND_COLORS.at(-1)).color;
}

export class SSTHistoryChart {
  constructor(container) {
    this.container = container;
    this.canvas = null;
    this.tooltip = null;
    this.data = null;
    this.layout = null;
    this._onMove = this._onPointerMove.bind(this);
    this._onLeave = this._onPointerLeave.bind(this);
    this._onResize = this._onResize.bind(this);
  }

  async load(lat, lon) {
    this._showSkeleton();
    try {
      const res = await fetch(`/api/sst-history?lat=${lat}&lon=${lon}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      this.data = await res.json();
      if (!this.data?.history?.length && !this.data?.forecast?.length) throw new Error("No temperature history available");
    } catch (err) {
      this._showError(err.message);
      return;
    }

    this._build();
    this._draw();
    window.addEventListener("resize", this._onResize);
  }

  destroy() {
    window.removeEventListener("resize", this._onResize);
    if (this.canvas) {
      this.canvas.removeEventListener("pointermove", this._onMove);
      this.canvas.removeEventListener("pointerleave", this._onLeave);
    }
    this.container.innerHTML = "";
  }

  _build() {
    this.container.innerHTML = "";

    const canvas = document.createElement("canvas");
    canvas.style.cssText = "width:100%;height:220px;cursor:crosshair;display:block;touch-action:pan-y";
    canvas.addEventListener("pointermove", this._onMove);
    canvas.addEventListener("pointerleave", this._onLeave);
    this.canvas = canvas;
    this.container.appendChild(canvas);

    this.container.style.position = "relative";
    const tip = document.createElement("div");
    tip.style.cssText = `
      position:absolute;pointer-events:none;display:none;
      background:#102033;color:#fff;font-family:Inter,ui-sans-serif,system-ui,sans-serif;
      font-size:11px;line-height:1.45;padding:7px 9px;border-radius:8px;white-space:nowrap;
      box-shadow:0 6px 18px rgba(15,35,55,.18);z-index:10;
    `;
    this.tooltip = tip;
    this.container.appendChild(tip);

    this._sizeCanvas();
  }

  _sizeCanvas() {
    const compact = window.matchMedia("(max-width: 700px)").matches;
    this.canvas.style.height = compact ? "176px" : "220px";

    const dpr = window.devicePixelRatio || 1;
    const w = this.canvas.offsetWidth || this.container.clientWidth || 600;
    const h = this.canvas.offsetHeight || (compact ? 176 : 220);
    this.canvas.width = Math.round(w * dpr);
    this.canvas.height = Math.round(h * dpr);
    const ctx = this.canvas.getContext("2d");
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    this._computeLayout(w, h, compact);
  }

  _computeLayout(w, h, compact) {
    const pad = compact
      ? { top: 24, right: 8, bottom: 30, left: 30 }
      : { top: 28, right: 14, bottom: 32, left: 36 };

    const all = [...(this.data.history || []), ...(this.data.forecast || [])];
    const allSST = all.map(p => Number(p.sst)).filter(Number.isFinite);
    const minSST = Math.floor(Math.min(...allSST)) - 1;
    const maxSST = Math.ceil(Math.max(...allSST)) + 1;
    const dates = all.map(p => p.date).filter(Boolean).sort();
    const dateMin = dates.at(0);
    const dateMax = dates.at(-1);
    const totalMs = new Date(dateMax) - new Date(dateMin) || 1;

    this.layout = {
      w, h, pad, compact, minSST, maxSST, dateMin, dateMax, totalMs,
      plotW: w - pad.left - pad.right,
      plotH: h - pad.top - pad.bottom,
    };
  }

  _draw() {
    const ctx = this.canvas.getContext("2d");
    const { w, h, pad, compact, minSST, maxSST, dateMin, totalMs, plotW, plotH } = this.layout;
    ctx.clearRect(0, 0, w, h);

    const xOf = date => pad.left + (new Date(date) - new Date(dateMin)) / totalMs * plotW;
    const yOf = sst => pad.top + (1 - (sst - minSST) / (maxSST - minSST)) * plotH;
    this._xOf = xOf;
    this._yOf = yOf;

    // Horizontal guides and temperature labels.
    ctx.lineWidth = 1;
    ctx.font = `${compact ? 9 : 10}px Inter, ui-sans-serif, system-ui, sans-serif`;
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    const step = (maxSST - minSST) <= 8 ? 2 : 3;
    for (let t = Math.ceil(minSST); t <= maxSST; t += step) {
      const y = yOf(t);
      ctx.strokeStyle = "#edf1f5";
      ctx.beginPath();
      ctx.moveTo(pad.left, y);
      ctx.lineTo(pad.left + plotW, y);
      ctx.stroke();
      ctx.fillStyle = "#8a99aa";
      ctx.fillText(`${t}°`, pad.left - 6, y);
    }

    const hist = this.data.history || [];
    const fc = this.data.forecast || [];

    // Forecast area, visually separate from observations.
    if (fc.length) {
      const start = hist.length ? xOf(hist.at(-1).date) : xOf(fc[0].date);
      ctx.fillStyle = "rgba(14,165,233,.055)";
      ctx.fillRect(start, pad.top, Math.max(0, pad.left + plotW - start), plotH);
    }

    // Subtle fill below observed history.
    if (hist.length > 1) {
      ctx.beginPath();
      hist.forEach((pt, i) => {
        const x = xOf(pt.date), y = yOf(pt.sst);
        i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
      });
      ctx.lineTo(xOf(hist.at(-1).date), pad.top + plotH);
      ctx.lineTo(xOf(hist[0].date), pad.top + plotH);
      ctx.closePath();
      const fill = ctx.createLinearGradient(0, pad.top, 0, pad.top + plotH);
      fill.addColorStop(0, "rgba(14,165,233,.13)");
      fill.addColorStop(1, "rgba(14,165,233,0)");
      ctx.fillStyle = fill;
      ctx.fill();
    }

    // Observed history line.
    if (hist.length > 1) {
      for (let i = 0; i < hist.length - 1; i++) {
        const a = hist[i], b = hist[i + 1];
        ctx.beginPath();
        ctx.moveTo(xOf(a.date), yOf(a.sst));
        ctx.lineTo(xOf(b.date), yOf(b.sst));
        ctx.strokeStyle = bandColor((a.sst + b.sst) / 2);
        ctx.lineWidth = compact ? 2 : 2.25;
        ctx.stroke();
      }
    }

    // Forecast line, including bridge from last observation.
    if (fc.length) {
      const bridge = [hist.at(-1), ...fc].filter(Boolean);
      ctx.beginPath();
      ctx.setLineDash([5, 4]);
      ctx.strokeStyle = "#0ea5e9";
      ctx.lineWidth = 2;
      bridge.forEach((pt, i) => {
        const x = xOf(pt.date), y = yOf(pt.sst);
        i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
      });
      ctx.stroke();
      ctx.setLineDash([]);
    }

    // Today divider at the observation/forecast boundary when possible.
    const boundaryPt = hist.at(-1);
    if (boundaryPt && fc.length) {
      const x = xOf(boundaryPt.date);
      ctx.strokeStyle = "#b9c5d1";
      ctx.lineWidth = 1;
      ctx.setLineDash([3, 4]);
      ctx.beginPath();
      ctx.moveTo(x, pad.top);
      ctx.lineTo(x, pad.top + plotH);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = "#718094";
      ctx.font = `${compact ? 9 : 10}px Inter, ui-sans-serif, system-ui, sans-serif`;
      ctx.textAlign = "center";
      ctx.textBaseline = "alphabetic";
      ctx.fillText("Today", Math.min(Math.max(x, pad.left + 20), pad.left + plotW - 20), h - 8);
    }

    // Sparse month labels: fewer on mobile.
    ctx.fillStyle = "#8a99aa";
    ctx.font = `${compact ? 9 : 10}px Inter, ui-sans-serif, system-ui, sans-serif`;
    ctx.textAlign = "center";
    ctx.textBaseline = "alphabetic";
    const monthFmt = new Intl.DateTimeFormat("en", { month: "short", timeZone: "UTC" });
    let cur = new Date(`${dateMin}T00:00:00Z`);
    cur.setUTCDate(1);
    let monthIndex = 0;
    const every = compact ? 3 : 2;
    while (cur <= new Date(this.layout.dateMax)) {
      const date = cur.toISOString().slice(0, 10);
      const x = xOf(date);
      if (monthIndex % every === 0 && x >= pad.left + 8 && x <= pad.left + plotW - 8) {
        ctx.fillText(monthFmt.format(cur), x, h - 8);
      }
      cur.setUTCMonth(cur.getUTCMonth() + 1);
      monthIndex += 1;
    }

    // Compact key at the top of the chart.
    ctx.textBaseline = "middle";
    ctx.textAlign = "left";
    ctx.font = `${compact ? 9 : 10}px Inter, ui-sans-serif, system-ui, sans-serif`;
    ctx.strokeStyle = "#0ea5e9";
    ctx.lineWidth = 2;
    ctx.setLineDash([]);
    ctx.beginPath(); ctx.moveTo(pad.left, 11); ctx.lineTo(pad.left + 14, 11); ctx.stroke();
    ctx.fillStyle = "#65778b"; ctx.fillText("Observed", pad.left + 19, 11);
    const keyX = pad.left + (compact ? 78 : 88);
    ctx.setLineDash([4, 3]);
    ctx.beginPath(); ctx.moveTo(keyX, 11); ctx.lineTo(keyX + 14, 11); ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillText("7-day outlook", keyX + 19, 11);
  }

  _onPointerMove(e) {
    if (!this.layout || !this.data) return;
    const rect = this.canvas.getBoundingClientRect();
    const mouseX = e.clientX - rect.left;
    const all = [
      ...(this.data.history || []).map(p => ({ ...p, type: "observed" })),
      ...(this.data.forecast || []).map(p => ({ ...p, type: "forecast" })),
    ];
    if (!all.length) return;

    let best = null;
    let bestDist = Infinity;
    for (const pt of all) {
      const d = Math.abs(this._xOf(pt.date) - mouseX);
      if (d < bestDist) { bestDist = d; best = pt; }
    }
    if (!best || bestDist > Math.max(18, this.layout.plotW / all.length * 3)) {
      this.tooltip.style.display = "none";
      return;
    }

    this._draw();
    const ctx = this.canvas.getContext("2d");
    const x = this._xOf(best.date);
    const y = this._yOf(best.sst);
    ctx.beginPath();
    ctx.arc(x, y, 4, 0, Math.PI * 2);
    ctx.fillStyle = best.type === "forecast" ? "#0ea5e9" : bandColor(best.sst);
    ctx.strokeStyle = "#fff";
    ctx.lineWidth = 1.5;
    ctx.fill();
    ctx.stroke();

    const label = new Date(`${best.date}T00:00:00Z`).toLocaleDateString("en", {
      month: "short", day: "numeric", year: "numeric", timeZone: "UTC"
    });
    this.tooltip.innerHTML = `<span style="color:#b9c5d1">${label}${best.type === "forecast" ? " · outlook" : ""}</span><br><strong style="font-size:13px">${Number(best.sst).toFixed(1)}°C</strong>`;
    this.tooltip.style.display = "block";

    const tipW = 132;
    const left = Math.min(Math.max(4, x + 10), this.layout.w - tipW - 4);
    const top = Math.max(4, Math.min(y - 12, this.layout.h - 48));
    this.tooltip.style.left = `${left}px`;
    this.tooltip.style.top = `${top}px`;
  }

  _onPointerLeave() {
    if (this.tooltip) this.tooltip.style.display = "none";
    if (this.data) this._draw();
  }

  _onResize() {
    if (!this.data) return;
    this._sizeCanvas();
    this._draw();
  }

  _showSkeleton() {
    this.container.innerHTML = `
      <div style="height:176px;display:flex;align-items:center;justify-content:center;color:#718094;font:12px Inter,ui-sans-serif,system-ui,sans-serif">
        Loading temperature history…
      </div>`;
  }

  _showError(msg) {
    this.container.innerHTML = `
      <div style="height:176px;display:flex;align-items:center;justify-content:center;padding:20px;text-align:center;color:#718094;font:12px Inter,ui-sans-serif,system-ui,sans-serif">
        Temperature history is temporarily unavailable.
      </div>`;
  }
}
