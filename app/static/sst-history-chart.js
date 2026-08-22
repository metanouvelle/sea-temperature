/**
 * Responsive 1-year SST history + 7-day outlook chart.
 * History and forecast use separate visual spans so a short forecast remains legible.
 */

const OBSERVED_COLOR = "#0ea5e9";
const FORECAST_COLOR = "#0ea5e9";

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
      if (!this.data?.history?.length && !this.data?.forecast?.length) {
        throw new Error("No temperature history available");
      }
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
    this.canvas.style.height = compact ? "168px" : "210px";
    const dpr = window.devicePixelRatio || 1;
    const w = this.canvas.offsetWidth || this.container.clientWidth || 600;
    const h = this.canvas.offsetHeight || (compact ? 168 : 210);
    this.canvas.width = Math.round(w * dpr);
    this.canvas.height = Math.round(h * dpr);
    this.canvas.getContext("2d").setTransform(dpr, 0, 0, dpr, 0, 0);
    this._computeLayout(w, h, compact);
  }

  _computeLayout(w, h, compact) {
    const pad = compact
      ? { top: 24, right: 8, bottom: 28, left: 30 }
      : { top: 28, right: 14, bottom: 30, left: 36 };
    const hist = this.data.history || [];
    const fc = this.data.forecast || [];
    const allSST = [...hist, ...fc].map(p => Number(p.sst)).filter(Number.isFinite);
    const minSST = Math.floor(Math.min(...allSST)) - 1;
    const maxSST = Math.ceil(Math.max(...allSST)) + 1;
    const plotW = w - pad.left - pad.right;
    const plotH = h - pad.top - pad.bottom;

    // Reserve enough room for seven forecast days to be readable on every viewport.
    const forecastRatio = fc.length ? (compact ? 0.17 : 0.13) : 0;
    const gap = fc.length ? (compact ? 5 : 7) : 0;
    const historyW = Math.max(1, plotW * (1 - forecastRatio) - gap);
    const forecastW = Math.max(1, plotW - historyW - gap);

    this.layout = { w, h, pad, compact, minSST, maxSST, plotW, plotH, historyW, forecastW, gap };
  }

  _xForPoint(pt, type) {
    const { pad, historyW, forecastW, gap } = this.layout;
    const hist = this.data.history || [];
    const fc = this.data.forecast || [];
    if (type === "forecast") {
      const idx = Math.max(0, fc.findIndex(p => p.date === pt.date));
      const denom = Math.max(1, fc.length - 1);
      return pad.left + historyW + gap + (idx / denom) * forecastW;
    }
    const idx = Math.max(0, hist.findIndex(p => p.date === pt.date));
    const denom = Math.max(1, hist.length - 1);
    return pad.left + (idx / denom) * historyW;
  }

  _yOf(sst) {
    const { pad, minSST, maxSST, plotH } = this.layout;
    return pad.top + (1 - (sst - minSST) / (maxSST - minSST)) * plotH;
  }

  _draw() {
    const ctx = this.canvas.getContext("2d");
    const { w, h, pad, compact, minSST, maxSST, plotW, plotH, historyW, forecastW, gap } = this.layout;
    const hist = this.data.history || [];
    const fc = this.data.forecast || [];
    ctx.clearRect(0, 0, w, h);

    ctx.lineWidth = 1;
    ctx.font = `${compact ? 9 : 10}px Inter,ui-sans-serif,system-ui,sans-serif`;
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    const step = (maxSST - minSST) <= 8 ? 2 : 3;
    for (let t = Math.ceil(minSST); t <= maxSST; t += step) {
      const y = this._yOf(t);
      ctx.strokeStyle = "#edf1f5";
      ctx.beginPath();
      ctx.moveTo(pad.left, y);
      ctx.lineTo(pad.left + plotW, y);
      ctx.stroke();
      ctx.fillStyle = "#8a99aa";
      ctx.fillText(`${t}°`, pad.left - 6, y);
    }

    if (fc.length) {
      const forecastStart = pad.left + historyW + gap;
      ctx.fillStyle = "rgba(14,165,233,.055)";
      ctx.fillRect(forecastStart, pad.top, forecastW, plotH);
      ctx.strokeStyle = "#b9c5d1";
      ctx.setLineDash([3, 4]);
      ctx.beginPath();
      ctx.moveTo(forecastStart - gap / 2, pad.top);
      ctx.lineTo(forecastStart - gap / 2, pad.top + plotH);
      ctx.stroke();
      ctx.setLineDash([]);
    }

    if (hist.length > 1) {
      ctx.beginPath();
      hist.forEach((pt, i) => {
        const x = this._xForPoint(pt, "observed");
        const y = this._yOf(pt.sst);
        i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
      });
      ctx.lineTo(this._xForPoint(hist.at(-1), "observed"), pad.top + plotH);
      ctx.lineTo(this._xForPoint(hist[0], "observed"), pad.top + plotH);
      ctx.closePath();
      const fill = ctx.createLinearGradient(0, pad.top, 0, pad.top + plotH);
      fill.addColorStop(0, "rgba(14,165,233,.12)");
      fill.addColorStop(1, "rgba(14,165,233,0)");
      ctx.fillStyle = fill;
      ctx.fill();

      ctx.beginPath();
      hist.forEach((pt, i) => {
        const x = this._xForPoint(pt, "observed");
        const y = this._yOf(pt.sst);
        i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
      });
      ctx.strokeStyle = OBSERVED_COLOR;
      ctx.lineWidth = compact ? 2 : 2.25;
      ctx.stroke();
    }

    if (fc.length) {
      const points = [];
      if (hist.length) points.push({ ...hist.at(-1), _type: "observed" });
      points.push(...fc.map(p => ({ ...p, _type: "forecast" })));
      ctx.beginPath();
      ctx.setLineDash([5, 4]);
      ctx.strokeStyle = FORECAST_COLOR;
      ctx.lineWidth = 2;
      points.forEach((pt, i) => {
        const x = this._xForPoint(pt, pt._type);
        const y = this._yOf(pt.sst);
        i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
      });
      ctx.stroke();
      ctx.setLineDash([]);
    }

    // History month labels only; forecast gets its own compact label.
    if (hist.length) {
      ctx.fillStyle = "#8a99aa";
      ctx.font = `${compact ? 9 : 10}px Inter,ui-sans-serif,system-ui,sans-serif`;
      ctx.textAlign = "center";
      ctx.textBaseline = "alphabetic";
      const monthFmt = new Intl.DateTimeFormat("en", { month: "short", timeZone: "UTC" });
      const every = compact ? 3 : 2;
      let lastMonth = -1;
      let visibleMonth = 0;
      hist.forEach((pt, idx) => {
        const d = new Date(`${pt.date}T00:00:00Z`);
        const month = d.getUTCMonth();
        if (month === lastMonth) return;
        lastMonth = month;
        if (visibleMonth++ % every !== 0) return;
        const x = this._xForPoint(pt, "observed");
        if (x > pad.left + 10 && x < pad.left + historyW - 12) {
          ctx.fillText(monthFmt.format(d), x, h - 7);
        }
      });
    }

    if (fc.length) {
      const forecastCenter = pad.left + historyW + gap + forecastW / 2;
      ctx.fillStyle = "#718094";
      ctx.textAlign = "center";
      ctx.font = `${compact ? 9 : 10}px Inter,ui-sans-serif,system-ui,sans-serif`;
      ctx.fillText("7 days", forecastCenter, h - 7);
    }

    // Compact key.
    ctx.textBaseline = "middle";
    ctx.textAlign = "left";
    ctx.font = `${compact ? 9 : 10}px Inter,ui-sans-serif,system-ui,sans-serif`;
    ctx.strokeStyle = OBSERVED_COLOR;
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
    const points = [
      ...(this.data.history || []).map(p => ({ ...p, type: "observed" })),
      ...(this.data.forecast || []).map(p => ({ ...p, type: "forecast" })),
    ];
    let best = null;
    let bestDist = Infinity;
    for (const pt of points) {
      const d = Math.abs(this._xForPoint(pt, pt.type) - mouseX);
      if (d < bestDist) { bestDist = d; best = pt; }
    }
    if (!best || bestDist > 24) {
      this.tooltip.style.display = "none";
      this._draw();
      return;
    }

    this._draw();
    const ctx = this.canvas.getContext("2d");
    const x = this._xForPoint(best, best.type);
    const y = this._yOf(best.sst);
    ctx.beginPath();
    ctx.arc(x, y, 4, 0, Math.PI * 2);
    ctx.fillStyle = best.type === "forecast" ? FORECAST_COLOR : OBSERVED_COLOR;
    ctx.strokeStyle = "#fff";
    ctx.lineWidth = 1.5;
    ctx.fill();
    ctx.stroke();

    const label = new Date(`${best.date}T00:00:00Z`).toLocaleDateString("en", {
      month: "short", day: "numeric", year: "numeric", timeZone: "UTC"
    });
    const kind = best.type === "forecast" ? "7-day outlook" : "Observed";
    this.tooltip.innerHTML = `<span style="color:#b9c5d1">${label}</span><br><strong style="font-size:13px">${Number(best.sst).toFixed(1)}°C</strong> <span style="color:#b9c5d1">· ${kind}</span>`;
    this.tooltip.style.display = "block";
    const tipW = 160;
    this.tooltip.style.left = `${Math.min(Math.max(4, x + 10), this.layout.w - tipW - 4)}px`;
    this.tooltip.style.top = `${Math.max(4, Math.min(y - 12, this.layout.h - 48))}px`;
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
    this.container.innerHTML = `<div style="height:168px;display:flex;align-items:center;justify-content:center;color:#718094;font:12px Inter,ui-sans-serif,system-ui,sans-serif">Loading temperature history…</div>`;
  }

  _showError() {
    this.container.innerHTML = `<div style="height:168px;display:flex;align-items:center;justify-content:center;padding:20px;text-align:center;color:#718094;font:12px Inter,ui-sans-serif,system-ui,sans-serif">Temperature history is temporarily unavailable.</div>`;
  }
}
