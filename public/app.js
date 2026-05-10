/* ============================================================
   app.js — Revenue Analytics Dashboard
   UI is a renderer only. Zero KPI math here.
   All business logic lives in the semantic API.
   ============================================================ */

'use strict';

// ── Constants ────────────────────────────────────────────────
const API = {
  HEALTH:   '/api/health',
  REGISTRY: '/api/registry',
  META:     '/api/meta',
  CUSTOMERS:'/api/customers',
  QUERY:    '/api/query',
  ORDERS:   '/api/orders',
};

const PAGE_SIZE = 50;
const CHART_COLORS = {
  revenue: { line: '#8b5cf6', fill: 'rgba(139,92,246,0.40)' },
  profit:  { line: '#14b8a6', fill: 'rgba(20,184,166,0.40)' },
  region:  ['#8b5cf6','#14b8a6','#3b82f6','#f59e0b','#f43f5e','#10b981'],
  margin:  { line: '#3b82f6', fill: 'rgba(59,130,246,0.40)' },
};

// ── App State ────────────────────────────────────────────────
const state = {
  dateStart: '',
  dateEnd:   '',
  regions:   [],          // selected canonical region names
  customerIds: [],        // selected customer_id values
  tableStatus: '',        // order_status filter (table only)
  tablePage: 1,
  tableRows: [],
  metricRegistry: {},     // metric_id -> definition object
  charts: {},             // chart instances keyed by id
  customerMap: {},        // customer_id -> customer_name
  sourceMode: 'sql',      // 'sql' or 'csv'
};
// ── URL State Management ──────────────────────────────────────
function syncURLToState() {
  const params = new URLSearchParams(window.location.search);
  if (params.has('start')) state.dateStart = params.get('start');
  if (params.has('end'))   state.dateEnd   = params.get('end');
  if (params.has('region')) state.regions  = params.get('region').split(',').filter(Boolean);
  if (params.has('customer')) state.customerIds = params.get('customer').split(',').filter(Boolean);
}

function syncStateToURL() {
  const params = new URLSearchParams();
  if (state.dateStart) params.set('start', state.dateStart);
  if (state.dateEnd)   params.set('end', state.dateEnd);
  if (state.regions.length) params.set('region', state.regions.join(','));
  if (state.customerIds.length) params.set('customer', state.customerIds.join(','));
  
  const newURL = window.location.pathname + '?' + params.toString();
  window.history.replaceState({ path: newURL }, '', newURL);
}

// ── Helpers ──────────────────────────────────────────────────
function fmt(value, type) {
  if (value === null || value === undefined) return '—';
  if (type === 'currency') {
    return '$' + Number(value).toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
  }
  if (type === 'percentage') {
    return Number(value).toFixed(2) + '%';
  }
  return String(value);
}

function fmtDelta(delta, deltaUnit, type) {
  if (delta === null || delta === undefined) return null;
  const sign   = delta >= 0 ? '+' : '';
  const cls    = delta >= 0 ? 'delta-positive' : 'delta-negative';
  const arrow  = delta >= 0 ? '▲' : '▼';
  let display;
  if (type === 'currency') {
    display = sign + '$' + Math.abs(delta).toLocaleString('en-US', { maximumFractionDigits: 0 });
  } else {
    display = sign + Number(delta).toFixed(2) + (deltaUnit === 'pp' ? ' pp' : '%');
  }
  return { html: `<span class="${cls}">${arrow} ${display} vs prev period</span>`, cls };
}

function nullSafe(v) { return (v === null || v === undefined || v === '') ? '—' : v; }

function showLoading(id, show) {
  const el = document.getElementById('loading-' + id);
  if (el) el.style.display = show ? 'flex' : 'none';
}

function showEmpty(id, show) {
  const el = document.getElementById('empty-' + id);
  if (el) { el.hidden = !show; }
}

// ── Toast ─────────────────────────────────────────────────────
function toast(message, type = 'error') {
  const container = document.getElementById('toast-container');
  const t = document.createElement('div');
  t.className = `toast toast-${type}`;
  t.innerHTML = `<span>${message}</span><button class="toast-dismiss" aria-label="Dismiss">✕</button>`;
  t.querySelector('.toast-dismiss').onclick = () => removeToast(t);
  container.appendChild(t);
  setTimeout(() => removeToast(t), 7000);
}

function removeToast(el) {
  el.style.animation = 'toast-out 0.25s ease forwards';
  setTimeout(() => el.remove(), 250);
}

// ── API fetch wrapper ─────────────────────────────────────────
async function apiFetch(url, options = {}) {
  // Add source mode to headers
  const headers = { 
    ...options.headers, 
    'X-Source-Mode': state.sourceMode 
  };
  const res = await fetch(url, { ...options, headers });
  const data = await res.json();
  if (!res.ok) throw new Error(data?.error?.message || `API error ${res.status}`);
  return data;
}

// ── Build standard query payload ─────────────────────────────
function buildQueryPayload(metricId, groupBy = 'none', timeGrain = 'month') {
  return {
    metric_id: metricId,
    filters: {
      date_range: { start: state.dateStart, end: state.dateEnd },
      region: state.regions,
      customer_id: state.customerIds,
      order_status: [],
    },
    group_by: groupBy,
    time_grain: timeGrain,
    comparison: 'previous_period',
  };
}

// ── Chart.js helpers ──────────────────────────────────────────
const chartDefaults = {
  responsive: true,
  maintainAspectRatio: true,
  animation: { duration: 500 },
  plugins: {
    legend: { display: false },
    tooltip: {
      backgroundColor: '#1a1e2a',
      borderColor: 'rgba(255,255,255,0.08)',
      borderWidth: 1,
      titleColor: '#94a3b8',
      bodyColor: '#f1f5f9',
      padding: 10,
    },
  },
  scales: {
    x: {
      grid: { display: false },
      ticks: { color: '#475569', font: { size: 11, family: 'Plus Jakarta Sans' } },
      border: { display: false }
    },
    y: {
      grid: { display: false },
      ticks: { color: '#475569', font: { size: 11, family: 'Plus Jakarta Sans' } },
      border: { display: false }
    },
  },
};

function destroyChart(id) {
  if (state.charts[id]) { state.charts[id].destroy(); delete state.charts[id]; }
}

function renderLineChart(canvasId, labels, values, colorKey) {
  destroyChart(canvasId);
  const ctx = document.getElementById(canvasId)?.getContext('2d');
  if (!ctx) return;
  const c = CHART_COLORS[colorKey];
  const gradient = ctx.createLinearGradient(0, 0, 0, 400);
  gradient.addColorStop(0, c.fill);
  gradient.addColorStop(1, 'rgba(0,0,0,0)');
  state.charts[canvasId] = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        data: values,
        borderColor: c.line,
        backgroundColor: gradient,
        borderWidth: 2.5,
        pointRadius: 0,
        pointHoverRadius: 5,
        pointBackgroundColor: c.line,
        pointBorderColor: '#fff',
        pointBorderWidth: 2,
        fill: true,
        tension: 0.45,
      }],
    },
    options: {
      ...chartDefaults,
      plugins: {
        ...chartDefaults.plugins,
        tooltip: {
          ...chartDefaults.plugins.tooltip,
          callbacks: { label: ctx => nullSafe(ctx.parsed.y) },
        },
      },
    },
  });
}

function renderBarChart(canvasId, labels, values) {
  destroyChart(canvasId);
  const ctx = document.getElementById(canvasId)?.getContext('2d');
  if (!ctx) return;
  state.charts[canvasId] = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        data: values,
        backgroundColor: CHART_COLORS.region,
        borderRadius: 6,
        borderSkipped: false,
      }],
    },
    options: {
      ...chartDefaults,
      indexAxis: 'y',
      onClick: (e, elements) => {
        if (!elements.length) return;
        const region = labels[elements[0].index];
        const isSelected = state.regions.includes(region) && state.regions.length === 1;
        state.regions = isSelected ? [] : [region];
        
        Array.from(document.querySelectorAll('#region-dropdown input')).forEach(cb => {
          cb.checked = state.regions.includes(cb.value);
        });
        updateRegionTrigger();
        refreshAll();
      },
      plugins: {
        ...chartDefaults.plugins,
        tooltip: {
          ...chartDefaults.plugins.tooltip,
          callbacks: { label: ctx => fmt(ctx.parsed.x, 'currency') },
        },
      },
      scales: {
        x: { ...chartDefaults.scales.x, ticks: { ...chartDefaults.scales.x.ticks, callback: v => fmt(v, 'currency') } },
        y: chartDefaults.scales.y,
      },
    },
  });
}

function renderMarginChart(canvasId, series, distribution) {
  destroyChart(canvasId);
  const ctx = document.getElementById(canvasId)?.getContext('2d');
  if (!ctx) return;
  // Render as line with optional distribution annotation
  const labels = series.map(p => p.period_start);
  const values = series.map(p => p.value);
  const c = CHART_COLORS.margin;
  const gradient = ctx.createLinearGradient(0, 0, 0, 400);
  gradient.addColorStop(0, c.fill);
  gradient.addColorStop(1, 'rgba(0,0,0,0)');
  const datasets = [{
    label: 'Margin %',
    data: values,
    borderColor: c.line,
    backgroundColor: gradient,
    borderWidth: 2.5,
    pointRadius: 0,
    pointHoverRadius: 5,
    pointBackgroundColor: c.line,
    pointBorderColor: '#fff',
    pointBorderWidth: 2,
    fill: true,
    tension: 0.45,
  }];
  if (distribution) {
    datasets.push({
      label: 'Q1',
      data: labels.map(() => distribution.q1),
      borderColor: 'rgba(59,130,246,0.3)',
      borderDash: [4, 4],
      borderWidth: 1,
      pointRadius: 0,
      fill: false,
    });
    datasets.push({
      label: 'Q3',
      data: labels.map(() => distribution.q3),
      borderColor: 'rgba(59,130,246,0.3)',
      borderDash: [4, 4],
      borderWidth: 1,
      pointRadius: 0,
      fill: false,
    });
  }
  state.charts[canvasId] = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets },
    options: {
      ...chartDefaults,
      plugins: {
        ...chartDefaults.plugins,
        legend: { display: !!distribution, labels: { color: '#475569', font: { size: 11 } } },
        tooltip: {
          ...chartDefaults.plugins.tooltip,
          callbacks: { label: ctx => ctx.dataset.label + ': ' + fmt(ctx.parsed.y, 'percentage') },
        },
      },
      scales: {
        x: chartDefaults.scales.x,
        y: { ...chartDefaults.scales.y, ticks: { ...chartDefaults.scales.y.ticks, callback: v => v + '%' } },
      },
    },
  });
}

function renderSparkline(canvasId, series, colorKey) {
  destroyChart(canvasId);
  const ctx = document.getElementById(canvasId)?.getContext('2d');
  if (!ctx || !series || series.length === 0) return;
  const c = CHART_COLORS[colorKey] || CHART_COLORS.revenue;
  state.charts[canvasId] = new Chart(ctx, {
    type: 'line',
    data: {
      labels: series.map(p => p.period_start),
      datasets: [{
        data: series.map(p => p.value),
        borderColor: c.line,
        backgroundColor: c.fill,
        borderWidth: 1.5,
        pointRadius: 0,
        fill: true,
        tension: 0.4,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 300 },
      plugins: { legend: { display: false }, tooltip: { enabled: false } },
      scales: { x: { display: false }, y: { display: false } },
    },
  });
}

// ── KPI Card renderer ─────────────────────────────────────────
function setKpiLoading(loading) {
  document.querySelectorAll('.kpi-value, .kpi-delta, .kpi-sparkline-wrap').forEach(el => {
    if (loading) el.classList.add('skeleton');
    else el.classList.remove('skeleton');
  });
}

function renderKpiCard(metricId, data) {
  const idMap = {
    revenue_total_usd: { val: 'kpi-revenue-value', delta: 'kpi-revenue-delta', spark: 'sparkline-revenue', color: 'revenue', type: 'currency' },
    profit_total_usd:  { val: 'kpi-profit-value',  delta: 'kpi-profit-delta',  spark: 'sparkline-profit',  color: 'profit',  type: 'currency' },
    profit_margin_pct: { val: 'kpi-margin-value',  delta: 'kpi-margin-delta',  spark: 'sparkline-margin',  color: 'margin',  type: 'percentage' },
    revenue_growth_pct:{ val: 'kpi-growth-value',  delta: 'kpi-growth-delta',  spark: 'sparkline-growth',  color: 'revenue', type: 'percentage' },
  };
  const ids = idMap[metricId];
  if (!ids) return;
  const metaDef = state.metricRegistry[metricId] || {};
  // Value
  document.getElementById(ids.val).textContent = fmt(data.value, ids.type);
  // Delta
  const deltaEl = document.getElementById(ids.delta);
  const d = fmtDelta(data.delta, data.delta_unit, ids.type);
  if (metricId === 'revenue_growth_pct') {
    deltaEl.innerHTML = d ? d.html : '<span class="delta-neutral">Growth vs prior period</span>';
  } else {
    deltaEl.innerHTML = d ? d.html : '<span class="delta-neutral">No comparison data</span>';
  }
  // Sparkline (from backend series only)
  if (data.series && data.series.length > 0) {
    renderSparkline(ids.spark, data.series, ids.color);
  }
  // Store tooltip content
  const tipEl = document.getElementById('tooltip-' + metricId);
  if (tipEl) tipEl.textContent = metaDef.definition || '';
}

// ── Data loading ──────────────────────────────────────────────
async function loadKpis() {
  const metrics = ['revenue_total_usd', 'profit_total_usd', 'profit_margin_pct', 'revenue_growth_pct'];
  let insightRev, insightGrowth;
  
  await Promise.all(metrics.map(async metricId => {
    try {
      const payload = buildQueryPayload(metricId, 'none', 'month');
      const data = await apiFetch(API.QUERY, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      renderKpiCard(metricId, data);
      
      if (metricId === 'revenue_total_usd') insightRev = data.value;
      if (metricId === 'revenue_growth_pct') insightGrowth = data.value;
    } catch (err) {
      toast(`KPI error (${metricId}): ${err.message}`);
    }
  }));

  // Update Smart Insights deterministic text
  const insightsEl = document.getElementById('smart-insights-text');
  if (insightsEl && insightRev !== undefined) {
    let text = `Revenue for the selected period reached <strong>${fmt(insightRev, 'currency')}</strong>. `;
    if (insightGrowth === null || insightGrowth === undefined) {
      text += `No comparison data is available for the previous period.`;
    } else if (insightGrowth > 0) {
      text += `This marks a <strong>positive growth of ${insightGrowth.toFixed(1)}%</strong> compared to the previous period.`;
    } else if (insightGrowth < 0) {
      text += `This reflects a <strong>decline of ${Math.abs(insightGrowth).toFixed(1)}%</strong> compared to the previous period.`;
    } else {
      text += `Revenue remained exactly flat compared to the previous period.`;
    }
    insightsEl.innerHTML = text;
  }
}

async function loadRevenueTrend() {
  showLoading('revenue-trend', true);
  showEmpty('revenue-trend', false);
  try {
    const data = await apiFetch(API.QUERY, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(buildQueryPayload('revenue_total_usd', 'order_date', 'month')),
    });
    const series = data.series || [];
    if (!series.length) { showEmpty('revenue-trend', true); return; }
    renderLineChart('chart-revenue-trend', series.map(p => p.period_start), series.map(p => p.value), 'revenue');
  } catch (err) { toast('Revenue trend: ' + err.message); }
  finally { showLoading('revenue-trend', false); }
}

async function loadProfitTrend() {
  showLoading('profit-trend', true);
  showEmpty('profit-trend', false);
  try {
    const data = await apiFetch(API.QUERY, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(buildQueryPayload('profit_total_usd', 'order_date', 'month')),
    });
    const series = data.series || [];
    if (!series.length) { showEmpty('profit-trend', true); return; }
    renderLineChart('chart-profit-trend', series.map(p => p.period_start), series.map(p => p.value), 'profit');
  } catch (err) { toast('Profit trend: ' + err.message); }
  finally { showLoading('profit-trend', false); }
}

async function loadRegionChart() {
  showLoading('region', true);
  showEmpty('region', false);
  try {
    // time_grain required by server even for region breakdown
    const data = await apiFetch(API.QUERY, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(buildQueryPayload('revenue_total_usd', 'region', 'month')),
    });
    const breakdown = data.breakdown || [];
    if (!breakdown.length) { showEmpty('region', true); return; }
    renderBarChart('chart-region', breakdown.map(b => b.key), breakdown.map(b => b.value));
  } catch (err) { toast('Region chart: ' + err.message); }
  finally { showLoading('region', false); }
}

async function loadMarginDist() {
  showLoading('margin-dist', true);
  showEmpty('margin-dist', false);
  try {
    const data = await apiFetch(API.QUERY, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(buildQueryPayload('profit_margin_pct', 'order_date', 'month')),
    });
    const series = data.series || [];
    if (!series.length) { showEmpty('margin-dist', true); return; }
    renderMarginChart('chart-margin-dist', series, data.distribution || null);
  } catch (err) { toast('Margin distribution: ' + err.message); }
  finally { showLoading('margin-dist', false); }
}

async function loadTable() {
  showLoading('table', true);
  showEmpty('table', false);
  const statusFilter = document.getElementById('table-status-filter')?.value;
  const payload = {
    filters: {
      date_range: { start: state.dateStart, end: state.dateEnd },
      region: state.regions,
      customer_id: state.customerIds,
      order_status: statusFilter ? [statusFilter] : [],
    },
  };
  try {
    const data = await apiFetch(API.ORDERS, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    state.tableRows = data.rows || [];
    state.tablePage = 1;
    document.getElementById('table-row-count').textContent = `${state.tableRows.length} rows`;
    renderTablePage();
  } catch (err) { toast('Orders table: ' + err.message); }
  finally { showLoading('table', false); }
}

function renderTablePage() {
  const tbody = document.getElementById('orders-tbody');
  const total = state.tableRows.length;
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
  state.tablePage = Math.min(state.tablePage, totalPages);
  const start = (state.tablePage - 1) * PAGE_SIZE;
  const rows = state.tableRows.slice(start, start + PAGE_SIZE);
  showEmpty('table', rows.length === 0);
  tbody.innerHTML = rows.map(r => {
    const statusCls = r.order_status === 'COMPLETED' ? 'badge-completed'
                    : r.order_status === 'PENDING'   ? 'badge-pending'
                    : r.order_status === 'CANCELLED' ? 'badge-cancelled' : 'badge-other';
    const profitCls = (r.profit_usd !== null && r.profit_usd < 0) ? ' cell-negative' : '';
    return `<tr>
      <td><code style="font-size:11px;color:#94a3b8">${nullSafe(r.order_id)}</code></td>
      <td>${nullSafe(r.order_date)}</td>
      <td title="${nullSafe(r.customer_id)}">${nullSafe(r.customer_name)}</td>
      <td>${nullSafe(r.region)}</td>
      <td><span class="badge ${statusCls}">${nullSafe(r.order_status)}</span></td>
      <td class="cell-num">${fmt(r.revenue_usd, 'currency')}</td>
      <td class="cell-num">${fmt(r.cost_usd, 'currency')}</td>
      <td class="cell-num${profitCls}">${fmt(r.profit_usd, 'currency')}</td>
      <td class="cell-num">${fmt(r.margin_pct, 'percentage')}</td>
      <td>${nullSafe(r.category)}</td>
      <td>${nullSafe(r.payment_method)}</td>
    </tr>`;
  }).join('');
  document.getElementById('page-indicator').textContent = `Page ${state.tablePage} of ${totalPages}`;
  document.getElementById('page-prev').disabled = state.tablePage <= 1;
  document.getElementById('page-next').disabled = state.tablePage >= totalPages;
}

// ── Full refresh ──────────────────────────────────────────────
async function refreshAll() {
  setKpiLoading(true);
  document.querySelectorAll('.chart-container').forEach(el => el.classList.add('skeleton'));
  
  try {
    await Promise.all([
      loadKpis(),
      loadRevenueTrend(),
      loadProfitTrend(),
      loadRegionChart(),
      loadMarginDist(),
      loadTable(),
    ]);
  } finally {
    setKpiLoading(false);
    document.querySelectorAll('.chart-container').forEach(el => el.classList.remove('skeleton'));
  }
}

// ── Filter Bar setup ──────────────────────────────────────────
async function initMeta() {
  try {
    const meta = await apiFetch(API.META);
    // Set default date range from dataset min/max ONLY if state is empty
    const startEl = document.getElementById('filter-date-start');
    const endEl   = document.getElementById('filter-date-end');
    if (meta.date_min && !state.dateStart) { state.dateStart = meta.date_min; }
    if (meta.date_max && !state.dateEnd)   { state.dateEnd   = meta.date_max; }
    
    // Always sync state back to inputs
    if (state.dateStart) startEl.value = state.dateStart;
    if (state.dateEnd)   endEl.value   = state.dateEnd;

    console.log("Meta initialized. State:", state.dateStart, "to", state.dateEnd);
    // Header row count and freshness
    document.getElementById('header-source').textContent = `🗄️ Source: ${meta.dataset?.path || 'warehouse.db'}`;
    document.getElementById('header-rows').textContent = `${meta.dataset?.rows?.toLocaleString() || '?'} records`;
    if (meta.dataset?.last_modified) {
      const dateObj = new Date(meta.dataset.last_modified);
      document.getElementById('header-pipeline-date').textContent = `Pipeline As Of: ${dateObj.toLocaleString()}`;
    }
    // Status filter options (table only)
    const sel = document.getElementById('table-status-filter');
    (meta.order_statuses || []).forEach(s => {
      const opt = document.createElement('option');
      opt.value = s; opt.textContent = s;
      sel.appendChild(opt);
    });
    // Region multiselect
    buildRegionDropdown(meta.regions || []);
  } catch (err) {
    toast('Failed to load metadata: ' + err.message);
  }
}

async function initRegistry() {
  try {
    const reg = await apiFetch(API.REGISTRY);
    (reg.metrics || []).forEach(m => { state.metricRegistry[m.metric_id] = m; });
  } catch (err) {
    toast('Failed to load metric registry: ' + err.message);
  }
}

function buildRegionDropdown(regions) {
  const dropdown = document.getElementById('region-dropdown');
  dropdown.innerHTML = '';
  regions.forEach(region => {
    const label = document.createElement('label');
    label.className = 'multiselect-option';
    const cb = document.createElement('input');
    cb.type = 'checkbox'; cb.value = region;
    cb.addEventListener('change', () => updateRegionTrigger());
    label.appendChild(cb);
    label.appendChild(document.createTextNode(region));
    dropdown.appendChild(label);
  });
}

function updateRegionTrigger() {
  const checked = Array.from(document.querySelectorAll('#region-dropdown input:checked')).map(cb => cb.value);
  state.regions = checked;
  document.getElementById('region-trigger-text').textContent = checked.length === 0
    ? 'All Regions' : `${checked.length} selected`;
  // Auto-refresh the dashboard when a filter checkbox is toggled
  refreshAll();
}

function initRegionMultiselect() {
  const trigger = document.getElementById('region-trigger');
  const dropdown = document.getElementById('region-dropdown');
  trigger.addEventListener('click', e => {
    e.stopPropagation();
    const open = trigger.getAttribute('aria-expanded') === 'true';
    trigger.setAttribute('aria-expanded', String(!open));
    dropdown.hidden = open;
  });
  document.addEventListener('click', () => {
    trigger.setAttribute('aria-expanded', 'false');
    dropdown.hidden = true;
  });
  dropdown.addEventListener('click', e => e.stopPropagation());
}

// ── Customer search ───────────────────────────────────────────
let customerSearchTimer = null;

function initCustomerSearch() {
  const input = document.getElementById('customer-search-input');
  const dropdown = document.getElementById('customer-dropdown');

  input.addEventListener('input', () => {
    clearTimeout(customerSearchTimer);
    customerSearchTimer = setTimeout(async () => {
      const q = input.value.trim();
      if (q.length < 1) { dropdown.hidden = true; return; }
      try {
        const data = await apiFetch(`${API.CUSTOMERS}?q=${encodeURIComponent(q)}&limit=10`);
        renderCustomerDropdown(data.customers || [], dropdown);
      } catch (_) {}
    }, 250);
  });

  document.addEventListener('click', e => {
    if (!input.contains(e.target) && !dropdown.contains(e.target)) dropdown.hidden = true;
  });
}

function renderCustomerDropdown(customers, dropdown) {
  if (!customers.length) { dropdown.hidden = true; return; }
  dropdown.innerHTML = customers.map(c =>
    `<div class="customer-option" data-id="${c.customer_id}" data-name="${c.customer_name}">${c.customer_name}</div>`
  ).join('');
  dropdown.hidden = false;
  dropdown.querySelectorAll('.customer-option').forEach(el => {
    el.addEventListener('click', () => {
      addCustomerTag(el.dataset.id, el.dataset.name);
      dropdown.hidden = true;
      document.getElementById('customer-search-input').value = '';
    });
  });
}

function addCustomerTag(id, name) {
  if (state.customerIds.includes(id)) return;
  state.customerIds.push(id);
  const tags = document.getElementById('selected-customers');
  const tag = document.createElement('span');
  tag.className = 'tag';
  tag.dataset.id = id;
  tag.innerHTML = `${name}<button class="tag-remove" aria-label="Remove ${name}">✕</button>`;
  tag.querySelector('.tag-remove').onclick = () => {
    state.customerIds = state.customerIds.filter(c => c !== id);
    tag.remove();
  };
  tags.appendChild(tag);
}

// ── Modals ────────────────────────────────────────────────────
function initModal() {
  // Metric Definition Modal
  document.querySelectorAll('.kpi-info-btn').forEach(btn => {
    btn.addEventListener('click', e => {
      e.stopPropagation();
      const metricId = btn.dataset.metric;
      const def = state.metricRegistry[metricId];
      if (!def) return;
      document.getElementById('modal-metric-name').textContent = def.name || metricId;
      document.getElementById('modal-definition').textContent  = def.definition || '—';
      document.getElementById('modal-unit').textContent        = `Unit: ${def.unit || '—'} · Type: ${def.type || '—'}`;
      document.getElementById('metric-modal-overlay').hidden = false;
    });
  });
  document.getElementById('modal-close-btn').onclick = () => {
    document.getElementById('metric-modal-overlay').hidden = true;
  };
  document.getElementById('metric-modal-overlay').addEventListener('click', e => {
    if (e.target === e.currentTarget) document.getElementById('metric-modal-overlay').hidden = true;
  });

  // Drill-down Modal
  document.getElementById('drill-modal-close-btn').onclick = () => {
    document.getElementById('drill-modal-overlay').hidden = true;
  };
  document.getElementById('drill-modal-overlay').addEventListener('click', e => {
    if (e.target === e.currentTarget) document.getElementById('drill-modal-overlay').hidden = true;
  });

  document.querySelectorAll('.kpi-card').forEach(card => {
    card.addEventListener('click', e => {
      if (e.target.closest('.kpi-info-btn') || e.target.closest('.kpi-sparkline')) return;
      const metricMap = {
        'kpi-revenue': 'revenue_total_usd',
        'kpi-profit': 'profit_total_usd',
        'kpi-margin': 'profit_margin_pct',
        'kpi-growth': 'revenue_growth_pct'
      };
      openDrillDown(metricMap[card.id]);
    });
  });
}

async function openDrillDown(metricId) {
  if (!metricId) return;
  const overlay = document.getElementById('drill-modal-overlay');
  const tbody = document.getElementById('drill-tbody');
  const subtitle = document.getElementById('drill-modal-subtitle');
  
  const def = state.metricRegistry[metricId];
  subtitle.textContent = `Analyzing: ${def ? def.name : metricId} (Top Transactions)`;
  tbody.innerHTML = '<tr><td colspan="5" style="text-align: center; padding: 20px;">Fetching transactions...</td></tr>';
  overlay.hidden = false;
  
  try {
    const payload = buildQueryPayload(metricId, 'none', 'none');
    const data = await apiFetch(`${API.ORDERS}?page=1&page_size=10`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
         filters: payload.filters,
         sort: { field: metricId.includes('profit') ? 'profit_usd' : 'revenue_usd', direction: 'desc' }
      }),
    });
    
    const allRows = data.rows || [];
    if (allRows.length === 0) {
      tbody.innerHTML = '<tr><td colspan="5" style="text-align: center; padding: 20px;">No transactions found for the selected period.</td></tr>';
      return;
    }

    const sortField = metricId.includes('profit') ? 'profit_usd' : 'revenue_usd';
    allRows.sort((a, b) => (b[sortField] || 0) - (a[sortField] || 0));
    const topRecords = allRows.slice(0, 10);
    
    tbody.innerHTML = '';
    topRecords.forEach(r => {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>${r.order_id || '—'}</td>
        <td>${r.order_date || '—'}</td>
        <td>${r.customer_name || '—'}</td>
        <td class="col-num">${fmt(r.revenue_usd, 'currency')}</td>
        <td class="col-num">${fmt(r.profit_usd, 'currency')}</td>
      `;
      tbody.appendChild(tr);
    });
  } catch (err) {
    tbody.innerHTML = `<tr><td colspan="5" style="text-align: center; color: var(--accent-rose); padding: 20px;">Error: ${err.message}</td></tr>`;
  }
}

// ── Apply filters btn ─────────────────────────────────────────
function initFilterApply() {
  const applyFilters = () => {
    state.dateStart = document.getElementById('filter-date-start').value;
    state.dateEnd   = document.getElementById('filter-date-end').value;
    if (!state.dateStart || !state.dateEnd) {
      toast('Please select a start and end date.', 'warn'); return;
    }
    if (state.dateStart > state.dateEnd) {
      toast('Start date must be before end date.', 'warn'); return;
    }
    syncStateToURL();
    refreshAll();
  };

  document.getElementById('filter-date-start').addEventListener('change', applyFilters);
  document.getElementById('filter-date-end').addEventListener('change', applyFilters);
}

// ── Table controls & Export ───────────────────────────────────
function exportAuditCsv() {
  if (!state.tableRows || state.tableRows.length === 0) {
    toast('No data to export', 'warn'); return;
  }
  const headers = ['order_id', 'order_date', 'customer_name', 'region', 'region_group', 'country', 'revenue_usd', 'cost_usd', 'profit_usd', 'margin_pct', 'order_status', 'payment_method', 'category'];
  let csv = headers.join(',') + '\n';
  state.tableRows.forEach(r => {
    csv += headers.map(h => {
      const val = r[h] === null ? '' : String(r[h]).replace(/"/g, '""');
      return `"${val}"`;
    }).join(',') + '\n';
  });
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.setAttribute('href', url);
  link.setAttribute('download', `audit_export_${new Date().toISOString().slice(0,10)}.csv`);
  link.style.visibility = 'hidden';
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

function initTableControls() {
  document.getElementById('table-status-filter').addEventListener('change', () => loadTable());
  document.getElementById('page-prev').addEventListener('click', () => { state.tablePage--; renderTablePage(); });
  document.getElementById('page-next').addEventListener('click', () => { state.tablePage++; renderTablePage(); });
  document.getElementById('btn-export-csv').addEventListener('click', exportAuditCsv);
}

// ── Health check ──────────────────────────────────────────────
async function checkHealth() {
  const pill = document.getElementById('header-status');
  try {
    await apiFetch(API.HEALTH);
    pill.textContent = 'API Online';
    pill.className = 'status-pill status-ok';
  } catch {
    pill.textContent = 'API Offline';
    pill.className = 'status-pill status-error';
    toast('Cannot reach the API server. Ensure the backend server is running.', 'error');
  }
}

// ── Boot ──────────────────────────────────────────────────────
async function boot() {
  syncURLToState();
  await checkHealth();
  await Promise.all([initMeta(), initRegistry()]);
  
  // Final safeguard: ensure dates are present
  if (!state.dateStart || !state.dateEnd) {
      toast("Data range missing. Dashboard may be incomplete.", "warn");
  }
  
  initRegionMultiselect();
  initCustomerSearch();
  initModal();
  initFilterApply();
  initTableControls();
  initSourceToggle();

  // If URL had customers, they are already in state.customerIds
  // But we need to fetch their names to render tags
  if (state.customerIds.length) {
    // Customer map is built during initCustomerSearch or similar
    // For now we render with IDs, names will update as we search
    state.customerIds.forEach(id => {
      renderCustomerTag(id, id);
    });
  }

  // Initial load
  await refreshAll();
}

function initSourceToggle() {
  const toggle = document.getElementById('source-toggle-input');
  toggle.addEventListener('change', () => {
    state.sourceMode = toggle.checked ? 'sql' : 'csv';
    
    // Update UI labels immediately
    const sourceLabel = document.getElementById('header-source');
    if (state.sourceMode === 'sql') {
        sourceLabel.style.color = 'var(--accent-teal)';
    } else {
        sourceLabel.style.color = 'var(--accent-amber)';
    }
    
    refreshAll();
  });
}

document.addEventListener('DOMContentLoaded', boot);
