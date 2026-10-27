/* NEXUS dashboard. Plain JavaScript, no dependencies. Every number on screen comes from the
   Master's HTTP API (which reads PostgreSQL / the live scheduler); nothing is hard-coded. */
'use strict';

const $ = (s, r = document) => r.querySelector(s);
const PALETTE = ['#2f5bea', '#e0691e', '#15a06b', '#c2379b', '#7a5af5', '#d4a017', '#0f9bb5', '#d1405a'];
const STATE = { tab: 'overview', connected: false, workers: [], colors: {}, logSeq: 0, selDataset: null, selJob: null, policy: null, selBench: null };

/* ---------------------------------------------------------------- helpers ---- */
function el(tag, attrs, ...kids) {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (k === 'class') n.className = v;
    else if (k === 'text') n.textContent = v;
    else if (k.startsWith('on')) n.addEventListener(k.slice(2), v);
    else if (v !== false && v != null) n.setAttribute(k, v === true ? '' : v);
  }
  for (const c of kids.flat()) if (c != null && c !== false) n.append(c.nodeType ? c : document.createTextNode(String(c)));
  return n;
}
const num = (v, d = 1) => (v == null || isNaN(v)) ? '–' : Number(v).toFixed(d);
function fmtTime(iso) { if (!iso) return '–'; const d = new Date(iso); return isNaN(d) ? '–' : d.toLocaleTimeString([], { hour12: false }) + '.' + String(d.getMilliseconds()).padStart(3, '0'); }
function fmtBytes(b) { if (b == null) return '–'; const u = ['B', 'KB', 'MB', 'GB']; let i = 0, v = Number(b); while (v >= 1024 && i < 3) { v /= 1024; i++; } return v.toFixed(i ? 1 : 0) + ' ' + u[i]; }
function fmtSec(s) { if (s == null) return '–'; s = Number(s); return s < 1 ? (s * 1000).toFixed(0) + ' ms' : s.toFixed(2) + ' s'; }
function badge(text, kind) { return el('span', { class: 'badge b-' + (kind || 'mute'), text }); }
function statusBadge(s) {
  const m = { COMPLETED: 'ok', AVAILABLE: 'ok', HEALTHY: 'ok', OK: 'ok', TRAINED: 'ok', RUNNING: 'info', ASSIGNED: 'info', PENDING: 'warn', UPLOADING: 'warn',
    DEGRADED: 'warn', NOT_ACTIVATED: 'warn', INSUFFICIENT_DATA: 'warn', FAILED: 'bad', CORRUPT: 'bad', MISSING: 'bad', UNAVAILABLE: 'bad', INACTIVE: 'mute' };
  return badge(s, m[s] || 'mute');
}
function bar(v, max = 100, hotAt = 85) {
  const pct = Math.max(0, Math.min(100, (v / max) * 100));
  return el('span', {}, el('span', { class: 'bar ' + (pct >= hotAt ? 'hot' : pct >= 65 ? 'warm' : '') }, el('i', { style: `width:${pct}%` })), num(v, 0) + (max === 100 ? '%' : ''));
}
function workerColor(id) {
  if (!(id in STATE.colors)) STATE.colors[id] = PALETTE[Object.keys(STATE.colors).length % PALETTE.length];
  return STATE.colors[id];
}
function table(cols, rows, opts = {}) {
  if (!rows || !rows.length) return el('div', { class: 'empty', text: opts.empty || 'nothing here yet' });
  const thead = el('thead', {}, el('tr', {}, cols.map(c => el('th', { class: c.num ? 'num' : '', text: c.h }))));
  const tbody = el('tbody', {}, rows.map(r => {
    const tr = el('tr', opts.onClick ? { class: 'click', onclick: () => opts.onClick(r) } : {},
      cols.map(c => { const v = c.f(r); return el('td', { class: (c.num ? 'num ' : '') + (c.wrap ? 'wrap' : '') }, v == null ? '–' : v); }));
    return tr;
  }));
  return el('table', {}, thead, tbody);
}
function setMsg(node, text, ok) { node.textContent = text; node.className = 'msg ' + (ok ? 'ok' : 'err'); }
function put(sel, node) { const t = $(sel); t.replaceChildren(node); }

async function api(path, opts = {}) {
  let res;
  try { res = await fetch(path, opts); }
  catch (e) { setConn(false, 'Master unreachable'); throw new Error('Master unreachable'); }
  setConn(true);
  let body = null; const text = await res.text();
  try { body = text ? JSON.parse(text) : null; } catch (_) { body = text; }
  if (!res.ok) { const err = new Error((body && body.error) || ('HTTP ' + res.status)); err.body = body; err.status = res.status; throw err; }
  return body;
}
const post = (path, data) => api(path, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams(data || {}) });

function setConn(ok, why) {
  STATE.connected = ok;
  $('#conn').className = 'pill ' + (ok ? 'live' : 'dead');
  $('#connText').textContent = ok ? 'live' : (why || 'disconnected');
  const b = $('#banner');
  if (ok) b.classList.add('hidden');
  else { b.classList.remove('hidden'); b.textContent = 'Cannot reach the NEXUS Master (' + (why || 'disconnected') + '). Data shown may be stale; retrying every 2 s…'; }
}

function showModal(node) { $('#modalContent').replaceChildren(node); $('#modal').classList.remove('hidden'); }
$('#modalClose').onclick = () => $('#modal').classList.add('hidden');
$('#modal').onclick = e => { if (e.target.id === 'modal') $('#modal').classList.add('hidden'); };
document.addEventListener('keydown', e => { if (e.key === 'Escape') $('#modal').classList.add('hidden'); });

/* ------------------------------------------------------------------- tabs ---- */
function selectTab(name) {
  if (!$('#tab-' + name)) name = 'overview';
  STATE.tab = name;
  document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.id === 'tab-' + name));
  document.querySelectorAll('#tabs button').forEach(b => b.classList.toggle('active', b.dataset.tab === name));
  if (location.hash !== '#' + name) history.replaceState(null, '', '#' + name);
  refresh();
}
$('#tabs').addEventListener('click', e => { const b = e.target.closest('button'); if (b) selectTab(b.dataset.tab); });
window.addEventListener('hashchange', () => selectTab(location.hash.slice(1)));

/* --------------------------------------------------------------- overview ---- */
async function loadOverview() {
  const o = await api('/api/overview');
  STATE.policy = o.policy;
  $('#policyPill').textContent = 'policy: ' + o.policy;
  const hn = $('#hostNote');
  if (o.shared_host_warning) { hn.textContent = o.shared_host_note; hn.classList.remove('hidden'); } else hn.classList.add('hidden');

  const w = o.workers || {}, t = o.tasks || {}, j = o.jobs || {}, s = o.storage || {};
  const kpi = (v, l, sub) => el('div', { class: 'kpi' }, el('div', { class: 'v', text: String(v) }), el('div', { class: 'l', text: l }), sub ? el('div', { class: 's', text: sub }) : null);
  if (STATE.tab === 'overview') {
    $('#kpis').replaceChildren(
      kpi(`${w.healthy}/${w.total}`, 'Workers healthy', `${w.capacity} task slots`),
      kpi(t.pending, 'Tasks pending', `${t.assigned} assigned · ${t.running} running`),
      kpi(t.completed, 'Tasks completed', `${t.failed} failed · ${t.total} total`),
      kpi(j.total, 'Jobs', `${j.running} running · ${j.completed} done`),
      kpi(s.datasets, 'Datasets', `${s.blocks} blocks · ${s.replicas} replicas · ${fmtBytes(s.bytes)}`),
      kpi(o.model_loaded ? 'trained' : 'none', 'Duration model', o.model_loaded ? 'AI policy active' : 'AI policy falls back'),
      kpi(Math.floor(o.uptime_s / 60) + ' min', 'Master uptime', 'DB ' + (o.db_ok ? 'connected' : 'DOWN')));
    put('#ovHosts', table([
      { h: 'Host', f: r => r.hostname }, { h: 'Workers', f: r => r.ids }, { h: 'CPU cores', num: 1, f: r => r.cores },
      { h: 'RAM', num: 1, f: r => fmtBytes(r.memory_mb * 1048576) }, { h: '', f: r => r.shared_host ? badge('shared CPU/RAM', 'warn') : null }], o.hosts, { empty: 'no healthy Workers' }));
  }
}

async function loadWorkers(into) {
  const ws = await api('/api/workers');
  STATE.workers = ws;
  ws.forEach(w => workerColor(w.worker_id));
  return ws;
}
function workerStatus(w) {
  if (w.healthy) return badge('healthy', 'ok');
  if (w.status === 'ACTIVE') return badge('heartbeat expired', 'bad');
  return badge('inactive', 'mute');
}
// CPU pinning exactly as the Worker reported it (verified by the Worker with sched_getaffinity).
function affinityBadge(w) {
  const s = w.affinity_status;
  const tip = w.affinity_detail || '';
  if (s === 'APPLIED') return el('span', { title: tip }, badge('pinned', 'ok'), ' CPU ' + w.cpu_affinity);
  if (s === 'PARTIAL') return el('span', { title: tip }, badge('partial', 'warn'), ' CPU ' + w.cpu_affinity);
  if (s === 'FAILED') return el('span', { title: tip }, badge('pin FAILED', 'bad'), ' not pinned');
  if (s === 'NOT_REQUESTED') return el('span', { title: tip }, badge('not pinned', 'mute'));
  return '–';
}
function workersTable(ws, compact) {
  const cols = [
    { h: 'Worker', f: w => el('span', {}, el('i', { class: 'dot', style: `background:${workerColor(w.worker_id)}` }), ' ', w.worker_id) },
    { h: 'Status', f: workerStatus },
    { h: 'CPU', f: w => bar(w.cpu) }, { h: 'RAM', f: w => bar(w.ram) },
    { h: 'Tasks running / max', f: w => el('span', { title: 'max concurrent tasks the Master may assign (NEXUS_WORKER_CAPACITY)' }, el('span', { class: 'bar' }, el('i', { style: `width:${Math.min(100, w.running_tasks / Math.max(1, w.capacity) * 100)}%` })), `${w.running_tasks} / ${w.capacity}`) },
    { h: 'CPU affinity', f: affinityBadge },
    { h: 'Exec. threads', num: 1, f: w => w.pool_threads == null ? '–' : w.pool_threads },
    { h: 'Heartbeat', num: 1, f: w => w.heartbeat_age_s == null ? '–' : num(w.heartbeat_age_s, 1) + ' s ago' }];
  if (!compact) cols.push(
    { h: 'Disk', f: w => bar(w.disk) }, { h: 'RAM free', num: 1, f: w => w.ram_available_mb == null ? '–' : fmtBytes(w.ram_available_mb * 1048576) },
    { h: 'Load 1m', num: 1, f: w => num(w.load1, 2) }, { h: 'Process RSS', num: 1, f: w => w.process_rss_mb == null ? '–' : num(w.process_rss_mb, 0) + ' MB' },
    { h: 'Blocks', num: 1, f: w => w.blocks_stored }, { h: 'Block data', num: 1, f: w => fmtBytes(w.storage_bytes) },
    { h: 'Host', f: w => `${w.hostname} (${w.cpu_cores} cores)` });
  return table(cols, ws, { empty: 'No Worker has registered yet. Start one: ./scripts/start_worker.sh W1' });
}

/* ------------------------------------------------------------------ tasks ---- */
function taskRow(t) {
  return [
    { h: 'Task', f: t => 'TASK-' + String(t.task_id).padStart(3, '0') }, { h: 'Job', num: 1, f: t => t.job_id },
    { h: 'Type', f: t => t.task_type + (t.role !== 'STANDALONE' ? ' · ' + t.role : '') }, { h: 'Prio', num: 1, f: t => t.priority },
    { h: 'Status', f: t => statusBadge(t.status) },
    { h: 'Worker', f: t => t.assigned_worker ? el('span', {}, el('i', { class: 'dot', style: `background:${workerColor(t.assigned_worker)}` }), ' ', t.assigned_worker) : '–' },
    { h: 'Try', num: 1, f: t => `${t.attempts}/${t.max_attempts}` }, { h: 'Policy', f: t => t.policy || '–' },
    { h: 'Waited', num: 1, f: t => fmtSec(t.queue_wait_s) }, { h: 'Ran', num: 1, f: t => fmtSec(t.run_s) },
    { h: 'Predicted', num: 1, f: t => t.predicted_seconds == null ? '–' : fmtSec(t.predicted_seconds) },
    { h: 'Result / error', wrap: 1, f: t => t.last_error && t.status !== 'COMPLETED' ? t.last_error : (t.result || '').slice(0, 90) }];
}
async function loadTasks() {
  const status = $('#taskFilter').value;
  const ts = await api('/api/tasks?limit=40' + (status ? '&status=' + status : ''));
  put('#tasksTable', table(taskRow(), ts, { empty: 'No tasks yet. Submit one above.', onClick: t => showTask(t.task_id) }));
  const hs = await api('/api/history?limit=25');
  put('#historyTable', table([
    { h: 'Exec', num: 1, f: h => h.execution_id }, { h: 'Task', num: 1, f: h => h.task_id }, { h: 'Worker', f: h => h.worker_id }, { h: 'Type', f: h => h.task_type },
    { h: 'Try', num: 1, f: h => h.attempt }, { h: 'Status', f: h => statusBadge(h.status) }, { h: 'Started', f: h => fmtTime(h.started_at) }, { h: 'Finished', f: h => fmtTime(h.finished_at) },
    { h: 'Duration', num: 1, f: h => fmtSec(h.execution_time) }, { h: 'Concurrent', num: 1, f: h => h.concurrent_at_start },
    { h: 'CPU at assign', num: 1, f: h => h.cpu_at_assign == null ? '–' : h.cpu_at_assign + '%' }, { h: 'Policy', f: h => h.scheduler_policy }, { h: 'Error', wrap: 1, f: h => h.error }], hs,
    { empty: 'No task has finished yet.' }));
}
function candidatesTable(json) {
  let c = []; try { c = JSON.parse(json || '[]'); } catch (_) {}
  return table([{ h: 'Worker', f: x => x.worker }, { h: 'CPU', num: 1, f: x => num(x.cpu, 0) + '%' }, { h: 'RAM', num: 1, f: x => num(x.ram, 0) + '%' },
    { h: 'Tasks', num: 1, f: x => `${x.running}/${x.capacity}` }, { h: 'Heartbeat age', num: 1, f: x => x.hb_age_s < 0 ? '–' : num(x.hb_age_s, 1) + ' s' },
    { h: 'Eligible', f: x => x.eligible ? 'yes' : 'no' }, { h: 'Score', num: 1, f: x => num(x.score, 3) },
    { h: 'Predicted', num: 1, f: x => x.predicted_s == null ? '–' : num(x.predicted_s, 3) + ' s' }, { h: 'Note', wrap: 1, f: x => x.note }], c);
}
async function showTask(id) {
  try {
    const t = await api('/api/tasks/' + id);
    const box = el('div', {}, el('h2', {}, 'TASK-' + String(t.task_id).padStart(3, '0'), ' ', statusBadge(t.status)),
      el('p', { class: 'muted', text: `${t.task_type} · job ${t.job_id} · priority ${t.priority} · attempt ${t.attempts}/${t.max_attempts}` }),
      t.result ? el('pre', { class: 'json', text: t.result }) : null, t.last_error ? el('p', { class: 'msg err', text: t.last_error }) : null,
      el('h2', {}, 'Scheduling decisions'),
      ...t.decisions.map(d => el('div', {}, el('p', {}, badge('attempt ' + d.attempt, 'info'), ' ', `${d.policy}${d.fallback ? ' → ' + d.effective_policy + ' (fallback)' : ''} → `, el('b', { text: d.worker_id }), ' · ', d.reason),
        candidatesTable(d.candidates))),
      el('h2', {}, 'Executions'),
      table([{ h: 'Try', num: 1, f: e => e.attempt }, { h: 'Worker', f: e => e.worker_id }, { h: 'Status', f: e => statusBadge(e.status) }, { h: 'Started', f: e => fmtTime(e.started_at) },
        { h: 'Finished', f: e => fmtTime(e.finished_at) }, { h: 'Duration', num: 1, f: e => fmtSec(e.execution_time) }, { h: 'Error', wrap: 1, f: e => e.error }], t.executions));
    showModal(box);
  } catch (e) { alert(e.message); }
}
$('#tfType').onchange = () => $('#tfBlockWrap').classList.toggle('hidden', !$('#tfType').value.startsWith('BLOCK_'));
$('#taskForm').onsubmit = async ev => {
  ev.preventDefault();
  const n = Math.max(1, Math.min(200, +$('#tfCount').value || 1));
  let ok = 0, last = '';
  try {
    for (let i = 0; i < n; i++) {
      const d = { type: $('#tfType').value, scale: $('#tfScale').value, priority: $('#tfPrio').value };
      if (!$('#tfBlockWrap').classList.contains('hidden')) d.block_id = $('#tfBlock').value;
      const r = await post('/api/tasks', d); ok++; last = 'TASK-' + r.task_id;
    }
    setMsg($('#tfMsg'), `Submitted ${ok} task(s) (last: ${last}).`, true);
  } catch (e) { setMsg($('#tfMsg'), `Submitted ${ok}; then failed: ${e.message}`, false); }
  refresh();
};
$('#taskFilter').onchange = refresh;

/* ------------------------------------------------------------- scheduling ---- */
async function loadScheduling() {
  const p = await api('/api/policy');
  const cards = p.available.map(a => el('button', { class: 'policy' + (a.name === p.policy ? ' active' : ''), type: 'button', onclick: async () => {
      try { await post('/api/policy', { policy: a.name }); setMsg($('#policyMsg'), 'Active policy: ' + a.name, true); } catch (e) { setMsg($('#policyMsg'), e.message, false); }
      refresh();
    } },
    el('b', { text: a.name + (a.name === p.policy ? '  ✓ active' : '') }), el('span', { text: 'Task order: ' + a.task_order }), el('span', { text: 'Worker choice: ' + a.worker_choice }),
    a.name === 'AI_ASSISTED' ? el('span', { text: p.model_loaded ? 'Model loaded: predictions drive placement' : 'No trained model yet → uses RESOURCE_AWARE' }) : null));
  $('#policyCards').replaceChildren(...cards);
  const ds = await api('/api/decisions?limit=40');
  put('#decisionsTable', table([
    { h: 'Time', f: d => fmtTime(d.created_at) }, { h: 'Task', f: d => 'TASK-' + String(d.task_id).padStart(3, '0') }, { h: 'Type', f: d => d.task_type }, { h: 'Prio', num: 1, f: d => d.priority },
    { h: 'Try', num: 1, f: d => d.attempt }, { h: 'Policy', f: d => d.fallback ? `${d.policy} → ${d.effective_policy}` : d.policy },
    { h: 'Worker', f: d => d.worker_id }, { h: 'Score', num: 1, f: d => num(d.score, 3) },
    { h: 'Predicted', num: 1, f: d => d.predicted_seconds == null ? '–' : fmtSec(d.predicted_seconds) }, { h: 'Actual', num: 1, f: d => d.actual_seconds == null ? '–' : fmtSec(d.actual_seconds) },
    { h: 'Why', wrap: 1, f: d => d.reason }], ds, {
      empty: 'No scheduling decision recorded yet.',
      onClick: d => showModal(el('div', {}, el('h2', {}, `Decision for TASK-${d.task_id}`), el('p', { class: 'muted', text: d.reason }), candidatesTable(d.candidates))) }));
}

/* ---------------------------------------------------------------- storage ---- */
function datasetHealth(d) { return !d.reconstructable ? badge('cannot reconstruct', 'bad') : (d.healthy_replicas < d.replicas || d.blocks_readable < d.block_count) ? badge('degraded', 'warn') : badge('ok', 'ok'); }
async function loadStorage() {
  const ds = await api('/api/datasets');
  put('#datasetsTable', table([
    { h: 'ID', num: 1, f: d => d.dataset_id }, { h: 'Name', f: d => d.name }, { h: 'Size', num: 1, f: d => fmtBytes(d.original_size) },
    { h: 'Blocks', num: 1, f: d => d.block_count }, { h: 'Block size', num: 1, f: d => fmtBytes(d.block_size) }, { h: 'Replication', num: 1, f: d => d.replication },
    { h: 'Replicas', num: 1, f: d => `${d.healthy_replicas}/${d.replicas}` }, { h: 'Readable blocks', num: 1, f: d => `${d.blocks_readable}/${d.block_count}` },
    { h: 'Text', f: d => d.text_aligned ? 'yes' : 'no' }, { h: 'Health', f: datasetHealth }, { h: 'SHA-256', f: d => d.sha256.slice(0, 12) + '…' }, { h: 'Uploaded', f: d => fmtTime(d.created_at) }], ds,
    { empty: 'No dataset yet. Upload a file above.', onClick: d => { STATE.selDataset = d.dataset_id; loadDatasetDetail(); } }));
  if (STATE.selDataset) await loadDatasetDetail();
  const sel = $('#mrDataset');
  if (sel) fillDatasetSelect(ds);
}
async function loadDatasetDetail() {
  const card = $('#datasetDetail');
  let d;
  try { d = await api('/api/datasets/' + STATE.selDataset); } catch (e) { card.classList.add('hidden'); return; }
  card.classList.remove('hidden');
  const msg = el('div', { class: 'msg', id: 'dsMsg' });
  const act = (label, fn) => el('button', { onclick: async ev => { ev.target.disabled = true; try { await fn(); } catch (e) { setMsg(msg, e.message, false); } ev.target.disabled = false; loadDatasetDetail(); } }, label);
  const chipKind = r => !r.worker_online ? 'mute' : r.status === 'HEALTHY' ? 'ok' : 'bad';
  card.replaceChildren(
    el('h2', {}, `Dataset ${d.dataset_id}: ${d.name}`, ' ', statusBadge(d.health), el('small', { text: `${fmtBytes(d.original_size)} · ${d.block_count} blocks · replication ${d.replication} · sha256 ${d.sha256.slice(0, 16)}…` })),
    el('div', { class: 'form-row' },
      act('Verify checksums', async () => { const r = await post(`/api/datasets/${d.dataset_id}/verify`); setMsg(msg, `Checked ${r.replicas_checked} replicas: ${r.healthy} healthy, ${r.corrupt} corrupt, ${r.missing} missing, ${r.unreachable} unreachable. Reconstructable now: ${r.reconstructable_now}`, r.reconstructable_now); }),
      act('Repair replicas', async () => { const r = await post(`/api/datasets/${d.dataset_id}/repair`); setMsg(msg, `Created ${r.replicas_created} replica(s); ${r.blocks_still_below_replication} block(s) still below the replication factor; ${r.unrecoverable_blocks.length} unrecoverable.`, !r.unrecoverable_blocks.length); }),
      el('a', { href: `/api/datasets/${d.dataset_id}/download`, download: d.name }, el('button', { type: 'button', text: 'Download (reconstruct)' })),
      d.text_aligned ? act('Run word count (MapReduce)', async () => { const r = await post('/api/mapreduce', { dataset_id: d.dataset_id, reducers: 2 }); STATE.selJob = r.job_id; selectTab('mapreduce'); }) : null),
    msg,
    el('p', { class: 'muted', text: `${d.blocks_ok} OK · ${d.blocks_degraded} degraded · ${d.blocks_unavailable} unavailable · ${d.replicas_usable}/${d.replicas_total} replicas usable · ${d.reconstructable ? 'file can be reconstructed' : 'FILE CANNOT BE RECONSTRUCTED'}` }),
    el('div', { class: 'blocks' }, d.blocks.map(b => el('div', { class: 'block ' + b.state },
      el('b', { text: `${b.block_id}` }), ` · ${fmtBytes(b.size)} · `, statusBadge(b.state), el('br'), el('span', { class: 'muted', text: b.checksum.slice(0, 16) + '…' }), el('br'),
      b.replicas.map(r => el('span', { class: 'chip b-' + chipKind(r), title: `${r.status}${r.worker_online ? '' : ' (Worker offline)'}${r.last_verified_at ? ' · verified ' + fmtTime(r.last_verified_at) : ''}`, text: `${r.worker}${r.worker_online ? '' : ' ⏻'}${r.status === 'HEALTHY' ? '' : ' ' + r.status}` }))))));
}
$('#uploadForm').onsubmit = async ev => {
  ev.preventDefault();
  const f = $('#ufFile').files[0]; if (!f) return;
  const msg = $('#ufMsg'); setMsg(msg, `Uploading ${f.name} (${fmtBytes(f.size)})…`, true);
  const q = new URLSearchParams({ name: f.name, block_size: $('#ufBlock').value, replication: $('#ufRepl').value, text: $('#ufText').checked ? '1' : '0' });
  try {
    const r = await api('/api/datasets?' + q, { method: 'POST', headers: { 'Content-Type': 'application/octet-stream' }, body: f });
    setMsg(msg, `Dataset ${r.dataset_id} stored: ${r.blocks} blocks, ${r.replicas_stored} replicas.` + (r.warning ? ' Warning: ' + r.warning : ''), !r.warning);
    STATE.selDataset = r.dataset_id;
  } catch (e) { setMsg(msg, 'Upload failed: ' + e.message, false); }
  refresh();
};

/* -------------------------------------------------------------- mapreduce ---- */
function fillDatasetSelect(ds) {
  const sel = $('#mrDataset'); const cur = sel.value;
  sel.replaceChildren(...ds.filter(d => d.text_aligned).map(d => el('option', { value: d.dataset_id, text: `#${d.dataset_id} ${d.name} (${d.block_count} blocks)` })));
  if (!sel.children.length) sel.append(el('option', { value: '', text: 'upload a text-aligned dataset first' }));
  if (cur) sel.value = cur;
}
async function loadMapReduce() {
  fillDatasetSelect(await api('/api/datasets'));
  const js = await api('/api/jobs?limit=25&type=MAPREDUCE_WORDCOUNT');
  put('#jobsTable', table([
    { h: 'Job', num: 1, f: j => j.job_id }, { h: 'Type', f: j => j.job_type }, { h: 'Status', f: j => statusBadge(j.status) }, { h: 'Prio', num: 1, f: j => j.priority },
    { h: 'Tasks', num: 1, f: j => `${j.tasks_done}/${j.tasks}` }, { h: 'Dataset', num: 1, f: j => j.dataset_id }, { h: 'Elapsed', num: 1, f: j => fmtSec(j.elapsed_s) },
    { h: 'Submitted', f: j => fmtTime(j.submitted_at) }, { h: 'Result', wrap: 1, f: j => (j.result || '').slice(0, 100) }], js,
    { empty: 'No job yet.', onClick: j => { STATE.selJob = j.job_id; loadJobDetail(); } }));
  if (STATE.selJob) await loadJobDetail();
}
async function loadJobDetail() {
  const card = $('#jobDetail');
  let j; try { j = await api('/api/jobs/' + STATE.selJob); } catch (e) { card.classList.add('hidden'); return; }
  card.classList.remove('hidden');
  const t0 = new Date(j.submitted_at).getTime();
  const end = Math.max(...j.stages.map(s => new Date(s.completed_at || Date.now()).getTime()), t0 + 1);
  const span = end - t0;
  const stageRows = j.stages.map(s => {
    const a = (new Date(s.started_at).getTime() - t0) / span * 100, w = Math.max(0.8, ((new Date(s.completed_at || Date.now()).getTime()) - new Date(s.started_at).getTime()) / span * 100);
    return el('div', { class: 'tl-row' }, el('span', {}, s.stage_name, ' ', statusBadge(s.status)), el('div', { class: 'tl-bar' }, el('i', { style: `left:${a}%;width:${w}%` })),
      el('span', { class: 'num', text: fmtSec(s.duration_s) + (s.task_count ? ` · ${s.task_count}` : '') }));
  });
  const maxC = j.top_words.length ? j.top_words[0].count : 1;
  card.replaceChildren(
    el('h2', {}, `Job ${j.job_id} · ${j.job_type}`, ' ', statusBadge(j.status), el('small', { text: `dataset ${j.dataset_id ?? '–'} (${j.dataset_name ?? ''}) · ${j.params} · elapsed ${fmtSec(j.elapsed_s)} · ${j.tasks_done}/${j.tasks_total} tasks` })),
    j.result ? el('p', { class: 'muted', text: j.result }) : null,
    j.stages.length ? el('div', { class: 'timeline' }, stageRows) : null,
    el('h2', {}, 'Tasks'),
    table([{ h: 'Task', num: 1, f: t => t.task_id }, { h: 'Role', f: t => t.role }, { h: 'Block / partition', f: t => t.block_id || ('partition ' + t.partition_idx) },
      { h: 'Status', f: t => statusBadge(t.status) }, { h: 'Worker', f: t => t.assigned_worker }, { h: 'Try', num: 1, f: t => t.attempts }, { h: 'Time', num: 1, f: t => fmtSec(t.exec_s) },
      { h: 'Result', wrap: 1, f: t => t.last_error && t.status !== 'COMPLETED' ? t.last_error : t.result }], j.tasks),
    j.top_words.length ? el('div', {}, el('h2', {}, 'Top words (final result, from mr_results)'),
      el('div', { class: 'words' }, j.top_words.map(w => el('div', { class: 'w-row' }, el('span', { text: w.word }), el('div', { class: 'w-bar' }, el('i', { style: `width:${w.count / maxC * 100}%` })), el('span', { class: 'num', text: w.count }))))) : null);
}
$('#mrForm').onsubmit = async ev => {
  ev.preventDefault();
  try { const r = await post('/api/mapreduce', { dataset_id: $('#mrDataset').value, reducers: $('#mrRed').value, priority: $('#mrPrio').value }); STATE.selJob = r.job_id; setMsg($('#mrMsg'), `Job ${r.job_id} started.`, true); }
  catch (e) { setMsg($('#mrMsg'), e.message, false); }
  refresh();
};

/* -------------------------------------------------------------- resources ---- */
function lineChart(canvas, series, opts = {}) {
  const dpr = window.devicePixelRatio || 1, W = canvas.clientWidth, H = canvas.clientHeight;
  canvas.width = W * dpr; canvas.height = H * dpr;
  const g = canvas.getContext('2d'); g.scale(dpr, dpr);
  const css = getComputedStyle(document.documentElement), line = css.getPropertyValue('--line').trim() || '#ccc', muted = css.getPropertyValue('--muted').trim() || '#888';
  const L = 36, R = 8, T = 8, B = 20, pw = W - L - R, ph = H - T - B;
  const pts = series.flatMap(s => s.points);
  if (!pts.length) { g.fillStyle = muted; g.font = '12px system-ui'; g.fillText('no samples in this window yet', L + 10, H / 2); return; }
  const x0 = Math.min(...pts.map(p => p.x)), x1 = Math.max(...pts.map(p => p.x), x0 + 1);
  const ymax = opts.ymax || Math.max(1, ...pts.map(p => p.y)) * 1.15;
  g.strokeStyle = line; g.fillStyle = muted; g.font = '11px system-ui'; g.lineWidth = 1;
  for (let i = 0; i <= 4; i++) { const y = T + ph - ph * i / 4; g.beginPath(); g.moveTo(L, y); g.lineTo(W - R, y); g.stroke(); g.fillText(num(ymax * i / 4, ymax < 10 ? 1 : 0), 4, y + 4); }
  g.fillText(new Date(x0).toLocaleTimeString([], { hour12: false }), L, H - 5);
  const lbl = new Date(x1).toLocaleTimeString([], { hour12: false }); g.fillText(lbl, W - R - g.measureText(lbl).width, H - 5);
  for (const s of series) {
    g.strokeStyle = s.color; g.lineWidth = 1.8; g.beginPath();
    s.points.forEach((p, i) => { const x = L + (p.x - x0) / (x1 - x0) * pw, y = T + ph - Math.min(1, p.y / ymax) * ph; i ? g.lineTo(x, y) : g.moveTo(x, y); });
    g.stroke();
  }
}
async function loadResources() {
  const [ms, ws, conc] = await Promise.all([api('/api/metrics?minutes=' + $('#chartMinutes').value), loadWorkers(), api('/api/concurrency?limit=80')]);
  const by = {};
  ms.forEach(m => (by[m.worker_id] = by[m.worker_id] || []).push(m));
  const ids = Object.keys(by).sort();
  const mk = f => ids.map(id => ({ name: id, color: workerColor(id), points: by[id].map(m => ({ x: m.t_ms, y: f(m) ?? 0 })) }));
  lineChart($('#chCpu'), mk(m => m.cpu), { ymax: 100 }); lineChart($('#chRam'), mk(m => m.ram), { ymax: 100 }); lineChart($('#chRun'), mk(m => m.running_tasks));
  $('#legend').replaceChildren(...ids.map(id => el('span', {}, el('i', { style: `background:${workerColor(id)}` }), id)));

  put('#concStats', el('div', {}, el('p', {}, el('b', { text: 'Cluster: ' }), `max ${conc.cluster_max_concurrent} tasks were executing at the same moment.`),
    table([{ h: 'Worker', f: w => w.worker_id }, { h: 'Host', f: w => w.hostname }, { h: 'Configured limit', num: 1, f: w => w.capacity },
      { h: 'Max observed concurrent', num: 1, f: w => w.max_concurrent }, { h: 'Tasks completed', num: 1, f: w => w.completed }, { h: 'Avg duration', num: 1, f: w => w.avg_exec_s == null ? '–' : fmtSec(w.avg_exec_s) }], conc.per_worker)));
  $('#concNote').textContent = conc.note;
  gantt($('#chGantt'), conc.timeline);
}
function gantt(canvas, tl) {
  const dpr = window.devicePixelRatio || 1, W = canvas.clientWidth, H = canvas.clientHeight;
  canvas.width = W * dpr; canvas.height = H * dpr;
  const g = canvas.getContext('2d'); g.scale(dpr, dpr);
  const css = getComputedStyle(document.documentElement), muted = css.getPropertyValue('--muted').trim(), line = css.getPropertyValue('--line').trim();
  g.font = '11px system-ui'; g.fillStyle = muted;
  if (!tl.length) { g.fillText('no finished task with timestamps yet', 10, 20); return; }
  // zoom on the latest burst of activity: the last 30 s before the newest finished task
  const newest = Math.max(...tl.map(t => t.end_ms));
  tl = tl.filter(t => t.end_ms >= newest - 30000);
  const t0 = Math.min(...tl.map(t => t.start_ms)), t1 = Math.max(...tl.map(t => t.end_ms));
  const workers = [...new Set(tl.map(t => t.worker_id))].sort();
  const L = 50, R = 8, top = 6, laneH = Math.min(70, (H - 24 - top) / workers.length);
  workers.forEach((w, wi) => {
    const rows = [];                                    // greedy sub-lanes for overlapping tasks of one Worker
    const items = tl.filter(t => t.worker_id === w).sort((a, b) => a.start_ms - b.start_ms);
    items.forEach(t => { let r = rows.findIndex(end => end <= t.start_ms); if (r < 0) { r = rows.length; rows.push(0); } rows[r] = t.end_ms; t._r = r; });
    const rh = Math.max(4, Math.min(16, (laneH - 6) / Math.max(1, rows.length)));
    g.strokeStyle = line; g.beginPath(); g.moveTo(L, top + wi * laneH + laneH); g.lineTo(W - R, top + wi * laneH + laneH); g.stroke();
    g.fillStyle = muted; g.fillText(w, 4, top + wi * laneH + 14);
    items.forEach(t => {
      const x = L + (t.start_ms - t0) / Math.max(1, t1 - t0) * (W - L - R), wd = Math.max(2, (t.end_ms - t.start_ms) / Math.max(1, t1 - t0) * (W - L - R));
      g.fillStyle = workerColor(w); g.globalAlpha = .85; g.fillRect(x, top + wi * laneH + 3 + t._r * rh, wd, rh - 1.5); g.globalAlpha = 1;
    });
  });
  g.fillStyle = muted; g.fillText('time →  (' + ((t1 - t0) / 1000).toFixed(1) + ' s window, latest activity; stacked bars = tasks running at the same time on one Worker)', L, H - 6);
}
$('#chartMinutes').onchange = refresh;

/* --------------------------------------------------------------------- ML ---- */
async function loadMl() {
  const m = await api('/api/ml/status');
  const lr = m.latest_trained_run, last = m.latest_run;
  const metric = (l, v, s) => el('div', { class: 'kpi' }, el('div', { class: 'v', text: v }), el('div', { class: 'l', text: l }), s ? el('div', { class: 's', text: s }) : null);
  put('#mlStatus', el('div', {},
    el('div', { class: 'kpis' },
      metric('Active model', m.model_loaded ? (m.model_kind === 'xgboost' ? 'XGBoost' : 'Random Forest') : 'none', m.model_loaded ? `from run #${m.model_run_id}` : 'AI_ASSISTED uses the RESOURCE_AWARE fallback'),
      metric('XGBoost library', m.xgboost_available ? 'v' + m.xgboost_version : 'not built in', m.xgboost_available ? `rounds ${m.xgb_rounds}, depth ${m.xgb_max_depth}, eta ${m.xgb_eta}` : 'run scripts/install_xgboost.sh'),
      metric('Training pool', String(m.training_pool_size), `real completed executions · need ≥ ${m.min_samples}`),
      metric('Active policy', STATE.policy || '–', ''),
      lr ? metric('MAE (held-out)', fmtSec(lr.mae), `baseline "predict the mean": ${fmtSec(lr.baseline_mae)}`) : metric('MAE', '–', 'no evaluation yet'),
      lr ? metric('RMSE (held-out)', fmtSec(lr.rmse), `baseline: ${fmtSec(lr.baseline_rmse)}`) : metric('RMSE', '–', ''),
      m.live_accuracy ? metric('Live MAE', fmtSec(m.live_accuracy.mae), `over ${m.live_accuracy.n} scheduled tasks`) : metric('Live MAE', '–', 'no AI-scheduled task finished yet')),
    last ? el('p', { class: 'muted', text: `Latest run #${last.run_id}: ${last.status} — ${last.message}` }) : el('p', { class: 'muted', text: 'No training run yet. Run tasks to build history, then train.' })));
  put('#mlRuns', table([{ h: 'Run', num: 1, f: r => r.run_id }, { h: 'Algorithm', f: r => el('span', { title: r.hyperparams || '' }, r.model_type === 'xgboost' ? 'XGBoost' : 'Random Forest') }, { h: 'Status', f: r => statusBadge(r.status) }, { h: 'Samples', num: 1, f: r => r.n_samples }, { h: 'Train', num: 1, f: r => r.n_train }, { h: 'Test', num: 1, f: r => r.n_test },
    { h: 'MAE', num: 1, f: r => r.mae == null ? '–' : fmtSec(r.mae) }, { h: 'RMSE', num: 1, f: r => r.rmse == null ? '–' : fmtSec(r.rmse) }, { h: 'Baseline MAE', num: 1, f: r => r.baseline_mae == null ? '–' : fmtSec(r.baseline_mae) },
    { h: 'Compared with (same split)', f: r => r.compare_model ? `${r.compare_model}: MAE ${fmtSec(r.compare_mae)} / RMSE ${fmtSec(r.compare_rmse)}` : '–' },
    { h: 'Finished', f: r => fmtTime(r.finished_at) }, { h: 'Message', wrap: 1, f: r => r.message }], m.runs, { empty: 'No training run yet.' }));
  put('#mlPreds', table([{ h: 'Task', num: 1, f: p => p.task_id }, { h: 'Type', f: p => p.task_type }, { h: 'Worker', f: p => p.worker_id }, { h: 'Predicted', num: 1, f: p => fmtSec(p.predicted_seconds) },
    { h: 'Actual', num: 1, f: p => p.actual_seconds == null ? 'running…' : fmtSec(p.actual_seconds) }, { h: 'Error', num: 1, f: p => p.actual_seconds == null ? '–' : fmtSec(Math.abs(p.actual_seconds - p.predicted_seconds)) }, { h: 'When', f: p => fmtTime(p.created_at) }],
    m.recent_predictions, { empty: 'No prediction yet. Select the AI_ASSISTED policy once a model is trained and submit tasks.' }));
}
async function trainModel(type) {
  setMsg($('#mlMsg'), 'training ' + type + '…', true);
  try {
    const r = await post('/api/ml/train', { model_type: type });
    const cmp = r.compare_model ? ` · ${r.compare_model} on the same split: MAE ${fmtSec(r.compare_mae)}` : '';
    setMsg($('#mlMsg'), `${r.model_type} ${r.status}: ${r.message}` + (r.n_test ? ` (MAE ${fmtSec(r.mae)}, RMSE ${fmtSec(r.rmse)}, baseline MAE ${fmtSec(r.baseline_mae)})` : '') + cmp, r.status === 'TRAINED');
  } catch (e) { setMsg($('#mlMsg'), e.message, false); }
  refresh();
}
$('#mlTrain').onclick = () => trainModel('xgboost');
$('#mlTrainRf').onclick = () => trainModel('random_forest');

/* ------------------------------------------------------------- benchmarks ---- */
// Horizontal bar chart; every value comes from /api/benchmarks/:id (measured, never invented).
function barChart(canvas, labels, values, unit, colors) {
  const dpr = window.devicePixelRatio || 1, W = canvas.clientWidth || 300, H = 22 * labels.length + 26;
  canvas.style.height = H + 'px'; canvas.width = W * dpr; canvas.height = H * dpr;
  const g = canvas.getContext('2d'); g.scale(dpr, dpr);
  const css = getComputedStyle(document.documentElement), txt = css.getPropertyValue('--text').trim() || '#222', muted = css.getPropertyValue('--muted').trim() || '#888';
  const max = Math.max(1e-9, ...values.filter(v => v != null));
  const L = 118, R = 70;
  g.font = '12px system-ui'; g.textBaseline = 'middle';
  labels.forEach((lab, i) => {
    const y = 8 + i * 22, v = values[i];
    g.fillStyle = txt; g.fillText(lab, 4, y + 9);
    if (v == null) { g.fillStyle = muted; g.fillText('n/a', L, y + 9); return; }
    const w = Math.max(1, (W - L - R) * v / max);
    g.fillStyle = colors[i]; g.fillRect(L, y, w, 16);
    g.fillStyle = txt; g.fillText(v.toFixed(v < 10 ? 2 : 1) + ' ' + unit, L + w + 6, y + 9);
  });
}
const POLICY_COLORS = { FCFS: '#2f5bea', ROUND_ROBIN: '#e0691e', PRIORITY: '#15a06b', RESOURCE_AWARE: '#7a5af5', AI_ASSISTED: '#c2379b' };
const POLICY_LABEL = { FCFS: 'FCFS', ROUND_ROBIN: 'Round Robin', PRIORITY: 'Priority', RESOURCE_AWARE: 'Resource-Aware', AI_ASSISTED: 'AI (XGBoost)' };
const mean = a => { a = a.filter(v => v != null); return a.length ? a.reduce((x, y) => x + y, 0) / a.length : null; };

// One row per policy: the mean over repeats (a single run is its own mean).
function aggregate(results) {
  const by = {};
  results.forEach(r => (by[r.policy] = by[r.policy] || []).push(r));
  return Object.keys(by).map(p => {
    const rs = by[p], sum = k => rs.reduce((a, r) => a + (r[k] || 0), 0);
    const util = mean(rs.map(r => mean((r.worker_stats || []).map(w => w.utilization))));
    return { policy: p, runs: rs.length, status: rs.every(r => r.status === 'COMPLETED') ? 'COMPLETED' : rs.map(r => r.status).find(s => s !== 'COMPLETED'),
      makespan: mean(rs.map(r => r.makespan_s)), exec: mean(rs.map(r => r.avg_exec_s)), wait: mean(rs.map(r => r.avg_wait_s)), tps: mean(rs.map(r => r.throughput_tps)),
      completed: sum('tasks_completed'), total: sum('tasks_total'), failed: sum('tasks_failed'), retries: sum('retries'), cpu: mean(rs.map(r => r.avg_cpu)), ram: mean(rs.map(r => r.avg_ram)),
      util, local: sum('local_tasks'), blocks: sum('block_tasks'), preds: sum('ai_predictions'), fallbacks: sum('ai_fallbacks'),
      mae: mean(rs.map(r => r.pred_mae)), rmse: mean(rs.map(r => r.pred_rmse)), workers: rs[rs.length - 1].worker_stats || [] };
  });
}

async function loadBenchmarks() {
  const list = await api('/api/benchmarks');
  const running = list.running;
  $('#bfRun').disabled = running; $('#bfCancel').classList.toggle('hidden', !running);
  if (STATE.selBench == null && list.benchmarks.length) STATE.selBench = list.benchmarks[0].benchmark_id;
  const latest = list.benchmarks.length ? list.benchmarks[0].benchmark_id : null;
  put('#benchList', table([
    { h: '#', num: 1, f: b => b.benchmark_id }, { h: 'Name', f: b => b.name }, { h: 'Status', f: b => statusBadge(b.status) },
    { h: 'Policies', f: b => b.policies.split(',').map(p => POLICY_LABEL[p] || p).join(', ') },
    { h: 'Workload', f: b => { const w = JSON.parse(b.workload); return `${w.count}× ${w.type}, scale ${w.scale_min}-${w.scale_max}, seed ${w.seed}` + (w.repeats > 1 ? `, ${w.repeats} repeats` : ''); } },
    { h: 'Workers', num: 1, f: b => b.n_workers }, { h: 'Model', f: b => b.model_kind || '–' }, { h: 'Started', f: b => fmtTime(b.started_at) },
    { h: '', f: b => b.benchmark_id === latest ? badge('latest run', 'info') : badge('historical', 'mute') }], list.benchmarks,
    { empty: 'No benchmark has been run yet. Choose policies above and press "Run benchmark".', onClick: b => { STATE.selBench = b.benchmark_id; refresh(); } }));
  const box = $('#benchDetail');
  if (STATE.selBench == null) { box.classList.add('hidden'); return; }
  let b;
  try { b = await api('/api/benchmarks/' + STATE.selBench); } catch (e) { box.classList.add('hidden'); return; }
  box.classList.remove('hidden');
  const rows = aggregate(b.results);
  const wl = JSON.parse(b.workload);
  const head = el('h2', {}, `Benchmark #${b.benchmark_id} — ${b.name} `, statusBadge(b.status), ' ', b.benchmark_id === latest ? badge('latest run', 'info') : badge('historical run — not the latest', 'warn'));
  const kids = [head,
    el('p', { class: 'muted', text: `${wl.count} tasks per policy (${wl.type}, scale ${wl.scale_min}-${wl.scale_max}, priorities ${wl.priorities}, seed ${wl.seed}), ${wl.repeats} repeat(s), ${b.n_workers} Worker(s) [${b.worker_ids}]. ` +
      `Model available to AI_ASSISTED at start: ${b.model_kind || 'none'}. ` + (b.note ? 'Note: ' + b.note : '') })];
  if (b.status === 'RUNNING') kids.push(el('p', { class: 'msg ok', text: `running… ${b.results.length} of ${b.policies.split(',').length * wl.repeats} policy runs finished (results below are what has been measured so far)` }));
  if (b.status === 'FAILED' && b.note) kids.push(el('p', { class: 'msg err', text: b.note }));
  if (!rows.length) { kids.push(el('div', { class: 'empty', text: 'No policy run has finished yet.' })); box.replaceChildren(...kids); return; }
  const best = key => { const v = rows.filter(r => r[key] != null).map(r => r[key]); return v.length ? Math.min(...v) : null; };
  const bm = best('makespan');
  kids.push(table([
    { h: 'Policy', f: r => el('span', {}, el('i', { class: 'dot', style: `background:${POLICY_COLORS[r.policy]}` }), ' ', POLICY_LABEL[r.policy] || r.policy, r.policy === 'AI_ASSISTED' && r.fallbacks ? badge(`fell back ${r.fallbacks}×`, 'warn') : null) },
    { h: 'Status', f: r => statusBadge(r.status) },
    { h: 'Makespan', num: 1, f: r => el('span', {}, fmtSec(r.makespan), r.makespan === bm ? ' ★' : '') },
    { h: 'Avg exec', num: 1, f: r => fmtSec(r.exec) }, { h: 'Avg wait', num: 1, f: r => fmtSec(r.wait) },
    { h: 'Throughput', num: 1, f: r => num(r.tps, 2) + ' t/s' },
    { h: 'Done', num: 1, f: r => `${r.completed}/${r.total}` }, { h: 'Failed', num: 1, f: r => r.failed }, { h: 'Retries', num: 1, f: r => r.retries },
    { h: 'Worker util.', num: 1, f: r => r.util == null ? '–' : num(r.util * 100, 0) + '%' },
    { h: 'CPU @assign', num: 1, f: r => num(r.cpu, 0) + '%' }, { h: 'RAM @assign', num: 1, f: r => num(r.ram, 0) + '%' },
    { h: 'Data-local', num: 1, f: r => r.blocks ? `${r.local}/${r.blocks}` : 'n/a' },
    { h: 'Predicted vs actual (MAE / RMSE)', num: 1, f: r => r.policy === 'AI_ASSISTED' && r.mae != null ? `${fmtSec(r.mae)} / ${fmtSec(r.rmse)} (${r.preds} tasks)` : '–' }], rows));
  kids.push(el('p', { class: 'muted', text: '★ = lowest makespan. Makespan = last completion − first submission. Wait = submission → execution start. Utilisation = busy time ÷ (makespan × the Worker\'s max concurrent tasks), averaged over Workers that ran tasks.' }));
  const charts = el('div', { class: 'charts' });
  const mk = (title, key, unit, scale = 1) => {
    const c = el('canvas', { style: 'width:100%' });
    charts.append(el('figure', {}, el('figcaption', { text: title }), c));
    setTimeout(() => barChart(c, rows.map(r => POLICY_LABEL[r.policy] || r.policy), rows.map(r => r[key] == null ? null : r[key] * scale), unit, rows.map(r => POLICY_COLORS[r.policy])), 0);
  };
  mk('Makespan (lower is better)', 'makespan', 's'); mk('Average wait time (lower is better)', 'wait', 's');
  mk('Average execution time', 'exec', 's'); mk('Throughput (higher is better)', 'tps', 'tasks/s');
  mk('Average Worker utilisation', 'util', '%', 100); mk('Retries', 'retries', '');
  kids.push(charts);
  const ai = rows.find(r => r.policy === 'AI_ASSISTED');
  if (ai && ai.fallbacks) kids.push(el('p', { class: 'msg err', text: `AI_ASSISTED used the RESOURCE_AWARE fallback for ${ai.fallbacks} of ${ai.total} decisions (no usable trained model). Train the model on the AI model tab first for a real XGBoost comparison.` }));
  const wrows = rows.flatMap(r => r.workers.map(w => ({ policy: r.policy, ...w })));
  kids.push(el('h3', { text: 'Per-Worker load (last repeat of each policy)' }));
  kids.push(table([{ h: 'Policy', f: w => POLICY_LABEL[w.policy] || w.policy }, { h: 'Worker', f: w => w.worker }, { h: 'Tasks', num: 1, f: w => w.tasks },
    { h: 'Busy', num: 1, f: w => fmtSec(w.busy_s) }, { h: 'Max tasks', num: 1, f: w => w.capacity }, { h: 'CPU affinity', f: w => w.cpu_affinity || 'not pinned' },
    { h: 'Utilisation', f: w => bar(w.utilization * 100) }], wrows));
  box.replaceChildren(...kids);
}
$('#bfType').onchange = async () => {
  const block = $('#bfType').value.startsWith('BLOCK_');
  $('#bfDsWrap').classList.toggle('hidden', !block);
  if (block) { const ds = await api('/api/datasets'); $('#bfDataset').replaceChildren(...ds.map(d => el('option', { value: d.dataset_id, text: `#${d.dataset_id} ${d.name} (${d.block_count} blocks)` }))); }
};
$('#benchForm').onsubmit = async e => {
  e.preventDefault();
  const pols = [...document.querySelectorAll('input[name=pol]:checked')].map(c => c.value);
  if (!pols.length) return setMsg($('#bfMsg'), 'Select at least one policy.', false);
  const d = { policies: pols.join(','), type: $('#bfType').value, count: $('#bfCount').value, scale_min: $('#bfMin').value, scale_max: $('#bfMax').value,
    priorities: $('#bfPrio').value, seed: $('#bfSeed').value, repeats: $('#bfRep').value, retrain_before: $('#bfRetrain').checked ? '1' : '0',
    name: 'GUI benchmark ' + new Date().toLocaleString() };
  if ($('#bfType').value.startsWith('BLOCK_')) d.dataset_id = $('#bfDataset').value;
  try { const r = await post('/api/benchmarks', d); STATE.selBench = r.benchmark_id; setMsg($('#bfMsg'), `Benchmark #${r.benchmark_id} started — results appear below as each policy finishes.`, true); }
  catch (err) { setMsg($('#bfMsg'), err.message, false); }
  refresh();
};
$('#bfCancel').onclick = async () => { try { await post('/api/benchmarks/cancel'); setMsg($('#bfMsg'), 'Cancelling after the current task batch…', true); } catch (e) { setMsg($('#bfMsg'), e.message, false); } };

/* ------------------------------------------------------------------- logs ---- */
async function loadLogs() {
  const r = await api('/api/logs?after=' + STATE.logSeq + '&limit=300');
  const box = $('#logBox'), tagSel = $('#logTag');
  const stick = box.scrollTop + box.clientHeight >= box.scrollHeight - 30;
  for (const l of r.lines) {
    if (![...tagSel.options].some(o => o.value === l.tag)) tagSel.append(el('option', { value: l.tag, text: l.tag }));
    const row = el('div', { 'data-tag': l.tag }, el('span', { class: 't', text: new Date(l.ts).toLocaleTimeString([], { hour12: false }) + ' ' }), el('span', { class: 'tag', text: '[' + l.tag + '] ' }), l.msg);
    box.append(row);
  }
  STATE.logSeq = r.last_seq;
  while (box.childElementCount > 600) box.firstChild.remove();
  box.childNodes.forEach(n => n.style.display = (!tagSel.value || n.dataset.tag === tagSel.value) ? '' : 'none');
  if (stick) box.scrollTop = box.scrollHeight;
}
$('#logTag').onchange = () => { $('#logBox').childNodes.forEach(n => n.style.display = (!$('#logTag').value || n.dataset.tag === $('#logTag').value) ? '' : 'none'); };

/* ------------------------------------------------------------ refresh loop ---- */
let busy = false;
async function refresh() {
  if (busy) return; busy = true;
  try {
    await loadOverview();
    switch (STATE.tab) {
      case 'overview': {
        const ws = await loadWorkers();
        put('#ovWorkers', workersTable(ws, true));
        const ts = await api('/api/tasks?limit=8');
        put('#ovTasks', table(taskRow(), ts, { empty: 'No tasks yet.', onClick: t => showTask(t.task_id) }));
        break;
      }
      case 'workers': put('#workersTable', workersTable(await loadWorkers(), false)); break;
      case 'tasks': await loadTasks(); break;
      case 'scheduling': await loadScheduling(); break;
      case 'storage': await loadStorage(); break;
      case 'mapreduce': await loadMapReduce(); break;
      case 'resources': await loadResources(); break;
      case 'ml': await loadMl(); break;
      case 'benchmarks': await loadBenchmarks(); break;
      case 'logs': await loadLogs(); break;
    }
  } catch (e) { if (STATE.connected) console.error(e); }
  busy = false;
}
setInterval(refresh, 2000);
window.addEventListener('resize', () => { if (STATE.tab === 'resources') refresh(); });
selectTab(location.hash.slice(1) || 'overview');
