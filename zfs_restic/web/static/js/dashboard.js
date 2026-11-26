// Live status, dataset/storage cards and run history.
import { $, esc, bytes, dur, fmtTime, ago, pill, api, state } from "./util.js";

let lastState = null, lastRunId = null;
const listeners = state.listeners;

export async function pollStatus() {
  try {
    const s = await api("/api/status?log_lines=400");
    state.status = s;
    $("authBanner").classList.toggle("hidden", s.auth_enabled);
    $("historyBanner").classList.toggle("hidden", !s.history_warning);
    $("historyBanner").textContent = s.history_warning || "";
    const running = s.live_status === "running";
    $("statePill").className = "pill " + (running ? "running" : "idle");
    $("statePill").textContent = running ? `running ${s.job}` : "idle";
    const lc = s.last_completed_run;
    $("stateText").textContent = running ? s.current_task : (lc.finish_time ? `last backup: ${lc.outcome}, ${ago(lc.finish_time)}` : "");
    $("btnBackup").disabled = running; $("btnCheck").disabled = running;
    $("liveSection").classList.toggle("hidden", !running);
    if (running) renderLive(s);
    if (lastState === "running" && !running) { loadRuns(true); loadHealth(); listeners.forEach(fn => fn("job-finished")); }
    if (running && lastRunId !== s.run_id) loadRuns(true);
    if (lastState === null) listeners.forEach(fn => fn("status"));
    lastState = s.live_status; lastRunId = s.run_id;
  } catch (e) { $("stateText").textContent = "status unavailable: " + e.message; }
  setTimeout(pollStatus, lastState === "running" ? 2000 : 8000);
}

function renderLive(s) {
  $("liveKind").textContent = `${s.job} #${s.run_id}`;
  $("liveDataset").textContent = s.dataset_total ? `dataset ${s.dataset_index}/${s.dataset_total}` : "";
  $("liveTask").textContent = s.current_task;
  const p = s.progress;
  $("liveProgressWrap").classList.toggle("hidden", !p);
  if (p) {
    const pct = (p.percent || 0) * 100;
    $("liveBar").style.width = pct.toFixed(1) + "%";
    $("lvPct").textContent = pct.toFixed(1) + "%";
    $("lvBytes").textContent = `${bytes(p.bytes_done)} / ${bytes(p.total_bytes)}`;
    $("lvFiles").textContent = `${p.files_done ?? 0} / ${p.total_files ?? "?"}`;
    $("lvEta").textContent = p.eta_seconds ? dur(p.eta_seconds) : "–";
    $("lvErr").textContent = p.error_count || 0;
    $("lvCurrent").textContent = (p.current_files || []).join("\n");
  }
  const log = $("liveLog"), atBottom = log.scrollTop + log.clientHeight >= log.scrollHeight - 20;
  log.textContent = s.log.join("\n");
  if (atBottom) log.scrollTop = log.scrollHeight;
}

export async function startJob(kind) {
  const opts = { method: "POST" };
  if (kind === "check") {
    const subset = $("subset").value.trim();
    opts.json = subset ? { read_data_subset: subset } : {};
  }
  try { await api(`/api/${kind}?trigger=ui`, opts); lastState = null; pollStatus(); }
  catch (e) { alert(e.message); }
}

// ---------- datasets, storage, health ----------
export async function loadHealth() {
  try {
    const h = await api("/api/health", { allowStatus: [503] });
    if (h.error) throw new Error(h.error);
    state.datasets = h.datasets.map(d => d.dataset);
    $("healthPill").innerHTML = h.ok ? `<span class="pill success">all good</span>`
      : `<span class="pill failure">${h.storage?.warning && !h.datasets.some(d => d.stale) ? "storage nearly full" : "needs attention"}</span>`;
    let html = h.datasets.map(d => {
      const cls = d.stale ? "stale" : d.age_hours > h.max_age_hours * 0.75 ? "aging" : "";
      return `<div class="card ${cls}">
        <div class="name">${esc(d.dataset)}</div>
        <div class="age">${esc(ago(d.latest_time))}</div>
        <div class="muted small">${d.snapshots} snapshots · latest <span class="mono">${esc(d.latest_id || "–")}</span></div>
        <div class="muted small">size ${bytes(d.latest_total_bytes)} · last added ${bytes(d.latest_data_added)}</div>
      </div>`;
    }).join("") || `<span class="muted">No datasets in config.</span>`;
    const st = h.storage;
    if (st && st.remote) {
      html += `<div class="card ${st.warning ? "stale" : ""}">
        <div class="name">${esc(st.remote)}: <span class="muted">storage</span></div>
        <div class="age">${st.percent_used != null ? st.percent_used.toFixed(0) + "% used" : "?"}</div>
        <div class="bar ${st.warning ? "warnbar" : ""}"><div style="width:${Math.min(st.percent_used || 0, 100)}%"></div></div>
        <div class="muted small">${bytes(st.used)} of ${bytes(st.total)} · ${bytes(st.free)} free${st.trashed ? ` · ${bytes(st.trashed)} in trash` : ""}</div>
      </div>`;
    } else if (st && st.error) {
      html += `<div class="card aging"><div class="name">storage</div><div class="err">${esc(st.error)}</div></div>`;
    }
    $("cards").innerHTML = html;
    state.listeners.forEach(fn => fn("datasets"));
  } catch (e) {
    $("healthPill").innerHTML = `<span class="pill failure">repo unreachable</span>`;
    $("cards").innerHTML = `<div class="err">${esc(e.message)}</div>`;
  }
}

// ---------- history ----------
let runOffset = 0; const RUN_PAGE = 25; const openRuns = new Set();
export async function loadRuns(reset) {
  if (reset) runOffset = 0;
  const runs = await api(`/api/runs?limit=${reset ? Math.max(RUN_PAGE, runOffset) : RUN_PAGE}&offset=${reset ? 0 : runOffset}`);
  const rows = runs.map(r => {
    const d = ((r.finished_at ? new Date(r.finished_at) : Date.now()) - new Date(r.started_at)) / 1000;
    const isBackup = r.kind === "backup";
    return `<tr class="clickable" data-run="${r.id}">
      <td>${esc(fmtTime(r.started_at))}</td><td>${esc(r.kind)}</td><td class="muted">${esc(r.trigger)}</td>
      <td>${pill(r.outcome)}</td><td class="num">${dur(d)}</td>
      <td class="num">${isBackup ? (r.datasets - r.datasets_failed) + "/" + r.datasets : "–"}</td>
      <td class="num">${isBackup ? bytes(r.data_added) : "–"}</td></tr>
      <tr class="${openRuns.has(r.id) ? "" : "hidden"}" id="detail-${r.id}"><td colspan="7" class="detail"></td></tr>`;
  }).join("");
  if (reset) { $("runs").innerHTML = rows; runOffset = Math.max(RUN_PAGE, runOffset); }
  else { $("runs").insertAdjacentHTML("beforeend", rows); runOffset += RUN_PAGE; }
  $("btnMoreRuns").disabled = runs.length < RUN_PAGE && !reset;
  if (!runs.length && reset) $("runs").innerHTML = `<tr><td colspan="7" class="muted">No runs yet.</td></tr>`;
  openRuns.forEach(id => $("detail-" + id) && showRun(id));
}

async function showRun(id) {
  const cell = $("detail-" + id).firstElementChild;
  const r = await api(`/api/runs/${id}`);
  const results = r.results.length ? `<table style="margin-bottom:10px"><thead><tr><th>Dataset</th><th>Outcome</th><th>Snapshot</th><th class="num">New</th><th class="num">Changed</th><th class="num">Added</th><th class="num">Processed</th><th>Error</th></tr></thead><tbody>` +
    r.results.map(x => `<tr><td class="mono">${esc(x.dataset)}</td><td>${pill(x.outcome)}</td><td class="mono">${esc((x.snapshot_id || "–").slice(0, 8))}</td>
      <td class="num">${x.files_new ?? "–"}</td><td class="num">${x.files_changed ?? "–"}</td><td class="num">${bytes(x.data_added)}</td><td class="num">${bytes(x.total_bytes)}</td><td class="err">${esc(x.error || "")}</td></tr>`).join("") + `</tbody></table>` : "";
  cell.innerHTML = `${r.details ? `<div style="white-space:pre-wrap;margin-bottom:10px">${esc(r.details)}</div>` : ""}${results}<pre class="log">${esc(r.log || "(no log)")}</pre>`;
}

export function initDashboard() {
  $("btnBackup").onclick = () => confirm("Start a backup of all configured datasets now?") && startJob("backup");
  $("btnCheck").onclick = () => startJob("check");
  $("btnMoreRuns").onclick = () => loadRuns(false);
  $("runs").addEventListener("click", (ev) => {
    const tr = ev.target.closest("tr[data-run]"); if (!tr) return;
    const id = +tr.dataset.run, det = $("detail-" + id);
    if (openRuns.has(id)) { openRuns.delete(id); det.classList.add("hidden"); }
    else { openRuns.add(id); det.classList.remove("hidden"); showRun(id); }
  });
  loadRuns(true);
  loadHealth();
  setInterval(loadHealth, 5 * 60 * 1000);
}
