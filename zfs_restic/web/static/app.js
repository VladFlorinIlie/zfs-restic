"use strict";
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const bytes = (n) => { if (n == null) return "–"; const u = ["B","KiB","MiB","GiB","TiB"]; let i = 0; while (Math.abs(n) >= 1024 && i < u.length - 1) { n /= 1024; i++; } return (i ? n.toFixed(1) : n) + " " + u[i]; };
const dur = (s) => { if (s == null || isNaN(s)) return "–"; s = Math.round(s); const h = Math.floor(s/3600), m = Math.floor(s%3600/60), x = s%60; return h ? `${h}h ${m}m` : m ? `${m}m ${x}s` : `${x}s`; };
const fmtTime = (t) => t ? new Date(t).toLocaleString(undefined, {dateStyle: "medium", timeStyle: "short"}) : "–";
const ago = (t) => { if (!t) return "never"; const s = (Date.now() - new Date(t)) / 1000; return s < 90 ? "just now" : s < 5400 ? `${Math.round(s/60)} min ago` : s < 172800 ? `${(s/3600).toFixed(1)} h ago` : `${Math.round(s/86400)} days ago`; };
const pill = (o) => `<span class="pill ${esc(o)}">${esc(o)}</span>`;

async function api(path, opts = {}) {
  const r = await fetch(path, opts);
  let body = null; try { body = await r.json(); } catch {}
  if (!r.ok && r.status !== 503) throw new Error((body && (body.message + (body.details ? "\n" + body.details : ""))) || `HTTP ${r.status}`);
  return body;
}

// ---------- live status ----------
let lastState = null, lastRunId = null;
async function pollStatus() {
  try {
    const s = await api("/api/status?log_lines=400");
    $("authBanner").classList.toggle("hidden", s.auth_enabled);
    const running = s.live_status === "running";
    $("statePill").className = "pill " + (running ? "running" : "idle");
    $("statePill").textContent = running ? `running ${s.job}` : "idle";
    const lc = s.last_completed_run;
    $("stateText").textContent = running ? s.current_task : (lc.finish_time ? `last backup: ${lc.outcome}, ${ago(lc.finish_time)}` : "");
    $("btnBackup").disabled = running; $("btnCheck").disabled = running;
    $("liveSection").classList.toggle("hidden", !running);
    if (running) {
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
    if (lastState === "running" && !running) { loadRuns(true); loadHealth(); loadSnaps(true); }
    if (running && lastRunId !== s.run_id) loadRuns(true);
    lastState = s.live_status; lastRunId = s.run_id;
  } catch (e) { $("stateText").textContent = "status unavailable: " + e.message; }
  setTimeout(pollStatus, lastState === "running" ? 2000 : 8000);
}

async function startJob(kind) {
  const opts = { method: "POST" };
  if (kind === "check") {
    const subset = $("subset").value.trim();
    opts.headers = { "Content-Type": "application/json" };
    opts.body = JSON.stringify(subset ? { read_data_subset: subset } : {});
  }
  try { await api(`/api/${kind}?trigger=ui`, opts); lastState = null; pollStatus(); }
  catch (e) { alert(e.message); }
}
$("btnBackup").onclick = () => confirm("Start a backup of all configured datasets now?") && startJob("backup");
$("btnCheck").onclick = () => startJob("check");

// ---------- datasets / health ----------
async function loadHealth() {
  try {
    const h = await api("/api/health");
    if (h.error) throw new Error(h.error);
    $("healthPill").innerHTML = h.ok ? `<span class="pill success">all fresh</span>` : `<span class="pill failure">stale backups</span>`;
    $("cards").innerHTML = h.datasets.map(d => {
      const cls = d.stale ? "stale" : d.age_hours > h.max_age_hours * 0.75 ? "aging" : "";
      return `<div class="card ${cls}">
        <div class="name">${esc(d.dataset)}</div>
        <div class="age">${esc(ago(d.latest_time))}</div>
        <div class="muted small">${d.snapshots} snapshots · latest <span class="mono">${esc(d.latest_id || "–")}</span></div>
        <div class="muted small">size ${bytes(d.latest_total_bytes)} · last added ${bytes(d.latest_data_added)}</div>
      </div>`;
    }).join("") || `<span class="muted">No datasets in config.</span>`;
    const f = $("snapFilter"), cur = f.value;
    f.innerHTML = `<option value="">All datasets</option>` + h.datasets.map(d => `<option>${esc(d.dataset)}</option>`).join("");
    f.value = cur;
  } catch (e) {
    $("healthPill").innerHTML = `<span class="pill failure">repo unreachable</span>`;
    $("cards").innerHTML = `<div class="err">${esc(e.message)}</div>`;
  }
}

// ---------- history ----------
let runOffset = 0; const RUN_PAGE = 25; const openRuns = new Set();
async function loadRuns(reset) {
  if (reset) runOffset = 0;
  const runs = await api(`/api/runs?limit=${reset ? Math.max(RUN_PAGE, runOffset) : RUN_PAGE}&offset=${reset ? 0 : runOffset}`);
  const rows = runs.map(r => {
    const d = r.finished_at ? (new Date(r.finished_at) - new Date(r.started_at)) / 1000 : (Date.now() - new Date(r.started_at)) / 1000;
    return `<tr class="clickable" data-run="${r.id}">
      <td>${esc(fmtTime(r.started_at))}</td><td>${esc(r.kind)}</td><td class="muted">${esc(r.trigger)}</td>
      <td>${pill(r.outcome)}</td><td class="num">${dur(d)}</td>
      <td class="num">${r.kind === "backup" ? (r.datasets - r.datasets_failed) + "/" + r.datasets : "–"}</td>
      <td class="num">${r.kind === "backup" ? bytes(r.data_added) : "–"}</td></tr>
      <tr class="${openRuns.has(r.id) ? "" : "hidden"}" id="detail-${r.id}"><td colspan="7" class="detail"></td></tr>`;
  }).join("");
  if (reset) { $("runs").innerHTML = rows; runOffset = Math.max(RUN_PAGE, runOffset); }
  else { $("runs").insertAdjacentHTML("beforeend", rows); runOffset += RUN_PAGE; }
  $("btnMoreRuns").disabled = runs.length < RUN_PAGE && !reset;
  if (!runs.length && reset) $("runs").innerHTML = `<tr><td colspan="7" class="muted">No runs yet.</td></tr>`;
  openRuns.forEach(id => $("detail-" + id) && showRun(id));
}
$("btnMoreRuns").onclick = () => loadRuns(false);
$("runs").addEventListener("click", (ev) => {
  const tr = ev.target.closest("tr[data-run]"); if (!tr) return;
  const id = +tr.dataset.run, det = $("detail-" + id);
  if (openRuns.has(id)) { openRuns.delete(id); det.classList.add("hidden"); }
  else { openRuns.add(id); det.classList.remove("hidden"); showRun(id); }
});
async function showRun(id) {
  const cell = $("detail-" + id).firstElementChild;
  const r = await api(`/api/runs/${id}`);
  const results = r.results.length ? `<table style="margin-bottom:10px"><thead><tr><th>Dataset</th><th>Outcome</th><th>Snapshot</th><th class="num">New</th><th class="num">Changed</th><th class="num">Added</th><th class="num">Processed</th><th>Error</th></tr></thead><tbody>` +
    r.results.map(x => `<tr><td class="mono">${esc(x.dataset)}</td><td>${pill(x.outcome)}</td><td class="mono">${esc((x.snapshot_id || "–").slice(0, 8))}</td>
      <td class="num">${x.files_new ?? "–"}</td><td class="num">${x.files_changed ?? "–"}</td><td class="num">${bytes(x.data_added)}</td><td class="num">${bytes(x.total_bytes)}</td><td class="err">${esc(x.error || "")}</td></tr>`).join("") + `</tbody></table>` : "";
  cell.innerHTML = `${r.details ? `<div style="white-space:pre-wrap;margin-bottom:10px">${esc(r.details)}</div>` : ""}${results}<pre class="log">${esc(r.log || "(no log)")}</pre>`;
}

// ---------- snapshots + browser ----------
let snapshots = [];
async function loadSnaps(refresh) {
  try { snapshots = await api("/api/snapshots" + (refresh ? "?refresh=1" : "")); renderSnaps(); }
  catch (e) { $("snaps").innerHTML = `<tr><td colspan="7" class="err">${esc(e.message)}</td></tr>`; }
}
function datasetOf(s) { const ds = [...$("snapFilter").options].map(o => o.value).filter(Boolean); return (s.tags || []).find(t => ds.includes(t)) || (s.tags || []).find(t => !t.includes("@")) || "–"; }
function renderSnaps() {
  const f = $("snapFilter").value;
  const list = snapshots.filter(s => !f || (s.tags || []).includes(f));
  $("snaps").innerHTML = list.slice(0, 300).map(s => `<tr>
    <td>${esc(fmtTime(s.time))}</td><td class="mono">${esc(s.short_id)}</td><td class="mono">${esc(datasetOf(s))}</td>
    <td class="num">${bytes(s.summary?.data_added)}</td><td class="num">${bytes(s.summary?.total_bytes_processed)}</td>
    <td class="num">${s.summary ? (s.summary.total_files_processed ?? "–") : "–"}</td>
    <td><a class="link" data-browse="${esc(s.short_id)}" data-root="${esc(s.paths[0])}">browse</a></td></tr>`).join("")
    || `<tr><td colspan="7" class="muted">No snapshots.</td></tr>`;
}
$("snapFilter").onchange = renderSnaps;
$("btnRefreshSnaps").onclick = () => loadSnaps(true);
$("snaps").addEventListener("click", (ev) => { const a = ev.target.closest("[data-browse]"); if (a) browse(a.dataset.browse, a.dataset.root, a.dataset.root); });

let br = null;
async function browse(snap, root, path) {
  br = { snap, root, path };
  $("browser").classList.remove("hidden");
  $("brSnap").textContent = snap;
  const rel = path.slice(root.length).split("/").filter(Boolean);
  let acc = root;
  $("brCrumbs").innerHTML = `<a data-path="${esc(root)}">/</a>` + rel.map(p => { acc += "/" + p; return ` / <a data-path="${esc(acc)}">${esc(p)}</a>`; }).join("");
  $("brBody").innerHTML = `<span class="muted">Listing… (first listing of a snapshot loads the index and can take a while)</span>`;
  try {
    const entries = await api(`/api/snapshots/${snap}/ls?path=${encodeURIComponent(path)}`);
    const dl = (p, dir) => `/api/snapshots/${snap}/download?path=${encodeURIComponent(p)}${dir ? "&dir=1" : ""}`;
    $("brBody").innerHTML = `<table><thead><tr><th>Name</th><th class="num">Size</th><th>Modified</th><th></th></tr></thead><tbody>` +
      entries.map(e => `<tr>
        <td class="mono">${e.type === "dir" ? `<a class="link" data-path="${esc(e.path)}">📁 ${esc(e.name)}/</a>` : esc((e.type === "symlink" ? "↪ " : "") + e.name)}</td>
        <td class="num">${e.type === "file" ? bytes(e.size) : ""}</td><td class="muted">${esc(fmtTime(e.mtime))}</td>
        <td>${e.type === "file" ? `<a class="link" href="${dl(e.path)}">download</a>` : e.type === "dir" ? `<a class="link" href="${dl(e.path, true)}">zip</a>` : ""}</td></tr>`).join("") +
      `</tbody></table>` + (entries.length ? "" : `<div class="muted">Empty directory.</div>`);
    $("brZip").onclick = () => { location.href = dl(path, true); };
  } catch (e) { $("brBody").innerHTML = `<div class="err">${esc(e.message)}</div>`; }
}
$("browser").addEventListener("click", (ev) => { const a = ev.target.closest("a[data-path]"); if (a) browse(br.snap, br.root, a.dataset.path); });
$("brClose").onclick = () => $("browser").classList.add("hidden");

// ---------- boot ----------
pollStatus(); loadRuns(true); loadHealth().then(() => loadSnaps(false));
setInterval(loadHealth, 5 * 60 * 1000);
