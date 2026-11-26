// Snapshot list, file browser (download / restore to server) and snapshot diff.
import { $, esc, bytes, fmtTime, api, state, datasetOf, downloadUrl, restoreToServer } from "./util.js";

let br = null, diffData = null;

export async function loadSnaps(refresh) {
  try { state.snapshots = await api("/api/snapshots" + (refresh ? "?refresh=1" : "")); renderSnaps(); }
  catch (e) { $("snaps").innerHTML = `<tr><td colspan="7" class="err">${esc(e.message)}</td></tr>`; }
}

export function refreshDatasetSelects() {
  for (const id of ["snapFilter", "findDataset"]) {
    const f = $(id), cur = f.value;
    f.innerHTML = `<option value="">All datasets</option>` + state.datasets.map(d => `<option>${esc(d)}</option>`).join("");
    f.value = cur;
  }
}

function previousOf(snap) {
  // snapshots are newest first; the previous one is the next older snapshot with the same paths
  const key = JSON.stringify(snap.paths);
  const idx = state.snapshots.indexOf(snap);
  return state.snapshots.slice(idx + 1).find(s => JSON.stringify(s.paths) === key);
}

function renderSnaps() {
  const f = $("snapFilter").value;
  const list = state.snapshots.filter(s => !f || (s.tags || []).includes(f));
  $("snaps").innerHTML = list.slice(0, 300).map(s => {
    const prev = previousOf(s);
    return `<tr>
    <td>${esc(fmtTime(s.time))}</td><td class="mono">${esc(s.short_id)}</td><td class="mono">${esc(datasetOf(s))}</td>
    <td class="num">${bytes(s.summary?.data_added)}</td><td class="num">${bytes(s.summary?.total_bytes_processed)}</td>
    <td class="num">${s.summary ? (s.summary.total_files_processed ?? "–") : "–"}</td>
    <td class="row" style="gap:12px"><a class="link" data-browse="${esc(s.short_id)}" data-root="${esc(s.paths[0])}">browse</a>
      ${prev ? `<a class="link" data-diff="${esc(prev.short_id)}:${esc(s.short_id)}" title="compare with ${esc(prev.short_id)}">changes</a>` : ""}</td></tr>`;
  }).join("") || `<tr><td colspan="7" class="muted">No snapshots.</td></tr>`;
}

export async function browse(snap, root, path) {
  br = { snap, root, path };
  $("browser").classList.remove("hidden");
  $("brSnap").textContent = snap;
  $("brRestore").classList.toggle("hidden", !state.status?.restore?.available);
  const rel = path.slice(root.length).split("/").filter(Boolean);
  let acc = root;
  $("brCrumbs").innerHTML = `<a data-path="${esc(root)}">/</a>` + rel.map(p => { acc += "/" + p; return ` / <a data-path="${esc(acc)}">${esc(p)}</a>`; }).join("");
  $("brBody").innerHTML = `<span class="muted">Listing… (the first listing of a snapshot loads the index and can take a while)</span>`;
  $("browser").scrollIntoView({ behavior: "smooth", block: "start" });
  try {
    const entries = await api(`/api/snapshots/${snap}/ls?path=${encodeURIComponent(path)}`);
    const canRestore = state.status?.restore?.available;
    $("brBody").innerHTML = `<table><thead><tr><th>Name</th><th class="num">Size</th><th>Modified</th><th></th></tr></thead><tbody>` +
      entries.map(e => `<tr>
        <td class="mono">${e.type === "dir" ? `<a class="link" data-path="${esc(e.path)}">📁 ${esc(e.name)}/</a>` : esc((e.type === "symlink" ? "↪ " : "") + e.name)}</td>
        <td class="num">${e.type === "file" ? bytes(e.size) : ""}</td><td class="muted">${esc(fmtTime(e.mtime))}</td>
        <td class="row" style="gap:12px">${e.type === "file" ? `<a class="link" href="${downloadUrl(snap, e.path)}">download</a>` : e.type === "dir" ? `<a class="link" href="${downloadUrl(snap, e.path, true)}">zip</a>` : ""}
          ${canRestore && e.type !== "symlink" ? `<a class="link" data-restore="${esc(e.path)}" data-dir="${e.type === "dir" ? 1 : 0}">restore to server</a>` : ""}</td></tr>`).join("") +
      `</tbody></table>` + (entries.length ? "" : `<div class="muted">Empty directory.</div>`);
  } catch (e) { $("brBody").innerHTML = `<div class="err">${esc(e.message)}</div>`; }
}

async function showDiff(oldId, newId) {
  $("diffPanel").classList.remove("hidden");
  $("diffTitle").textContent = `${oldId} → ${newId}`;
  $("diffBody").innerHTML = `<span class="muted">Comparing… (reads both directory trees)</span>`;
  $("diffPanel").scrollIntoView({ behavior: "smooth", block: "start" });
  try { diffData = await api(`/api/snapshots/${oldId}/diff/${newId}`); renderDiff(); }
  catch (e) { $("diffBody").innerHTML = `<div class="err">${esc(e.message)}</div>`; }
}

function renderDiff() {
  if (!diffData) return;
  const st = diffData.statistics || {}, f = $("diffFilter").value;
  const cls = { "+": "mod-add", "-": "mod-del", "M": "mod-mod" };
  const rows = diffData.changes.filter(c => !f || c.modifier === f).slice(0, 2000);
  $("diffBody").innerHTML = `<div class="stats">
      <div><span class="muted small">Added</span><b class="mod-add">${st.added?.files ?? 0} files · ${bytes(st.added?.bytes)}</b></div>
      <div><span class="muted small">Removed</span><b class="mod-del">${st.removed?.files ?? 0} files · ${bytes(st.removed?.bytes)}</b></div>
      <div><span class="muted small">Modified</span><b class="mod-mod">${st.changed_files ?? 0} files</b></div>
      <div><span class="muted small">Total changes</span><b>${diffData.total_changes}</b></div></div>
    ${diffData.truncated ? `<div class="msg-warn small">Showing the first ${diffData.changes.length} changes.</div>` : ""}
    <pre class="log">${rows.map(c => `<span class="${cls[c.modifier] || ""}">${esc(c.modifier.padEnd(2))}${esc(c.path)}</span>`).join("\n") || "No changes."}</pre>`;
}

export function initSnapshots() {
  $("snapFilter").onchange = renderSnaps;
  $("btnRefreshSnaps").onclick = () => loadSnaps(true);
  $("snaps").addEventListener("click", (ev) => {
    const a = ev.target.closest("[data-browse]"); if (a) return browse(a.dataset.browse, a.dataset.root, a.dataset.root);
    const d = ev.target.closest("[data-diff]"); if (d) { const [o, n] = d.dataset.diff.split(":"); showDiff(o, n); }
  });
  $("browser").addEventListener("click", (ev) => {
    const a = ev.target.closest("a[data-path]"); if (a) return browse(br.snap, br.root, a.dataset.path);
    const r = ev.target.closest("a[data-restore]"); if (r) restoreToServer(br.snap, r.dataset.restore, r.dataset.dir === "1");
  });
  $("brZip").onclick = () => { location.href = downloadUrl(br.snap, br.path, true); };
  $("brRestore").onclick = () => restoreToServer(br.snap, br.path, true);
  $("brClose").onclick = () => $("browser").classList.add("hidden");
  $("diffFilter").onchange = renderDiff;
  $("diffClose").onclick = () => $("diffPanel").classList.add("hidden");
}
