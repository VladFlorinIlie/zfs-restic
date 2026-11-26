// Search every snapshot for a file and show its history.
import { $, esc, bytes, fmtTime, api, state, datasetOf, downloadUrl, restoreToServer } from "./util.js";

function latestPerPaths() {
  const latest = {};
  for (const s of state.snapshots) {          // newest first
    const key = JSON.stringify(s.paths);
    if (!latest[key]) latest[key] = s;
  }
  return latest;
}

function renderResults(res) {
  if (!res.results.length) { $("findResults").innerHTML = `<p class="muted">No matches in ${res.snapshots_searched} snapshots.</p>`; return; }
  const latest = latestPerPaths();
  const byId = Object.fromEntries(state.snapshots.map(s => [s.id, s]));
  const html = res.results.map((r, i) => {
    const newest = r.seen[0], oldest = r.seen[r.seen.length - 1];
    const snap = byId[newest.snapshot_id];
    const latestSnap = snap && latest[JSON.stringify(snap.paths)];
    const gone = latestSnap && latestSnap.id !== newest.snapshot_id;
    const sizes = new Set(r.seen.map(x => x.size)).size;
    return `<tr class="clickable" data-i="${i}">
      <td class="mono">${esc(r.path)}${gone ? `<span class="tag mod-del" title="not in the newest snapshot ${esc(latestSnap.short_id)}">gone since ${esc(fmtTime(latestSnap.time))}</span>` : ""}</td>
      <td class="mono">${esc(snap ? datasetOf(snap) : "")}</td>
      <td class="num">${r.seen.length}</td><td>${esc(fmtTime(oldest.time))}</td><td>${esc(fmtTime(newest.time))}</td>
      <td class="num">${r.type === "file" ? bytes(newest.size) + (sizes > 1 ? ` <span class="muted small">(${sizes} versions)</span>` : "") : esc(r.type)}</td></tr>
      <tr class="hidden" id="find-${i}"><td colspan="6" class="detail">
        <table><thead><tr><th>Snapshot</th><th>Time</th><th class="num">Size</th><th>Modified</th><th></th></tr></thead><tbody>
        ${r.seen.map(x => `<tr><td class="mono">${esc(x.short_id || x.snapshot_id.slice(0, 8))}</td><td>${esc(fmtTime(x.time))}</td>
          <td class="num">${r.type === "file" ? bytes(x.size) : ""}</td><td class="muted">${esc(fmtTime(x.mtime))}</td>
          <td class="row" style="gap:12px"><a class="link" href="${downloadUrl(x.snapshot_id, r.path, r.type === "dir")}">${r.type === "dir" ? "zip" : "download"}</a>
          ${state.status?.restore?.available ? `<a class="link" data-restore="${esc(x.snapshot_id)}" data-path="${esc(r.path)}" data-dir="${r.type === "dir" ? 1 : 0}">restore to server</a>` : ""}</td></tr>`).join("")}
        </tbody></table></td></tr>`;
  }).join("");
  $("findResults").innerHTML = `<p class="muted small">${res.results.length} paths, ${res.matches} matches in ${res.snapshots_searched} snapshots${res.truncated ? " (truncated, refine the pattern)" : ""}.</p>
    <table><thead><tr><th>Path</th><th>Dataset</th><th class="num">In snapshots</th><th>First seen</th><th>Last seen</th><th class="num">Size</th></tr></thead><tbody>${html}</tbody></table>`;
}

export function initFind() {
  $("findForm").onsubmit = async (ev) => {
    ev.preventDefault();
    const q = new URLSearchParams({ pattern: $("findPattern").value.trim(), dataset: $("findDataset").value, ignore_case: $("findIcase").checked ? "1" : "0" });
    $("findResults").innerHTML = `<p class="muted">Searching all snapshots…</p>`;
    try {
      if (!state.snapshots.length) state.snapshots = await api("/api/snapshots");
      renderResults(await api("/api/find?" + q));
    } catch (e) { $("findResults").innerHTML = `<div class="err">${esc(e.message)}</div>`; }
  };
  $("findResults").addEventListener("click", (ev) => {
    const r = ev.target.closest("a[data-restore]");
    if (r) return restoreToServer(r.dataset.restore, r.dataset.path, r.dataset.dir === "1");
    const tr = ev.target.closest("tr[data-i]");
    if (tr && !ev.target.closest("a")) $("find-" + tr.dataset.i).classList.toggle("hidden");
  });
}
