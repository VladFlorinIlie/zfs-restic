// Shared helpers and app state.
export const $ = (id) => document.getElementById(id);
export const esc = (s) => String(s ?? "").replace(/[&<>"']/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c]));
export const bytes = (n) => {
  if (n == null) return "–";
  const u = ["B", "KiB", "MiB", "GiB", "TiB"]; let i = 0;
  while (Math.abs(n) >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return (i ? n.toFixed(1) : n) + " " + u[i];
};
export const dur = (s) => {
  if (s == null || isNaN(s)) return "–";
  s = Math.round(s); const h = Math.floor(s / 3600), m = Math.floor(s % 3600 / 60), x = s % 60;
  return h ? `${h}h ${m}m` : m ? `${m}m ${x}s` : `${x}s`;
};
export const fmtTime = (t) => t ? new Date(t).toLocaleString(undefined, {dateStyle: "medium", timeStyle: "short"}) : "–";
export const ago = (t) => {
  if (!t) return "never";
  const s = (Date.now() - new Date(t)) / 1000;
  return s < 90 ? "just now" : s < 5400 ? `${Math.round(s / 60)} min ago` : s < 172800 ? `${(s / 3600).toFixed(1)} h ago` : `${Math.round(s / 86400)} days ago`;
};
export const pill = (o) => `<span class="pill ${esc(o)}">${esc(o)}</span>`;

export async function api(path, opts = {}) {
  if (opts.json !== undefined) {
    opts = {...opts, body: JSON.stringify(opts.json), headers: {"Content-Type": "application/json", ...(opts.headers || {})}};
    delete opts.json;
  }
  const r = await fetch(path, opts);
  let body = null; try { body = await r.json(); } catch {}
  if (!r.ok && !opts.allowStatus?.includes(r.status)) {
    const err = new Error((body && (body.message + (body.details ? "\n" + body.details : ""))) || `HTTP ${r.status}`);
    err.status = r.status; err.body = body; throw err;
  }
  return body;
}

// Small shared state, updated by the status poller.
export const state = { status: null, snapshots: [], datasets: [], listeners: [] };
export function onStatus(fn) { state.listeners.push(fn); }

export function setMsg(el, text, kind = "") { el.className = "small " + (kind ? "msg-" + kind : ""); el.textContent = text; }

export function datasetOf(snap) {
  const tags = snap.tags || [];
  return tags.find(t => state.datasets.includes(t)) || tags.find(t => !t.includes("@")) || "–";
}

export const downloadUrl = (snap, path, dir) =>
  `/api/snapshots/${snap}/download?path=${encodeURIComponent(path)}${dir ? "&dir=1" : ""}`;

export async function restoreToServer(snapshotId, path, isDir) {
  const st = state.status?.restore;
  if (!st?.available) { alert(st?.message || "Restores to the server are not available."); return; }
  if (!confirm(`Restore ${isDir ? "folder" : "file"}\n${path}\nfrom snapshot ${snapshotId.slice(0, 8)} into a new folder under ${st.message}?\n\nNothing existing is overwritten.`)) return;
  try {
    const r = await api("/api/restore?trigger=ui", {method: "POST", json: {snapshot_id: snapshotId, path, is_dir: isDir}});
    alert(`Restore started. Files will appear in:\n${r.target}\n\nProgress is shown at the top of the page.`);
  } catch (e) { alert(e.message); }
}
