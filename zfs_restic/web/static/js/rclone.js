// rclone overview and Google Drive (re)authorisation.
import { $, esc, fmtTime, api, state, setMsg } from "./util.js";

let overview = null;

function authCmd() {
  const id = $("rcClientId").value.trim(), secret = $("rcClientSecret").value.trim();
  if (!id) return `rclone authorize "drive"`;
  return `rclone authorize "drive" "${id}" "${secret || "<your client secret>"}"`;
}

// Pre-fill the form from the remote being edited so re-authorising keeps its client and scope.
function prefill() {
  const r = overview?.remotes.find(x => x.name === $("rcName").value.trim());
  if (r?.drive) {
    $("rcScope").value = r.drive.scope === "drive.file" ? "drive.file" : "drive";
    $("rcClientId").value = r.options.client_id || "";
    $("rcRoot").placeholder = r.drive.root_folder_id ? `keep current (${r.drive.root_folder_id})` : "empty = none";
  }
  $("rcAuthCmd").textContent = authCmd();
}

export async function loadRclone() {
  try { overview = await api("/api/rclone"); }
  catch (e) { $("rcOverview").innerHTML = `<div class="err">${esc(e.message)}</div>`; return; }
  if (!overview.available) { $("rcOverview").innerHTML = `<p class="err">rclone is not installed in this container.</p>`; return; }
  $("rcVersion").textContent = overview.version;
  const repoRemote = overview.repository_remote;
  if (!$("rcName").value) { $("rcName").value = repoRemote || "gdrive"; prefill(); }
  const remotes = overview.remotes.map(r => {
    const d = r.drive;
    const opts = Object.entries(r.options).filter(([k]) => k !== "token").map(([k, v]) => `${esc(k)} = ${esc(v)}`).join("<br>");
    return `<div class="remote">
      <div class="row"><b class="mono">${esc(r.name)}</b><span class="muted">${esc(r.type)}</span>
        ${r.name === repoRemote ? `<span class="pill success">repository</span>` : ""}
        <span class="spacer" style="flex:1"></span>
        <button class="small" data-test="${esc(r.name)}">Test connection</button><span class="small" id="rctest-${esc(r.name)}"></span></div>
      ${d ? `<div class="small" style="margin-top:6px">client: ${d.service_account ? "service account" : d.own_client_id ? `<span class="msg-ok">own client_id</span>` : `<span class="msg-warn">rclone's shared client_id</span>`}
        · scope ${esc(d.scope)} · token: ${d.token.present ? (d.token.has_refresh_token ? `<span class="msg-ok">has refresh token</span>` : `<span class="msg-err">no refresh token</span>`) : "none"}
        ${d.root_folder_id ? ` · root folder ${esc(d.root_folder_id)}` : ""}</div>` : ""}
      ${opts ? `<div class="mono small muted" style="margin-top:6px">${opts}</div>` : ""}
      ${r.warnings.map(w => `<div class="warn small">⚠ ${esc(w)}</div>`).join("")}
    </div>`;
  }).join("");
  $("rcOverview").innerHTML = `
    <p class="small">Config file: <span class="mono">${esc(overview.config_file || "?")}</span>
      ${overview.config_writable ? "" : `<span class="pill partial">read-only</span>`}
      · repository uses ${repoRemote ? `remote <b class="mono">${esc(repoRemote)}</b>` : "no rclone remote"}</p>
    ${overview.error ? `<div class="err">${esc(overview.error)}</div>` : ""}
    ${remotes || `<p class="muted">No remotes configured.</p>`}`;
  $("rcVersions").innerHTML = overview.versions.map(v => `<tr><td>${esc(fmtTime(v.saved_at))}</td><td class="muted">${esc(v.note || "")}</td>
    <td><a class="link" data-rcrestore="${v.id}">restore this version</a></td></tr>`).join("")
    || `<tr><td colspan="3" class="muted">None yet. The config is saved here before every change made from this page.</td></tr>`;
  const canEdit = overview.config_writable && state.status?.auth_enabled;
  $("rcSave").disabled = !canEdit;
  $("rcSave").title = !state.status?.auth_enabled ? "Set AUTH_TOKEN to change rclone from the UI" : !overview.config_writable ? "rclone config is read-only" : "";
}

export function initRclone() {
  for (const id of ["rcClientId", "rcClientSecret"]) $(id).addEventListener("input", () => { $("rcAuthCmd").textContent = authCmd(); });
  $("rcName").addEventListener("change", prefill);
  $("rcOverview").addEventListener("click", async (ev) => {
    const b = ev.target.closest("button[data-test]"); if (!b) return;
    const out = $("rctest-" + b.dataset.test);
    setMsg(out, "testing…");
    try {
      const r = await api("/api/rclone/test", { method: "POST", json: { remote: b.dataset.test } });
      r.ok ? setMsg(out, `✓ connected (${r.seconds.toFixed(1)} s)`, "ok") : setMsg(out, "✗ " + r.error, "err");
    } catch (e) { setMsg(out, e.message, "err"); }
  });
  $("rcForm").onsubmit = async (ev) => {
    ev.preventDefault();
    const name = $("rcName").value.trim();
    const existing = overview?.remotes.find(r => r.name === name);
    if (existing && !confirm(`Update the existing remote "${name}"${name === overview.repository_remote ? ", which your backups use" : ""}? The current rclone config is kept as a previous version.`)) return;
    setMsg($("rcMsg"), "saving…");
    try {
      const r = await api("/api/rclone/drive", { method: "POST", json: {
        name, scope: $("rcScope").value, client_id: $("rcClientId").value.trim(), client_secret: $("rcClientSecret").value.trim(),
        root_folder_id: $("rcRoot").value.trim(), token: $("rcToken").value.trim() } });
      r.test.ok ? setMsg($("rcMsg"), "✓ Saved and connected to Google Drive.", "ok")
                : setMsg($("rcMsg"), "Saved, but the connection test failed:\n" + r.test.error, "err");
      $("rcToken").value = ""; $("rcClientSecret").value = "";
      loadRclone();
    } catch (e) { setMsg($("rcMsg"), e.message, "err"); }
  };
  $("rcVersions").addEventListener("click", async (ev) => {
    const a = ev.target.closest("a[data-rcrestore]"); if (!a) return;
    if (!confirm("Replace the current rclone config with this version? The current one is kept as a previous version.")) return;
    try { await api(`/api/rclone/versions/${a.dataset.rcrestore}/restore`, { method: "POST", json: {} }); loadRclone(); }
    catch (e) { alert(e.message); }
  });
}
