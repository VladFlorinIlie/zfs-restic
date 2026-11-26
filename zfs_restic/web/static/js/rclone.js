// rclone overview and OAuth (re)authorisation of remotes, for any remote type.
import { $, esc, fmtTime, api, state, setMsg } from "./util.js";

let overview = null;

const remoteByName = (name) => overview?.remotes.find(r => r.name === name);
const currentType = () => $("rcType").value;

function authCmd() {
  const type = currentType() || "drive";
  const id = $("rcClientId").value.trim(), secret = $("rcClientSecret").value.trim();
  if (!id) return `rclone authorize "${type}"`;
  return `rclone authorize "${type}" "${id}" "${secret || "<your client secret>"}"`;
}

// Fill the form from the remote being edited (existing: its type and client; new: creatable types).
function prefill() {
  const r = remoteByName($("rcName").value.trim());
  const types = r ? [r.type] : (overview?.creatable_types || ["drive"]);
  $("rcType").innerHTML = types.map(t => `<option>${esc(t)}</option>`).join("");
  $("rcType").disabled = !!r;
  $("rcClientId").value = r?.options?.client_id || "";
  const isDrive = currentType() === "drive";
  document.querySelectorAll(".drive-only").forEach(el => el.classList.toggle("hidden", !isDrive));
  $("rcDriveNote").classList.toggle("hidden", !isDrive);
  $("rcScope").value = "";
  $("rcRoot").placeholder = r?.drive?.root_folder_id ? `keep current (${r.drive.root_folder_id})` : "empty = none";
  const blocked = r && !r.can_reauthorise;
  setMsg($("rcMsg"), blocked ? `"${r.name}" is a ${r.type} remote, which doesn't use OAuth tokens: edit it with rclone config.` : "", blocked ? "warn" : "");
  $("rcAuthCmd").textContent = authCmd();
  updateSaveState(blocked);
}

function updateSaveState(blocked = false) {
  const canEdit = overview?.config_writable && state.status?.auth_enabled && !blocked;
  $("rcSave").disabled = !canEdit;
  $("rcSave").title = !state.status?.auth_enabled ? "Set AUTH_TOKEN to change rclone from the UI"
    : !overview?.config_writable ? "rclone config is read-only" : "";
}

function renderRemote(r, repoRemote) {
  const d = r.drive;
  const opts = Object.entries(r.options).filter(([k]) => k !== "token").map(([k, v]) => `${esc(k)} = ${esc(v)}`).join("<br>");
  const tokenState = !r.oauth ? "" : ` · token: ${r.token.present
    ? (r.token.has_refresh_token ? `<span class="msg-ok">has refresh token</span>` : (r.token.expiry && !String(r.token.expiry).startsWith("0001") ? `<span class="msg-err">no refresh token</span>` : `<span class="msg-ok">present</span>`))
    : `<span class="msg-err">none</span>`}`;
  const client = !r.oauth ? "" : d?.service_account ? "client: service account"
    : `client: ${r.own_client_id ? `<span class="msg-ok">own client_id</span>` : (r.type === "drive" ? `<span class="msg-warn">rclone's shared client_id</span>` : "rclone default")}`;
  return `<div class="remote">
    <div class="row"><b class="mono">${esc(r.name)}</b><span class="muted">${esc(r.type)}</span>
      ${r.name === repoRemote ? `<span class="pill success">repository</span>` : ""}
      <span class="spacer" style="flex:1"></span>
      ${r.can_reauthorise ? `<button class="small" data-edit="${esc(r.name)}">Re-authorise</button>` : ""}
      <button class="small" data-test="${esc(r.name)}">Test connection</button><span class="small" id="rctest-${esc(r.name)}"></span></div>
    ${r.oauth ? `<div class="small" style="margin-top:6px">${client}${tokenState}${d ? ` · scope ${esc(d.scope)}` : ""}</div>` : ""}
    ${opts ? `<div class="mono small muted" style="margin-top:6px">${opts}</div>` : ""}
    ${r.warnings.map(w => `<div class="warn small">⚠ ${esc(w)}</div>`).join("")}
  </div>`;
}

export async function loadRclone() {
  try { overview = await api("/api/rclone"); }
  catch (e) { $("rcOverview").innerHTML = `<div class="err">${esc(e.message)}</div>`; return; }
  if (!overview.available) { $("rcOverview").innerHTML = `<p class="err">rclone is not installed in this container.</p>`; return; }
  $("rcVersion").textContent = overview.version;
  const be = overview.backend || {};
  const repoRemote = overview.repository_remote;
  const remotes = overview.remotes.map(r => renderRemote(r, repoRemote)).join("");
  $("rcOverview").innerHTML = `
    <p class="small">Config file: <span class="mono">${esc(overview.config_file || "?")}</span>
      ${overview.config_writable ? "" : `<span class="pill partial">read-only</span>`}
      · repository backend: <b>${esc(be.name || "unknown")}</b>${repoRemote ? ` (remote <b class="mono">${esc(repoRemote)}</b>)` : ""}</p>
    ${be.scheme && be.scheme !== "rclone" ? `<p class="muted small">The repository doesn't use rclone, so nothing on this tab affects your backups.</p>` : ""}
    ${overview.error ? `<div class="err">${esc(overview.error)}</div>` : ""}
    ${remotes || `<p class="muted">No remotes configured.</p>`}`;
  $("rcRemoteList").innerHTML = overview.remotes.filter(r => r.can_reauthorise).map(r => `<option value="${esc(r.name)}">${esc(r.type)}</option>`).join("");
  if (!$("rcName").value) {
    const repo = remoteByName(repoRemote);
    $("rcName").value = repo?.can_reauthorise ? repo.name : (overview.remotes.find(r => r.can_reauthorise)?.name || "");
  }
  prefill();
  $("rcVersions").innerHTML = overview.versions.map(v => `<tr><td>${esc(fmtTime(v.saved_at))}</td><td class="muted">${esc(v.note || "")}</td>
    <td><a class="link" data-rcrestore="${v.id}">restore this version</a></td></tr>`).join("")
    || `<tr><td colspan="3" class="muted">None yet. The config is saved here before every change made from this page.</td></tr>`;
}

export function initRclone() {
  for (const id of ["rcClientId", "rcClientSecret"]) $(id).addEventListener("input", () => { $("rcAuthCmd").textContent = authCmd(); });
  $("rcType").addEventListener("change", () => { $("rcAuthCmd").textContent = authCmd(); });
  $("rcName").addEventListener("change", prefill);
  $("rcOverview").addEventListener("click", async (ev) => {
    const edit = ev.target.closest("button[data-edit]");
    if (edit) { $("rcName").value = edit.dataset.edit; prefill(); $("rcForm").scrollIntoView({ behavior: "smooth" }); return; }
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
    const existing = remoteByName(name);
    const what = existing ? `Re-authorise the ${existing.type} remote "${name}"${name === overview.repository_remote ? ", which your backups use" : ""}?`
                          : `Create a new ${currentType()} remote "${name}"?`;
    if (!confirm(what + " The current rclone config is kept as a previous version.")) return;
    setMsg($("rcMsg"), "saving…");
    try {
      const r = await api("/api/rclone/authorize", { method: "POST", json: {
        name, type: existing ? undefined : currentType(), client_id: $("rcClientId").value.trim(),
        client_secret: $("rcClientSecret").value.trim(), scope: $("rcScope").value,
        root_folder_id: $("rcRoot").value.trim(), token: $("rcToken").value.trim() } });
      r.test.ok ? setMsg($("rcMsg"), `✓ Saved and connected (${r.type}).`, "ok")
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
