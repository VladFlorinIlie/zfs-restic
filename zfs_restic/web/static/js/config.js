// config.yml editor with validation, retention preview and version history.
import { $, esc, fmtTime, api, state, setMsg } from "./util.js";

let loaded = null;   // {content, sha256, writable, path}

const dirty = () => loaded && $("cfgEditor").value !== loaded.content;

function updateState() {
  const canSave = loaded?.writable && state.status?.auth_enabled;
  $("cfgSave").disabled = !canSave || !dirty();
  $("cfgSave").title = !state.status?.auth_enabled ? "Set AUTH_TOKEN to edit the config from the UI"
    : !loaded?.writable ? "config.yml is mounted read-only" : "";
  $("cfgState").innerHTML = !loaded ? "" : !loaded.writable ? `<span class="pill partial">read-only</span>`
    : dirty() ? `<span class="pill running">unsaved changes</span>` : `<span class="pill success">saved</span>`;
}

function renderValidation(v) {
  if (!v) { $("cfgValidation").innerHTML = ""; return; }
  const parts = [];
  if (v.ok) parts.push(`<div class="msg-ok">✓ Valid: ${v.summary.datasets.length} dataset(s); forget args: <span class="mono">${esc(v.summary.retention_args.join(" ") || "(none)")}</span></div>`);
  if (v.errors.length) parts.push(`<ul class="plain msg-err">${v.errors.map(e => `<li>${esc(e)}</li>`).join("")}</ul>`);
  if (v.warnings.length) parts.push(`<ul class="plain msg-warn">${v.warnings.map(e => `<li>${esc(e)}</li>`).join("")}</ul>`);
  $("cfgValidation").innerHTML = parts.join("");
}

export async function loadConfig() {
  try {
    loaded = await api("/api/config");
    $("cfgEditor").value = loaded.content;
    $("cfgPath").textContent = loaded.path + (loaded.exists ? "" : " (does not exist yet)");
    $("cfgEditor").readOnly = !loaded.writable;
    renderValidation(loaded.validation);
    updateState();
    loadVersions();
  } catch (e) { setMsg($("cfgMsg"), e.message, "err"); }
}

async function loadVersions() {
  const vs = await api("/api/config/versions");
  $("cfgVersions").innerHTML = vs.map(v => `<tr><td>${esc(fmtTime(v.saved_at))}</td><td class="muted">${esc(v.note || "")}</td>
    <td class="num">${v.size} B</td><td><a class="link" data-version="${v.id}">load into editor</a></td></tr>`).join("")
    || `<tr><td colspan="4" class="muted">No previous versions yet. Each save from the UI keeps the version it replaced.</td></tr>`;
}

async function validate() {
  try { const v = await api("/api/config/validate", { method: "POST", json: { content: $("cfgEditor").value } }); renderValidation(v); return v; }
  catch (e) { setMsg($("cfgMsg"), e.message, "err"); }
}

async function preview() {
  $("retentionSection").classList.remove("hidden");
  $("retentionBody").innerHTML = `<span class="muted">Running restic forget --dry-run…</span>`;
  try {
    const p = await api("/api/retention/preview", { method: "POST", json: { content: $("cfgEditor").value } });
    $("retArgs").textContent = p.args?.length ? p.args.join(" ") : "";
    if (p.note) { $("retentionBody").innerHTML = `<p class="muted">${esc(p.note)}</p>`; return; }
    $("retentionBody").innerHTML = p.groups.map(g => {
      const all = g.keep.length + g.remove.length;
      const heavy = g.remove.length && g.remove.length >= all / 2;
      return `<div class="remote"><b class="mono">${esc(g.dataset || g.paths.join(", "))}</b>
        <span class="${g.remove.length ? (heavy ? "msg-err" : "msg-warn") : "msg-ok"}"> keeps ${g.keep.length}, removes ${g.remove.length} of ${all}</span>
        ${g.remove.length ? `<details style="margin-top:6px"><summary class="small">Would remove</summary><div class="mono small">${g.remove.map(s => `${esc(s.short_id)} · ${esc(fmtTime(s.time))}`).join("<br>")}</div></details>` : ""}
        <details style="margin-top:4px"><summary class="small">Would keep</summary><div class="mono small">${g.keep.map(s => `${esc(s.short_id)} · ${esc(fmtTime(s.time))} <span class="muted">${esc(s.reasons.join(", "))}</span>`).join("<br>")}</div></details></div>`;
    }).join("") || `<p class="muted">No snapshots yet.</p>`;
  } catch (e) { $("retentionBody").innerHTML = `<div class="err">${esc(e.message)}</div>`; }
}

async function save() {
  const v = await validate();
  if (!v?.ok) { setMsg($("cfgMsg"), "Fix the errors before saving.", "err"); return; }
  if (v.warnings.length && !confirm("Save despite these warnings?\n\n" + v.warnings.join("\n"))) return;
  try {
    loaded = await api("/api/config", { method: "PUT", json: { content: $("cfgEditor").value, sha256: loaded.sha256 } });
    $("cfgEditor").value = loaded.content;
    renderValidation(loaded.validation);
    setMsg($("cfgMsg"), "Saved. The next backup uses the new config.", "ok");
    updateState(); loadVersions();
  } catch (e) { setMsg($("cfgMsg"), e.message, "err"); }
}

export function initConfig() {
  const ed = $("cfgEditor");
  ed.addEventListener("input", updateState);
  ed.addEventListener("keydown", (ev) => {   // Tab inserts two spaces (YAML has no tabs)
    if (ev.key === "Tab" && !ev.shiftKey && !ed.readOnly) { ev.preventDefault(); ed.setRangeText("  ", ed.selectionStart, ed.selectionEnd, "end"); updateState(); }
    if ((ev.ctrlKey || ev.metaKey) && ev.key === "s") { ev.preventDefault(); if (!$("cfgSave").disabled) save(); }
  });
  $("cfgValidate").onclick = validate;
  $("cfgPreview").onclick = preview;
  $("cfgSave").onclick = save;
  $("cfgReload").onclick = () => (!dirty() || confirm("Discard unsaved changes?")) && loadConfig();
  $("cfgVersions").addEventListener("click", async (ev) => {
    const a = ev.target.closest("a[data-version]"); if (!a) return;
    if (dirty() && !confirm("Replace the editor contents with this version?")) return;
    const v = await api(`/api/config/versions/${a.dataset.version}`);
    ed.value = v.content; updateState(); validate();
    setMsg($("cfgMsg"), `Loaded version from ${fmtTime(v.saved_at)}. Save to restore it.`, "warn");
  });
  window.addEventListener("beforeunload", (ev) => { if (dirty()) { ev.preventDefault(); ev.returnValue = ""; } });
}

export const configLoaded = () => loaded !== null;
export const refreshConfigState = updateState;
