// Recovery kit export and notification test.
import { $, esc, ago, api, state, setMsg } from "./util.js";

export function renderRecovery() {
  const s = state.status;
  if (!s) return;
  const exported = s.recovery_kit_exported_at;
  $("kitExported").innerHTML = exported ? `<span class="muted">last exported with secrets ${esc(ago(exported))}</span>`
    : `<span class="pill partial">never exported</span>`;
  for (const [id, fmt] of [["kitSecrets", ""], ["kitMd", "&format=md"]]) {
    const a = $(id);
    a.href = `/api/recovery-kit?secrets=1${fmt}`;
    a.classList.toggle("disabled", !s.auth_enabled);
    a.title = s.auth_enabled ? "" : "Set AUTH_TOKEN first: without it anyone on your network could download the repository password";
  }
  $("notifChannels").innerHTML = s.notifications.length
    ? "Configured: " + s.notifications.map(c => `<span class="pill success">${esc(c)}</span>`).join(" ")
    : "No notification channels configured.";
  $("notifTest").disabled = !s.notifications.length;
}

export function initRecovery() {
  $("notifTest").onclick = async () => {
    setMsg($("notifMsg"), "sending…");
    try {
      const r = await api("/api/notifications/test", { method: "POST", json: {}, allowStatus: [502] });
      const failed = Object.entries(r.results).filter(([, ok]) => !ok).map(([k]) => k);
      failed.length ? setMsg($("notifMsg"), `Failed: ${failed.join(", ")}\n${r.log.join("\n")}`, "err")
                    : setMsg($("notifMsg"), `✓ Sent via ${Object.keys(r.results).join(", ")}`, "ok");
    } catch (e) { setMsg($("notifMsg"), e.message, "err"); }
  };
  $("kitSecrets").addEventListener("click", () => {
    if (state.status) state.status.recovery_kit_exported_at = new Date().toISOString();
    setTimeout(renderRecovery, 500);
  });
}
