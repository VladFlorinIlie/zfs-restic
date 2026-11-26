// Entry point: tab routing and module wiring.
import { $, onStatus, state } from "./util.js";
import { initDashboard, pollStatus } from "./dashboard.js";
import { initSnapshots, loadSnaps, refreshDatasetSelects } from "./snapshots.js";
import { initFind } from "./find.js";
import { initConfig, loadConfig, configLoaded, refreshConfigState } from "./config.js";
import { initRclone, loadRclone } from "./rclone.js";
import { initRecovery, renderRecovery } from "./recovery.js";

const loaders = {
  snapshots: () => loadSnaps(false),
  config: () => { if (!configLoaded()) loadConfig(); },
  rclone: loadRclone,
  recovery: renderRecovery,
};

function showTab(name) {
  if (!document.getElementById("tab-" + name)) name = "dashboard";
  document.querySelectorAll(".tab").forEach(t => t.classList.toggle("active", t.id === "tab-" + name));
  document.querySelectorAll("#tabs a").forEach(a => a.classList.toggle("active", a.dataset.tab === name));
  loaders[name]?.();
}

document.querySelectorAll("#tabs a").forEach(a => a.addEventListener("click", () => { location.hash = a.dataset.tab; }));
window.addEventListener("hashchange", () => showTab(location.hash.slice(1)));

onStatus((event) => {
  if (event === "datasets") refreshDatasetSelects();
  if (event === "job-finished") loadSnaps(true);
  if (event === "status") { renderRecovery(); refreshConfigState(); }
});

initDashboard(); initSnapshots(); initFind(); initConfig(); initRclone(); initRecovery();
pollStatus();
showTab(location.hash.slice(1) || "dashboard");
