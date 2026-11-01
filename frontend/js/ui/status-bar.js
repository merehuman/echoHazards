import { fetchHealth, fetchPipelineStatus } from '../api/client.js';

const STATUS_HTML = {
  ok:       '⬤ <span class="live">API CONNECTED</span>',
  degraded: '⬤ <span class="status-error">API DEGRADED</span>',
  offline:  '⬤ <span class="status-error">API OFFLINE</span>',
};

export async function loadStatus() {
  const statusEl = document.getElementById('db-status');
  try {
    const health = await fetchHealth();
    statusEl.innerHTML = health.status === 'ok' ? STATUS_HTML.ok : STATUS_HTML.degraded;
  } catch {
    statusEl.innerHTML = STATUS_HTML.offline;
    return;
  }

  // A pipeline-status failure leaves the total as "—" without marking a reachable API offline.
  try {
    const pipeline = await fetchPipelineStatus();
    document.getElementById('total-count').textContent =
      pipeline.total_incidents.toLocaleString() + ' incidents';
  } catch {
    // keep placeholder
  }
}
