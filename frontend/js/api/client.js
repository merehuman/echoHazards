import { API_BASE } from '../config.js';

export class ApiError extends Error {
  constructor(status) {
    super(`API ${status}`);
    this.name = 'ApiError';
    this.status = status;
  }
}

async function getJson(path, { signal } = {}) {
  const res = await fetch(`${API_BASE}${path}`, { signal });
  if (!res.ok) throw new ApiError(res.status);
  return res.json();
}

/** Pass an AbortSignal to cancel; a cancelled request rejects with a DOMException named 'AbortError'. */
export function fetchIncidents({ lat, lng, radiusKm, source, medium, page = 1, pageSize }, { signal } = {}) {
  const params = new URLSearchParams({
    lat,
    lng,
    radius_km: radiusKm,
    page,
    page_size: pageSize,
  });
  if (source) params.set('source', source);
  if (medium) params.set('medium', medium);
  return getJson(`/incidents?${params}`, { signal });
}

export function fetchIncident(id) {
  return getJson(`/incidents/${encodeURIComponent(id)}`);
}

export function fetchHealth() {
  return getJson('/health');
}

export function fetchPipelineStatus() {
  return getJson('/pipeline/status');
}
