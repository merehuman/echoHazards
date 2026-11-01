import { API_BASE } from '../config.js';

export class ApiError extends Error {
  constructor(status) {
    super(`API ${status}`);
    this.name = 'ApiError';
    this.status = status;
  }
}

async function getJson(path) {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) throw new ApiError(res.status);
  return res.json();
}

export function fetchIncidents({ lat, lng, radiusKm, source, medium, page = 1, pageSize }) {
  const params = new URLSearchParams({
    lat,
    lng,
    radius_km: radiusKm,
    page,
    page_size: pageSize,
  });
  if (source) params.set('source', source);
  if (medium) params.set('medium', medium);
  return getJson(`/incidents?${params}`);
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
