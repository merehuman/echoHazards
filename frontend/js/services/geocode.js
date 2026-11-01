import { NOMINATIM_URL } from '../config.js';

/** Resolves a US address/city/zip to { lat, lng }, or null when nothing matches. */
export async function geocode(query, { signal } = {}) {
  const params = new URLSearchParams({ q: query, format: 'json', limit: 1, countrycodes: 'us' });
  const res = await fetch(`${NOMINATIM_URL}?${params}`, { headers: { 'Accept-Language': 'en' }, signal });
  const results = await res.json();
  if (!results.length) return null;
  return { lat: parseFloat(results[0].lat), lng: parseFloat(results[0].lon) };
}
