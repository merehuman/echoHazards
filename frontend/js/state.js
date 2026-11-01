import { DEFAULT_RADIUS_KM } from './config.js';

/**
 * Single source of truth for the search UI.
 *
 * status:  'idle' (no search yet) | 'loading' | 'ready' | 'error'
 * error:   message for the error banner; cleared only by a successful search,
 *          so it stays visible while a retry is loading.
 */
const state = {
  lat: null,
  lng: null,
  radiusKm: DEFAULT_RADIUS_KM,
  source: '',
  medium: '',
  status: 'idle',
  error: null,
  total: 0,
  results: [],
  activeIncidentId: null,
};

const listeners = new Set();

export function getState() {
  return { ...state };
}

export function setState(patch) {
  const prev = { ...state };
  Object.assign(state, patch);
  const next = { ...state };
  listeners.forEach((listener) => listener(next, prev));
}

/** listener(state, prevState) runs after every setState. Returns an unsubscribe function. */
export function subscribe(listener) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function changed(state, prev, keys) {
  return keys.some((key) => state[key] !== prev[key]);
}
