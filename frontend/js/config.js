const isLocalHost = ['localhost', '127.0.0.1'].includes(window.location.hostname);

export const API_BASE = isLocalHost ? 'http://localhost:8000' : window.location.origin;
export const NOMINATIM_URL = 'https://nominatim.openstreetmap.org/search';

export const PAGE_SIZE = 200;
export const DEFAULT_RADIUS_KM = 25;

export const MAP_DEFAULT_CENTER = [39.5, -98.35];
export const MAP_DEFAULT_ZOOM = 4;
export const SEARCH_ZOOM = 10;
export const FOCUS_MIN_ZOOM = 12;

// CARTO raster tiles now watermark every tile without an API key.
// OpenFreeMap Positron is the same light basemap, served as vector tiles with no key.
export const BASEMAP_STYLE_URL = 'https://tiles.openfreemap.org/styles/positron';
export const MAP_ATTRIBUTION =
  '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://openfreemap.org">OpenFreeMap</a> | NRC / EPA ECHO / EPA TRI';
