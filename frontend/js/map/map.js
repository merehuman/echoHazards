import {
  BASEMAP_STYLE_URL,
  FOCUS_MIN_ZOOM,
  MAP_ATTRIBUTION,
  MAP_DEFAULT_CENTER,
  MAP_DEFAULT_ZOOM,
} from '../config.js';

let map = null;
let radiusCircle = null;

/** Creates the Leaflet map. onLocationPick({ lat, lng }) fires when the user clicks the map. */
export function initMap(elementId, { onLocationPick }) {
  // Zoom animation is off so the MapLibre vector layer stays aligned with Leaflet markers.
  map = L.map(elementId, { center: MAP_DEFAULT_CENTER, zoom: MAP_DEFAULT_ZOOM, zoomAnimation: false });
  L.maplibreGL({ style: BASEMAP_STYLE_URL, attributionControl: false }).addTo(map);
  map.attributionControl.addAttribution(MAP_ATTRIBUTION);

  map.on('click', (e) => onLocationPick({ lat: e.latlng.lat, lng: e.latlng.lng }));
  return map;
}

export function getMap() {
  return map;
}

export function setView(lat, lng, zoom) {
  map.setView([lat, lng], zoom);
}

/** Centers on a point, zooming in to at least FOCUS_MIN_ZOOM but never zooming out. */
export function focusLocation(lat, lng) {
  map.setView([lat, lng], Math.max(map.getZoom(), FOCUS_MIN_ZOOM));
}

export function drawRadiusCircle(lat, lng, radiusKm) {
  if (radiusCircle) map.removeLayer(radiusCircle);
  if (lat === null) return;
  radiusCircle = L.circle([lat, lng], {
    radius: radiusKm * 1000,
    className: 'radius-circle',
    interactive: false,
  }).addTo(map);
}
