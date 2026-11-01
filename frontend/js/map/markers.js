import { sourceTone } from '../sources.js';
import { getMap } from './map.js';
import { createPopupContent } from './popup.js';

let markers = {};

function pinIcon(source) {
  return L.divIcon({
    html: `<div class="pin-wrap ${sourceTone(source)}"><div class="pin-svg"><svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 20 20"><circle cx="10" cy="10" r="7"/></svg></div></div>`,
    iconSize: [32, 32],
    iconAnchor: [16, 16],
    popupAnchor: [0, -16],
    className: '',
  });
}

function clearMarkers() {
  const map = getMap();
  Object.values(markers).forEach((m) => map.removeLayer(m));
  markers = {};
}

/** Replaces all markers. onSelect(id) fires on marker click; onShowRecord(id) from popup links. */
export function renderMarkers(items, { onSelect, onShowRecord }) {
  const map = getMap();
  clearMarkers();
  items.forEach((inc) => {
    const marker = L.marker([inc.lat, inc.lng], { icon: pinIcon(inc.source) });
    marker.bindPopup(createPopupContent(inc, { onShowRecord }), { maxWidth: 280 });
    marker.on('click', () => onSelect(inc.id));
    marker.addTo(map);
    markers[inc.id] = marker;
  });
}

export function openMarkerPopup(id) {
  if (markers[id]) markers[id].openPopup();
}
