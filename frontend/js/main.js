import { fetchIncidents } from './api/client.js';
import { PAGE_SIZE, SEARCH_ZOOM } from './config.js';
import { drawRadiusCircle, focusLocation, initMap, setView } from './map/map.js';
import { openMarkerPopup, renderMarkers } from './map/markers.js';
import { geocode } from './services/geocode.js';
import { changed, getState, setState, subscribe } from './state.js';
import { initFilters, renderFilters } from './ui/filters.js';
import { initHelpPopover } from './ui/help-popover.js';
import { initRecordModal, openRecordModal } from './ui/record-modal.js';
import { initResultsList, renderActiveCard, renderResultsList, scrollToCard } from './ui/results-list.js';
import { initSearchForm, renderRadius, showPickedLocation } from './ui/search-form.js';
import { loadStatus } from './ui/status-bar.js';

async function runSearch() {
  const { lat, lng, radiusKm, source, medium } = getState();
  if (lat === null) return;
  setState({ status: 'loading' });
  try {
    const data = await fetchIncidents({ lat, lng, radiusKm, source, medium, pageSize: PAGE_SIZE });
    setState({
      status: 'ready',
      error: null,
      total: data.total,
      results: data.results,
      activeIncidentId: null,
    });
  } catch {
    setState({ status: 'error', error: 'Failed to fetch incidents. Is the API running?' });
  }
}

function updateSearch(patch) {
  setState(patch);
  runSearch();
}

async function searchAddress(query) {
  setState({ status: 'loading' });
  let location;
  try {
    location = await geocode(query);
  } catch {
    setState({ status: 'error', error: 'Geocoding failed. Check your connection.' });
    return;
  }
  if (!location) {
    setState({ status: 'error', error: 'Location not found.' });
    return;
  }
  setView(location.lat, location.lng, SEARCH_ZOOM);
  updateSearch(location);
}

function selectFromList(id) {
  const inc = getState().results.find((r) => r.id === id);
  if (!inc) return;
  setState({ activeIncidentId: id });
  focusLocation(inc.lat, inc.lng);
  openMarkerPopup(id);
}

function selectFromMap(id) {
  setState({ activeIncidentId: id });
  scrollToCard(id);
}

subscribe((state, prev) => {
  if (changed(state, prev, ['status', 'error', 'results', 'total'])) renderResultsList(state);
  if (changed(state, prev, ['results'])) {
    renderMarkers(state.results, { onSelect: selectFromMap, onShowRecord: openRecordModal });
    drawRadiusCircle(state.lat, state.lng, state.radiusKm);
  }
  if (changed(state, prev, ['activeIncidentId', 'results'])) {
    renderActiveCard(state.activeIncidentId, prev.activeIncidentId);
  }
  if (changed(state, prev, ['source', 'medium'])) renderFilters(state);
  if (changed(state, prev, ['radiusKm'])) renderRadius(state.radiusKm);
});

initMap('map', {
  onLocationPick: ({ lat, lng }) => {
    showPickedLocation(lat, lng);
    updateSearch({ lat, lng });
  },
});
initSearchForm({
  onSubmit: searchAddress,
  onRadiusChange: (radiusKm) => updateSearch({ radiusKm }),
});
initFilters({ onChange: (type, value) => updateSearch({ [type]: value }) });
initResultsList({ onSelect: selectFromList });
initRecordModal();
initHelpPopover();
loadStatus();
