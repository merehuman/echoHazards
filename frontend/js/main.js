import { fetchIncidents } from './api/client.js';
import { PAGE_SIZE, RADIUS_DEBOUNCE_MS, SEARCH_ZOOM } from './config.js';
import { drawRadiusCircle, focusLocation, initMap, setView } from './map/map.js';
import { openMarkerPopup, renderMarkers } from './map/markers.js';
import { geocode } from './services/geocode.js';
import { changed, getState, setState, subscribe } from './state.js';
import { initFilters, renderFilters } from './ui/filters.js';
import { initHelpPopover } from './ui/help-popover.js';
import { initPagination, renderPagination } from './ui/pagination.js';
import { initRecordModal, openRecordModal } from './ui/record-modal.js';
import { initResultsList, renderActiveCard, renderResultsList, scrollToCard } from './ui/results-list.js';
import { initSearchForm, renderRadius, showPickedLocation } from './ui/search-form.js';
import { loadStatus } from './ui/status-bar.js';

// Only the most recent geocode/search request may update state; starting a new one aborts the last.
let inflight = null;
let radiusTimer = null;

function beginRequest() {
  inflight?.abort();
  inflight = new AbortController();
  return inflight;
}

const isAbort = (err) => err?.name === 'AbortError';

/** Applies patch and fetches the resulting page. Without a location, only applies the patch. */
async function runSearch(patch = {}) {
  clearTimeout(radiusTimer);
  const next = { ...getState(), ...patch };
  if (next.lat === null) {
    setState(patch);
    return;
  }

  const request = beginRequest();
  setState({ ...patch, status: 'loading' });
  const { lat, lng, radiusKm, source, medium, page } = next;

  let data;
  try {
    data = await fetchIncidents(
      { lat, lng, radiusKm, source, medium, page, pageSize: PAGE_SIZE },
      { signal: request.signal },
    );
  } catch (err) {
    if (isAbort(err) || request !== inflight) return;
    setState({ status: 'error', error: 'Failed to fetch incidents. Is the API running?' });
    return;
  }
  if (request !== inflight) return;

  // Results shrank since this page was requested (e.g. new data); fall back to the last page.
  if (data.total_pages > 0 && page > data.total_pages) {
    runSearch({ page: data.total_pages });
    return;
  }

  setState({
    status: 'ready',
    error: null,
    total: data.total,
    totalPages: data.total_pages,
    pageSize: data.page_size,
    results: data.results,
    activeIncidentId: null,
  });
}

/** Any change to location, radius, or filters starts over at page 1. */
function newSearch(patch) {
  runSearch({ ...patch, page: 1, total: 0, totalPages: 0 });
}

function goToPage(page) {
  const { totalPages, status } = getState();
  if (status === 'loading' || page < 1 || page > totalPages) return;
  runSearch({ page });
}

function changeRadius(radiusKm) {
  setState({ radiusKm });
  clearTimeout(radiusTimer);
  radiusTimer = setTimeout(() => newSearch({}), RADIUS_DEBOUNCE_MS);
}

async function searchAddress(query) {
  clearTimeout(radiusTimer);
  const request = beginRequest();
  setState({ status: 'loading' });

  let location;
  try {
    location = await geocode(query, { signal: request.signal });
  } catch (err) {
    if (isAbort(err) || request !== inflight) return;
    setState({ status: 'error', error: 'Geocoding failed. Check your connection.' });
    return;
  }
  if (request !== inflight) return;
  if (!location) {
    setState({ status: 'error', error: 'Location not found.' });
    return;
  }
  setView(location.lat, location.lng, SEARCH_ZOOM);
  newSearch(location);
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
  if (changed(state, prev, ['page', 'totalPages', 'total', 'status'])) renderPagination(state);
  if (changed(state, prev, ['source', 'medium'])) renderFilters(state);
  if (changed(state, prev, ['radiusKm'])) renderRadius(state.radiusKm);
});

initMap('map', {
  onLocationPick: ({ lat, lng }) => {
    showPickedLocation(lat, lng);
    newSearch({ lat, lng });
  },
});
initSearchForm({ onSubmit: searchAddress, onRadiusChange: changeRadius });
initFilters({ onChange: (type, value) => newSearch({ [type]: value }) });
initResultsList({ onSelect: selectFromList });
initPagination({ getPage: () => getState().page, onPageChange: goToPage });
initRecordModal();
initHelpPopover();
loadStatus();
