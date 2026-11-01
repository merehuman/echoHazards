import { mediumTone, sourceTone } from '../sources.js';
import { formatLocation, formatMaterial, formatQuantity } from '../utils/format.js';
import { escapeHtml } from '../utils/html.js';

const countEl = document.getElementById('results-count');
const listEl = document.getElementById('results-list');
const errorEl = document.getElementById('error-msg');

const cardId = (id) => `card-${id}`;

/** onSelect(id) fires when a result card is clicked. */
export function initResultsList({ onSelect }) {
  listEl.addEventListener('click', (e) => {
    const card = e.target.closest('.incident-card');
    if (card) onSelect(card.dataset.incidentId);
  });
}

function cardHtml(inc) {
  const qty = formatQuantity(inc);
  const medTone = mediumTone(inc.medium);

  return `<div class="incident-card ${sourceTone(inc.source)}" id="${escapeHtml(cardId(inc.id))}" data-incident-id="${escapeHtml(inc.id)}">
      <div class="card-top">
        <div class="card-material">${escapeHtml(formatMaterial(inc, 80))}</div>
        <div class="card-type-badge">${escapeHtml(inc.incident_type.toUpperCase())}</div>
      </div>
      ${inc.responsible_party ? `<div class="card-party">${escapeHtml(inc.responsible_party)}</div>` : ''}
      <div class="card-meta">
        <span>${escapeHtml(inc.incident_date || '—')}</span>
        <span>${escapeHtml(formatLocation(inc))}</span>
        ${qty ? `<span>${escapeHtml(qty)}</span>` : ''}
        ${medTone ? `<span class="medium-tag ${medTone}">${escapeHtml(inc.medium.toUpperCase())}</span>` : ''}
      </div>
    </div>`;
}

function renderLoading() {
  countEl.innerHTML = 'Searching…';
  listEl.innerHTML = `<div class="state-msg">
    <div class="spinner"></div>
    <p class="state-detail">Querying database…</p></div>`;
}

function renderError(message) {
  countEl.textContent = 'Error';
  listEl.innerHTML = `<div class="state-msg">
    <div class="icon icon-error">⚠</div><p>${escapeHtml(message)}</p></div>`;
}

function summaryHtml({ total, results, page, pageSize }) {
  if (!results.length) return `<strong>${total.toLocaleString()}</strong> incidents found`;
  const start = (page - 1) * pageSize + 1;
  const end = start + results.length - 1;
  return `Showing <strong>${start.toLocaleString()}–${end.toLocaleString()}</strong> of <strong>${total.toLocaleString()}</strong> incidents`;
}

function renderReady(state) {
  countEl.innerHTML = summaryHtml(state);
  if (!state.results.length) {
    listEl.innerHTML = `<div class="state-msg"><div class="icon">◌</div>
      <p>No incidents found<br>within ${escapeHtml(state.radiusKm)}km.<br>Try expanding the radius.</p></div>`;
    return;
  }
  listEl.innerHTML = state.results.map(cardHtml).join('');
  listEl.scrollTop = 0;
}

/** Renders the count, list body, and error banner for the current search status. */
export function renderResultsList(state) {
  errorEl.textContent = state.error ? '⚠ ' + state.error : '';
  errorEl.classList.toggle('visible', Boolean(state.error));

  if (state.status === 'loading') renderLoading();
  else if (state.status === 'error') renderError(state.error);
  else if (state.status === 'ready') renderReady(state);
}

export function renderActiveCard(activeId, prevActiveId) {
  if (prevActiveId) document.getElementById(cardId(prevActiveId))?.classList.remove('active');
  if (activeId) document.getElementById(cardId(activeId))?.classList.add('active');
}

export function scrollToCard(id) {
  document.getElementById(cardId(id))?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}
