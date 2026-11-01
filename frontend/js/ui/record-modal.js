import { fetchIncident } from '../api/client.js';
import { NRC_RAW_FIELDS, sourceLabel } from '../sources.js';
import { escapeHtml } from '../utils/html.js';

const modal = document.getElementById('record-modal');
const titleEl = document.getElementById('record-modal-title');
const bodyEl = document.getElementById('record-modal-body');

const fieldHtml = (value, label) => `
  <div class="record-field">
    ${label ? `<span class="record-field-label">${escapeHtml(label)}</span>` : ''}
    <span class="record-field-value">${escapeHtml(value)}</span>
  </div>`;

function nrcRecordHtml(data) {
  const raw = data.raw || {};
  const fields = NRC_RAW_FIELDS
    .filter(([key]) => raw[key])
    .map(([key, label]) => fieldHtml(raw[key], label))
    .join('');

  return `
    ${fields || fieldHtml('No NRC fields available.')}
    <div class="record-note">
      NRC does not publish individual spill pages online. This record is the original data saved from the
      <a href="https://nrc.uscg.mil/FOIAdata.aspx" target="_blank" rel="noopener noreferrer">NRC FOIA spreadsheet release</a>
      (incident SEQNOS ${escapeHtml(data.source_id || raw.SEQNOS || '—')}).
    </div>
    <details class="record-raw">
      <summary>Full original record (JSON)</summary>
      <pre>${escapeHtml(JSON.stringify(raw, null, 2))}</pre>
    </details>`;
}

function genericRecordHtml(data) {
  return `
    <div class="record-note">Original ${escapeHtml(data.source)} record from the federal source database.</div>
    <details class="record-raw" open>
      <summary>Full original record (JSON)</summary>
      <pre>${escapeHtml(JSON.stringify(data.raw || {}, null, 2))}</pre>
    </details>`;
}

export function closeRecordModal() {
  modal.classList.remove('open');
  modal.setAttribute('aria-hidden', 'true');
  bodyEl.innerHTML = '';
}

export async function openRecordModal(id) {
  titleEl.textContent = 'Loading original record…';
  bodyEl.innerHTML = fieldHtml('Loading…');
  modal.classList.add('open');
  modal.setAttribute('aria-hidden', 'false');

  try {
    const data = await fetchIncident(id);
    titleEl.textContent = `${sourceLabel(data.source)} · Original Record`;
    bodyEl.innerHTML = data.source === 'NRC' ? nrcRecordHtml(data) : genericRecordHtml(data);
  } catch {
    titleEl.textContent = 'Original Record';
    bodyEl.innerHTML = fieldHtml('Could not load the original record. Make sure the API is running.');
  }
}

export function initRecordModal() {
  modal.querySelectorAll('[data-action="close-modal"]').forEach((el) => {
    el.addEventListener('click', closeRecordModal);
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeRecordModal();
  });
}
