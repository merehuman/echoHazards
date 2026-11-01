import { governmentPortalLabel, governmentPortalUrl, mediumTone, sourceLabel, sourceTone } from '../sources.js';
import { formatLocation, formatMaterial, formatQuantity } from '../utils/format.js';
import { escapeHtml } from '../utils/html.js';

function sourceLinksHtml(inc) {
  const parts = [];
  if (inc.source === 'NRC') {
    parts.push(`<button type="button" class="popup-link" data-action="raw">View original NRC record</button>`);
  } else {
    const govUrl = governmentPortalUrl(inc.source, inc.source_id);
    if (govUrl) {
      parts.push(`<button type="button" class="popup-link" data-action="external" data-report-url="${escapeHtml(govUrl)}">${escapeHtml(governmentPortalLabel(inc.source))}</button>`);
    }
  }
  if (!parts.length) return '';
  return `<div class="popup-links">${parts.join('')}</div>`;
}

function popupHtml(inc) {
  const qty = formatQuantity(inc);
  const medTone = mediumTone(inc.medium);

  return `
    <div class="popup-material">${escapeHtml(formatMaterial(inc, 120))}</div>
    <div class="popup-type ${sourceTone(inc.source)}">${escapeHtml(inc.incident_type.toUpperCase())} · ${escapeHtml(sourceLabel(inc.source))}</div>
    <div class="popup-row">DATE <span>${escapeHtml(inc.incident_date || '—')}</span></div>
    <div class="popup-row">LOCATION <span>${escapeHtml(formatLocation(inc))}</span></div>
    ${qty ? `<div class="popup-row">QUANTITY <span>${escapeHtml(qty)}</span></div>` : ''}
    ${inc.responsible_party ? `<div class="popup-row">RESPONSIBLE <span>${escapeHtml(inc.responsible_party)}</span></div>` : ''}
    ${medTone ? `<div class="popup-row">MEDIUM <span class="popup-medium ${medTone}">${escapeHtml(inc.medium.toUpperCase())}</span></div>` : ''}
    ${sourceLinksHtml(inc)}`;
}

/**
 * Builds the popup body as a DOM element so its click handler is attached once,
 * no matter how many times Leaflet re-opens the popup.
 */
export function createPopupContent(inc, { onShowRecord }) {
  const el = document.createElement('div');
  el.innerHTML = popupHtml(inc);
  el.addEventListener('click', (ev) => {
    const btn = ev.target.closest('[data-action]');
    if (!btn) return;
    ev.preventDefault();
    ev.stopPropagation();
    if (btn.dataset.action === 'external') {
      window.open(btn.dataset.reportUrl, '_blank', 'noopener,noreferrer');
    } else if (btn.dataset.action === 'raw') {
      onShowRecord(inc.id);
    }
  });
  return el;
}
