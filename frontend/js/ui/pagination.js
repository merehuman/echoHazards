const nav = document.getElementById('pagination');
const prevBtn = nav.querySelector('[data-action="prev"]');
const nextBtn = nav.querySelector('[data-action="next"]');
const positionEl = nav.querySelector('[data-role="position"]');

/** onPageChange(page) fires with the requested page number. */
export function initPagination({ getPage, onPageChange }) {
  prevBtn.addEventListener('click', () => onPageChange(getPage() - 1));
  nextBtn.addEventListener('click', () => onPageChange(getPage() + 1));
}

/** Hidden until a search has results; buttons are disabled at the ends and while loading. */
export function renderPagination({ page, totalPages, total, status }) {
  nav.hidden = total === 0 || (status !== 'ready' && status !== 'loading');
  if (nav.hidden) return;

  const loading = status === 'loading';
  positionEl.textContent = `Page ${page.toLocaleString()} of ${totalPages.toLocaleString()}`;
  prevBtn.disabled = loading || page <= 1;
  nextBtn.disabled = loading || page >= totalPages;
}
