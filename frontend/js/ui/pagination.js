const nav = document.getElementById('pagination');
const prevBtn = nav.querySelector('[data-action="prev"]');
const nextBtn = nav.querySelector('[data-action="next"]');
const positionEl = nav.querySelector('[data-role="position"]');

/** onPageChange(page) fires with the requested page number. */
export function initPagination({ getPage, onPageChange }) {
  prevBtn.addEventListener('click', () => onPageChange(getPage() - 1));
  nextBtn.addEventListener('click', () => onPageChange(getPage() + 1));
}

/**
 * Hidden until a search has results. Buttons are disabled at the ends; while loading they are only
 * aria-disabled, because disabling the focused button would drop keyboard focus to <body>.
 */
export function renderPagination({ page, totalPages, total, status }) {
  nav.hidden = total === 0 || (status !== 'ready' && status !== 'loading');
  if (nav.hidden) return;

  const loading = status === 'loading';
  positionEl.textContent = `Page ${page.toLocaleString()} of ${totalPages.toLocaleString()}`;

  const focused = document.activeElement;
  prevBtn.disabled = page <= 1;
  nextBtn.disabled = page >= totalPages;
  [prevBtn, nextBtn].forEach((btn) => btn.setAttribute('aria-disabled', String(loading || btn.disabled)));

  if (focused?.disabled) {
    const other = focused === prevBtn ? nextBtn : prevBtn;
    if (!other.disabled) other.focus();
  }
}
