const GROUPS = {
  source: document.getElementById('source-chips'),
  medium: document.getElementById('medium-chips'),
};

/** onChange(type, value) fires when a chip is clicked; type is 'source' or 'medium'. */
export function initFilters({ onChange }) {
  Object.entries(GROUPS).forEach(([type, group]) => {
    group.addEventListener('click', (e) => {
      const chip = e.target.closest('.chip');
      if (!chip) return;
      onChange(type, chip.dataset[type]);
    });
  });
}

export function renderFilters({ source, medium }) {
  const selected = { source, medium };
  Object.entries(GROUPS).forEach(([type, group]) => {
    group.querySelectorAll('.chip').forEach((chip) => {
      chip.classList.toggle('active', chip.dataset[type] === selected[type]);
    });
  });
}
