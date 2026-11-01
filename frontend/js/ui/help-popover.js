export function initHelpPopover() {
  const btn = document.getElementById('help-btn');
  const popover = document.getElementById('help-popover');

  const close = () => {
    popover.classList.remove('open');
    btn.setAttribute('aria-expanded', 'false');
  };

  btn.addEventListener('click', (e) => {
    e.stopPropagation();
    const open = popover.classList.toggle('open');
    btn.setAttribute('aria-expanded', open ? 'true' : 'false');
  });

  document.addEventListener('click', (e) => {
    if (!popover.classList.contains('open')) return;
    if (btn.contains(e.target) || popover.contains(e.target)) return;
    close();
  });

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') close();
  });
}
