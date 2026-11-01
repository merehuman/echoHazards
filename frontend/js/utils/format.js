export function truncate(text, maxLength) {
  return text.length > maxLength ? text.substring(0, maxLength) + '…' : text;
}

export function formatMaterial(inc, maxLength) {
  return inc.material ? truncate(inc.material, maxLength) : 'Unknown';
}

export function formatLocation(inc) {
  return [inc.city, inc.state].filter(Boolean).join(', ') || '—';
}

/** Returns e.g. "1,200 GAL", or null when there is no positive quantity. */
export function formatQuantity(inc) {
  if (!inc.quantity || inc.quantity <= 0) return null;
  return `${inc.quantity.toLocaleString()} ${inc.quantity_unit || ''}`.trim();
}
