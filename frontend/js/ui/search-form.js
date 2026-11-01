const input = () => document.getElementById('address-input');

/** onSubmit(query) fires on button click or Enter; onRadiusChange(km) fires as the slider moves. */
export function initSearchForm({ onSubmit, onRadiusChange }) {
  const submit = () => {
    const query = input().value.trim();
    if (query) onSubmit(query);
  };
  document.getElementById('search-btn').addEventListener('click', submit);
  input().addEventListener('keydown', (e) => {
    if (e.key === 'Enter') submit();
  });

  const slider = document.getElementById('radius-slider');
  slider.addEventListener('input', () => onRadiusChange(parseInt(slider.value, 10)));
}

export function renderRadius(radiusKm) {
  document.getElementById('radius-display').textContent = `${radiusKm} km`;
}

/** Shows the coordinates of a map-picked location in place of a typed address. */
export function showPickedLocation(lat, lng) {
  input().value = '';
  input().placeholder = `${lat.toFixed(4)}, ${lng.toFixed(4)}`;
}
