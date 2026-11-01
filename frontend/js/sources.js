const SOURCE_LABELS = {
  NRC:  'NRC Spills',
  ECHO: 'ECHO Violations',
  TRI:  'TRI Releases',
};

// Tone classes are defined in css/tokens.css.
const SOURCE_TONES = { NRC: 'tone-nrc', ECHO: 'tone-echo', TRI: 'tone-tri' };
const MEDIUM_TONES = { water: 'tone-water', air: 'tone-air', land: 'tone-land' };

export function sourceLabel(source) {
  return SOURCE_LABELS[source] || source || '—';
}

export function sourceTone(source) {
  return SOURCE_TONES[source] || '';
}

export function mediumTone(medium) {
  return MEDIUM_TONES[medium] || '';
}

export function governmentPortalUrl(source, sourceId) {
  if (!sourceId) return null;
  switch (source) {
    case 'ECHO':
      return `https://echo.epa.gov/detailed-facility-report?fid=${encodeURIComponent(String(sourceId).trim())}`;
    case 'TRI': {
      const trifd = String(sourceId).split('_')[0].trim();
      if (!trifd) return null;
      return `https://enviro.epa.gov/enviro/tris_control_v2.tris_print?pPrev=1&tris_id=${encodeURIComponent(trifd)}`;
    }
    default:
      return null;
  }
}

export function governmentPortalLabel(source) {
  switch (source) {
    case 'ECHO': return 'View EPA ECHO facility report';
    case 'TRI': return 'View EPA TRI facility report';
    default: return 'View government report';
  }
}

/** NRC raw-record keys shown in the record modal, in display order. */
export const NRC_RAW_FIELDS = [
  ['SEQNOS', 'NRC incident number'],
  ['MAT_NAME_OF_MATERIAL', 'Material'],
  ['MAT_AMOUNT_OF_MATERIAL', 'Amount'],
  ['MAT_UNIT_OF_MEASURE', 'Unit'],
  ['COMMON_INCIDENT_DATE_TIME', 'Incident date/time'],
  ['CALL_RESPONSIBLE_COMPANY', 'Responsible company'],
  ['COMMON_INCIDENT_LOCATION', 'Incident location'],
  ['COMMON_LOCATION_STREET1', 'Street'],
  ['COMMON_LOCATION_NEAREST_CITY', 'City'],
  ['COMMON_LOCATION_STATE', 'State'],
  ['COMMON_LOCATION_ZIP', 'ZIP'],
  ['COMMON_INCIDENT_CAUSE', 'Cause'],
  ['COMMON_INCIDENT_NATURE', 'Nature'],
];
