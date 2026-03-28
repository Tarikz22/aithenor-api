const { createClient } = require('@supabase/supabase-js');
const XLSX = require('xlsx');
const axios = require('axios');

// --------------------
// INIT
// --------------------
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  throw new Error('Missing Supabase environment variables');
}

const supabase = createClient(supabaseUrl, supabaseKey);

// --------------------
// LIBRARIES
// --------------------
const ACTION_LIBRARY = [
  {
    action_id: 'RET_PRICING_01',
    driver: 'pricing',
    segment: 'retail',
    title: 'Adjust BAR premium vs comp set',
    description:
      'Reduce transient BAR premium on shoulder days where ARI > 105 and MPI < 95 to restore share without damaging overall rate positioning.',
    priority: 'high'
  },
  {
    action_id: 'RET_PRICING_02',
    driver: 'pricing',
    segment: 'retail',
    title: 'Deploy fenced tactical offers',
    description:
      'Introduce targeted, fenced discounts (mobile, geo, LOS) on low-demand dates to stimulate demand without public rate dilution.',
    priority: 'high'
  },
  {
    action_id: 'RET_VIS_01',
    driver: 'visibility',
    segment: 'retail',
    title: 'Boost OTA visibility',
    description:
      'Increase OTA ranking and exposure during low MPI periods through visibility boosters and preferred placements.',
    priority: 'medium'
  },
  {
    action_id: 'RET_VIS_02',
    driver: 'visibility',
    segment: 'retail',
    title: 'Activate digital demand',
    description:
      'Increase brand.com and paid channel campaigns during declining MPI periods to rebuild demand flow.',
    priority: 'medium'
  },
  {
    action_id: 'RET_CONV_01',
    driver: 'conversion',
    segment: 'retail',
    title: 'Optimize website conversion',
    description:
      'Improve booking conversion through offer clarity, UX improvements, and simplified booking paths.',
    priority: 'medium'
  },
  {
    action_id: 'RET_CONV_02',
    driver: 'conversion',
    segment: 'retail',
    title: 'Fix OTA content & parity',
    description:
      'Enhance OTA conversion by improving content quality and ensuring strict rate parity across channels.',
    priority: 'medium'
  },
  {
    action_id: 'MIX_01',
    driver: 'mix_strategy',
    segment: 'retail',
    title: 'Shift toward higher-rated demand',
    description:
      'Reduce reliance on low-rated demand and reallocate inventory toward higher ADR segments.',
    priority: 'high'
  }
];

const OWNER_DEPARTMENT_BY_DRIVER = {
  pricing: 'Revenue',
  visibility: 'Revenue & Marketing',
  conversion: 'Revenue & Marketing',
  mix_strategy: 'Commercial',
  none: 'Commercial'
};

// --------------------
// HELPERS
// --------------------
function normalizeKey(value) {
  return (value || '')
    .toString()
    .trim()
    .toLowerCase()
    .replace(/[_\-]+/g, ' ')
    .replace(/\s+/g, ' ');
}

function getMetricFromRow(row, possibleKeys) {
  for (const key of possibleKeys) {
    if (row[key] !== undefined && row[key] !== null && row[key] !== '') {
      const value = parseFloat(row[key]);
      if (!Number.isNaN(value)) return value;
    }
  }
  return null;
}

function getDeterministicIndex(key, length) {
  let hash = 0;
  const str = key.toString();

  for (let i = 0; i < str.length; i += 1) {
    hash = (hash << 5) - hash + str.charCodeAt(i);
    hash |= 0;
  }

  return Math.abs(hash) % length;
}

function formatDateToYMD(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return null;

  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');

  return `${year}-${month}-${day}`;
}

function parseExcelDate(value) {
  if (!value && value !== 0) return null;

  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value;
  }

  if (typeof value === 'number') {
    const parsed = XLSX.SSF.parse_date_code(value);
    if (parsed) {
      return new Date(Date.UTC(parsed.y, parsed.m - 1, parsed.d));
    }
  }

  const parsedDate = new Date(value);
  if (!Number.isNaN(parsedDate.getTime())) {
    return parsedDate;
  }

  return null;
}

function getIsoWeekInfo(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    return null;
  }

  const workingDate = new Date(
    Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate())
  );

  const dayNumber = workingDate.getUTCDay() || 7;
  workingDate.setUTCDate(workingDate.getUTCDate() + 4 - dayNumber);

  const yearStart = new Date(Date.UTC(workingDate.getUTCFullYear(), 0, 1));
  const weekNumber = Math.ceil((((workingDate - yearStart) / 86400000) + 1) / 7);

  return {
    isoYear: workingDate.getUTCFullYear(),
    isoWeek: weekNumber
  };
}

function findSheetByAliases(workbook, aliases) {
  const names = workbook.SheetNames || [];
  const normalizedMap = new Map(names.map((name) => [normalizeKey(name), name]));

  for (const alias of aliases) {
    const exact = normalizedMap.get(normalizeKey(alias));
    if (exact) return exact;
  }

  for (const name of names) {
    const normalizedName = normalizeKey(name);
    if (aliases.some((alias) => normalizedName.includes(normalizeKey(alias)))) {
      return name;
    }
  }

  return null;
}

function getSheetRows(workbook, aliases) {
  const sheetName = findSheetByAliases(workbook, aliases);
  if (!sheetName) return [];

  const sheet = workbook.Sheets[sheetName];

  const rowsDefault = XLSX.utils.sheet_to_json(sheet, { defval: null });
  const rowsOffset3 = XLSX.utils.sheet_to_json(sheet, { range: 3, defval: null });

  const hasKpiHeaders = (rows) => {
    if (!rows.length) return false;
    const keys = Object.keys(rows[0]).map(normalizeKey);
    return keys.some((k) => k.includes('rgi')) && keys.some((k) => k.includes('ari'));
  };

  if (hasKpiHeaders(rowsDefault)) return rowsDefault;
  if (hasKpiHeaders(rowsOffset3)) return rowsOffset3;

  return rowsDefault.length ? rowsDefault : rowsOffset3;
}

function getRowValue(row, candidateKeys) {
  if (!row || typeof row !== 'object') return undefined;

  const entries = Object.entries(row);

  for (const candidate of candidateKeys) {
    const normalizedCandidate = normalizeKey(candidate);
    const match = entries.find(([key]) => normalizeKey(key) === normalizedCandidate);
    if (match && match[1] !== null && match[1] !== undefined && `${match[1]}`.trim() !== '') {
      return match[1];
    }
  }

  for (const candidate of candidateKeys) {
    const normalizedCandidate = normalizeKey(candidate);
    const partial = entries.find(([key]) => normalizeKey(key).includes(normalizedCandidate));
    if (partial && partial[1] !== null && partial[1] !== undefined && `${partial[1]}`.trim() !== '') {
      return partial[1];
    }
  }

  return undefined;
}

function toNumber(value) {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }

  if (value === null || value === undefined) {
    return null;
  }

  const cleaned = value.toString().replace(/,/g, '').replace(/%/g, '').trim();
  if (!cleaned) return null;

  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
}

function averageMetric(rows, candidateKeys) {
  const values = rows
    .map((row) => toNumber(getRowValue(row, candidateKeys)))
    .filter((value) => value !== null);

  if (!values.length) return null;

  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function capitalizePriority(priority) {
  if (!priority) return 'Medium';
  return priority.charAt(0).toUpperCase() + priority.slice(1);
}

function safeFixed(value, digits = 1) {
  return typeof value === 'number' && Number.isFinite(value) ? value.toFixed(digits) : 'n/a';
}

function summarizeDiagnosis(diagnosis, focus, driver) {
  const avgMPI = diagnosis?.metrics?.avgMPI;
  const avgARI = diagnosis?.metrics?.avgARI;
  const avgRGI = diagnosis?.metrics?.avgRGI;
  const focusSegment = focus?.focus_segment || 'retail';
  const driverReason = driver?.driver_reason || 'Commercial underperformance requires action.';

  return `${focusSegment} underperformance observed (MPI ${safeFixed(avgMPI)}, ARI ${safeFixed(avgARI)}, RGI ${safeFixed(avgRGI)}). ${driverReason}`;
}

function getExpectedImpactValue(action) {
  return action?.financial_impact?.impact_range?.high || null;
}

function extractPeriodMetadata(strRows) {
  const candidateDates = strRows
    .map((row) => getRowValue(row, ['Date', 'Business Date', 'Stay Date', 'Day', 'Report Date']))
    .map(parseExcelDate)
    .filter(Boolean)
    .sort((a, b) => a.getTime() - b.getTime());

  const snapshotDate = new Date();
  const snapshotDateYmd = formatDateToYMD(snapshotDate);

  if (!candidateDates.length) {
    return {
      snapshot_date: snapshotDateYmd,
      period_type: 'weekly',
      period_start: null,
      period_end: null,
      period_key: null,
      period_label: 'Unknown Period'
    };
  }

  const periodStart = candidateDates[0];
  const periodEnd = candidateDates[candidateDates.length - 1];
  const isoWeekInfo = getIsoWeekInfo(periodStart);

  const formatter = new Intl.DateTimeFormat('en-GB', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    timeZone: 'UTC'
  });

  return {
    snapshot_date: snapshotDateYmd,
    period_type: 'weekly',
    period_start: formatDateToYMD(periodStart),
    period_end: formatDateToYMD(periodEnd),
    period_key: isoWeekInfo
      ? `${isoWeekInfo.isoYear}-W${String(isoWeekInfo.isoWeek).padStart(2, '0')}`
      : null,
    period_label: `${formatter.format(periodStart)} → ${formatter.format(periodEnd)}`
  };
}

async function getWorkbookFromRequest(req) {
  const fileUrl = req.body?.fileUrl;
  if (!fileUrl) {
    throw new Error('Missing fileUrl in request body');
  }

  const downloadResponse = await axios.get(fileUrl, {
    responseType: 'arraybuffer',
    timeout: 30000,
    maxContentLength: 15 * 1024 * 1024,
    maxBodyLength: 15 * 1024 * 1024
  });

  return XLSX.read(Buffer.from(downloadResponse.data), { type: 'buffer' });
}

function detectDataContext(workbook) {
  const sheets = workbook.SheetNames || [];

  function getHeadersFromSheetAliases(aliases) {
    const sheetName = findSheetByAliases(workbook, aliases);
    if (!sheetName) return [];

    const sheet = workbook.Sheets[sheetName];
    if (!sheet) return [];

    const rowsDefault = XLSX.utils.sheet_to_json(sheet, { defval: null });
    const rowsOffset3 = XLSX.utils.sheet_to_json(sheet, { range: 3, defval: null });

    const pickRows = (rows) => {
      if (!rows.length) return [];
      return Object.keys(rows[0])
        .map(normalizeKey)
        .filter((h) => h && !h.includes('empty'));
    };

    const defaultHeaders = pickRows(rowsDefault);
    const offsetHeaders = pickRows(rowsOffset3);

    const defaultHasKpis =
      defaultHeaders.some((h) => h.includes('mpi')) ||
      defaultHeaders.some((h) => h.includes('ari')) ||
      defaultHeaders.some((h) => h.includes('rgi'));

    const offsetHasKpis =
      offsetHeaders.some((h) => h.includes('mpi')) ||
      offsetHeaders.some((h) => h.includes('ari')) ||
      offsetHeaders.some((h) => h.includes('rgi'));

    if (aliases.some((a) => normalizeKey(a).includes('str'))) {
      if (offsetHasKpis) return offsetHeaders;
      if (defaultHasKpis) return defaultHeaders;
    }

    if (defaultHeaders.length) return defaultHeaders;
    return offsetHeaders;
  }

  const strHeaders = getHeadersFromSheetAliases(['STR Daily Report', 'STR', 'Daily STR']);
  const pmsHeaders = getHeadersFromSheetAliases([
    'PMS Market Segment Report',
    'PMS',
    'Market Segment'
  ]);

  const allHeaders = [strHeaders, pmsHeaders];

  const has_str = strHeaders.length > 0;
  const has_mpi_ari_rgi =
    strHeaders.some((h) => h.includes('mpi')) &&
    strHeaders.some((h) => h.includes('ari')) &&
    strHeaders.some((h) => h.includes('rgi'));
  const has_segmentation = pmsHeaders.length > 0;
  const has_demand_data = false;
  const has_ly = allHeaders.some((headers) =>
    headers.some((h) => h.includes('ly') || h.includes('last year'))
  );
  const has_pace = pmsHeaders.some(
    (h) => h.includes('on books') || h.includes('forecast') || h.includes('pace')
  );
  const has_kpi_trend = allHeaders.some((headers) =>
    headers.some((h) => h.includes('change') || h.includes('% change') || h.includes('trend'))
  );

  let data_level = 4;
  if (has_str && has_segmentation) data_level = 1;
  else if (has_str) data_level = 2;
  else if (has_segmentation) data_level = 3;

  let confidence = 'low';
  if (data_level === 1 && has_mpi_ari_rgi && has_ly) confidence = 'high';
  else if ((data_level === 1 || data_level === 2 || data_level === 3) && (has_mpi_ari_rgi || has_segmentation)) {
    confidence = 'medium';
  }

  return {
    data_level,
    flags: {
      has_str,
      has_mpi_ari_rgi,
      has_segmentation,
      has_demand_data,
      has_ly,
      has_pace,
      has_kpi_trend
    },
    confidence,
    detection_details: {
      sheets_found: sheets,
      str_headers: strHeaders,
      pms_headers: pmsHeaders
    }
  };
}

function buildDiagnosisFromSTR(strRows) {
  if (!strRows.length) {
    return {
      performance_status: 'unknown',
      diagnosis_type: 'unknown',
      trend_status: 'stable',
      metrics: {
        avgMPI: null,
        avgARI: null,
        avgRGI: null,
        avgOcc: null
      }
    };
  }

  const avgMPI = averageMetric(strRows, ['MPI', 'MPI (Index)', 'Occupancy Index']);
  const avgARI = averageMetric(strRows, ['ARI', 'ARI (Index)', 'ADR Index']);
  const avgRGI = averageMetric(strRows, ['RGI', 'RGI (Index)', 'RevPAR Index']);
  const avgOcc = averageMetric(strRows, ['Occupancy %', 'Hotel Occupancy %']);
  const mpiChange = averageMetric(strRows, ['MPI % Change', 'MPI %']);
  const rgiChange = averageMetric(strRows, ['RGI % Change', 'RGI %']);

  let performance_status = 'balanced';
  if (avgMPI !== null && avgRGI !== null && avgMPI >= 100 && avgRGI >= 100) {
    performance_status = 'strong';
  } else if ((avgMPI !== null && avgMPI < 100) || (avgRGI !== null && avgRGI < 100)) {
    performance_status = 'underperforming';
  }

  let diagnosis_type = 'share_loss';
  if (avgOcc !== null && avgMPI !== null && avgARI !== null && avgOcc >= 85 && avgMPI >= 100 && avgARI < 100) {
    diagnosis_type = 'compression_mismanagement';
  } else if (avgMPI !== null && avgARI !== null && avgMPI < 100 && avgARI > 100) {
    diagnosis_type = 'pricing_resistance';
  } else if (avgMPI !== null && avgARI !== null && avgARI < 100 && avgMPI <= 100) {
    diagnosis_type = 'discount_inefficiency';
  } else if (avgMPI !== null && avgARI !== null && avgMPI < 95 && avgARI >= 95 && avgARI <= 105) {
    diagnosis_type = 'visibility_gap';
  } else if (avgMPI !== null && avgMPI < 100) {
    diagnosis_type = 'share_loss';
  } else if (
    avgMPI !== null &&
    avgARI !== null &&
    avgRGI !== null &&
    avgMPI >= 100 &&
    avgARI > 100 &&
    avgRGI > 100
  ) {
    diagnosis_type = 'healthy';
  }

  let trend_status = 'stable';
  if ((mpiChange !== null && mpiChange > 0) || (rgiChange !== null && rgiChange > 0)) {
    trend_status = 'improving';
  } else if ((mpiChange !== null && mpiChange < 0) || (rgiChange !== null && rgiChange < 0)) {
    trend_status = 'worsening';
  }

  return {
    performance_status,
    diagnosis_type,
    trend_status,
    metrics: {
      avgMPI,
      avgARI,
      avgRGI,
      avgOcc
    }
  };
}

function buildFocusFromPMS(pmsRows, diagnosis) {
  if (!pmsRows.length) {
    return {
      focus_segment: 'unknown',
      focus_reason: 'No PMS data available',
      segment_analysis: []
    };
  }

  function mapSegment(name = '') {
    const n = name.toLowerCase();

    if (
      n.includes('transient') ||
      n.includes('retail') ||
      n.includes('ota') ||
      n.includes('booking') ||
      n.includes('expedia') ||
      n.includes('direct')
    ) return 'retail';

    if (
      n.includes('corporate') ||
      n.includes('negotiated') ||
      n.includes('lnr') ||
      n.includes('company')
    ) return 'negotiated';

    if (
      n.includes('group') ||
      n.includes('mice') ||
      n.includes('event') ||
      n.includes('conference')
    ) return 'groups';

    return 'other';
  }

  const segmentData = {};

  pmsRows.forEach((row) => {
    const name = row['market segment name'] || row['Market Segment Name'] || '';
    const segment = mapSegment(name);

    const rnTY = Number(row['room nights on books ty'] || row['Room Nights On Books TY'] || 0);
    const rnLY = Number(row['room nights on books ly'] || row['Room Nights On Books LY'] || 0);
    const revTY = Number(row['booked revenue ty'] || row['Booked Revenue TY'] || 0);
    const revLY = Number(row['booked revenue ly'] || row['Booked Revenue LY'] || 0);

    if (!segmentData[segment]) {
      segmentData[segment] = { rnTY: 0, rnLY: 0, revTY: 0, revLY: 0 };
    }

    segmentData[segment].rnTY += rnTY;
    segmentData[segment].rnLY += rnLY;
    segmentData[segment].revTY += revTY;
    segmentData[segment].revLY += revLY;
  });

  const segmentAnalysis = Object.entries(segmentData).map(([segment, data]) => ({
    segment,
    rnGrowth: data.rnLY > 0 ? (data.rnTY - data.rnLY) / data.rnLY : 0,
    revGrowth: data.revLY > 0 ? (data.revTY - data.revLY) / data.revLY : 0
  }));

  let focus_segment = 'retail';

  switch (diagnosis.diagnosis_type) {
    case 'compression_mismanagement':
      focus_segment = 'groups';
      break;
    case 'healthy':
      focus_segment = 'none';
      break;
    default:
      focus_segment = 'retail';
      break;
  }

  const worstSegment = [...segmentAnalysis].sort((a, b) => a.rnGrowth - b.rnGrowth)[0];
  if (worstSegment && worstSegment.rnGrowth < -0.1) {
    focus_segment = worstSegment.segment;
  }

  return {
    focus_segment,
    focus_reason: `Focus on ${focus_segment} segment due to alignment with ${diagnosis.diagnosis_type} and observed performance gaps`,
    segment_analysis: segmentAnalysis
  };
}

function buildDriverFromDiagnosis(diagnosis, focus, strRows = [], pmsRows = []) {
  const avgMPI = Number(diagnosis?.metrics?.avgMPI || 0);
  const avgARI = Number(diagnosis?.metrics?.avgARI || 0);
  const avgRGI = Number(diagnosis?.metrics?.avgRGI || 0);
  const avgOcc = Number(diagnosis?.metrics?.avgOcc || 0);
  const trendStatus = diagnosis?.trend_status || 'stable';
  const focusSegment = focus?.focus_segment || 'other';

  function safeNum(value) {
    const n = Number(value);
    return Number.isFinite(n) ? n : 0;
  }

  function normalizeSegmentName(name = '') {
    const s = String(name).toLowerCase().trim();

    if (
      s.includes('retail') ||
      s.includes('transient') ||
      s.includes('ota') ||
      s.includes('online') ||
      s.includes('bar') ||
      s.includes('rack') ||
      s.includes('promo') ||
      s.includes('discount')
    ) return 'retail';

    if (
      s.includes('negotiated') ||
      s.includes('corporate') ||
      s.includes('corp') ||
      s.includes('local corporate') ||
      s.includes('qualified')
    ) return 'negotiated';

    if (
      s.includes('group') ||
      s.includes('wholesale') ||
      s.includes('crew') ||
      s.includes('mice') ||
      s.includes('conference')
    ) return 'groups';

    return 'other';
  }

  function getSegmentName(row = {}) {
    return (
      row.segment ||
      row.Segment ||
      row['Market Segment'] ||
      row['market segment'] ||
      row['Segment Name'] ||
      row['segment name'] ||
      ''
    );
  }

  function getSegmentADR(row = {}) {
    return safeNum(
      row.adr ||
      row.ADR ||
      row['Average Rate'] ||
      row['Avg Rate'] ||
      row['Average ADR'] ||
      row['Revenue per Room'] ||
      0
    );
  }

  const segmentBuckets = {
    retail: [],
    negotiated: [],
    groups: [],
    other: []
  };

  for (const row of pmsRows) {
    const segment = normalizeSegmentName(getSegmentName(row));
    const adr = getSegmentADR(row);
    if (adr > 0) segmentBuckets[segment].push(adr);
  }

  function avg(arr) {
    if (!arr.length) return 0;
    return arr.reduce((a, b) => a + b, 0) / arr.length;
  }

  function stddev(arr) {
    if (arr.length < 2) return 0;
    const mean = avg(arr);
    const variance = arr.reduce((sum, x) => sum + Math.pow(x - mean, 2), 0) / arr.length;
    return Math.sqrt(variance);
  }

  const segmentAvgADR = {
    retail: avg(segmentBuckets.retail),
    negotiated: avg(segmentBuckets.negotiated),
    groups: avg(segmentBuckets.groups),
    other: avg(segmentBuckets.other)
  };

  const allMajorSegmentADRs = Object.entries(segmentAvgADR)
    .filter(([key, value]) => ['retail', 'negotiated', 'groups'].includes(key) && value > 0)
    .map(([, value]) => value);

  const overallAvgSegmentADR = avg(allMajorSegmentADRs);
  const overallSegmentADRStdDev = stddev(allMajorSegmentADRs);
  const focusSegmentADR = safeNum(segmentAvgADR[focusSegment]);

  const isLowRatedMixPressure =
    focusSegmentADR > 0 &&
    overallAvgSegmentADR > 0 &&
    (
      focusSegmentADR < overallAvgSegmentADR ||
      focusSegmentADR < (overallAvgSegmentADR - 0.5 * overallSegmentADRStdDev)
    );

  const result = {
    primary_driver: 'visibility',
    secondary_driver: null,
    driver_reason: 'Default fallback: share underperformance without a stronger confirmed cross-driver pattern.',
    rule_triggered: 'share_loss_fallback',
    confidence: 'low',
    driver_context: {
      focus_segment: focusSegment,
      focus_segment_adr: Number(focusSegmentADR.toFixed(2)),
      overall_avg_segment_adr: Number(overallAvgSegmentADR.toFixed(2)),
      overall_segment_adr_stddev: Number(overallSegmentADRStdDev.toFixed(2)),
      is_low_rated_mix_pressure: isLowRatedMixPressure
    }
  };

  if (avgMPI >= 100 && avgARI > 100 && avgRGI > 100) {
    return {
      ...result,
      primary_driver: 'none',
      secondary_driver: null,
      driver_reason:
        'The hotel is holding both price premium and share above market, indicating healthy premium performance rather than an actionable driver issue.',
      rule_triggered: 'healthy_premium',
      confidence: 'high'
    };
  }

  if (avgMPI < 100 && avgARI > 100 && isLowRatedMixPressure) {
    return {
      ...result,
      primary_driver: 'mix_strategy',
      secondary_driver: 'pricing',
      driver_reason:
        'The hotel is priced above market but not capturing enough share, while the focused segment ADR sits below the blended segment benchmark, indicating mix pressure rather than pure pricing alone.',
      rule_triggered: 'mix_constraint',
      confidence: 'high'
    };
  }

  if (avgMPI < 100 && avgARI > 100) {
    return {
      ...result,
      primary_driver: 'pricing',
      secondary_driver: null,
      driver_reason:
        'High ARI with weak MPI indicates the hotel is carrying a price premium that demand is not fully accepting, pointing to pricing resistance.',
      rule_triggered: 'pricing_resistance',
      confidence: 'high'
    };
  }

  if (avgARI < 100 && avgMPI <= 100) {
    return {
      ...result,
      primary_driver: 'conversion',
      secondary_driver: null,
      driver_reason:
        'The hotel is trading below market rate without generating sufficient share gain, indicating discount inefficiency and weak conversion.',
      rule_triggered: 'discount_inefficiency',
      confidence: 'high'
    };
  }

  if ((avgMPI < 95 && trendStatus === 'declining') || avgMPI < 92) {
    return {
      ...result,
      primary_driver: 'visibility',
      secondary_driver: null,
      driver_reason:
        'Share capture is weak and worsening versus market, which points to a visibility issue rather than a pure pricing problem.',
      rule_triggered: 'visibility_gap',
      confidence: 'medium'
    };
  }

  if (avgOcc >= 78 && avgARI <= 100) {
    return {
      ...result,
      primary_driver: 'pricing',
      secondary_driver: null,
      driver_reason:
        'Occupancy is already relatively strong, but rate premium is not being maximized, suggesting missed pricing opportunity under stronger demand conditions.',
      rule_triggered: 'missed_pricing_opportunity',
      confidence: 'medium'
    };
  }

  return result;
}

function buildActionsFromDriver(driver, focus) {
  const primaryDriver = driver?.primary_driver;
  const secondaryDriver = driver?.secondary_driver;
  const segment = focus?.focus_segment;

  if (segment !== 'retail') {
    return [];
  }

  const primaryActions = ACTION_LIBRARY.filter(
    (action) => action.driver === primaryDriver && action.segment === 'retail'
  );
  const primary = primaryActions[0] || null;

  let secondary = null;

  if (secondaryDriver) {
    const secondaryActions = ACTION_LIBRARY.filter(
      (action) => action.driver === secondaryDriver && action.segment === 'retail'
    );
    secondary = secondaryActions[0] || null;
  } else {
    const fallbackMap = {
      pricing: 'conversion',
      visibility: 'conversion',
      conversion: 'visibility',
      mix_strategy: 'pricing'
    };

    const fallbackDriver = fallbackMap[primaryDriver];
    const fallbackActions = ACTION_LIBRARY.filter(
      (action) => action.driver === fallbackDriver && action.segment === 'retail'
    );
    secondary = fallbackActions[0] || null;
  }

  return [primary, secondary]
    .filter(Boolean)
    .slice(0, 2)
    .map((action) => ({
      action_id: action.action_id,
      // Stable join key for DB + frontend (Phase 4 foundation); mirrors action_id until taxonomy expands.
      finding_key: action.action_id,
      driver: action.driver,
      segment: action.segment,
      title: action.title,
      description: action.description,
      priority: action.priority
    }));
}

function buildFinancialImpact({ driver, diagnosis, action, detection, pmsRows = [], strRows = [] }) {
  const adrValues = pmsRows
    .map((row) => Number(row.adr ?? row.ADR ?? 0))
    .filter((value) => !Number.isNaN(value) && value > 0);

  const rnValues = pmsRows
    .map((row) => Number(row.room_nights ?? row.roomNights ?? row.rn ?? row.RN ?? 0))
    .filter((value) => !Number.isNaN(value) && value > 0);

  const mpiValues = strRows
    .map((row) => Number(row.mpi ?? row.MPI ?? 0))
    .filter((value) => !Number.isNaN(value) && value > 0);

  const avgADR =
    adrValues.length > 0
      ? adrValues.reduce((a, b) => a + b, 0) / adrValues.length
      : null;

  const totalRN =
    rnValues.length > 0
      ? rnValues.reduce((a, b) => a + b, 0)
      : null;

  const avgMPI =
    mpiValues.length > 0
      ? mpiValues.reduce((a, b) => a + b, 0) / mpiValues.length
      : null;

  if (!avgADR || !totalRN || !avgMPI) {
    return {
      impact_type: 'revenue_uplift',
      impact_range: { low: null, high: null },
      impact_timeline: 'unknown',
      market_context: 'unknown',
      executive_summary: 'Insufficient PMS or STR data to estimate financial impact credibly.',
      calculation_logic: 'Insufficient PMS or STR data to estimate financial impact credibly.',
      confidence: 'low'
    };
  }

  let recoveryFactor = 0.15;

  if (avgMPI < 90) {
    recoveryFactor = 0.4;
  } else if (avgMPI < 95) {
    recoveryFactor = 0.25;
  } else if (avgMPI < 98) {
    recoveryFactor = 0.15;
  } else {
    recoveryFactor = 0.08;
  }

  const trend =
    diagnosis?.trend_status ||
    detection?.trend_status ||
    detection?.trend ||
    detection?.performance_trend ||
    'stable';

  if (trend === 'worsening' || trend === 'declining') recoveryFactor *= 0.8;
  if (trend === 'improving') recoveryFactor *= 1.1;

  let marketContext = 'competitive';
  if (avgMPI < 90) {
    marketContext = 'demand_available';
  } else if (avgMPI > 100) {
    marketContext = 'constrained';
  }

  const driverCategory =
    driver?.driver_category ||
    driver?.primary_driver ||
    driver?.category ||
    action?.driver ||
    'conversion';

  let impactTimeline = 'mid_term';
  if (driverCategory === 'pricing' || driverCategory === 'pricing_positioning') {
    impactTimeline = 'short_term';
  } else if (driverCategory === 'conversion') {
    impactTimeline = 'short_to_mid';
  } else if (driverCategory === 'visibility') {
    impactTimeline = 'mid_to_long';
  } else if (driverCategory === 'mix_strategy') {
    impactTimeline = 'mid_term';
  }

  let actionType = 'conversion';
  if (
    action?.driver === 'pricing' ||
    driverCategory === 'pricing' ||
    driverCategory === 'pricing_positioning'
  ) {
    actionType = 'pricing';
  } else if (action?.driver === 'visibility' || driverCategory === 'visibility') {
    actionType = 'visibility';
  } else if (action?.driver === 'conversion' || driverCategory === 'conversion') {
    actionType = 'conversion';
  } else if (driverCategory === 'mix_strategy') {
    actionType = 'mix_strategy';
  }

  let actionMultiplier = 1.0;
  if (actionType === 'pricing') actionMultiplier = 1.2;
  if (actionType === 'visibility') actionMultiplier = 0.7;
  if (actionType === 'mix_strategy') actionMultiplier = 0.9;
  recoveryFactor *= actionMultiplier;

  if (actionType === 'pricing' && marketContext === 'constrained') recoveryFactor *= 1.2;
  if (actionType === 'pricing' && marketContext === 'demand_available') recoveryFactor *= 0.8;

  recoveryFactor = Math.max(0.03, Math.min(recoveryFactor, 0.60));

  const recoverableRN = totalRN * recoveryFactor;
  const rawImpact = recoverableRN * avgADR;
  const low = Math.round(rawImpact * 0.35);
  const high = Math.round(rawImpact * 0.65);

  let confidence = 'medium';
  if (avgADR > 0 && totalRN > 0 && avgMPI < 95) confidence = 'high';
  if (avgMPI >= 98) confidence = 'low';
  if (actionType === 'pricing' && marketContext === 'constrained') confidence = 'high';
  if (actionType === 'visibility') confidence = 'medium';

  let executiveSummary = 'Moderate revenue opportunity with manageable execution risk.';
  if (actionType === 'pricing' && marketContext === 'constrained') {
    executiveSummary =
      'Immediate and high-impact revenue opportunity driven by pricing optimization in a high-demand environment.';
  } else if (actionType === 'pricing' && marketContext === 'demand_available') {
    executiveSummary =
      'Pricing adjustments can unlock incremental revenue, though impact remains dependent on underlying demand levels.';
  } else if (actionType === 'conversion') {
    executiveSummary =
      'Conversion improvements provide steady and reliable revenue gains through better demand capture.';
  } else if (actionType === 'visibility') {
    executiveSummary =
      'Visibility enhancements support long-term revenue growth but will not generate immediate uplift.';
  } else if (actionType === 'mix_strategy') {
    executiveSummary =
      'Business mix adjustments can improve revenue quality, though impact materializes progressively over time.';
  }

  return {
    impact_type: 'revenue_uplift',
    impact_range: { low, high },
    impact_timeline: impactTimeline,
    market_context: marketContext,
    executive_summary: executiveSummary,
    calculation_logic: `Based on STR MPI ${avgMPI.toFixed(1)}, PMS room nights ${Math.round(totalRN)}, PMS ADR ${Math.round(avgADR)}, ${marketContext} market context, ${actionType} action type, ${trend} trend, and a ${Math.round(recoveryFactor * 100)}% adjusted recovery factor.`,
    confidence
  };
}

function buildTotalOpportunity(actions = []) {
  let grossLow = 0;
  let grossHigh = 0;
  const driverImpact = {};

  actions.forEach((action) => {
    const low = action.financial_impact?.impact_range?.low || 0;
    const high = action.financial_impact?.impact_range?.high || 0;

    grossLow += low;
    grossHigh += high;

    const driver = action.driver || 'other';
    if (!driverImpact[driver]) driverImpact[driver] = 0;
    driverImpact[driver] += high;
  });

  let overlapFactor = 1;
  if (actions.length === 2) overlapFactor = 0.85;
  if (actions.length >= 3) overlapFactor = 0.7;

  const adjustedLow = Math.round(grossLow * overlapFactor);
  const adjustedHigh = Math.round(grossHigh * overlapFactor);

  let priorityDriver = 'mixed';
  let maxImpact = 0;
  Object.entries(driverImpact).forEach(([driver, value]) => {
    if (value > maxImpact) {
      maxImpact = value;
      priorityDriver = driver;
    }
  });

  let confidence = 'medium';
  const highConfidenceActions = actions.filter(
    (action) => action.financial_impact?.confidence === 'high'
  ).length;
  if (actions.length === 0) confidence = 'low';
  else if (highConfidenceActions >= Math.ceil(actions.length / 2)) confidence = 'high';

  let summary = 'Revenue opportunity exists across multiple levers with moderate overlap.';
  if (priorityDriver === 'pricing') {
    summary = 'Primary revenue recovery is driven by pricing, with some overlap across actions.';
  } else if (priorityDriver === 'conversion') {
    summary = 'Conversion improvements represent the main revenue opportunity, with moderate overlap.';
  } else if (priorityDriver === 'visibility') {
    summary = 'Revenue growth depends on visibility improvements, with longer realization and overlap across actions.';
  } else if (priorityDriver === 'mix_strategy') {
    summary = 'Revenue opportunity is linked to business mix optimization, with moderate overlap across actions.';
  }

  return {
    gross_opportunity: { low: Math.round(grossLow), high: Math.round(grossHigh) },
    adjusted_opportunity: { low: adjustedLow, high: adjustedHigh },
    overlap_factor: overlapFactor,
    priority_driver: priorityDriver,
    confidence,
    summary
  };
}

function buildRecommendationsPayload({ hotelCode, periodMeta, diagnosis, focus, driver, actions }) {
  return actions.map((action) => ({
    hotel_name: hotelCode,
    title: action.title,
    finding_key: action.action_id || action.finding_key || null,
    department: OWNER_DEPARTMENT_BY_DRIVER[action.driver] || 'Commercial',
    finding: summarizeDiagnosis(diagnosis, focus, driver),
    impact_value: getExpectedImpactValue(action),
    impact_type: action.financial_impact?.impact_type || 'revenue_uplift',
    expected_impact_value: getExpectedImpactValue(action),
    status: 'open',
    period: periodMeta.period_label,
    root_cause:
      driver.driver_reason || diagnosis.diagnosis_type || 'Strategic commercial opportunity identified.',
    expected_outcome:
      action.financial_impact?.executive_summary ||
      'Improve commercial performance through targeted action.',
    owner_department: OWNER_DEPARTMENT_BY_DRIVER[action.driver] || 'Commercial',
    priority: capitalizePriority(action.priority),
    driver: action.driver || null,
    segment: focus.focus_segment || null,
    snapshot_date: periodMeta.snapshot_date,
    period_type: periodMeta.period_type,
    period_start: periodMeta.period_start,
    period_end: periodMeta.period_end,
    period_key: periodMeta.period_key,
    period_label: periodMeta.period_label
  }));
}

function buildActionsPayload({ hotelCode, periodMeta, focus, actions }) {
  return actions.map((action) => ({
    hotel_name: hotelCode,
    period: periodMeta.period_label,
    snapshot_date: periodMeta.snapshot_date,
    period_type: periodMeta.period_type,
    period_start: periodMeta.period_start,
    period_end: periodMeta.period_end,
    period_key: periodMeta.period_key,
    period_label: periodMeta.period_label,
    title: action.title,
    finding_key: action.action_id || action.finding_key || null,
    action_text: action.description,
    priority: action.priority || null,
    driver: action.driver || null,
    segment: focus.focus_segment || null
  }));
}

// --------------------
// MAIN HANDLER
// --------------------
async function handler(req, res) {
  try {
    const hotelCode = (req.body?.hotelCode || 'Unknown Hotel').toString().trim();
    const workbook = await getWorkbookFromRequest(req);

    const strRows = getSheetRows(workbook, ['STR Daily Report', 'STR', 'Daily STR']);
    if (!strRows.length) {
      return res.status(400).json({ error: 'STR sheet not found or empty' });
    }

    const pmsRows = getSheetRows(workbook, ['PMS Market Segment Report', 'PMS', 'Market Segment']);
    const detection = detectDataContext(workbook);
    const diagnosis = buildDiagnosisFromSTR(strRows);
    const focus = buildFocusFromPMS(pmsRows, diagnosis);
    const driver = buildDriverFromDiagnosis(diagnosis, focus, strRows, pmsRows);

    const baseActions = buildActionsFromDriver(driver, focus);
    const enrichedActions = baseActions.map((action) => ({
      ...action,
      financial_impact: buildFinancialImpact({
        driver,
        diagnosis,
        action,
        detection,
        pmsRows,
        strRows
      })
    }));

    const totalOpportunity = buildTotalOpportunity(enrichedActions);
    const periodMeta = extractPeriodMetadata(strRows);

    const enginePayload = {
      success: true,
      detection,
      diagnosis,
      focus,
      driver,
      total_opportunity: totalOpportunity,
      actions: enrichedActions
    };

console.log('DEBUG about to insert engine_outputs');
console.log('DEBUG engine_payload hotel_code:', hotelCode);
console.log('DEBUG engine_payload period_label:', periodMeta.period_label);

const { data: engineInsertData, error: engineSaveError } = await supabase
  .from('engine_outputs')
  .insert({
    hotel_code: hotelCode,
    snapshot_date: periodMeta.snapshot_date,
    generated_at: new Date().toISOString(),
    source_file_name: req.body?.originalFileName || null,
    period_label: periodMeta.period_label,
    engine_json: enginePayload
  })
  .select();

console.log('DEBUG engine_outputs insert data:', engineInsertData);
console.log('DEBUG engine_outputs insert error:', engineSaveError);

if (engineSaveError) {
  throw engineSaveError;
}

    const recommendationsPayload = buildRecommendationsPayload({
      hotelCode,
      periodMeta,
      diagnosis,
      focus,
      driver,
      actions: enrichedActions
    });

    if (recommendationsPayload.length > 0) {
      const { error: recommendationsError } = await supabase
        .from('Recommendations')
        .insert(recommendationsPayload);

      if (recommendationsError) {
        throw recommendationsError;
      }
    }

    const actionsPayload = buildActionsPayload({
      hotelCode,
      periodMeta,
      focus,
      actions: enrichedActions
    });

    if (actionsPayload.length > 0) {
      const { error: actionsError } = await supabase.from('actions').insert(actionsPayload);
      if (actionsError) {
        throw actionsError;
      }
    }

    console.log('DEBUG engine_outputs rows:', 1);
    console.log('DEBUG recommendations rows:', recommendationsPayload.length);
    console.log('DEBUG actions rows:', actionsPayload.length);
    console.log('DEBUG first recommendation:', recommendationsPayload[0] || null);
    console.log('DEBUG first action:', actionsPayload[0] || null);

    return res.status(200).json({
      success: true,
      hotelCode,
      period: periodMeta.period_label,
      engine: enginePayload,
      recommendations_count: recommendationsPayload.length,
      actions_count: actionsPayload.length
    });
  } catch (error) {
    console.error('Analyze handler error:', error);
    return res.status(500).json({
      error: error.message || 'Processing failed'
    });
  }
}

module.exports = handler;
