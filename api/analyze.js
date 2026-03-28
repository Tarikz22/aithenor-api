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

/** Max distinct retail issues per run — favor sharp, non-overlapping cards over volume. */
const MAX_RETAIL_ISSUES_PER_RUN = 3;

/** Legacy fallback: max flattened actions when wrapping old driver-only path. */
const MAX_LEGACY_RETAIL_ACTIONS = 5;

/** Max library actions attached to a single issue (executive readability). */
const MAX_ACTIONS_PER_RETAIL_ISSUE = 3;

/** Minimum STR daily rows required to score a calendar week (thin weeks skipped). */
const MIN_STR_DAYS_PER_WEEK = 4;

/** Adjacent ISO weeks merge if MPI and ARI are within these index points (episode same regime). */
const EPISODE_MERGE_MPI_ARI_MAX_DELTA = 4;

const PRIORITY_SORT_RANK = { high: 0, medium: 1, low: 2 };

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

function libraryIndexByActionId(actionId) {
  const idx = ACTION_LIBRARY.findIndex((a) => a.action_id === actionId);
  return idx === -1 ? 999 : idx;
}

/**
 * Retail-only multi-finding: all library actions for primary (and secondary / fallback) drivers,
 * deduped by action_id, ordered by issue tier then priority then stable library order.
 */
function buildActionsFromDriver(driver, focus) {
  const primaryDriver = driver?.primary_driver;
  const secondaryDriver = driver?.secondary_driver;
  const segment = focus?.focus_segment;

  if (segment !== 'retail') {
    return [];
  }

  if (!primaryDriver || primaryDriver === 'none') {
    return [];
  }

  const fallbackMap = {
    pricing: 'conversion',
    visibility: 'conversion',
    conversion: 'visibility',
    mix_strategy: 'pricing'
  };

  const fallbackDriver = fallbackMap[primaryDriver];

  function retailActionsForDriver(drv) {
    if (!drv || drv === 'none') return [];
    return ACTION_LIBRARY.filter((a) => a.driver === drv && a.segment === 'retail');
  }

  const seenIds = new Set();
  const seenTitleNorm = new Set();
  const staged = [];

  function normalizeTitle(t) {
    return (t || '')
      .toString()
      .trim()
      .toLowerCase()
      .replace(/\s+/g, ' ');
  }

  function stageActions(actions, sourceTier) {
    for (const action of actions) {
      if (seenIds.has(action.action_id)) continue;
      const tnorm = normalizeTitle(action.title);
      if (tnorm && seenTitleNorm.has(tnorm)) continue;
      seenIds.add(action.action_id);
      if (tnorm) seenTitleNorm.add(tnorm);
      staged.push({
        action,
        sourceTier,
        priorityRank: PRIORITY_SORT_RANK[action.priority] ?? 1,
        libIdx: libraryIndexByActionId(action.action_id)
      });
    }
  }

  // Primary diagnosis driver: include every distinct retail library action for that driver.
  stageActions(retailActionsForDriver(primaryDriver), 0);

  if (secondaryDriver) {
    stageActions(retailActionsForDriver(secondaryDriver), 1);
  } else if (fallbackDriver && fallbackDriver !== primaryDriver) {
    // Complementary driver (e.g. pricing + conversion): up to two actions, same as prior breadth cap per leg.
    stageActions(retailActionsForDriver(fallbackDriver).slice(0, 2), 2);
  }

  staged.sort((a, b) => {
    if (a.sourceTier !== b.sourceTier) return a.sourceTier - b.sourceTier;
    if (a.priorityRank !== b.priorityRank) return a.priorityRank - b.priorityRank;
    return a.libIdx - b.libIdx;
  });

  const capped = staged.slice(0, MAX_LEGACY_RETAIL_ACTIONS);

  return capped.map(({ action }) => ({
    action_id: action.action_id,
    finding_key: action.action_id,
    driver: action.driver,
    segment: action.segment,
    title: action.title,
    description: action.description,
    priority: action.priority
  }));
}

// --- Retail issue layer (Phase 4: issue-led, recommendations primary, actions per issue) ---

function retailIssueFindingKey(issueFamily) {
  const safe = String(issueFamily || 'unknown').replace(/[^a-z0-9_]/gi, '_');
  return `RET_ISSUE_${safe}`;
}

function actionsFromLibraryByIds(ids) {
  return ids.map((id) => ACTION_LIBRARY.find((a) => a.action_id === id)).filter(Boolean);
}

/**
 * Disjoint action sets per issue family so co-existing cards do not repeat the same library rows.
 * (Within a single issue, 2 actions max where it helps execution clarity.)
 */
function pickRetailLibraryActionsForFamily(family) {
  switch (family) {
    case 'mix_constraint':
      // Mix-only lever on this card — pricing levers stay on pricing_resistance (no shared library rows).
      return actionsFromLibraryByIds(['MIX_01']);
    case 'pricing_resistance':
      // Full retail pricing pair — does not repeat MIX_01 or visibility/conversion rows.
      return actionsFromLibraryByIds(['RET_PRICING_01', 'RET_PRICING_02']);
    case 'discount_inefficiency':
      // Conversion path only — keep OTA/visibility levers for visibility_gap when it survives suppression.
      return actionsFromLibraryByIds(['RET_CONV_01', 'RET_CONV_02']);
    case 'visibility_gap':
      // Channel exposure only — digital/brand campaigns separated from OTA row used here.
      return actionsFromLibraryByIds(['RET_VIS_02']);
    case 'missed_pricing_opportunity':
      // Uplift capture without cloning full pricing_resistance pair.
      return actionsFromLibraryByIds(['RET_PRICING_01']);
    case 'share_loss_fallback':
      return actionsFromLibraryByIds(['RET_VIS_01', 'RET_CONV_01']);
    default:
      return [];
  }
}

const RETAIL_ISSUE_TITLES = {
  mix_constraint: 'Retail mix quality and rate positioning are jointly constraining share',
  pricing_resistance: 'Retail pricing is ahead of demand acceptance versus the competitive set',
  discount_inefficiency: 'Retail discounting is not converting into adequate occupancy share',
  visibility_gap: 'Retail demand capture is weak relative to market visibility',
  missed_pricing_opportunity: 'Strong retail occupancy is not translating into optimal rate capture',
  share_loss_fallback: 'Retail share is under pressure versus the competitive baseline'
};

const RETAIL_ISSUE_ROOT_CAUSES = {
  mix_constraint:
    'Retail revenue quality is misaligned: segment-level ADR is soft versus blended benchmarks while headline indexes still show a rate premium, so the constraint is mix composition—not a single BAR tweak.',
  pricing_resistance:
    'Retail is carrying a rate premium versus the comp set without matching occupancy penetration, so the bottleneck is price acceptance at the index level, not channel plumbing.',
  discount_inefficiency:
    'Retail is already at or below market rate on the index yet share is weak, so deeper discounting is unlikely to fix the problem—the leak is conversion of existing demand into booked nights.',
  visibility_gap:
    'Retail MPI weakness persists without a high-ARI premium story, so the property is likely losing qualified demand upstream (reach and consideration) before rate can even be tested.',
  missed_pricing_opportunity:
    'Retail occupancy is holding but ADR index is not maximized relative to demand conditions, so the gap is rate capture on in-market guests—not filling empty rooms.',
  share_loss_fallback:
    'Retail MPI is soft without a clean single-index story; the next step is to isolate whether loss is upstream demand, booking friction, or rate architecture before stacking levers.'
};

const RETAIL_ISSUE_EXPECTED_OUTCOMES = {
  mix_constraint:
    'Shift retail contribution toward higher-rated segments and use fenced tactics so mix improvement does not rely on public BAR cuts alone.',
  pricing_resistance:
    'Recover retail occupancy index versus the comp set by realigning BAR and corridor logic with observed demand acceptance.',
  discount_inefficiency:
    'Turn existing retail discounting into booked nights by fixing booking-path friction and parity-driven leakage, not by deeper headline cuts.',
  visibility_gap:
    'Increase qualified retail demand before the booking window by lifting brand and paid demand generation where MPI erosion is structural.',
  missed_pricing_opportunity:
    'Lift retail ADR and RevPAR index while occupancy is already supportive—prioritize rate architecture, not share-recovery discounts.',
  share_loss_fallback:
    'Choose one validated retail lever (exposure, conversion, or rate) using segment proof points, then sequence the second wave after the first moves MPI.'
};

function retailIssueFindingText(family, diagnosis, focus) {
  const seg = focus?.focus_segment || 'retail';
  const m = diagnosis?.metrics || {};
  const mpi = safeFixed(m.avgMPI);
  const ari = safeFixed(m.avgARI);
  const rgi = safeFixed(m.avgRGI);
  const occ = safeFixed(m.avgOcc);
  const lines = {
    mix_constraint: `${seg}: mix-and-index conflict — MPI ${mpi}, ARI ${ari}, RGI ${rgi}. Segment economics suggest mix quality is dragging outcomes while indexes still imply rate tension.`,
    pricing_resistance: `${seg}: premium-without-share — MPI ${mpi} lags while ARI ${ari} stays above parity (RGI ${rgi}). The pattern is demand rejecting the current rate ladder versus competitors.`,
    discount_inefficiency: `${seg}: discount-without-share — MPI ${mpi} with ARI ${ari} at/below market (RGI ${rgi}). Rate is not the primary ceiling; execution is failing to convert available demand.`,
    visibility_gap: `${seg}: penetration gap — MPI ${mpi} weak without a sustained ARI premium narrative (ARI ${ari}, RGI ${rgi}). Losses likely occur before the booking funnel, not only at conversion.`,
    missed_pricing_opportunity: `${seg}: occupancy-supported rate gap — Occ ${occ}% with MPI ${mpi} and ARI ${ari}. Rooms are moving but indexes imply ADR headroom versus optimal retail capture.`,
    share_loss_fallback: `${seg}: undifferentiated underperformance — MPI ${mpi}, ARI ${ari}, RGI ${rgi}. No single index pattern dominates; isolate demand vs conversion vs rate with segment reads.`
  };
  return lines[family] || lines.share_loss_fallback;
}

function weekBucketKeyFromDate(date) {
  const info = getIsoWeekInfo(date);
  if (!info) return null;
  return `${info.isoYear}-W${String(info.isoWeek).padStart(2, '0')}`;
}

function getStrRowDate(row) {
  const raw = getRowValue(row, ['Date', 'Business Date', 'Stay Date', 'Day', 'Report Date']);
  return parseExcelDate(raw);
}

function minMaxYmdFromDates(dates) {
  const valid = dates.filter((d) => d instanceof Date && !Number.isNaN(d.getTime()));
  if (!valid.length) return { minYmd: null, maxYmd: null };
  const sorted = [...valid].sort((a, b) => a.getTime() - b.getTime());
  return {
    minYmd: formatDateToYMD(sorted[0]),
    maxYmd: formatDateToYMD(sorted[sorted.length - 1])
  };
}

/**
 * Group STR daily rows by calendar week (ISO-style bucket via getIsoWeekInfo).
 * @returns {Map<string, object[]>} weekKey -> rows
 */
function groupStrRowsByCalendarWeek(strRows) {
  const map = new Map();
  for (const row of strRows) {
    const d = getStrRowDate(row);
    if (!d) continue;
    const key = weekBucketKeyFromDate(d);
    if (!key) continue;
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(row);
  }
  return map;
}

function buildWindowMetricsFromRows(rows) {
  return {
    avgMPI: averageMetric(rows, ['MPI', 'MPI (Index)', 'Occupancy Index']),
    avgARI: averageMetric(rows, ['ARI', 'ARI (Index)', 'ADR Index']),
    avgRGI: averageMetric(rows, ['RGI', 'RGI (Index)', 'RevPAR Index']),
    avgOcc: averageMetric(rows, ['Occupancy %', 'Hotel Occupancy %'])
  };
}

function buildTrendFromWindowRows(rows) {
  const mpiChange = averageMetric(rows, ['MPI % Change', 'MPI %']);
  const rgiChange = averageMetric(rows, ['RGI % Change', 'RGI %']);
  if ((mpiChange !== null && mpiChange < 0) || (rgiChange !== null && rgiChange < 0)) {
    return 'worsening';
  }
  if ((mpiChange !== null && mpiChange > 0) || (rgiChange !== null && rgiChange > 0)) {
    return 'improving';
  }
  return 'stable';
}

function metricsSimilarForEpisodeMerge(a, b) {
  if (!a || !b) return false;
  const mpiA = Number(a.avgMPI);
  const mpiB = Number(b.avgMPI);
  const ariA = Number(a.avgARI);
  const ariB = Number(b.avgARI);
  if (!Number.isFinite(mpiA) || !Number.isFinite(mpiB)) return false;
  if (!Number.isFinite(ariA) || !Number.isFinite(ariB)) return false;
  return (
    Math.abs(mpiA - mpiB) <= EPISODE_MERGE_MPI_ARI_MAX_DELTA &&
    Math.abs(ariA - ariB) <= EPISODE_MERGE_MPI_ARI_MAX_DELTA
  );
}

function aggregateEpisodeMetrics(weekPayloads) {
  const list = weekPayloads.map((w) => w.metrics).filter(Boolean);
  if (!list.length) {
    return { avgMPI: null, avgARI: null, avgRGI: null, avgOcc: null };
  }
  const pick = (k) => {
    const vals = list.map((m) => m[k]).filter((v) => typeof v === 'number' && Number.isFinite(v));
    if (!vals.length) return null;
    return vals.reduce((s, v) => s + v, 0) / vals.length;
  };
  return {
    avgMPI: pick('avgMPI'),
    avgARI: pick('avgARI'),
    avgRGI: pick('avgRGI'),
    avgOcc: pick('avgOcc')
  };
}

function mergeWeeklyCandidatesIntoEpisodes(sortedCandidates) {
  const episodes = [];
  let cur = null;

  for (const c of sortedCandidates) {
    const canExtend =
      cur &&
      cur.family === c.family &&
      c.weekOrdinal === cur.lastWeekOrdinal + 1 &&
      metricsSimilarForEpisodeMerge(cur.lastMetrics, c.metrics);

    if (!canExtend) {
      if (cur) episodes.push(cur);
      cur = {
        family: c.family,
        primary_driver: c.primary_driver,
        weekKeys: [c.weekKey],
        weekOrdinals: [c.weekOrdinal],
        weekPayloads: [c],
        lastWeekOrdinal: c.weekOrdinal,
        lastMetrics: c.metrics,
        startYmd: c.startYmd,
        endYmd: c.endYmd
      };
    } else {
      cur.weekKeys.push(c.weekKey);
      cur.weekOrdinals.push(c.weekOrdinal);
      cur.weekPayloads.push(c);
      cur.lastWeekOrdinal = c.weekOrdinal;
      cur.lastMetrics = c.metrics;
      cur.endYmd = c.endYmd;
    }
  }
  if (cur) episodes.push(cur);
  return episodes;
}

function episodeSeverityScore(ep) {
  const agg = aggregateEpisodeMetrics(ep.weekPayloads);
  const mpi = Number(agg.avgMPI);
  if (Number.isFinite(mpi)) return 100 - mpi;
  return 0;
}

function rankAndCapEpisodes(episodes, maxCount) {
  return [...episodes]
    .sort((a, b) => {
      const sb = episodeSeverityScore(b);
      const sa = episodeSeverityScore(a);
      if (sb !== sa) return sb - sa;
      return (b.weekKeys?.length || 0) - (a.weekKeys?.length || 0);
    })
    .slice(0, maxCount);
}

function episodeFindingKey(family, startYmd, endYmd) {
  const fam = String(family || 'unknown').replace(/[^a-z0-9_]/gi, '_');
  const s = (startYmd || 'na').replace(/-/g, '');
  const e = (endYmd || 'na').replace(/-/g, '');
  return `RET_EP_${fam}_${s}_${e}`;
}

function materializeRetailEpisode(ep, focus) {
  const aggMetrics = aggregateEpisodeMetrics(ep.weekPayloads);
  const lastTrend =
    ep.weekPayloads[ep.weekPayloads.length - 1]?.trend || 'stable';
  const windowDiagnosis = { metrics: aggMetrics, trend_status: lastTrend };

  const finding_key = episodeFindingKey(ep.family, ep.startYmd, ep.endYmd);
  const lib = pickRetailLibraryActionsForFamily(ep.family);
  const cappedLib = lib.slice(0, MAX_ACTIONS_PER_RETAIL_ISSUE);
  const pri = cappedLib.some((a) => a.priority === 'high') ? 'high' : 'medium';

  return {
    finding_key,
    issue_family: ep.family,
    driver: ep.primary_driver,
    segment: 'retail',
    priority: pri,
    title: RETAIL_ISSUE_TITLES[ep.family] || RETAIL_ISSUE_TITLES.share_loss_fallback,
    finding: retailIssueFindingText(ep.family, windowDiagnosis, focus),
    root_cause: RETAIL_ISSUE_ROOT_CAUSES[ep.family] || RETAIL_ISSUE_ROOT_CAUSES.share_loss_fallback,
    expected_outcome:
      RETAIL_ISSUE_EXPECTED_OUTCOMES[ep.family] || RETAIL_ISSUE_EXPECTED_OUTCOMES.share_loss_fallback,
    rule_triggered: ep.family,
    _library_actions: cappedLib,
    episode_week_start: ep.startYmd,
    episode_week_end: ep.endYmd,
    episode_week_keys: ep.weekKeys,
    episode_week_count: ep.weekKeys.length,
    window_label: `${ep.startYmd} → ${ep.endYmd}`,
    temporal_layer: 'weekly_episode'
  };
}

/**
 * Internal weekly temporal pipeline: windows -> weekly specs -> episodes -> raw issue objects.
 * Returns { rawIssues, temporal_meta } or { rawIssues: [], temporal_meta } if unusable.
 */
function buildRetailIssuesFromWeeklyTemporal(strRows, focus, driver) {
  const byWeek = groupStrRowsByCalendarWeek(strRows);
  const sortedWeekKeys = [...byWeek.keys()].sort();

  const temporal_meta = {
    min_days_per_week: MIN_STR_DAYS_PER_WEEK,
    weekly_windows: [],
    fallback_reason: null
  };

  if (sortedWeekKeys.length < 1) {
    temporal_meta.fallback_reason = 'no_dated_str_rows';
    return { rawIssues: [], temporal_meta };
  }

  const weekOrdinalMap = new Map(sortedWeekKeys.map((k, i) => [k, i]));

  const candidates = [];

  for (const weekKey of sortedWeekKeys) {
    const rows = byWeek.get(weekKey) || [];
    if (rows.length < MIN_STR_DAYS_PER_WEEK) continue;

    const dates = rows.map((r) => getStrRowDate(r)).filter(Boolean);
    const { minYmd, maxYmd } = minMaxYmdFromDates(dates);
    const metrics = buildWindowMetricsFromRows(rows);
    const trend = buildTrendFromWindowRows(rows);
    const windowDiagnosis = { metrics, trend_status: trend };

    temporal_meta.weekly_windows.push({
      week_key: weekKey,
      day_count: rows.length,
      start_ymd: minYmd,
      end_ymd: maxYmd,
      avgMPI: metrics.avgMPI,
      avgARI: metrics.avgARI,
      avgRGI: metrics.avgRGI,
      avgOcc: metrics.avgOcc,
      trend_status: trend
    });

    const specs = detectRetailIssueSpecs(windowDiagnosis, focus, driver);
    for (const spec of specs) {
      candidates.push({
        family: spec.family,
        primary_driver: spec.primary_driver,
        weekKey,
        weekOrdinal: weekOrdinalMap.get(weekKey),
        metrics,
        trend,
        startYmd: minYmd,
        endYmd: maxYmd
      });
    }
  }

  if (!candidates.length) {
    temporal_meta.fallback_reason =
      temporal_meta.weekly_windows.length === 0 ? 'all_weeks_below_min_days' : 'no_weekly_specs';
    return { rawIssues: [], temporal_meta };
  }

  candidates.sort((a, b) => {
    if (a.weekOrdinal !== b.weekOrdinal) return a.weekOrdinal - b.weekOrdinal;
    const ra = a.family || '';
    const rb = b.family || '';
    return ra.localeCompare(rb);
  });

  const byFamily = new Map();
  for (const c of candidates) {
    if (!byFamily.has(c.family)) byFamily.set(c.family, []);
    byFamily.get(c.family).push(c);
  }

  let episodes = [];
  for (const famList of byFamily.values()) {
    famList.sort((a, b) => a.weekOrdinal - b.weekOrdinal);
    episodes = episodes.concat(mergeWeeklyCandidatesIntoEpisodes(famList));
  }

  episodes = rankAndCapEpisodes(episodes, MAX_RETAIL_ISSUES_PER_RUN);
  temporal_meta.episode_count = episodes.length;

  const rawIssues = episodes.map((ep) => materializeRetailEpisode(ep, focus));
  return { rawIssues, temporal_meta };
}

/**
 * Deterministic multi-issue detection (retail only). Returns ordered specs before enrichment.
 */
function detectRetailIssueSpecs(diagnosis, focus, driver) {
  if ((focus?.focus_segment || '') !== 'retail') return [];

  const avgMPI = Number(diagnosis?.metrics?.avgMPI ?? NaN);
  const avgARI = Number(diagnosis?.metrics?.avgARI ?? NaN);
  const avgRGI = Number(diagnosis?.metrics?.avgRGI ?? NaN);
  const avgOcc = Number(diagnosis?.metrics?.avgOcc ?? NaN);
  const trend = diagnosis?.trend_status || 'stable';
  const isMix = !!driver?.driver_context?.is_low_rated_mix_pressure;

  if (
    Number.isFinite(avgMPI) &&
    Number.isFinite(avgARI) &&
    Number.isFinite(avgRGI) &&
    avgMPI >= 100 &&
    avgARI > 100 &&
    avgRGI > 100
  ) {
    return [];
  }

  const raw = [];

  if (Number.isFinite(avgMPI) && Number.isFinite(avgARI) && avgMPI < 100 && avgARI > 100 && isMix) {
    raw.push({ family: 'mix_constraint', rank: 10, primary_driver: 'mix_strategy' });
  } else if (Number.isFinite(avgMPI) && Number.isFinite(avgARI) && avgMPI < 100 && avgARI > 100) {
    raw.push({ family: 'pricing_resistance', rank: 20, primary_driver: 'pricing' });
  }

  if (Number.isFinite(avgARI) && Number.isFinite(avgMPI) && avgARI < 100 && avgMPI <= 100) {
    raw.push({ family: 'discount_inefficiency', rank: 30, primary_driver: 'conversion' });
  }

  if (Number.isFinite(avgMPI) && ((avgMPI < 95 && trend === 'worsening') || avgMPI < 92)) {
    raw.push({ family: 'visibility_gap', rank: 40, primary_driver: 'visibility' });
  }

  if (
    Number.isFinite(avgOcc) &&
    Number.isFinite(avgARI) &&
    Number.isFinite(avgMPI) &&
    avgOcc >= 78 &&
    avgARI <= 100 &&
    avgMPI < 100 &&
    !(avgARI < 100 && avgMPI <= 100)
  ) {
    raw.push({ family: 'missed_pricing_opportunity', rank: 25, primary_driver: 'pricing' });
  }

  raw.sort((a, b) => a.rank - b.rank);

  let filtered = raw.filter((row) => row && row.family);
  if (filtered.some((r) => r.family === 'mix_constraint')) {
    filtered = filtered.filter((r) => r.family !== 'pricing_resistance');
  }
  if (filtered.some((r) => r.family === 'pricing_resistance' || r.family === 'mix_constraint')) {
    filtered = filtered.filter((r) => r.family !== 'missed_pricing_opportunity');
  }

  // Suppress visibility when rate-premium/share story already explains weak MPI (avoid two "share" cards).
  const hasPremiumShareStory = filtered.some(
    (r) => r.family === 'pricing_resistance' || r.family === 'mix_constraint'
  );
  if (hasPremiumShareStory) {
    filtered = filtered.filter((r) => r.family !== 'visibility_gap');
  }

  // Suppress visibility when conversion/discount narrative is active — both read as generic "weak MPI".
  if (filtered.some((r) => r.family === 'discount_inefficiency')) {
    filtered = filtered.filter((r) => r.family !== 'visibility_gap');
  }

  const seen = new Set();
  const deduped = [];
  for (const row of filtered) {
    if (seen.has(row.family)) continue;
    seen.add(row.family);
    deduped.push(row);
  }

  return deduped.slice(0, MAX_RETAIL_ISSUES_PER_RUN);
}

function materializeRetailIssue(spec, diagnosis, focus) {
  const { family, primary_driver } = spec;
  const lib = pickRetailLibraryActionsForFamily(family);
  const cappedLib = lib.slice(0, MAX_ACTIONS_PER_RETAIL_ISSUE);
  const pri = cappedLib.some((a) => a.priority === 'high') ? 'high' : 'medium';

  return {
    finding_key: retailIssueFindingKey(family),
    issue_family: family,
    driver: primary_driver,
    segment: 'retail',
    priority: pri,
    title: RETAIL_ISSUE_TITLES[family] || RETAIL_ISSUE_TITLES.share_loss_fallback,
    finding: retailIssueFindingText(family, diagnosis, focus),
    root_cause: RETAIL_ISSUE_ROOT_CAUSES[family] || RETAIL_ISSUE_ROOT_CAUSES.share_loss_fallback,
    expected_outcome:
      RETAIL_ISSUE_EXPECTED_OUTCOMES[family] || RETAIL_ISSUE_EXPECTED_OUTCOMES.share_loss_fallback,
    rule_triggered: family,
    _library_actions: cappedLib
  };
}

/** Single-issue fallback when retail is active but no multi-signal row fired (uses legacy driver + action picker). */
function buildRetailIssuesFromLegacyDriver(diagnosis, focus, driver) {
  const fd = driver?.primary_driver;
  if (!fd || fd === 'none') return [];

  const legacyActions = buildActionsFromDriver(driver, focus);
  if (!legacyActions.length) return [];

  const family = driver.rule_triggered || 'share_loss_fallback';
  const finding_key = retailIssueFindingKey(family);

  return [
    {
      finding_key,
      issue_family: family,
      driver: fd,
      segment: 'retail',
      priority: driver.confidence === 'high' ? 'high' : 'medium',
      title: RETAIL_ISSUE_TITLES[family] || driver.driver_reason?.slice(0, 80) || RETAIL_ISSUE_TITLES.share_loss_fallback,
      finding: summarizeDiagnosis(diagnosis, focus, driver),
      root_cause: driver.driver_reason || RETAIL_ISSUE_ROOT_CAUSES.share_loss_fallback,
      expected_outcome:
        RETAIL_ISSUE_EXPECTED_OUTCOMES[family] || RETAIL_ISSUE_EXPECTED_OUTCOMES.share_loss_fallback,
      rule_triggered: family,
      _library_actions: legacyActions.map((a) => ({
        action_id: a.action_id,
        driver: a.driver,
        segment: a.segment,
        title: a.title,
        description: a.description,
        priority: a.priority
      }))
    }
  ];
}

function issueProxyDriver(issue) {
  return {
    primary_driver: issue.driver,
    secondary_driver: null,
    driver_reason: issue.root_cause,
    rule_triggered: issue.rule_triggered,
    confidence: issue.priority === 'high' ? 'high' : 'medium',
    driver_context: {}
  };
}

function mapLibraryRowToActionShape(row) {
  return {
    action_id: row.action_id,
    finding_key: row.action_id,
    driver: row.driver,
    segment: row.segment,
    title: row.title,
    description: row.description,
    priority: row.priority
  };
}

function enrichRetailIssue(issue, ctx) {
  const { diagnosis, focus, detection, pmsRows, strRows } = ctx;
  const proxy = issueProxyDriver(issue);
  const actions = (issue._library_actions || []).map((row) => {
    const base = row.action_id ? mapLibraryRowToActionShape(row) : { ...row };
    return {
      ...base,
      financial_impact: buildFinancialImpact({
        driver: proxy,
        diagnosis,
        action: base,
        detection,
        pmsRows,
        strRows
      })
    };
  });

  const { _library_actions, ...rest } = issue;
  return { ...rest, actions };
}

function issueMaxImpactHigh(issue) {
  let max = 0;
  for (const a of issue.actions || []) {
    const h = a.financial_impact?.impact_range?.high;
    if (typeof h === 'number' && h > max) max = h;
  }
  return max || null;
}

/**
 * Flatten issues -> legacy actions[] shape: one entry per underlying action, all scoped to issue finding_key for joins.
 */
function flattenIssuesToLegacyActions(enrichedIssues) {
  const out = [];
  for (const issue of enrichedIssues) {
    for (const act of issue.actions || []) {
      out.push({
        ...act,
        finding_key: issue.finding_key,
        issue_finding_key: issue.finding_key,
        issue_family: issue.issue_family,
        issue_title: issue.title
      });
    }
  }
  return out;
}

function buildRecommendationsFromIssues({ hotelCode, periodMeta, focus, issues }) {
  return issues.map((issue) => ({
    hotel_name: hotelCode,
    title: issue.title,
    finding_key: issue.finding_key,
    department: OWNER_DEPARTMENT_BY_DRIVER[issue.driver] || 'Commercial',
    finding: issue.finding,
    impact_value: issueMaxImpactHigh(issue),
    impact_type: 'revenue_uplift',
    expected_impact_value: issueMaxImpactHigh(issue),
    status: 'open',
    period: periodMeta.period_label,
    root_cause: issue.root_cause,
    expected_outcome: issue.expected_outcome,
    owner_department: OWNER_DEPARTMENT_BY_DRIVER[issue.driver] || 'Commercial',
    priority: capitalizePriority(issue.priority),
    driver: issue.driver || null,
    segment: focus.focus_segment || null,
    snapshot_date: periodMeta.snapshot_date,
    period_type: periodMeta.period_type,
    period_start: periodMeta.period_start,
    period_end: periodMeta.period_end,
    period_key: periodMeta.period_key,
    period_label: periodMeta.period_label
  }));
}

function buildActionsFromIssues({ hotelCode, periodMeta, focus, issues }) {
  const rows = [];
  for (const issue of issues) {
    for (const act of issue.actions || []) {
      rows.push({
        hotel_name: hotelCode,
        period: periodMeta.period_label,
        snapshot_date: periodMeta.snapshot_date,
        period_type: periodMeta.period_type,
        period_start: periodMeta.period_start,
        period_end: periodMeta.period_end,
        period_key: periodMeta.period_key,
        period_label: periodMeta.period_label,
        title: issue.title,
        finding_key: issue.finding_key,
        action_text: act.description,
        priority: act.priority || null,
        driver: act.driver || null,
        segment: focus.focus_segment || null
      });
    }
  }
  return rows;
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

    let enrichedIssues = [];
    let enrichedActions;

    let retailTemporalMeta = null;

    if ((focus?.focus_segment || '') === 'retail') {
      const weekly = buildRetailIssuesFromWeeklyTemporal(strRows, focus, driver);
      retailTemporalMeta = weekly.temporal_meta;

      let rawIssues = weekly.rawIssues;
      if (!rawIssues.length) {
        const specs = detectRetailIssueSpecs(diagnosis, focus, driver);
        rawIssues = specs.map((spec) => materializeRetailIssue(spec, diagnosis, focus));
        if (!rawIssues.length) {
          rawIssues = buildRetailIssuesFromLegacyDriver(diagnosis, focus, driver);
        }
        if (retailTemporalMeta) {
          retailTemporalMeta.fallback_used = true;
          retailTemporalMeta.fallback_reason =
            retailTemporalMeta.fallback_reason || 'weekly_pipeline_empty';
        }
      } else if (retailTemporalMeta) {
        retailTemporalMeta.fallback_used = false;
      }

      const enrichCtx = { diagnosis, focus, detection, pmsRows, strRows };
      enrichedIssues = rawIssues.map((issue) => enrichRetailIssue(issue, enrichCtx));
      enrichedActions = flattenIssuesToLegacyActions(enrichedIssues);
    } else {
      const baseActions = buildActionsFromDriver(driver, focus);
      enrichedActions = baseActions.map((action) => ({
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
    }

    const totalOpportunity = buildTotalOpportunity(enrichedActions);
    const periodMeta = extractPeriodMetadata(strRows);

    const enginePayload = {
      success: true,
      detection,
      diagnosis,
      focus,
      driver,
      issues: (focus?.focus_segment || '') === 'retail' ? enrichedIssues : [],
      retail_temporal:
        (focus?.focus_segment || '') === 'retail' && retailTemporalMeta ? retailTemporalMeta : null,
      total_opportunity: totalOpportunity,
      // Back-compat: flattened per-action rows; each carries issue finding_key for joins.
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

    const recommendationsPayload =
      (focus?.focus_segment || '') === 'retail' && enrichedIssues.length > 0
        ? buildRecommendationsFromIssues({ hotelCode, periodMeta, focus, issues: enrichedIssues })
        : buildRecommendationsPayload({
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

    const actionsPayload =
      (focus?.focus_segment || '') === 'retail' && enrichedIssues.length > 0
        ? buildActionsFromIssues({ hotelCode, periodMeta, focus, issues: enrichedIssues })
        : buildActionsPayload({
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
