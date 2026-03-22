const { createClient } = require('@supabase/supabase-js');
const Anthropic = require('@anthropic-ai/sdk');
const XLSX = require('xlsx');
const axios = require('axios');

// --------------------
// INIT
// --------------------
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const anthropicApiKey = process.env.ANTHROPIC_API_KEY;

if (!supabaseUrl || !supabaseKey) {
  throw new Error('Missing Supabase environment variables');
}

const supabase = createClient(supabaseUrl, supabaseKey);
const anthropic = anthropicApiKey ? new Anthropic({ apiKey: anthropicApiKey }) : null;

// --------------------
// LIBRARY (v3.2 CORE)
// --------------------
const library = {
  Retail: {
    pricing_positioning: {
      department: 'Revenue',
      priority: 'high',
      root_causes: [
        'Price positioning above comp set without corresponding demand support or share performance',
        'Pricing approach not fully aligned with current demand elasticity and booking pace patterns',
        'Rate strategy prioritizing ADR stability over occupancy penetration and market share capture',
        'Yield management not fully optimized across peak and need periods, limiting compression benefits',
        'Pricing decisions not consistently aligned with demand cycles, reflecting gaps in demand anticipation'
      ],
      actions: [
        'Adjust pricing corridors dynamically across need and compression periods to improve share capture',
        'Recalibrate rate positioning against key competitors across booking windows and demand levels',
        'Optimize suite and room category pricing hierarchy to strengthen overall revenue contribution'
      ]
    },

    visibility_demand_capture: {
      department: 'Revenue & Marketing',
      priority: 'high',
      root_causes: [
        'Limited visibility across key distribution channels impacting overall demand capture',
        'Digital presence not fully optimized to generate qualified traffic and brand exposure',
        'Brand.com content and offer presentation not sufficiently compelling to drive direct demand'
      ],
      actions: [
        'Strengthen OTA positioning and visibility across high-impact booking windows',
        'Enhance digital marketing effectiveness through targeted campaigns and optimized budget allocation',
        'Improve Brand.com content, storytelling, and offer visibility to support direct channel performance'
      ]
    },

    conversion_channel_performance: {
      department: 'Revenue & Marketing',
      priority: 'high',
      root_causes: [
        'Conversion performance below potential across key distribution channels despite available demand',
        'Booking journey friction impacting user experience and limiting conversion efficiency'
      ],
      actions: [
        'Optimize booking engine and website journey to reduce drop-off and improve conversion rates',
        'Conduct structured channel performance reviews to identify and address conversion gaps'
      ]
    },

    commercial_strategy_mix: {
      department: 'Commercial',
      priority: 'high',
      root_causes: [
        'Current segment mix is not aligned with revenue optimization opportunities'
      ],
      actions: [
        'Rebalance segment mix toward higher contribution segments',
        'Refine commercial strategy to align with demand patterns and profitability drivers',
        'Conduct structured segment performance reviews to identify optimization opportunities'
      ]
    }
  }
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

function getRandomItem(array) {
  return array[Math.floor(Math.random() * array.length)];
}

function getMultipleRandomItems(array, count = 2) {
  const pool = [...array];
  const selected = [];

  while (pool.length && selected.length < count) {
    const index = Math.floor(Math.random() * pool.length);
    selected.push(pool.splice(index, 1)[0]);
  }

  return selected;
}

function findSheetByAliases(workbook, aliases) {
  const names = workbook.SheetNames || [];
  const normalizedMap = new Map(names.map(name => [normalizeKey(name), name]));

  for (const alias of aliases) {
    const exact = normalizedMap.get(normalizeKey(alias));
    if (exact) return exact;
  }

  for (const name of names) {
    const normalizedName = normalizeKey(name);
    if (aliases.some(alias => normalizedName.includes(normalizeKey(alias)))) {
      return name;
    }
  }

  return null;
}

function getSheetRows(workbook, aliases) {
  const sheetName = findSheetByAliases(workbook, aliases);
  if (!sheetName) {
    return [];
  }

  const sheet = workbook.Sheets[sheetName];

  const rowsDefault = XLSX.utils.sheet_to_json(sheet, { defval: null });
  const rowsOffset3 = XLSX.utils.sheet_to_json(sheet, { range: 3, defval: null });

  const hasKpiHeaders = (rows) => {
    if (!rows.length) return false;
    const keys = Object.keys(rows[0]).map(normalizeKey);
    return (
      keys.some(k => k.includes('rgi')) &&
      keys.some(k => k.includes('ari'))
    );
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

  const cleaned = value
    .toString()
    .replace(/,/g, '')
    .replace(/%/g, '')
    .trim();

  if (!cleaned) return null;

  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
}

function averageMetric(rows, candidateKeys) {
  const values = rows
    .map(row => toNumber(getRowValue(row, candidateKeys)))
    .filter(value => value !== null);

  if (!values.length) return null;

  return values.reduce((sum, value) => sum + value, 0) / values.length;
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

function extractPeriodLabel(strRows) {
  const candidateDates = strRows
    .map(row => getRowValue(row, ['Date', 'Business Date', 'Stay Date', 'Day', 'Report Date']))
    .map(parseExcelDate)
    .filter(Boolean)
    .sort((a, b) => a.getTime() - b.getTime());

  if (!candidateDates.length) {
    return new Date().toISOString().slice(0, 7);
  }

  const formatter = new Intl.DateTimeFormat('en-GB', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    timeZone: 'UTC'
  });

  return `${formatter.format(candidateDates[0])} → ${formatter.format(candidateDates[candidateDates.length - 1])}`;
}

function deriveDriverCategory(avgRGI, avgARI) {
  if (avgRGI === null || avgARI === null) {
    return 'pricing_positioning';
  }

  if (avgRGI < 100 && avgARI > 100) {
    return 'pricing_positioning';
  }

  if (avgRGI < 100 && avgARI < 100) {
    return 'visibility_demand_capture';
  }

  if (avgRGI >= 100 && avgARI < 100) {
    return 'conversion_channel_performance';
  }

  return 'commercial_strategy_mix';
}

function buildDiagnosisText({ avgMPI, avgARI, avgRGI, driverCategory, segmentFocus }) {
  const mpiText = avgMPI !== null ? `MPI is ${avgMPI.toFixed(1)}` : null;
  const ariText = avgARI !== null ? `ARI is ${avgARI.toFixed(1)}` : null;
  const rgiText = avgRGI !== null ? `RGI is ${avgRGI.toFixed(1)}` : null;

  const kpis = [mpiText, ariText, rgiText].filter(Boolean).join(', ');

  if (driverCategory === 'pricing_positioning') {
    return `${segmentFocus} is priced above market, with ARI at ${avgARI?.toFixed(1)}, while under-indexing occupancy, with MPI at ${avgMPI?.toFixed(1)}, resulting in RevPAR underperformance, with RGI at ${avgRGI?.toFixed(1)}. Current pricing strategy is not converting rate premium into market share.`;
  }

  if (driverCategory === 'visibility_demand_capture') {
    return `${segmentFocus} performance indicates a demand capture issue. ${kpis}. Market visibility and channel demand conversion appear below potential.`;
  }

  if (driverCategory === 'conversion_channel_performance') {
    return `${segmentFocus} performance indicates a conversion issue. ${kpis}. Available demand is not being converted efficiently into revenue share.`;
  }

  if (driverCategory === 'commercial_strategy_mix') {
    return `${segmentFocus} performance indicates a commercial mix optimization opportunity. ${kpis}. Current segment approach may not be maximizing revenue contribution.`;
  }

  return `${segmentFocus} performance shows a commercial opportunity. ${kpis}.`;
}

function buildDynamicTitle({ driverCategory, segmentFocus, avgMPI, avgARI, avgRGI }) {
  if (driverCategory === 'pricing_positioning') {
    return `${segmentFocus} Pricing Positioning — Occupancy Share Leakage in Soft Market`;
  }

  if (driverCategory === 'visibility_demand_capture') {
    return `${segmentFocus} Demand Capture Gap — Visibility and Traffic Underperformance`;
  }

  if (driverCategory === 'conversion_channel_performance') {
    return `${segmentFocus} Conversion Efficiency Issue — Demand Not Translating into Revenue`;
  }

  if (driverCategory === 'commercial_strategy_mix') {
    return `${segmentFocus} Commercial Mix Opportunity — Segment Strategy Misalignment`;
  }

  return `${segmentFocus} Revenue Opportunity Identified`;
}

function buildExpectedOutcome({ driverCategory, segmentFocus }) {
  if (driverCategory === 'pricing_positioning') {
    return `Improve ${segmentFocus.toLowerCase()} occupancy penetration, strengthen RevPAR index performance, and recover share without unnecessary ADR dilution.`;
  }

  if (driverCategory === 'visibility_demand_capture') {
    return `Increase qualified demand capture across key channels and improve ${segmentFocus.toLowerCase()} revenue contribution.`;
  }

  if (driverCategory === 'conversion_channel_performance') {
    return `Improve conversion efficiency, reduce demand leakage, and strengthen revenue capture from existing traffic.`;
  }

  if (driverCategory === 'commercial_strategy_mix') {
    return `Improve revenue quality through better segment balance and stronger alignment with profitable demand opportunities.`;
  }

  return `Strengthen ${segmentFocus.toLowerCase()} commercial performance.`;
}

function deriveMixSignal(pmsRows) {
  if (!pmsRows.length) return 'PMS segment mix signal not available';

  const retailRows = pmsRows.filter(row => {
    const segment = normalizeKey(
      getRowValue(row, ['Segment', 'Market Segment', 'Business Segment', 'Segment Name'])
    );
    return segment.includes('retail') || segment.includes('transient');
  });

  if (!retailRows.length) return 'Retail segment mix not isolated in PMS sheet';

  const totalRevenue = retailRows.reduce((sum, row) => {
    return sum + (toNumber(getRowValue(row, ['Revenue', 'Room Revenue', 'Rooms Revenue'])) || 0);
  }, 0);

  const yoy = averageMetric(retailRows, ['YoY', 'YOY', 'Revenue YoY', 'Revenue % Change']);

  const parts = [];
  if (totalRevenue > 0) {
    parts.push(`Retail revenue observed in PMS extract: ${Math.round(totalRevenue)}`);
  }
  if (yoy !== null) {
    parts.push(`Retail YoY signal: ${yoy.toFixed(1)}%`);
  }

  return parts.length ? parts.join(' | ') : 'Retail segment mix signal available but limited';
}

function deriveTargetSignal(profileRows) {
  if (!profileRows.length) return 'Hotel profile targets not available';

  const firstRow = profileRows[0];
  const occupancyTarget = toNumber(getRowValue(firstRow, ['Target Occupancy', 'Occupancy Target']));
  const adrTarget = toNumber(getRowValue(firstRow, ['Target ADR', 'ADR Target']));
  const revparTarget = toNumber(getRowValue(firstRow, ['Target RevPAR', 'RevPAR Target']));

  const parts = [];
  if (occupancyTarget !== null) parts.push(`Occupancy target ${occupancyTarget}`);
  if (adrTarget !== null) parts.push(`ADR target ${adrTarget}`);
  if (revparTarget !== null) parts.push(`RevPAR target ${revparTarget}`);

  return parts.length ? parts.join(' | ') : 'Hotel profile targets present but not readable';
}

function parseClaudeJson(rawText) {
  if (!rawText) return null;

  try {
    return JSON.parse(rawText);
  } catch {
    const start = rawText.indexOf('{');
    const end = rawText.lastIndexOf('}');
    if (start >= 0 && end > start) {
      try {
        return JSON.parse(rawText.slice(start, end + 1));
      } catch {
        return null;
      }
    }
  }

  return null;
}

async function composeExecutiveNarrative(input) {
  if (!anthropic) {
    return null;
  }

  try {
    const response = await anthropic.messages.create({
      model: 'claude-3-sonnet-20240229',
      max_tokens: 500,
      messages: [
        {
          role: 'user',
          content: `
You are a senior hotel commercial strategist.

Rewrite the recommendation below into precise executive language.
Do not invent new facts.
Use the provided KPI signals when relevant.

Return valid JSON only with this schema:
{
  "title": "string",
  "rootCause": "string",
  "actions": ["string", "string"],
  "expectedOutcome": "string"
}

Input:
Driver: ${input.driver}
Segment: ${input.segment}
Department: ${input.department}
Priority: ${input.priority}
Average RGI: ${input.avgRGI ?? 'n/a'}
Average ARI: ${input.avgARI ?? 'n/a'}
Mix Signal: ${input.mixSignal}
Target Signal: ${input.targetSignal}
Context: ${input.context || 'None'}

Root Cause:
${input.rootCauseText}

Actions:
${input.actions.join('\n')}
`
        }
      ]
    });

    const rawText = response.content?.[0]?.text || '';
    return parseClaudeJson(rawText);
  } catch (error) {
    console.error('Claude error:', error);
    return null;
  }
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

// --------------------
// MAIN HANDLER
// --------------------
async function handler(req, res) {
  try {
    const hotelCode = (req.body?.hotelCode || 'Unknown Hotel').toString().trim();
    const context = (req.body?.context || '').toString().trim();

    const workbook = await getWorkbookFromRequest(req);

    const strRows = getSheetRows(workbook, ['STR Daily Report', 'STR', 'Daily STR']);
    const pmsRows = getSheetRows(workbook, ['PMS Market Segment Report', 'PMS', 'Market Segment']);
    const profileRows = getSheetRows(workbook, ['Hotel Profile', 'Profile']);

    if (!strRows.length) {
      return res.status(400).json({ error: 'STR sheet not found or empty' });
    }

    const avgRGI = averageMetric(strRows, ['RGI', 'RGI (Index)', 'RevPAR Index', 'RevPAR Index (RGI)']);
    const avgARI = averageMetric(strRows, ['ARI', 'ARI (Index)', 'ADR Index', 'ADR Index (ARI)']);
    const avgMPI = averageMetric(strRows, ['MPI', 'MPI (Index)', 'Occupancy Index', 'Occ Index']);

    if (avgRGI === null || avgARI === null) {
      return res.status(400).json({ error: 'Required STR KPI columns (RGI/ARI) were not found' });
    }

    const period = extractPeriodLabel(strRows);
    const driverCategory = deriveDriverCategory(avgRGI, avgARI);
    const segmentFocus = 'Retail';
    const block = library[segmentFocus]?.[driverCategory];

    if (!block) {
      throw new Error(`Library block not found for ${segmentFocus}/${driverCategory}`);
    }

    const rootCauseText =
      driverCategory === 'commercial_strategy_mix'
        ? block.root_causes[0]
        : getRandomItem(block.root_causes);

    const actions = getMultipleRandomItems(block.actions, Math.min(2, block.actions.length));

    const mixSignal = deriveMixSignal(pmsRows);
    const targetSignal = deriveTargetSignal(profileRows);

    console.log('v3.2 driver:', driverCategory);
    console.log('v3.2 root cause seed:', rootCauseText);
    console.log('v3.2 selected actions:', actions);

    const aiNarrative = await composeExecutiveNarrative({
      rootCauseText,
      actions,
      driver: driverCategory,
      segment: segmentFocus,
      department: block.department,
      priority: block.priority,
      avgRGI: Number(avgRGI.toFixed(2)),
      avgARI: Number(avgARI.toFixed(2)),
      mixSignal,
      targetSignal,
      context
    });

const diagnosisText = buildDiagnosisText({
  avgMPI,
  avgARI,
  avgRGI,
  driverCategory,
  segmentFocus
});

const title =
  aiNarrative?.title ||
  buildDynamicTitle({
    driverCategory,
    segmentFocus,
    avgMPI,
    avgARI,
    avgRGI
  });

const finalRootCause = aiNarrative?.rootCause || rootCauseText;

const finalActions =
  Array.isArray(aiNarrative?.actions) && aiNarrative.actions.length
    ? aiNarrative.actions.slice(0, 3)
    : actions;

const expectedOutcome =
  aiNarrative?.expectedOutcome ||
  buildExpectedOutcome({
    driverCategory,
    segmentFocus
  });

const recommendationPayload = {
  hotel_name: hotelCode,
  period,
  title,
  finding: diagnosisText,
  root_cause: finalRootCause,
  expected_outcome: expectedOutcome,
  owner_department: block.department
};

    const { error: recommendationError } = await supabase
      .from('Recommendations')
      .insert([recommendationPayload]);

    if (recommendationError) {
      throw recommendationError;
    }

const actionsPayload = finalActions.map(actionText => ({
  hotel_name: hotelCode,
  period,
  title,
  action_text: actionText
}));

    const { error: actionsError } = await supabase
      .from('actions')
      .insert(actionsPayload);

    if (actionsError) {
      throw actionsError;
    }

    return res.status(200).json({
      message: 'v3.2 completed',
      hotelCode,
      period,
      driver: driverCategory
    });
  } catch (error) {
    console.error('Analyze handler error:', error);
    return res.status(500).json({
      error: error.message || 'Processing failed'
    });
  }
}

module.exports = handler;
