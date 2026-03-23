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
// LIBRARY
// --------------------
const library = {
  Retail: {
    pricing_positioning: {
      department: 'Revenue',
      priority: 'high',
      root_causes: [
        'Rate positioning remains above market despite insufficient demand support, limiting occupancy recovery and market share capture.',
        'Pricing strategy is maintaining ADR protection in periods where stronger occupancy penetration is required.',
        'Rate corridors appear too rigid against market conditions, reducing share capture in softer demand periods.'
      ],
      actions: [
        'Adjust BAR positioning in short booking windows (0–7 days) to align more closely with market conditions and stimulate occupancy recovery.',
        'Recalibrate pricing corridors by day type and demand pattern to improve competitiveness without unnecessary ADR dilution.',
        'Review room category price hierarchy to strengthen upsell logic while protecting base-category conversion.'
      ]
    },

    visibility_demand_capture: {
      department: 'Revenue & Marketing',
      priority: 'high',
      root_causes: [
        'Market visibility and demand penetration are below potential, indicating insufficient exposure relative to competitors.',
        'The property is not capturing enough qualified demand despite available market opportunity.',
        'Distribution and digital visibility appear insufficient to convert available market demand into occupancy share.'
      ],
      actions: [
        'Strengthen OTA and metasearch visibility in high-impact booking windows to improve demand capture.',
        'Refine digital campaign targeting and offer presentation to improve qualified traffic acquisition.',
        'Improve Brand.com merchandising, package visibility, and call-to-action clarity to support direct demand generation.'
      ]
    },

    conversion_channel_performance: {
      department: 'Revenue & Marketing',
      priority: 'high',
      root_causes: [
        'Available demand is not being efficiently converted into revenue share, indicating channel or booking-path inefficiencies.',
        'Traffic and demand appear present, but booking conversion is underperforming relative to market opportunity.',
        'The booking journey is likely creating friction that limits conversion efficiency and revenue capture.'
      ],
      actions: [
        'Audit booking engine conversion performance and remove friction points across the reservation path.',
        'Review channel contribution and conversion by source to identify leakage in high-demand periods.',
        'Strengthen offer clarity, urgency triggers, and parity discipline to improve conversion from existing traffic.'
      ]
    },

    commercial_strategy_mix: {
      department: 'Commercial',
      priority: 'high',
      root_causes: [
        'The current commercial mix is not fully aligned with the highest-value demand opportunities.',
        'Segment strategy appears misaligned with market conditions, limiting optimal revenue contribution.',
        'Commercial focus is not sufficiently concentrated on the segments and tactics with the strongest share potential.'
      ],
      actions: [
        'Rebalance segment focus toward demand pockets with stronger contribution and conversion potential.',
        'Review commercial strategy by segment and booking window to align effort with market opportunity.',
        'Conduct a structured segment performance review to refine targeting, pricing, and channel priorities.'
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

function getMetricFromRow(row, possibleKeys) {
  for (const key of possibleKeys) {
    if (row[key] !== undefined && row[key] !== null && row[key] !== '') {
      const value = parseFloat(row[key]);
      if (!Number.isNaN(value)) return value;
    }
  }
  return null;
}

function getRandomItem(array) {
  if (!Array.isArray(array) || !array.length) return null;
  return array[Math.floor(Math.random() * array.length)];
}

function getMultipleRandomItems(array, count = 2) {
  if (!Array.isArray(array) || !array.length) return [];
  const pool = [...array];
  const selected = [];

  while (pool.length && selected.length < count) {
    const index = Math.floor(Math.random() * pool.length);
    selected.push(pool.splice(index, 1)[0]);
  }

  return selected;
}

function computePriority(rgi, mpi, ari) {
  if (
    (rgi !== null && rgi < 90) ||
    (mpi !== null && mpi < 90) ||
    (ari !== null && ari > 120)
  ) return 'High';

  if (
    (rgi !== null && rgi < 100) ||
    (mpi !== null && mpi < 100) ||
    (ari !== null && ari > 105)
  ) return 'Medium';

  return 'Low';
}

function deriveDriverCategory(mpi, ari, rgi) {
  if (rgi === null || ari === null) {
    return 'pricing_positioning';
  }

  if (rgi < 100 && ari > 100) {
    return 'pricing_positioning';
  }

  if (rgi < 100 && ari < 100) {
    return 'visibility_demand_capture';
  }

  if (rgi >= 100 && ari < 100) {
    return 'conversion_channel_performance';
  }

  return 'commercial_strategy_mix';
}

function mapDriverToOwner(driverCategory, segmentFocus = 'Retail') {
  return library[segmentFocus]?.[driverCategory]?.department || 'Commercial';
}

function buildRootCauseText({ driverCategory, segmentFocus = 'Retail' }) {
  const block = library[segmentFocus]?.[driverCategory] || library.Retail?.[driverCategory];
  if (!block) {
    return 'A commercial performance gap has been identified and requires structured review.';
  }
  return getRandomItem(block.root_causes);
}

function buildExpectedOutcomeText({ driverCategory, segmentFocus }) {
  if (driverCategory === 'pricing_positioning') {
    return `Improve ${segmentFocus.toLowerCase()} occupancy penetration, strengthen RevPAR index performance, and recover market share with more responsive pricing.`;
  }

  if (driverCategory === 'visibility_demand_capture') {
    return `Increase qualified demand capture across key channels and improve ${segmentFocus.toLowerCase()} revenue contribution.`;
  }

  if (driverCategory === 'conversion_channel_performance') {
    return 'Improve conversion efficiency, reduce demand leakage, and strengthen revenue capture from existing traffic.';
  }

  if (driverCategory === 'commercial_strategy_mix') {
    return 'Improve revenue quality through stronger segment balance and better alignment with profitable demand opportunities.';
  }

  return `Strengthen ${segmentFocus.toLowerCase()} commercial performance.`;
}

function buildDiagnosisText({ mpi, ari, rgi, driverCategory, segmentFocus }) {
  const mpiText = mpi !== null ? `MPI is ${mpi.toFixed(1)}` : null;
  const ariText = ari !== null ? `ARI is ${ari.toFixed(1)}` : null;
  const rgiText = rgi !== null ? `RGI is ${rgi.toFixed(1)}` : null;
  const kpis = [mpiText, ariText, rgiText].filter(Boolean).join(', ');

  if (driverCategory === 'pricing_positioning') {
    return `${segmentFocus} is holding a price premium versus the market (${ariText}), but this is not converting into sufficient share performance (${rgiText}), indicating a pricing-positioning imbalance.`;
  }

  if (driverCategory === 'visibility_demand_capture') {
    return `${segmentFocus} is underpenetrating available demand. ${kpis}. Market visibility and share capture appear below potential relative to competitors.`;
  }

  if (driverCategory === 'conversion_channel_performance') {
    return `${segmentFocus} shows a conversion efficiency gap. ${kpis}. Available demand is not being translated into proportional revenue share.`;
  }

  if (driverCategory === 'commercial_strategy_mix') {
    return `${segmentFocus} shows a commercial mix optimization opportunity. ${kpis}. Current strategy is not maximizing revenue contribution or market share potential.`;
  }

  return `${segmentFocus} performance shows a commercial opportunity. ${kpis}.`;
}

function buildDynamicTitle({ driverCategory, segmentFocus }) {
  if (driverCategory === 'pricing_positioning') {
    return `${segmentFocus} Pricing Inefficiency — Share Loss vs Market`;
  }

  if (driverCategory === 'visibility_demand_capture') {
    return `${segmentFocus} Demand Capture Gap — Visibility Underperformance`;
  }

  if (driverCategory === 'conversion_channel_performance') {
    return `${segmentFocus} Conversion Efficiency Issue — Demand Not Translating into Revenue`;
  }

  if (driverCategory === 'commercial_strategy_mix') {
    return `${segmentFocus} Commercial Mix Opportunity — Strategy Misalignment`;
  }

  return `${segmentFocus} Revenue Opportunity Identified`;
}

function buildActionTexts({ driverCategory, segmentFocus = 'Retail' }) {
  const block = library[segmentFocus]?.[driverCategory] || library.Retail?.[driverCategory];
  if (!block) return ['Conduct a structured review of the commercial performance gap and define corrective actions.'];
  return getMultipleRandomItems(block.actions, Math.min(2, block.actions.length));
}

function buildRecommendationFromOpportunity(opportunity, hotelName, period) {
  const driverCategory = opportunity.driver;
  const segmentFocus = opportunity.segment || 'Retail';
const priority = 'High';

  return {
    hotel_name: hotelName,
    period,
    title: buildDynamicTitle({
      driverCategory,
      segmentFocus,
      mpi: opportunity.mpi ?? null,
      ari: opportunity.ari ?? null,
      rgi: opportunity.rgi ?? null
    }),
    finding: buildDiagnosisText({
      mpi: opportunity.mpi ?? null,
      ari: opportunity.ari ?? null,
      rgi: opportunity.rgi ?? null,
      driverCategory,
      segmentFocus
    }),
    root_cause: buildRootCauseText({
      driverCategory,
      segmentFocus,
      mpi: opportunity.mpi ?? null,
      ari: opportunity.ari ?? null,
      rgi: opportunity.rgi ?? null
    }),
    expected_outcome: buildExpectedOutcomeText({
      driverCategory,
      segmentFocus,
      mpi: opportunity.mpi ?? null,
      ari: opportunity.ari ?? null,
      rgi: opportunity.rgi ?? null
    }),
    owner_department: mapDriverToOwner(driverCategory, segmentFocus),
    priority,
    driver: driverCategory,
    segment: segmentFocus,
    mpi: opportunity.mpi ?? null,
    ari: opportunity.ari ?? null,
    rgi: opportunity.rgi ?? null,
    actions: buildActionTexts({
      driverCategory,
      segmentFocus,
      mpi: opportunity.mpi ?? null,
      ari: opportunity.ari ?? null,
      rgi: opportunity.rgi ?? null
    })
  };
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
    return keys.some(k => k.includes('rgi')) && keys.some(k => k.includes('ari'));
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
RGI: ${input.rgi ?? 'n/a'}
ARI: ${input.ari ?? 'n/a'}
MPI: ${input.mpi ?? 'n/a'}
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

    const rowKpis = strRows.map((row, index) => {
      const rgi = getMetricFromRow(row, ['RGI', 'RGI (Index)', 'RevPAR Index', 'RevPAR Index (RGI)']);
      const ari = getMetricFromRow(row, ['ARI', 'ARI (Index)', 'ADR Index', 'ADR Index (ARI)']);
      const mpi = getMetricFromRow(row, ['MPI', 'MPI (Index)', 'Occupancy Index', 'Occ Index']);

      return {
        rowIndex: index,
        row,
        rgi,
        ari,
        mpi
      };
    });

    const validRowKpis = rowKpis.filter(item => item.rgi !== null && item.ari !== null);

    if (validRowKpis.length === 0) {
      return res.status(400).json({ error: 'Required STR KPI columns (RGI/ARI) were not found' });
    }

    const opportunities = validRowKpis
      .filter(item => item.rgi < 100 || item.mpi < 100 || item.ari > 100)
      .map(item => {
        const driver = deriveDriverCategory(item.mpi, item.ari, item.rgi);

        return {
          rowIndex: item.rowIndex,
          segment: item.row?.segment || item.row?.Segment || item.row?.SEGMENT || 'Retail',
          mpi: item.mpi,
          ari: item.ari,
          rgi: item.rgi,
          driver,
          row: item.row
        };
      });

    const period = extractPeriodLabel(strRows);

let recommendations = opportunities
  .map(opportunity => buildRecommendationFromOpportunity(opportunity, hotelCode, period));

/* 🔥 REMOVE DUPLICATES HERE */
const uniqueRecommendationsMap = new Map();

recommendations.forEach(rec => {
  const key = `${rec.driver}_${rec.segment}`;

  if (!uniqueRecommendationsMap.has(key)) {
    uniqueRecommendationsMap.set(key, rec);
  }
});

recommendations = Array.from(uniqueRecommendationsMap.values());

/* 🔥 LIMIT TO TOP 5 */
recommendations = recommendations.slice(0, 5);

/* 🔥 SORT BY PRIORITY */
const priorityOrder = { High: 1, Medium: 2, Low: 3 };

recommendations.sort((a, b) => {
  return (priorityOrder[a.priority] || 99) - (priorityOrder[b.priority] || 99);
});

    if (recommendations.length === 0) {
      recommendations = [
        {
          hotel_name: hotelCode,
          period,
          title: 'Performance Aligned with Market Benchmarks',
          finding: 'Current performance is broadly aligned with market benchmarks, with no material KPI underperformance detected in the uploaded dataset.',
          root_cause: 'No major pricing, visibility, or conversion gap was identified in the current reporting view.',
          expected_outcome: 'Maintain current commercial discipline while monitoring for emerging shifts in market share, pricing power, and conversion performance.',
          owner_department: 'Commercial',
          priority: 'Low',
          driver: 'commercial_strategy_mix',
          segment: 'All Segments',
          mpi: null,
          ari: null,
          rgi: null,
          actions: ['Maintain current commercial discipline and continue monitoring market shifts.']
        }
      ];
    }

    const mixSignal = deriveMixSignal(pmsRows);
    const targetSignal = deriveTargetSignal(profileRows);

    const finalRecommendations = [];

    for (const recommendation of recommendations) {
      const aiNarrative = await composeExecutiveNarrative({
        rootCauseText: recommendation.root_cause,
        actions: recommendation.actions,
        driver: recommendation.driver,
        segment: recommendation.segment,
        department: recommendation.owner_department,
        priority: recommendation.priority,
        rgi: recommendation.rgi !== null && recommendation.rgi !== undefined
          ? Number(recommendation.rgi.toFixed(2))
          : null,
        ari: recommendation.ari !== null && recommendation.ari !== undefined
          ? Number(recommendation.ari.toFixed(2))
          : null,
        mpi: recommendation.mpi !== null && recommendation.mpi !== undefined
          ? Number(recommendation.mpi.toFixed(2))
          : null,
        mixSignal,
        targetSignal,
        context
      });

      finalRecommendations.push({
        ...recommendation,
        title: aiNarrative?.title || recommendation.title,
        root_cause: aiNarrative?.rootCause || recommendation.root_cause,
        expected_outcome: aiNarrative?.expectedOutcome || recommendation.expected_outcome,
        actions:
          Array.isArray(aiNarrative?.actions) && aiNarrative.actions.length
            ? aiNarrative.actions.slice(0, 3)
            : recommendation.actions
      });
    }

    const recommendationsPayload = finalRecommendations.map(item => ({
      hotel_name: item.hotel_name,
      period: item.period,
      title: item.title,
      finding: item.finding,
      root_cause: item.root_cause,
      expected_outcome: item.expected_outcome,
      owner_department: item.owner_department
    }));

    const { error: recommendationError } = await supabase
      .from('Recommendations')
      .insert(recommendationsPayload);

    if (recommendationError) {
      throw recommendationError;
    }

    const actionsPayload = finalRecommendations.flatMap(item =>
      (item.actions || []).map(actionText => ({
        hotel_name: item.hotel_name,
        period: item.period,
        title: item.title,
        action_text: actionText
      }))
    );

    if (actionsPayload.length > 0) {
      const { error: actionsError } = await supabase
        .from('actions')
        .insert(actionsPayload);

      if (actionsError) {
        throw actionsError;
      }
    }

    return res.status(200).json({
      message: 'v3.3 completed',
      hotelCode,
      period,
      recommendations: finalRecommendations.map(item => ({
        hotel_name: item.hotel_name,
        period: item.period,
        title: item.title,
        finding: item.finding,
        root_cause: item.root_cause,
        expected_outcome: item.expected_outcome,
        owner_department: item.owner_department,
        priority: item.priority,
        driver: item.driver,
        segment: item.segment,
        mpi: item.mpi,
        ari: item.ari,
        rgi: item.rgi,
        actions: item.actions
      }))
    });
  } catch (error) {
    console.error('Analyze handler error:', error);
    return res.status(500).json({
      error: error.message || 'Processing failed'
    });
  }
}

module.exports = handler;
