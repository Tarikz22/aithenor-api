const XLSX = require('xlsx');

let anthropic = null;
try {
  const Anthropic = require('@anthropic-ai/sdk');
  anthropic = new Anthropic({
    apiKey: process.env.ANTHROPIC_API_KEY
  });
} catch (err) {
  console.warn('Anthropic SDK not available or failed to initialize:', err.message);
}

function parsePercent(value) {
  if (value === null || value === undefined || value === '') return 0;
  const cleaned = value.toString().replace('%', '').replace(',', '.').trim();
  const num = parseFloat(cleaned);
  return Number.isNaN(num) ? 0 : num;
}

function parseNumber(value) {
  if (value === null || value === undefined || value === '') return 0;
  const cleaned = value.toString().replace(/,/g, '').trim();
  const num = parseFloat(cleaned);
  return Number.isNaN(num) ? 0 : num;
}

function parseDateValue(value) {
  if (!value) return null;
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value;

  const text = value.toString().trim();
  if (!text.includes('/')) return null;

  const parts = text.split('/');
  if (parts.length !== 3) return null;

  const [day, month, year] = parts;
  const dt = new Date(`${year}-${month}-${day}`);
  return Number.isNaN(dt.getTime()) ? null : dt;
}

function formatDate(d) {
  return `${String(d.getDate()).padStart(2, '0')}/${String(d.getMonth() + 1).padStart(2, '0')}/${d.getFullYear()}`;
}

function stripCodeFences(text) {
  if (!text) return text;
  return text
    .replace(/^```json\s*/i, '')
    .replace(/^```\s*/i, '')
    .replace(/\s*```$/i, '')
    .trim();
}

function aggregateCodes(segmentMap, codes) {
  return codes.reduce((acc, code) => {
    const item = segmentMap[code];
    if (!item) return acc;

    acc.roomNightsTY += item.roomNightsTY;
    acc.roomNightsLY += item.roomNightsLY;
    acc.revenueTY += item.revenueTY;
    acc.revenueLY += item.revenueLY;
    return acc;
  }, { roomNightsTY: 0, roomNightsLY: 0, revenueTY: 0, revenueLY: 0 });
}

async function composeExecutiveNarrative(payload) {
  if (!anthropic || !process.env.ANTHROPIC_API_KEY) return null;

  const prompt = `
You are writing for a hotel GM / Commercial Director.

Use the structured signals below to produce:
1. title
2. root_cause
3. expected_outcome
4. three short executive actions

Rules:
- Return valid JSON only.
- Do not invent data.
- Do not contradict the structured logic.
- Use KPI evidence naturally.
- Keep a professional, non-accusatory tone.
- Make the wording more executive and less repetitive than the input seeds.
- Keep each action concise and practical.
- If evidence is directional rather than conclusive, say "suggests", "indicates", or "appears".

STRUCTURED SIGNALS
- Market condition: ${payload.marketCondition}
- Hotel position: ${payload.performancePosition}
- Segment focus: ${payload.segmentFocus}
- Driver category: ${payload.driverCategory}
- Driver confidence: ${payload.driverConfidence}

KPI EVIDENCE
- MPI: ${payload.avgMPI}
- ARI: ${payload.avgARI}
- RGI: ${payload.avgRGI}
- Hotel Occupancy: ${payload.avgHotelOcc}
- Hotel ADR: ${payload.avgHotelADR}
- Comp Set Occupancy: ${payload.avgCompOcc}

BUSINESS MIX SIGNAL
${payload.mixSignal}

TARGET SIGNAL
${payload.targetSignal}

LIBRARY ROOT CAUSE
${payload.rootCauseSeed}

LIBRARY EXPECTED OUTCOME
${payload.expectedOutcomeSeed}

LIBRARY ACTIONS
1. ${payload.actions[0] || ''}
2. ${payload.actions[1] || ''}
3. ${payload.actions[2] || ''}

Return this JSON shape exactly:
{
  "title": "...",
  "root_cause": "...",
  "expected_outcome": "...",
  "actions": ["...", "...", "..."]
}
`;

  const response = await anthropic.messages.create({
    model: 'claude-3-haiku-20240307',
    max_tokens: 500,
    messages: [{ role: 'user', content: prompt }]
  });

  const text = stripCodeFences(response.content[0].text);
  return JSON.parse(text);
}

module.exports = async function analyzeHandler(req, res) {
  try {
    const { fileUrl, hotelCode } = req.body;

    const response = await fetch(fileUrl);
    const arrayBuffer = await response.arrayBuffer();
    const workbook = XLSX.read(Buffer.from(arrayBuffer), { type: 'buffer' });

    const strSheet = workbook.Sheets['STR Daily Report'] || workbook.Sheets[workbook.SheetNames[0]];
    const pmsSheet = workbook.Sheets['PMS Market Segment Report'] || null;
    const profileSheet = workbook.Sheets['Hotel Profile'] || null;

    if (!strSheet) {
      throw new Error('Missing sheet: STR Daily Report');
    }

    const strData = XLSX.utils.sheet_to_json(strSheet, { range: 3, defval: null });
    const pmsData = pmsSheet ? XLSX.utils.sheet_to_json(pmsSheet, { range: 3, defval: null }) : [];
    const profileRows = profileSheet ? XLSX.utils.sheet_to_json(profileSheet, { header: 1, defval: null }) : [];

    // ===== PERIOD EXTRACTION =====
    const dates = strData
      .map(row => parseDateValue(row['Date']))
      .filter(Boolean);

    let period = 'unknown';

    if (dates.length > 0) {
      const minDate = new Date(Math.min(...dates));
      const maxDate = new Date(Math.max(...dates));
      period = minDate.getTime() === maxDate.getTime()
        ? formatDate(minDate)
        : `${formatDate(minDate)} → ${formatDate(maxDate)}`;
    }

    let totalMPI = 0;
    let totalARI = 0;
    let totalRGI = 0;
    let totalCompOcc = 0;
    let totalHotelOcc = 0;
    let totalHotelADR = 0;
    let count = 0;

    strData.forEach((row) => {
      const mpi = parseNumber(row['MPI (Index)']);
      const ari = parseNumber(row['ARI (Index)']);
      const rgi = parseNumber(row['RGI (Index)']);
      const compOcc = parsePercent(row['Comp Set Occupancy %']);
      const hotelOcc = parsePercent(row['Hotel Occupancy %']);
      const hotelADR = parseNumber(row['Hotel ADR']);

      if (!Number.isNaN(mpi) && !Number.isNaN(ari) && !Number.isNaN(rgi) && !Number.isNaN(compOcc)) {
        totalMPI += mpi;
        totalARI += ari;
        totalRGI += rgi;
        totalCompOcc += compOcc;
        totalHotelOcc += hotelOcc;
        totalHotelADR += hotelADR;
        count++;
      }
    });

    if (count === 0) {
      return res.status(200).json({
        success: false,
        message: 'No valid STR rows found'
      });
    }

    const avgMPI = totalMPI / count;
    const avgARI = totalARI / count;
    const avgRGI = totalRGI / count;
    const avgCompOcc = totalCompOcc / count;
    const avgHotelOcc = totalHotelOcc / count;
    const avgHotelADR = totalHotelADR / count;
    const avgHotelRevPAR = (avgHotelOcc * avgHotelADR) / 100;

    let performancePosition = '';
    if (avgMPI >= 100) {
      performancePosition = 'outperforming';
    } else {
      performancePosition = 'underperforming';
    }

    // ===== SEVERITY =====
    let severity = 'low';
    if (avgMPI < 90) severity = 'critical';
    else if (avgMPI < 95) severity = 'high';
    else severity = 'medium';

    const triggerMet = avgMPI < 100 && avgARI > 100;

    if (!triggerMet) {
      return res.status(200).json({
        success: true,
        message: 'No issue detected',
        avgMPI,
        avgARI,
        avgRGI
      });
    }

    // ===== SCENARIO =====
    let scenario = 'unknown';
    if (avgCompOcc < 60) {
      scenario = 'market_down';
    } else {
      scenario = 'market_up';
    }

    // ===== STR v2 — SEGMENTATION ENTRY LAYER =====
    let segmentFocus = 'Retail';
    let segmentReason = 'Transient demand appears to be the most likely source of underperformance at this stage.';

    if (avgCompOcc < 50 && performancePosition === 'underperforming') {
      segmentFocus = 'Retail';
      segmentReason = 'In a soft market where the hotel is underperforming, the most likely first pressure point is transient retail demand, including pricing, visibility, conversion, or channel mix.';
    } else if (avgCompOcc < 50 && performancePosition === 'outperforming') {
      segmentFocus = 'Groups';
      segmentReason = 'In a soft market where the hotel is outperforming, stronger base business support is the most likely explanation, typically from group or negotiated demand.';
    } else if (scenario === 'market_down') {
      segmentFocus = 'Retail';
      segmentReason = 'When the market is soft overall, transient retail is the first segment to validate because it reacts fastest to weak demand conditions.';
    } else {
      segmentFocus = 'Negotiated';
      segmentReason = 'When the market is holding but the hotel under-indexes, the issue is more likely structural and linked to negotiated accounts, account production, or contracted base demand.';
    }

    // ===== HOTEL PROFILE TARGETS =====
    const profileMap = {};
    profileRows.forEach(row => {
      if (row && row[0]) {
        profileMap[row[0].toString().trim()] = row[1];
      }
    });

    const targetOcc = parsePercent(profileMap['Target Occupancy %']);
    const targetADR = parseNumber(profileMap['Target ADR']);
    const targetRevPAR = parseNumber(profileMap['Target RevPAR']);

    const occGapVsTarget = targetOcc ? +(avgHotelOcc - targetOcc).toFixed(1) : null;
    const adrGapVsTarget = targetADR ? +(avgHotelADR - targetADR).toFixed(0) : null;
    const revparGapVsTarget = targetRevPAR ? +(avgHotelRevPAR - targetRevPAR).toFixed(0) : null;

    // ===== PMS MARKET SEGMENT SIGNAL =====
    const segmentMap = {};

    pmsData.forEach(row => {
      const code = (row['Market Segment Code'] || '').toString().trim();
      if (!code) return;

      const rnTY = parseNumber(row['Room Nights On Books TY']);
      const rnLY = parseNumber(row['Room Nights On Books LY']);
      const revTY = parseNumber(row['Booked Revenue TY']);
      const revLY = parseNumber(row['Booked Revenue LY']);

      if (!segmentMap[code]) {
        segmentMap[code] = {
          roomNightsTY: 0,
          roomNightsLY: 0,
          revenueTY: 0,
          revenueLY: 0
        };
      }

      segmentMap[code].roomNightsTY += rnTY;
      segmentMap[code].roomNightsLY += rnLY;
      segmentMap[code].revenueTY += revTY;
      segmentMap[code].revenueLY += revLY;
    });

    const totalSegmentRN = Object.values(segmentMap).reduce((sum, s) => sum + s.roomNightsTY, 0);
    const totalSegmentRev = Object.values(segmentMap).reduce((sum, s) => sum + s.revenueTY, 0);

    const retailCodes = ['OTA', 'PKG', 'BAR', 'DIS', 'WHO'];
    const negotiatedCodes = ['NEG', 'NEG_QY', 'COR', 'RFP'];
    const groupCodes = ['GCO', 'GRP', 'GDP'];

    const retailMix = aggregateCodes(segmentMap, retailCodes);
    const negotiatedMix = aggregateCodes(segmentMap, negotiatedCodes);
    const groupMix = aggregateCodes(segmentMap, groupCodes);

    const retailShareRN = totalSegmentRN ? +((retailMix.roomNightsTY / totalSegmentRN) * 100).toFixed(1) : 0;
    const retailShareRev = totalSegmentRev ? +((retailMix.revenueTY / totalSegmentRev) * 100).toFixed(1) : 0;
    const retailYoYRN = retailMix.roomNightsLY ? +(((retailMix.roomNightsTY - retailMix.roomNightsLY) / retailMix.roomNightsLY) * 100).toFixed(1) : null;
    const retailYoYRev = retailMix.revenueLY ? +(((retailMix.revenueTY - retailMix.revenueLY) / retailMix.revenueLY) * 100).toFixed(1) : null;

    let mixSignal = 'No material retail mix signal identified from PMS data yet.';
    if (segmentFocus === 'Retail' && retailShareRN > 0) {
      if (retailYoYRN !== null && retailYoYRN < 0) {
        mixSignal = `Retail represents ${retailShareRN}% of room nights and ${retailShareRev}% of room revenue, with room nights down ${Math.abs(retailYoYRN)}% versus last year, suggesting weaker transient contribution.`;
      } else if (retailYoYRN !== null && retailYoYRN > 0) {
        mixSignal = `Retail represents ${retailShareRN}% of room nights and ${retailShareRev}% of room revenue, with room nights up ${retailYoYRN}% versus last year, indicating resilient transient contribution.`;
      } else {
        mixSignal = `Retail represents ${retailShareRN}% of room nights and ${retailShareRev}% of room revenue for the selected period.`;
      }
    } else if (segmentFocus === 'Negotiated') {
      const negShareRN = totalSegmentRN ? +((negotiatedMix.roomNightsTY / totalSegmentRN) * 100).toFixed(1) : 0;
      mixSignal = `Negotiated business represents ${negShareRN}% of room nights for the selected period.`;
    } else if (segmentFocus === 'Groups') {
      const grpShareRN = totalSegmentRN ? +((groupMix.roomNightsTY / totalSegmentRN) * 100).toFixed(1) : 0;
      mixSignal = `Group business represents ${grpShareRN}% of room nights for the selected period.`;
    }

    let targetSignal = 'No target calibration available.';
    if (occGapVsTarget !== null || adrGapVsTarget !== null || revparGapVsTarget !== null) {
      const bits = [];

      if (occGapVsTarget !== null) {
        bits.push(`occupancy is ${occGapVsTarget >= 0 ? '+' : ''}${occGapVsTarget}pts versus target`);
      }
      if (adrGapVsTarget !== null) {
        bits.push(`ADR is ${adrGapVsTarget >= 0 ? '+' : ''}${adrGapVsTarget} versus target`);
      }
      if (revparGapVsTarget !== null) {
        bits.push(`RevPAR is ${revparGapVsTarget >= 0 ? '+' : ''}${revparGapVsTarget} versus target`);
      }

      if (bits.length > 0) {
        targetSignal = bits.join(', ') + '.';
      }
    }

    // ===== v3 — SEGMENT DRIVER HYPOTHESIS LAYER =====
    let driverCategory = 'general_commercial_pressure';
    let driverConfidence = 'hypothesis';
    let driverReason = 'The current KPI pattern suggests a general commercial performance issue that should be validated with deeper segment-level data later.';

    if (segmentFocus === 'Retail') {
      if (avgARI > 100 && avgMPI < 100) {
        driverCategory = 'pricing_positioning';
        driverReason = 'The current KPI pattern suggests retail underperformance is likely driven by pricing positioning. The hotel appears to be holding rate above the comp set while failing to capture enough occupancy share. This is a hypothesis based on blended STR indexes and should later be validated with retail-specific ADR and channel data.';
      } else if (avgARI < 100 && avgMPI < 100) {
        driverCategory = 'visibility_demand_capture';
        driverReason = 'The current KPI pattern suggests retail underperformance is likely driven by weaker visibility or demand capture. The hotel is neither leading on rate nor occupancy penetration, which may indicate insufficient channel exposure, weaker digital presence, or low demand capture. This is a hypothesis based on blended STR indexes and should later be validated with channel-level data.';
      } else {
        driverCategory = 'conversion_channel_performance';
        driverReason = 'The current KPI pattern suggests retail underperformance is likely driven by conversion or channel-performance inefficiency. The hotel may be visible in the market but not converting demand efficiently into bookings. This is a hypothesis based on blended STR indexes and should later be validated with website, booking engine, and channel-conversion data.';
      }
    }

    // ===== STRUCTURED OUTPUT SEEDS =====
    const diagnosisText = `MPI is ${Math.round(avgMPI)}, ARI is ${Math.round(avgARI)}, and RGI is ${Math.round(avgRGI)}. Market demand is ${scenario === 'market_down' ? 'weak' : 'strong'} (Comp Occ ${Math.round(avgCompOcc)}%), and the hotel is ${performancePosition} versus the comp set.`;
    const rootCauseText = `${segmentReason} ${driverReason}`;
    const expectedOutcomeText = `Validating and addressing the likely ${driverCategory.replace(/_/g, ' ')} issue should improve ${segmentFocus.toLowerCase()} performance, strengthen market share, and support short-term revenue recovery.`;
    const recommendationTitle = `${segmentFocus} underperformance likely driven by ${driverCategory.replace(/_/g, ' ')} in a ${scenario === 'market_down' ? 'soft' : 'strong'} market`;

    const supabaseUrl = process.env.SUPABASE_URL;
    const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

    let actions = [];
    if (segmentFocus === 'Retail') {
      if (driverCategory === 'pricing_positioning') {
        actions = [
          'Validate whether retail pricing positioning is limiting share capture versus the comp set',
          'Review transient rate architecture, fenced offers, and need-date price competitiveness',
          'Align revenue and distribution teams on short-cycle pricing adjustments to recover occupancy without unnecessary ADR dilution'
        ];
      } else if (driverCategory === 'visibility_demand_capture') {
        actions = [
          'Validate whether weaker retail demand capture is linked to OTA visibility, digital presence, or distribution exposure',
          'Review channel visibility, parity, and share of voice across key booking windows',
          'Align marketing and distribution teams on a short-cycle visibility recovery plan to strengthen demand capture'
        ];
      } else {
        actions = [
          'Validate whether retail underperformance is linked to conversion inefficiency across direct and third-party channels',
          'Review website journey, booking engine friction, and channel conversion performance',
          'Align digital, marketing, and distribution teams on a short-cycle conversion improvement plan'
        ];
      }
    } else if (segmentFocus === 'Negotiated') {
      actions = [
        'Review negotiated account production, contracted rate positioning, and displaced account opportunities',
        'Validate whether the hotel is missing structural base demand from key corporate or government accounts',
        'Build an account-recovery plan with sales leadership focused on top production gaps and dormant accounts'
      ];
    } else if (segmentFocus === 'Groups') {
      actions = [
        'Review group base contribution, pipeline strength, and pace of conversion for upcoming need periods',
        'Validate whether group support is protecting occupancy or whether reliance on group business is masking transient weakness',
        'Align sales and revenue on a group optimization plan focused on need dates, conversion, and displacement quality'
      ];
    }

    let aiTitle = recommendationTitle;
    let aiRootCause = rootCauseText;
    let aiExpectedOutcome = expectedOutcomeText;
    let aiActions = actions;

    try {
      const aiNarrative = await composeExecutiveNarrative({
        marketCondition: scenario === 'market_down' ? 'soft' : 'strong',
        performancePosition,
        segmentFocus,
        driverCategory,
        driverConfidence,
        avgMPI: Math.round(avgMPI),
        avgARI: Math.round(avgARI),
        avgRGI: Math.round(avgRGI),
        avgHotelOcc: Math.round(avgHotelOcc),
        avgHotelADR: Math.round(avgHotelADR),
        avgCompOcc: Math.round(avgCompOcc),
        mixSignal,
        targetSignal,
        rootCauseSeed: rootCauseText,
        expectedOutcomeSeed: expectedOutcomeText,
        actions
      });

      if (aiNarrative) {
        aiTitle = aiNarrative.title || recommendationTitle;
        aiRootCause = aiNarrative.root_cause || rootCauseText;
        aiExpectedOutcome = aiNarrative.expected_outcome || expectedOutcomeText;
        aiActions = Array.isArray(aiNarrative.actions) && aiNarrative.actions.length ? aiNarrative.actions : actions;
      }
    } catch (err) {
      console.error('Claude composition fallback:', err.message);
    }

    const recommendation = {
      hotel_name: hotelCode,
      title: aiTitle,
      department: 'Commercial',
      finding: diagnosisText,
      root_cause: aiRootCause,
      expected_outcome: aiExpectedOutcome,
      hotel_id: hotelCode,
      impact_value: Math.round((100 - avgMPI) * 120),
      impact_type: 'EUR',
      is_repeat: false,
      expected_impact_value: Math.round((100 - avgMPI) * 120),
      status: 'open',
      period: period
    };

    console.log('STR v2 segment focus:', segmentFocus);
    console.log('STR v2 segment reason:', segmentReason);
    console.log('STR v2 diagnosis:', diagnosisText);
    console.log('STR v2 root cause:', rootCauseText);
    console.log('STR v2 expected outcome:', expectedOutcomeText);
    console.log('STR v3 driver category:', driverCategory);
    console.log('STR v3 driver confidence:', driverConfidence);
    console.log('STR v3 driver reason:', driverReason);
    console.log('v3.1 mix signal:', mixSignal);
    console.log('v3.1 target signal:', targetSignal);
    console.log('v3.1 ai title:', aiTitle);
    console.log('v3.1 ai root cause:', aiRootCause);
    console.log('v3.1 ai expected outcome:', aiExpectedOutcome);
    console.log('v3.1 ai actions:', aiActions);

    const recRes = await fetch(`${supabaseUrl}/rest/v1/Recommendations`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'apikey': supabaseKey,
        'Authorization': `Bearer ${supabaseKey}`,
        'Prefer': 'return=representation'
      },
      body: JSON.stringify(recommendation)
    });

    if (!recRes.ok) {
      const recError = await recRes.text();
      throw new Error(`Recommendation insert failed: ${recError}`);
    }

    for (const text of aiActions) {
      await fetch(`${supabaseUrl}/rest/v1/actions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'apikey': supabaseKey,
          'Authorization': `Bearer ${supabaseKey}`
        },
        body: JSON.stringify({
          hotel_name: hotelCode,
          title: aiTitle,
          action_text: text,
          status: 'open',
          expected_impact_value: Math.round((100 - avgMPI) * 120),
          period: period
        })
      });
    }

    return res.status(200).json({
      success: true,
      message: 'COM-001 STR v3.1 executed',
      avgMPI,
      avgARI,
      avgRGI,
      segmentFocus,
      driverCategory,
      mixSignal,
      targetSignal
    });
  } catch (error) {
    console.error('Analyze error:', error);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
};
