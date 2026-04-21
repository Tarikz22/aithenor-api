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
const MAX_RETAIL_ISSUES_PER_RUN = 5;

/** Legacy fallback: max flattened actions when wrapping old driver-only path. */
const MAX_LEGACY_RETAIL_ACTIONS = 5;

/** Max library actions attached to a single issue (executive readability). */
const MAX_ACTIONS_PER_RETAIL_ISSUE = 3;

/** Minimum STR daily rows required to score a calendar week (thin weeks skipped). */
const MIN_STR_DAYS_PER_WEEK = 4;

// Phase 2: Monthly granularity — divergence thresholds.
// A month must breach at least 2 of these 6 thresholds to generate a card.
const MONTHLY_OCC_DIVERGENCE_PT = 8; // percentage points vs period weighted average
const MONTHLY_ADR_DIVERGENCE_PCT = 0.1; // relative (10%) vs period weighted average ADR
const MONTHLY_REVPAR_DIVERGENCE_PCT = 0.12; // relative (12%)
const MONTHLY_MPI_DIVERGENCE_PT = 8; // index points vs period simple average
const MONTHLY_ARI_DIVERGENCE_PT = 8; // index points
const MONTHLY_RGI_DIVERGENCE_PT = 8; // index points
const MONTHLY_MIN_DAYS = 20; // minimum STR days in a month to qualify

// Phase 2: Forecast vs OTB gap — thresholds and window definitions.
// A card fires only when BOTH rn gap % AND revenue gap breach simultaneously.
const FORECAST_RN_GAP_PCT_THRESHOLD = 0.1; // 10% shortfall vs forecast RN
const FORECAST_REVENUE_GAP_THRESHOLD = 5000; // absolute revenue gap (reporting currency)
const FORECAST_BEAT_RN_PCT_THRESHOLD = 0.05; // 5% OTB above forecast → green card
const FORECAST_BEAT_REV_PCT_THRESHOLD = 0.03; // 3% revenue above forecast → confirmed beat
const FORECAST_MIN_WINDOW_COVERAGE_PCT = 0.05; // window must have ≥5% of capacity×days forecasted
const FORECAST_MIN_ABSOLUTE_RN = 5; // absolute floor when capacity cannot be inferred
const FORECAST_WINDOWS = [
  { label: 'Days 1–30', minLead: 1, maxLead: 30 },
  { label: 'Days 31–60', minLead: 31, maxLead: 60 },
  { label: 'Days 61–90', minLead: 61, maxLead: 90 }
];

// Phase 2: Segment mix shift detection — pattern thresholds.

// Pattern 1 — Displacement: premium segment loses share to discount segment.
// Both conditions must be met simultaneously.
const MIX_DISPLACEMENT_RN_SHIFT_PCT   = 0.02; // segment shifts ≥2% of total RN
const MIX_DISPLACEMENT_ADR_SPREAD_PCT = 0.10; // losing segment ADR ≥10% above gaining

// Pattern 2 — Concentration: one segment dominates total RN.
// Both conditions must be met simultaneously.
const MIX_CONCENTRATION_THRESHOLD     = 0.45; // single segment >45% of total RN
const MIX_CONCENTRATION_GROWTH_PCT    = 0.05; // AND grew ≥5% RN share vs LY

// Pattern 3 — Rate dilution: ADR erodes within a segment without volume loss.
const MIX_RATE_DILUTION_ADR_DROP_PCT  = 0.05; // ADR down ≥5% YoY for segment
const MIX_RATE_DILUTION_MIN_SHARE     = 0.10; // segment must be ≥10% of total RN

// Minimum dataset requirements — suppress below these floors.
const MIX_MIN_TOTAL_RN               = 100;  // total actualized RN
const MIX_MIN_PERIOD_DAYS            = 30;   // actualized period in days

/**
 * Phase 2: Returns rating tier 1–4 for a USALI segment bucket.
 * Tier 1 = highest value, Tier 4 = lowest value.
 * Used to determine whether a displacement is value-destructive.
 * Only tier 1/2 losing to tier 3/4 produces a displacement card.
 */
function getSegmentRatingTier(bucket) {
  if (!bucket) return 4;
  const b = String(bucket).toLowerCase();
  // Tier 1: highest ADR segments — direct, best available, negotiated
  if (b === 'transient_retail' || b === 'transient_negotiated') return 1;
  // Tier 2: qualified and corporate group — strong ADR, reliable
  if (b === 'transient_qualified' || b === 'group_corporate') return 2;
  // Tier 3: wholesale and association — lower ADR, higher volume
  if (b === 'transient_wholesale' || b === 'group_association' ||
      b === 'group_government') return 3;
  // Tier 4: discount, contract, SMERF, other — lowest ADR
  if (b === 'transient_discount' || b === 'contract' ||
      b === 'group_smerf' || b === 'group_wholesale' ||
      b === 'group_other' || b === 'other') return 4;
  return 4;
}

/**
 * Phase 2: Segment mix shift detection.
 * Detects three patterns in actualized PMS data:
 *   Pattern 1 — Displacement: premium segment losing share to discount
 *   Pattern 2 — Concentration: single segment dominance risk
 *   Pattern 3 — Rate dilution: ADR erosion without volume loss
 *
 * Returns array of mix_shift issue cards. Empty array if data
 * is insufficient or no patterns detected. Never throws.
 *
 * @param {object[]} pmsRows     - actualized PMS rows (rowsForEngine)
 * @param {string}   snapshotYmd - current snapshot date YYYY-MM-DD
 * @param {string}   periodStart - period start YYYY-MM-DD (from periodMeta)
 * @param {string}   periodEnd   - period end YYYY-MM-DD (from periodMeta)
 */
function buildMixShiftIssues(pmsRows, snapshotYmd, periodStart, periodEnd) {

  // --- Guard: need actualized PMS rows ---
  const rows = (pmsRows || []).filter(r =>
    r?._ingestion?.row_phase === 'actualized' ||
    r?._ingestion?.row_phase === 'undated'
  );
  if (!rows.length) return [];

  // --- Guard: check period length ---
  // Suppress on very short periods — mix reads are noisy on <30 days
  let periodDays = 0;
  if (periodStart && periodEnd) {
    const s = parseYmdToUtcDate(periodStart);
    const e = parseYmdToUtcDate(periodEnd);
    if (s && e) periodDays = Math.round((e - s) / 86400000) + 1;
  }
  if (periodDays > 0 && periodDays < MIX_MIN_PERIOD_DAYS) return [];

  // --- Helper: safe number from row with multiple key fallbacks ---
  const safeGet = (row, ...keys) => {
    for (const k of keys) {
      const v = toNumber(row[k]);
      if (v !== null) return v;
    }
    return null;
  };

  // --- Build per-segment aggregates ---
  // Key: USALI bucket string
  // Value: { displayName, bucket, rnTY, rnLY, revTY, revLY,
  //          adrTY (derived), adrLY (derived), tier }
  const segMap = {};

  for (const row of rows) {
    const segName   = row['Market Segment Name'] || row['market segment name'] || '';
    const bucket    = mapMarketSegmentNameToUsaliBucket(segName);
    const dispName  = usaliBucketToDisplayName(bucket, segName);

    const rnTY  = safeGet(row,
      'Room Nights TY (Actual / OTB)', 'Room Nights TY') || 0;
    const rnLY  = safeGet(row,
      'Room Nights LY Actual', 'Room Nights LY') || 0;
    const revTY = safeGet(row,
      'Revenue TY (Actual / OTB)', 'Revenue TY') || 0;
    const revLY = safeGet(row,
      'Revenue LY Actual', 'Revenue LY') || 0;

    if (!segMap[bucket]) {
      segMap[bucket] = {
        bucket,
        displayName: dispName,
        tier: getSegmentRatingTier(bucket),
        rnTY:  0, rnLY:  0,
        revTY: 0, revLY: 0
      };
    }
    segMap[bucket].rnTY  += rnTY;
    segMap[bucket].rnLY  += rnLY;
    segMap[bucket].revTY += revTY;
    segMap[bucket].revLY += revLY;
  }

  const segments = Object.values(segMap);

  // --- Compute totals ---
  const totalRNTY = segments.reduce((s, x) => s + x.rnTY, 0);
  const totalRNLY = segments.reduce((s, x) => s + x.rnLY, 0);

  // --- Guard: minimum total RN ---
  if (totalRNTY < MIX_MIN_TOTAL_RN) return [];

  // --- Guard: LY data present ---
  // If total LY is zero, all YoY comparisons are meaningless
  const hasLY = totalRNLY > 0;

  // --- Derive ADR per segment (revenue / room nights) ---
  for (const seg of segments) {
    seg.adrTY = seg.rnTY > 0 ? seg.revTY / seg.rnTY : null;
    seg.adrLY = seg.rnLY > 0 ? seg.revLY / seg.rnLY : null;
    // Share of total TY room nights
    seg.shareTY = totalRNTY > 0 ? seg.rnTY / totalRNTY : 0;
    // Share of total LY room nights
    seg.shareLY = totalRNLY > 0 ? seg.rnLY / totalRNLY : 0;
    // Share change (positive = grew, negative = shrank)
    seg.shareShift = seg.shareTY - seg.shareLY;
  }

  const results = [];

  // ─────────────────────────────────────────────────
  // PATTERN 1 — DISPLACEMENT
  // Premium segment (tier 1-2) loses share to discount (tier 3-4)
  // ─────────────────────────────────────────────────
  if (hasLY) {
    // Find segments that lost share (negative shift ≥ threshold)
    const losers = segments.filter(s =>
      s.shareShift <= -MIX_DISPLACEMENT_RN_SHIFT_PCT &&
      s.tier <= 2 // must be premium tier to be a value-destructive loss
    );

    // Find segments that gained share (positive shift ≥ threshold)
    const gainers = segments.filter(s =>
      s.shareShift >= MIX_DISPLACEMENT_RN_SHIFT_PCT &&
      s.tier >= 3 // must be discount tier — premium gaining is positive
    );

    // For each loser-gainer pair, check ADR spread
    for (const loser of losers) {
      for (const gainer of gainers) {
        if (loser.adrTY === null || gainer.adrTY === null) continue;

        // ADR spread: loser must be materially higher rated than gainer
        const adrSpread = loser.adrTY > 0
          ? (loser.adrTY - gainer.adrTY) / loser.adrTY : 0;

        if (adrSpread < MIX_DISPLACEMENT_ADR_SPREAD_PCT) continue;
        // Spread too small — not commercially meaningful

        // --- Quantify the revenue cost of displacement ---
        // Room nights shifted from loser to gainer (approximate)
        const shiftedRN = Math.abs(loser.shareShift) * totalRNTY;
        // Revenue destroyed = shifted RN × ADR difference
        const revenueCost = Math.round(shiftedRN * (loser.adrTY - gainer.adrTY));

        // --- Build narrative ---
        const loserSharePctTY = (loser.shareTY * 100).toFixed(1);
        const loserSharePctLY = (loser.shareLY * 100).toFixed(1);
        const gainerSharePctTY = (gainer.shareTY * 100).toFixed(1);
        const gainerSharePctLY = (gainer.shareLY * 100).toFixed(1);
        const adrSpreadPct = (adrSpread * 100).toFixed(1);
        const revFmt = v => new Intl.NumberFormat('en-GB',
          { maximumFractionDigits: 0 }).format(Math.round(v));

        const signalPara =
          `${loser.displayName} lost ${Math.abs(loser.shareShift * 100).toFixed(1)} ` +
          `percentage points of mix share (${loserSharePctLY}% LY → ${loserSharePctTY}% TY). ` +
          `${gainer.displayName} gained ${(gainer.shareShift * 100).toFixed(1)} ` +
          `percentage points (${gainerSharePctLY}% LY → ${gainerSharePctTY}% TY). ` +
          `ADR spread between segments: ${adrSpreadPct}%.`;

        const analysisPara =
          `${loser.displayName} ADR: ${loser.adrTY != null ? revFmt(loser.adrTY) : 'n/a'} ` +
          `vs ${gainer.displayName} ADR: ${gainer.adrTY != null ? revFmt(gainer.adrTY) : 'n/a'}. ` +
          `Occupancy is preserved but the rate base is being eroded — ` +
          `volume held at the cost of quality.`;

        const impactPara =
          `Estimated revenue cost of displacement: £${revFmt(revenueCost)} ` +
          `(${Math.round(shiftedRN)} room nights × ` +
          `${loser.adrTY != null && gainer.adrTY != null ?
            revFmt(loser.adrTY - gainer.adrTY) : 'n/a'} ADR differential).`;

        const decisionLine =
          `Defend ${loser.displayName} share — the ADR differential makes every ` +
          `displaced room night a compounding revenue loss. ` +
          `Do not allow ${gainer.displayName} to permanently replace ` +
          `${loser.displayName} in the mix.`;

        const executionActions = [
          `Audit why ${loser.displayName} is losing share — pricing, availability, ` +
          `or channel positioning are the likely causes.`,
          `Set a minimum share floor for ${loser.displayName} at ` +
          `${Math.max(loser.shareTY * 100, loser.shareLY * 100 * 0.9).toFixed(0)}% ` +
          `of room nights and enforce it in rate strategy meetings.`,
          `Review ${gainer.displayName} rate floors — if this segment is filling ` +
          `at the expense of ${loser.displayName}, the rate floor may be too accessible.`
        ];

        const confidence = hasLY && periodDays >= 60 ? 'high'
                         : hasLY && periodDays >= 30 ? 'medium' : 'low';

        results.push({
          finding_key:   `MIX_DISPLACEMENT_${loser.bucket}_${gainer.bucket}`,
          granularity:   'mix_shift',
          pattern:       'displacement',
          title: `Mix shift — ${loser.displayName} losing share to ${gainer.displayName}`,
          issue_family:  'mix_displacement',
          primary_driver: 'mix_strategy',
          priority: Math.abs(loser.shareShift) >= 0.10 ? 'high' : 'medium',
          confidence,
          commercial_narrative: [signalPara, analysisPara, impactPara]
            .join('\n\n'),
          enforced_decision_line: decisionLine,
          enforced_execution_actions: executionActions,
          card_metrics: {
            avgMPI: null,
            avgARI: null,
            avgRGI: null,
            avgOcc: null
          },
          quantification: {
            loser_segment:      loser.displayName,
            gainer_segment:     gainer.displayName,
            loser_share_ty:     Math.round(loser.shareTY * 1000) / 10,
            loser_share_ly:     Math.round(loser.shareLY * 1000) / 10,
            gainer_share_ty:    Math.round(gainer.shareTY * 1000) / 10,
            gainer_share_ly:    Math.round(gainer.shareLY * 1000) / 10,
            share_shift_pts:    Math.round(Math.abs(loser.shareShift) * 1000) / 10,
            adr_spread_pct:     Math.round(adrSpread * 1000) / 10,
            shifted_rn:         Math.round(shiftedRN),
            revenue_cost:       revenueCost,
            loser_adr_ty:       loser.adrTY !== null ? Math.round(loser.adrTY) : null,
            gainer_adr_ty:      gainer.adrTY !== null ? Math.round(gainer.adrTY) : null
          }
        });
      }
    }
  }

  // ─────────────────────────────────────────────────
  // PATTERN 2 — CONCENTRATION RISK
  // Single segment >45% of TY room nights AND grew vs LY
  // ─────────────────────────────────────────────────
  for (const seg of segments) {
    if (seg.shareTY < MIX_CONCENTRATION_THRESHOLD) continue;

    // Must also have grown vs LY (not just historically dominant)
    const shareGrowth = seg.shareTY - seg.shareLY;
    if (hasLY && shareGrowth < MIX_CONCENTRATION_GROWTH_PCT) continue;

    const revFmt = v => new Intl.NumberFormat('en-GB',
      { maximumFractionDigits: 0 }).format(Math.round(v));
    const revShare = totalRNTY > 0 && seg.revTY > 0
      ? ((seg.revTY / segments.reduce((s, x) => s + x.revTY, 0)) * 100).toFixed(1)
      : null;

    const signalPara =
      `${seg.displayName} now represents ${(seg.shareTY * 100).toFixed(1)}% ` +
      `of room nights` +
      (hasLY ? `, up from ${(seg.shareLY * 100).toFixed(1)}% last year` : '') +
      `. ` +
      (revShare ? `This segment accounts for ${revShare}% of total revenue. ` : '') +
      `Single-segment dependency above 45% creates material commercial fragility.`;

    const analysisPara =
      `If ${seg.displayName} softens by 20%, the hotel faces a ` +
      `${Math.round(seg.rnTY * 0.20)}-room-night exposure with no immediate ` +
      `fallback segment at equivalent scale. ` +
      `The current mix does not have a second segment capable of absorbing ` +
      `a withdrawal of this magnitude.`;

    const decisionLine =
      `Actively develop at least one alternative segment to reduce ` +
      `${seg.displayName} dependency below 40% within two booking cycles.`;

    const executionActions = [
      `Identify the two segments with the strongest growth trajectory ` +
      `in the current mix and invest in their development as alternatives ` +
      `to ${seg.displayName}.`,
      `Set a maximum allocation policy for ${seg.displayName} — ` +
      `cap this segment at ${Math.min(Math.round(seg.shareTY * 100) - 5, 44)}% ` +
      `of inventory in the next rate strategy cycle.`,
      `Review rate positioning for the underperforming segments ` +
      `to assess whether pricing or availability is suppressing their growth.`
    ];

    const confidence = hasLY ? 'high' : 'medium';

    results.push({
      finding_key:   `MIX_CONCENTRATION_${seg.bucket}`,
      granularity:   'mix_shift',
      pattern:       'concentration',
      title: `Concentration risk — ${seg.displayName} represents ` +
             `${(seg.shareTY * 100).toFixed(0)}% of room nights`,
      issue_family:  'mix_concentration',
      primary_driver: 'mix_strategy',
      priority:      seg.shareTY > 0.55 ? 'high' : 'medium',
      confidence,
      commercial_narrative: [signalPara, analysisPara].join('\n\n'),
      enforced_decision_line: decisionLine,
      enforced_execution_actions: executionActions,
      card_metrics: {
        avgMPI: null, avgARI: null, avgRGI: null, avgOcc: null
      },
      quantification: {
        dominant_segment:   seg.displayName,
        share_ty:           Math.round(seg.shareTY * 1000) / 10,
        share_ly:           hasLY ? Math.round(seg.shareLY * 1000) / 10 : null,
        share_growth_pts:   hasLY ? Math.round(shareGrowth * 1000) / 10 : null,
        rn_ty:              Math.round(seg.rnTY),
        rev_share_pct:      revShare !== null ? parseFloat(revShare) : null
      }
    });
  }

  // ─────────────────────────────────────────────────
  // PATTERN 3 — RATE DILUTION
  // Segment ADR drops ≥5% YoY while share holds flat or grows
  // ─────────────────────────────────────────────────
  if (hasLY) {
    for (const seg of segments) {
      // Must be meaningful share of total RN
      if (seg.shareTY < MIX_RATE_DILUTION_MIN_SHARE) continue;
      // Must have LY ADR to compare
      if (seg.adrTY === null || seg.adrLY === null || seg.adrLY === 0) continue;

      const adrDropPct = (seg.adrLY - seg.adrTY) / seg.adrLY;
      if (adrDropPct < MIX_RATE_DILUTION_ADR_DROP_PCT) continue;

      // Volume must not have dropped materially — if volume fell,
      // ADR drop may be a mix/length-of-stay artefact, not pure dilution
      const rnChangePct = seg.rnLY > 0
        ? (seg.rnTY - seg.rnLY) / seg.rnLY : 0;
      // Suppress if volume dropped >15% — ADR change is then ambiguous
      if (rnChangePct < -0.15) continue;

      // --- Quantify revenue destruction ---
      const revenueLost = Math.round(seg.rnTY * (seg.adrLY - seg.adrTY));
      const revFmt = v => new Intl.NumberFormat('en-GB',
        { maximumFractionDigits: 0 }).format(Math.round(v));

      const signalPara =
        `${seg.displayName} ADR declined ${(adrDropPct * 100).toFixed(1)}% ` +
        `year-on-year (${revFmt(seg.adrLY)} LY → ${revFmt(seg.adrTY)} TY) ` +
        `while room night share held at ${(seg.shareTY * 100).toFixed(1)}%. ` +
        `Rate was reduced without a corresponding volume justification.`;

      const analysisPara =
        `${seg.rnTY.toFixed(0)} room nights at ${revFmt(seg.adrLY - seg.adrTY)} ` +
        `lower ADR = £${revFmt(revenueLost)} in destroyed revenue this period. ` +
        `The rate concession was not purchased back in volume — ` +
        `room nights ${rnChangePct >= 0 ? 'grew' : 'fell'} ` +
        `${Math.abs(rnChangePct * 100).toFixed(1)}% vs LY. ` +
        `This is pure margin erosion.`;

      const decisionLine =
        `Restore ${seg.displayName} ADR toward last year's level — ` +
        `the rate concession is not being compensated by volume. ` +
        `Set a rate floor at ${revFmt(seg.adrLY * 0.97)} (3% below LY) ` +
        `and enforce it in the next rate review.`;

      const executionActions = [
        `Audit the rate events that caused ${seg.displayName} ADR to fall ` +
        `${(adrDropPct * 100).toFixed(1)}% — identify whether this was a ` +
        `deliberate strategy or an unmanaged drift.`,
        `Set a rate floor for ${seg.displayName} at ` +
        `${revFmt(seg.adrLY * 0.97)} and apply it immediately in ` +
        `all rate-loading tools.`,
        `Monitor ${seg.displayName} ADR weekly — if it does not recover ` +
        `by at least 3% within four weeks, escalate to a full segment ` +
        `rate strategy review.`
      ];

      const confidence = periodDays >= 60 ? 'high'
                       : periodDays >= 30 ? 'medium' : 'low';

      results.push({
        finding_key:   `MIX_RATE_DILUTION_${seg.bucket}`,
        granularity:   'mix_shift',
        pattern:       'rate_dilution',
        title: `Rate dilution — ${seg.displayName} ADR down ` +
               `${(adrDropPct * 100).toFixed(1)}% with flat volume`,
        issue_family:  'mix_rate_dilution',
        primary_driver: 'pricing',
        priority:      adrDropPct >= 0.10 ? 'high' : 'medium',
        confidence,
        commercial_narrative: [signalPara, analysisPara].join('\n\n'),
        enforced_decision_line: decisionLine,
        enforced_execution_actions: executionActions,
        card_metrics: {
          avgMPI: null, avgARI: null, avgRGI: null, avgOcc: null
        },
        quantification: {
          segment:          seg.displayName,
          adr_ty:           Math.round(seg.adrTY),
          adr_ly:           Math.round(seg.adrLY),
          adr_drop_pct:     Math.round(adrDropPct * 1000) / 10,
          adr_drop_abs:     Math.round(seg.adrLY - seg.adrTY),
          rn_ty:            Math.round(seg.rnTY),
          rn_change_pct:    Math.round(rnChangePct * 1000) / 10,
          revenue_lost:     revenueLost,
          share_ty:         Math.round(seg.shareTY * 1000) / 10
        }
      });
    }
  }

  // --- Sort results: pattern order, then priority, then revenue impact ---
  // Sort: displacement first, then concentration, then rate dilution.
  // Within each pattern: highest priority first, then highest revenue impact.
  const patternOrder = {
    displacement: 0, concentration: 1, rate_dilution: 2
  };
  const priorityOrder = { high: 0, medium: 1, low: 2 };

  results.sort((a, b) => {
    const po = (patternOrder[a.pattern] || 0) - (patternOrder[b.pattern] || 0);
    if (po !== 0) return po;
    const pr = (priorityOrder[a.priority] || 1) - (priorityOrder[b.priority] || 1);
    if (pr !== 0) return pr;
    const ra = Number(a.quantification?.revenue_cost ||
                      a.quantification?.revenue_lost || 0);
    const rb = Number(b.quantification?.revenue_cost ||
                      b.quantification?.revenue_lost || 0);
    return rb - ra;
  });

  return results;
}


/** Adjacent ISO weeks merge if MPI and ARI are within these index points (episode same regime). */
const EPISODE_MERGE_MPI_ARI_MAX_DELTA = 4;

const PRIORITY_SORT_RANK = { high: 0, medium: 1, low: 2 };

/** Locked hotel workbook tabs (aliases for sheet resolution). Hotel Profile is intentionally omitted — never ingested. */
const SHEET_ALIASES_STR = ['STR Daily Report', 'STR', 'Daily STR'];
const SHEET_ALIASES_PMS = ['PMS Market Segment Report', 'PMS', 'Market Segment'];
const SHEET_ALIASES_CORPORATE = [
  'Corporate Account Production',
  'Corporate Account',
  'Account Production'
];
const SHEET_ALIASES_DELPHI = ['Delphi Groups Pipeline', 'Delphi Groups', 'Groups Pipeline'];

/** Stay / business date column candidates (template-aligned). */
const INGESTION_DATE_KEYS = [
  'Date',
  'Business Date',
  'Stay Date',
  'Day',
  'Report Date',
  'Arrival Date',
  'Arrival',
  'Stay Night',
  'Night of Stay'
];

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
    return null;
  }

  if (typeof value === 'string') {
    const t = value.trim();
    if (!t) return null;

    // String that is an Excel serial (exports/CSV often stringify the number). Skip small
    // integers so values like "2026" are not treated as serials.
    if (/^-?\d+(\.\d+)?$/.test(t)) {
      const n = Number(t);
      if (Number.isFinite(n) && n >= 20000 && n < 2000000) {
        const parsed = XLSX.SSF.parse_date_code(n);
        if (parsed) {
          const d = new Date(Date.UTC(parsed.y, parsed.m - 1, parsed.d));
          if (!Number.isNaN(d.getTime())) return d;
        }
      }
    }

    const iso = t.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (iso) {
      const y = parseInt(iso[1], 10);
      const mo = parseInt(iso[2], 10);
      const day = parseInt(iso[3], 10);
      const dt = new Date(Date.UTC(y, mo - 1, day));
      if (!Number.isNaN(dt.getTime())) return dt;
    }

    const dm4 = t.match(/^(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{4})$/);
    if (dm4) {
      const a = parseInt(dm4[1], 10);
      const b = parseInt(dm4[2], 10);
      const y = parseInt(dm4[3], 10);
      let day;
      let month;
      if (a > 12) {
        day = a;
        month = b;
      } else if (b > 12) {
        day = b;
        month = a;
      } else {
        day = a;
        month = b;
      }
      const dt = new Date(Date.UTC(y, month - 1, day));
      if (!Number.isNaN(dt.getTime())) return dt;
    }

    const dm2 = t.match(/^(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{2})$/);
    if (dm2) {
      let y = parseInt(dm2[3], 10);
      y += y >= 70 ? 1900 : 2000;
      const a = parseInt(dm2[1], 10);
      const b = parseInt(dm2[2], 10);
      let day;
      let month;
      if (a > 12) {
        day = a;
        month = b;
      } else if (b > 12) {
        day = b;
        month = a;
      } else {
        day = a;
        month = b;
      }
      const dt = new Date(Date.UTC(y, month - 1, day));
      if (!Number.isNaN(dt.getTime())) return dt;
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

/**
 * Tabular sheets without STR KPI header heuristic (PMS / corporate / Delphi).
 */
function getSheetRowsTabular(workbook, aliases) {
  const sheetName = findSheetByAliases(workbook, aliases);
  if (!sheetName) return [];

  const sheet = workbook.Sheets[sheetName];
  const rowsDefault = XLSX.utils.sheet_to_json(sheet, { defval: null });
  const rowsOffset3 = XLSX.utils.sheet_to_json(sheet, { range: 3, defval: null });

  if (rowsDefault.length >= rowsOffset3.length) {
    return rowsDefault.length ? rowsDefault : rowsOffset3;
  }
  return rowsOffset3.length ? rowsOffset3 : rowsDefault;
}

/**
 * True if sheet_to_json row object keys include a column matching INGESTION_DATE_KEYS (header match only).
 */
function rowObjectHasIngestionDateHeaderKey(row) {
  if (!row || typeof row !== 'object') return false;
  const keyList = Object.keys(row);
  for (const candidate of INGESTION_DATE_KEYS) {
    const normalizedCandidate = normalizeKey(candidate);
    if (keyList.some((key) => normalizeKey(key) === normalizedCandidate)) return true;
    if (keyList.some((key) => normalizeKey(key).includes(normalizedCandidate))) return true;
  }
  return false;
}

/**
 * PMS tabs often have title rows before the real header. getSheetRows prefers row 1 as headers when
 * no STR KPI pattern matches, which yields wrong keys (no "Date"). Prefer default or range-3 slice
 * by which one actually exposes an ingestion date column on the first data row.
 */
function getPmsSheetRows(workbook) {
  const sheetName = findSheetByAliases(workbook, SHEET_ALIASES_PMS);
  if (!sheetName) return [];

  const sheet = workbook.Sheets[sheetName];
  const rowsDefault = XLSX.utils.sheet_to_json(sheet, { defval: null });
  const rowsOffset3 = XLSX.utils.sheet_to_json(sheet, { range: 3, defval: null });

  if (rowsDefault.length && rowObjectHasIngestionDateHeaderKey(rowsDefault[0])) {
    return rowsDefault;
  }
  if (rowsOffset3.length && rowObjectHasIngestionDateHeaderKey(rowsOffset3[0])) {
    return rowsOffset3;
  }

  return rowsDefault.length ? rowsDefault : rowsOffset3;
}

/** Same resolution order as getRowValue(…, INGESTION_DATE_KEYS); exposes matched column for diagnostics. */
function findIngestionDateMatch(row) {
  if (!row || typeof row !== 'object') return { raw: undefined, matchedHeaderKey: null };

  const entries = Object.entries(row);

  for (const candidate of INGESTION_DATE_KEYS) {
    const normalizedCandidate = normalizeKey(candidate);
    const match = entries.find(([key]) => normalizeKey(key) === normalizedCandidate);
    if (match && match[1] !== null && match[1] !== undefined && `${match[1]}`.trim() !== '') {
      return { raw: match[1], matchedHeaderKey: match[0] };
    }
  }

  for (const candidate of INGESTION_DATE_KEYS) {
    const normalizedCandidate = normalizeKey(candidate);
    const partial = entries.find(([key]) => normalizeKey(key).includes(normalizedCandidate));
    if (partial && partial[1] !== null && partial[1] !== undefined && `${partial[1]}`.trim() !== '') {
      return { raw: partial[1], matchedHeaderKey: partial[0] };
    }
  }

  return { raw: undefined, matchedHeaderKey: null };
}

function getRowStayDateYmd(row) {
  const raw = getRowValue(row, INGESTION_DATE_KEYS);
  const d = parseExcelDate(raw);
  return d ? formatDateToYMD(d) : null;
}

function mapMarketSegmentNameToUsaliBucket(name = '') {
  const raw = String(name || '').trim();
  if (!raw) return 'other';
  const s = raw.toLowerCase();

  if (/(group\s+corporate|corporate\s+group|corporate\s+meeting|group\s+meeting)/.test(s)) return 'group_corporate';
  if (/(association|convention|conference|trade\s+show|tradeshow)/.test(s)) return 'group_association';
  if (/(group\s+government|government\s+group|military\s+group)/.test(s)) return 'group_government';
  if (/(smerf|social|wedding|reunion|religious|fraternal|educational\s+group)/.test(s)) return 'group_smerf';
  if (/(group\s+wholesale|wholesale\s+group|tour\s+group|(^|\s)series(\s|$))/.test(s)) return 'group_wholesale';

  if (/(^|\s)(contract|crew|airline|permanent|long\s*term|long-term|extended\s+stay)(\s|$)/.test(s)) return 'contract';

  if (
    /(negotiated|lnr|consortia|consortium|government\s+negotiated|gov\s+negotiated)/.test(s) ||
    (/\bcorporate\b/.test(s) && !/\bgroup\b/.test(s))
  ) {
    return 'transient_negotiated';
  }

  if (/(wholesale|tour\s+operator|wholesaler)/.test(s)) return 'transient_wholesale';

  if (/(qualified|loyalty|member|\baaa\b|senior|government\s+rate|gov\s+rate|military\s+rate)/.test(s)) {
    return 'transient_qualified';
  }

  if (
    /(best\s+available|bar|rack|standard\s+rate|website\s+rate|\bretail\b|walk\s*-?in|direct)/.test(s)
  ) {
    return 'transient_retail';
  }

  if (
    /(discount|promotional|promo|opaque|\bota\b|online\s+travel|travel\s+agency|expedia|booking\.com|packages|\bpackage\b)/.test(s)
  ) {
    return 'transient_discount';
  }

  if (/\bgroup\b/.test(s)) return 'group_other';

  if (/\btransient\b/.test(s)) return 'transient_retail';

  return 'other';
}

function usaliBucketToDisplayName(bucket, originalMarketSegmentName = '') {
  const o = String(originalMarketSegmentName || '').trim();
  const m = {
    transient_retail: 'Retail Transient',
    transient_discount: 'Discount / OTA',
    transient_qualified: 'Qualified Rate',
    transient_negotiated: 'Negotiated',
    transient_wholesale: 'Wholesale Transient',
    group_corporate: 'Corporate Groups',
    group_association: 'Association & Convention',
    group_government: 'Government Groups',
    group_smerf: 'SMERF',
    group_wholesale: 'Wholesale Groups',
    group_other: 'Groups',
    contract: 'Contract / Crew'
  };
  if (bucket === 'other') return o || 'Unclassified';
  return m[bucket] || o || 'Unclassified';
}

function usaliBucketToLegacyFocusSegment(bucket) {
  if (!bucket || bucket === 'none') return 'retail';
  if (String(bucket).startsWith('group_')) return 'groups';
  if (bucket === 'transient_negotiated' || bucket === 'contract') return 'negotiated';
  if (bucket === 'other') return 'retail';
  if (String(bucket).startsWith('transient_')) return 'retail';
  return 'retail';
}

/** snapshotYmd: UTC calendar date of analysis run (upload / server time). */
function classifyRowAgainstSnapshot(stayYmd, snapshotYmd) {
  if (!snapshotYmd) return 'undated';
  if (!stayYmd) return 'undated';
  if (stayYmd <= snapshotYmd) return 'actualized';
  return 'future';
}

function filterStrRowsActualizedThroughSnapshot(strRows, snapshotYmd) {
  return (strRows || []).filter((row) => {
    const ymd = getRowStayDateYmd(row);
    if (!ymd) return false;
    return ymd <= snapshotYmd;
  });
}

function rowKeysIncludeForecastTy(row) {
  if (!row || typeof row !== 'object') return false;
  return Object.keys(row).some((k) => {
    const nk = normalizeKey(k);
    if (!nk.includes('forecast')) return false;
    if (nk.includes('ly') || nk.includes('last year') || nk.includes('stly')) return false;
    const v = row[k];
    if (v === null || v === undefined || `${v}`.trim() === '') return false;
    const n = toNumber(v);
    return n !== null;
  });
}

function rowKeysIncludeOnBooksTy(row) {
  if (!row || typeof row !== 'object') return false;
  return Object.keys(row).some((k) => {
    const nk = normalizeKey(k);
    if (nk.includes('ly') || nk.includes('last year') || nk.includes('stly')) return false;
    const looksOtb =
      nk.includes('on book') || nk.includes('on books') || (nk.includes('otb') && !nk.includes('ly'));
    if (!looksOtb) return false;
    const v = row[k];
    if (v === null || v === undefined || `${v}`.trim() === '') return false;
    return toNumber(v) !== null;
  });
}

function pmsFutureRowPhase(row) {
  if (rowKeysIncludeForecastTy(row)) return 'future_forecast';
  if (rowKeysIncludeOnBooksTy(row)) return 'future_otb';
  return 'future_otb';
}

function sheetHasStlyStyleColumns(sampleRow) {
  if (!sampleRow || typeof sampleRow !== 'object') return false;
  return Object.keys(sampleRow).some((k) => {
    const nk = normalizeKey(k);
    return nk.includes('ly') || nk.includes('last year') || nk.includes('stly');
  });
}

function attachIngestion(row, meta) {
  return { ...row, _ingestion: meta };
}

/**
 * PMS: classify rows; STLY exists only on this tab (flag at sheet level).
 * Engine path uses actualized + undated rows only (backward compatible if template has no dates).
 */
function normalizePmsRowsForIngestion(pmsRowsRaw, snapshotYmd) {
  const list = Array.isArray(pmsRowsRaw) ? pmsRowsRaw : [];

  const pmsDateDebugN = 3;
  for (let di = 0; di < Math.min(pmsDateDebugN, list.length); di += 1) {
    const row = list[di];
    const { raw, matchedHeaderKey } = findIngestionDateMatch(row);
    const parsedJs = parseExcelDate(raw);
    console.log('DEBUG PMS row date extraction', {
      rowIndex: di,
      matchedHeaderKey,
      rawDateCellValue: raw,
      parsedJsDate: parsedJs && !Number.isNaN(parsedJs.getTime()) ? parsedJs.toISOString() : null,
      stay_date_ymd: parsedJs && !Number.isNaN(parsedJs.getTime()) ? formatDateToYMD(parsedJs) : null
    });
  }

  const stlySupported = list.length ? sheetHasStlyStyleColumns(list[0]) : false;

  const counts = {
    actualized: 0,
    undated: 0,
    future_otb: 0,
    future_forecast: 0
  };

  const all = list.map((row) => {
    const stayYmd = getRowStayDateYmd(row);
    const coarse = classifyRowAgainstSnapshot(stayYmd, snapshotYmd);
    let rowPhase;
    if (coarse === 'undated') {
      rowPhase = 'undated';
      counts.undated += 1;
    } else if (coarse === 'actualized') {
      rowPhase = 'actualized';
      counts.actualized += 1;
    } else {
      rowPhase = pmsFutureRowPhase(row);
      if (rowPhase === 'future_forecast') counts.future_forecast += 1;
      else counts.future_otb += 1;
    }

    return attachIngestion(row, {
      tab: 'pms',
      stay_date_ymd: stayYmd,
      row_phase: rowPhase,
      stly_supported_tab: stlySupported,
      forward_kind: rowPhase.startsWith('future') ? 'otb_or_forecast' : null
    });
  });

  const rowsForEngine = all.filter(
    (r) => r._ingestion.row_phase === 'actualized' || r._ingestion.row_phase === 'undated'
  );

  return { all, rowsForEngine, counts, stly_supported_tab: stlySupported };
}

function normalizeCorporateRowsForIngestion(rowsRaw, snapshotYmd) {
  const list = Array.isArray(rowsRaw) ? rowsRaw : [];
  const counts = { actualized: 0, future_otb: 0, undated: 0 };

  const all = list.map((row) => {
    const stayYmd = getRowStayDateYmd(row);
    const base = classifyRowAgainstSnapshot(stayYmd, snapshotYmd);
    let rowPhase = base === 'future' ? 'future_otb' : base;
    if (rowPhase === 'actualized') counts.actualized += 1;
    else if (rowPhase === 'future_otb') counts.future_otb += 1;
    else counts.undated += 1;

    return attachIngestion(row, {
      tab: 'corporate_account_production',
      stay_date_ymd: stayYmd,
      row_phase: rowPhase,
      stly_supported_tab: false,
      forward_kind: rowPhase === 'future_otb' ? 'corporate_production_otb' : null
    });
  });

  return { all, counts };
}

function normalizeDelphiRowsForIngestion(rowsRaw, snapshotYmd) {
  const list = Array.isArray(rowsRaw) ? rowsRaw : [];
  const counts = { actualized: 0, future_otb: 0, undated: 0 };

  const all = list.map((row) => {
    const stayYmd = getRowStayDateYmd(row);
    const base = classifyRowAgainstSnapshot(stayYmd, snapshotYmd);
    let rowPhase = base === 'future' ? 'future_otb' : base;
    if (rowPhase === 'actualized') counts.actualized += 1;
    else if (rowPhase === 'future_otb') counts.future_otb += 1;
    else counts.undated += 1;

    return attachIngestion(row, {
      tab: 'delphi_groups_pipeline',
      stay_date_ymd: stayYmd,
      row_phase: rowPhase,
      stly_supported_tab: false,
      forward_kind: rowPhase === 'future_otb' ? 'group_pipeline' : null
    });
  });

  return { all, counts };
}

function buildWorkbookIngestionModel({
  snapshotYmd,
  strSheetName,
  strRowsRaw,
  strRowsActualized,
  pmsNormalized,
  corporateNormalized,
  delphiNormalized,
  pmsPaceComparator
}) {
  return {
    snapshot_date: snapshotYmd,
    snapshot_source: 'server_upload_time_utc',
    cutoff_rule: 'stay_date_on_or_before_snapshot_is_actualized; after_snapshot_is_forward',
    hotel_profile: { ingested: false, note: 'Hotel Profile tab is not read — unchanged by design.' },
    str: {
      sheet_resolved: strSheetName,
      str_actual_only_no_stly: true,
      row_counts: {
        raw: strRowsRaw.length,
        actualized_through_snapshot: strRowsActualized.length,
        excluded_future_or_undated: strRowsRaw.length - strRowsActualized.length
      }
    },
    pms: {
      stly_only_tab: true,
      stly_supported: pmsNormalized.stly_supported_tab,
      row_counts: pmsNormalized.counts,
      rows_classified_total: pmsNormalized.all.length
    },
    /** Normalized PMS comparator rows + readiness for future same-lead / weekly pace logic. */
    pms_pace_comparator: pmsPaceComparator || null,
    corporate_account_production: {
      row_counts: corporateNormalized.counts,
      rows_classified_total: corporateNormalized.all.length
    },
    delphi_groups_pipeline: {
      row_counts: delphiNormalized.counts,
      rows_classified_total: delphiNormalized.all.length
    },
    rows: {
      pms: pmsNormalized.all,
      corporate: corporateNormalized.all,
      delphi: delphiNormalized.all
    }
  };
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

function extractPeriodMetadata(strRows, snapshotDateYmd) {
  const snapshotYmd =
    snapshotDateYmd || formatDateToYMD(new Date());

  const candidateDates = strRows
    .map((row) => getRowValue(row, INGESTION_DATE_KEYS))
    .map(parseExcelDate)
    .filter(Boolean)
    .sort((a, b) => a.getTime() - b.getTime());

  if (!candidateDates.length) {
    return {
      snapshot_date: snapshotYmd,
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
    snapshot_date: snapshotYmd,
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

  const corporateHeaders = getHeadersFromSheetAliases(SHEET_ALIASES_CORPORATE);
  const delphiHeaders = getHeadersFromSheetAliases(SHEET_ALIASES_DELPHI);

  const has_str = strHeaders.length > 0;
  const has_mpi_ari_rgi =
    strHeaders.some((h) => h.includes('mpi')) &&
    strHeaders.some((h) => h.includes('ari')) &&
    strHeaders.some((h) => h.includes('rgi'));
  const has_segmentation = pmsHeaders.length > 0;
  const has_demand_data = false;
  /** STLY / LY comparators exist only on PMS in the locked template — not STR, corporate, or Delphi. */
  const has_ly = pmsHeaders.some((h) => h.includes('ly') || h.includes('last year') || h.includes('stly'));
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
      has_kpi_trend,
      has_corporate_production: corporateHeaders.length > 0,
      has_delphi_groups_pipeline: delphiHeaders.length > 0
    },
    confidence,
    detection_details: {
      sheets_found: sheets,
      str_headers: strHeaders,
      pms_headers: pmsHeaders,
      corporate_headers: corporateHeaders,
      delphi_headers: delphiHeaders
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
  const ariChange = averageMetric(strRows, ['ARI % Change', 'ARI %']);
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
    mpiVar: mpiChange,
    ariVar: ariChange,
    rgiVar: rgiChange,
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
      segment_analysis: [],
      active_signals: [],
      total_rn_ty: null,
      total_rn_ly: null,
      total_rev_ty: null,
      total_rev_ly: null,
      overall_adr_ty: null,
      overall_adr_ly: null,
      overall_adr_variance: null
    };
  }

  const pickNum = (...candidates) => {
    for (const c of candidates) {
      if (c === null || c === undefined || c === '') continue;
      const n = Number(c);
      if (Number.isFinite(n)) return n;
    }
    return 0;
  };

  // STEP 1 — Aggregate all segments from pmsRows (USALI buckets).
  const segmentData = {};
  for (const row of pmsRows) {
    const segName =
      row['Market Segment Name'] ||
      row['market segment name'] ||
      row['Market Segment'] ||
      '';
    const bucket = mapMarketSegmentNameToUsaliBucket(segName);
    const rnTY = pickNum(
      row['Room Nights TY (Actual / OTB)'],
      row['Occupancy On Books This Year']
    );
    const rnLY = pickNum(
      row['Room Nights LY Actual'],
      row['Occupancy On Books Last Year Actual']
    );
    const revTY = pickNum(
      row['Revenue TY (Actual / OTB)'],
      row['Booked Room Revenue This Year']
    );
    const revLY = pickNum(
      row['Revenue LY Actual'],
      row['Booked Room Revenue Last Year Actual']
    );
    if (!segmentData[bucket]) {
      segmentData[bucket] = { rnTY: 0, rnLY: 0, revTY: 0, revLY: 0 };
    }
    segmentData[bucket].rnTY += rnTY;
    segmentData[bucket].rnLY += rnLY;
    segmentData[bucket].revTY += revTY;
    segmentData[bucket].revLY += revLY;
  }

  const totalRnTY = Object.values(segmentData).reduce((s, d) => s + d.rnTY, 0);
  const totalRnLY = Object.values(segmentData).reduce((s, d) => s + d.rnLY, 0);
  const totalRevTY = Object.values(segmentData).reduce((s, d) => s + d.revTY, 0);
  const totalRevLY = Object.values(segmentData).reduce((s, d) => s + d.revLY, 0);

  const overallAdrTy = totalRnTY > 0 ? totalRevTY / totalRnTY : null;
  const overallAdrLy = totalRnLY > 0 ? totalRevLY / totalRnLY : null;
  const overallAdrVariance =
    overallAdrTy != null && overallAdrLy != null && overallAdrLy !== 0
      ? (overallAdrTy - overallAdrLy) / overallAdrLy
      : null;

  // STEP 2 — segment_analysis (per bucket).
  const segment_analysis = Object.entries(segmentData).map(([bucket, data]) => {
    const rnGrowth = data.rnLY > 0 ? (data.rnTY - data.rnLY) / data.rnLY : 0;
    const revGrowth = data.revLY > 0 ? (data.revTY - data.revLY) / data.revLY : 0;
    const adrTY = data.rnTY > 0 ? data.revTY / data.rnTY : null;
    const adrLY = data.rnLY > 0 ? data.revLY / data.rnLY : null;
    const adrVariance =
      adrTY != null && adrLY != null ? (adrTY - adrLY) / adrLY : 0;
    const shareTY = totalRnTY > 0 ? data.rnTY / totalRnTY : 0;
    const shareLY = totalRnLY > 0 ? data.rnLY / totalRnLY : 0;
    const shareShift = shareTY - shareLY;
    const tier = getSegmentRatingTier(bucket);
    return {
      segment: bucket,
      rnGrowth,
      revGrowth,
      adrTY,
      adrLY,
      adrVariance,
      shareTY,
      shareLY,
      shareShift,
      tier
    };
  });

  // STEP 3 — active_signals (all buckets with material volume / rate / mix movement).
  const active_signals = [];
  for (const row of segment_analysis) {
    const bucket = row.segment;
    const types = [];
    if (Math.abs(row.rnGrowth) > 0.08) types.push('volume');
    if (Math.abs(row.adrVariance) > 0.05) types.push('rate');
    if (Math.abs(row.shareShift) > 0.02) types.push('mix');
    if (!types.length) continue;
    active_signals.push({
      bucket,
      display_name: usaliBucketToDisplayName(bucket),
      tier: row.tier,
      rnGrowth: row.rnGrowth,
      adrVariance: row.adrVariance,
      shareShift: row.shareShift,
      signal_types: types
    });
  }

  // STEP 4 — primary focus_segment (backward-compatible string).
  let focus_segment = 'retail';
  if (diagnosis?.diagnosis_type === 'compression_mismanagement') {
    focus_segment = 'groups';
  } else if (diagnosis?.diagnosis_type === 'healthy') {
    focus_segment = 'none';
  } else if (active_signals.length) {
    const top = [...active_signals].sort(
      (a, b) => Math.abs(b.rnGrowth) - Math.abs(a.rnGrowth)
    )[0];
    focus_segment = usaliBucketToLegacyFocusSegment(top.bucket);
  } else {
    focus_segment = 'retail';
  }

  let focus_reason;
  if (diagnosis?.diagnosis_type === 'compression_mismanagement') {
    focus_reason =
      'Portfolio diagnosis indicates compression mismanagement — prioritizing groups segment for coordination and displacement risk.';
  } else if (diagnosis?.diagnosis_type === 'healthy') {
    focus_reason =
      'STR diagnosis is healthy — no single segment focus; segment mix signals are informational only.';
  } else if (active_signals.length) {
    const topSig = [...active_signals].sort(
      (a, b) => Math.abs(b.rnGrowth) - Math.abs(a.rnGrowth)
    )[0];
    const typeLabel = (topSig.signal_types || []).join(', ');
    focus_reason = `Strongest material segment signal: ${topSig.display_name} (${typeLabel || 'signals'}) — mapped to ${focus_segment} focus for downstream compatibility.`;
  } else {
    focus_reason =
      'No segment crossed volume, rate, or mix signal thresholds; defaulting to retail focus for the weekly reasoning pipeline.';
  }

  return {
    focus_segment,
    focus_reason,
    segment_analysis,
    active_signals,
    total_rn_ty: totalRnTY,
    total_rn_ly: totalRnLY,
    total_rev_ty: totalRevTY,
    total_rev_ly: totalRevLY,
    overall_adr_ty: overallAdrTy,
    overall_adr_ly: overallAdrLy,
    overall_adr_variance: overallAdrVariance
  };
}

function buildDriverFromDiagnosis(diagnosis, focus, strRows = [], pmsRows = []) {
  const avgMPI = Number(diagnosis?.metrics?.avgMPI || 0);
  const avgARI = Number(diagnosis?.metrics?.avgARI || 0);
  const avgRGI = Number(diagnosis?.metrics?.avgRGI || 0);
  const mpiVar = diagnosis?.mpiVar ?? null;
  const ariVar = diagnosis?.ariVar ?? null;
  const rgiVar = diagnosis?.rgiVar ?? null;
  const avgOcc = Number(diagnosis?.metrics?.avgOcc || 0);
  const trendStatus = diagnosis?.trend_status || 'stable';
  const focusSegment = focus?.focus_segment || 'other';

  function safeNum(value) {
    const n = Number(value);
    return Number.isFinite(n) ? n : 0;
  }

  const USALI_BUCKETS_FOR_ADR = [
    'transient_retail',
    'transient_discount',
    'transient_qualified',
    'transient_negotiated',
    'transient_wholesale',
    'group_corporate',
    'group_association',
    'group_government',
    'group_smerf',
    'group_wholesale',
    'group_other',
    'contract',
    'other'
  ];
  const segmentBuckets = Object.fromEntries(USALI_BUCKETS_FOR_ADR.map((k) => [k, []]));

  function getSegmentName(row = {}) {
    return (
      row['Market Segment Name'] ||
      row['market segment name'] ||
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

  for (const row of pmsRows) {
    const segment = mapMarketSegmentNameToUsaliBucket(getSegmentName(row));
    const adr = getSegmentADR(row);
    if (adr > 0 && segmentBuckets[segment]) segmentBuckets[segment].push(adr);
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

  const transientRetailMixKeys = [
    'transient_retail',
    'transient_discount',
    'transient_qualified',
    'transient_wholesale'
  ];
  const groupKeys = [
    'group_corporate',
    'group_association',
    'group_government',
    'group_smerf',
    'group_wholesale',
    'group_other'
  ];
  const flatAdrs = (keys) => keys.flatMap((k) => segmentBuckets[k] || []);

  const segmentAvgADR = {
    retail: avg(flatAdrs(transientRetailMixKeys)),
    negotiated: avg(segmentBuckets.transient_negotiated),
    groups: avg(flatAdrs(groupKeys)),
    other: avg([...(segmentBuckets.contract || []), ...(segmentBuckets.other || [])])
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
    const isRecovering = (mpiVar !== null && mpiVar > 0) && (rgiVar !== null && rgiVar > 0);
    const paceNotClosed = (mpiVar !== null && mpiVar > 0) && (rgiVar !== null && rgiVar > 0) && avgRGI < 100;

    if (isRecovering) {
      return {
        ...result,
        primary_driver: 'pricing',
        secondary_driver: null,
        driver_reason: paceNotClosed
          ? 'Hotel is in intentional volume recovery: ARI below market but MPI and RGI are improving vs LY. Rate discount is the strategy. Risk is that pace has not yet fully closed — monitor whether volume materializes before extending rate concessions further.'
          : 'Hotel is in intentional volume recovery: ARI below market and MPI improving vs LY. Strategy appears to be working. No correction required; monitor rate dilution risk.',
        rule_triggered: 'intentional_volume_recovery',
        confidence: 'medium'
      };
    }

    return {
      ...result,
      primary_driver: 'conversion',
      secondary_driver: null,
      driver_reason:
        'Hotel is trading below market rate without generating sufficient share gain, and performance is not improving vs LY. Rate is not the ceiling — execution is failing to convert available demand.',
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

/** Same metric bundle as narrative/finding text; used by dashboard KPI trigger (per issue). */
function snapshotCardMetricsFromDiagnosisLike(diagnosisLike) {
  if (!diagnosisLike || typeof diagnosisLike !== 'object') {
    return {
      avgMPI: null,
      avgARI: null,
      avgRGI: null,
      avgOcc: null,
      trend_status: null
    };
  }
  const m = diagnosisLike.metrics || {};
  return {
    avgMPI: m.avgMPI ?? null,
    avgARI: m.avgARI ?? null,
    avgRGI: m.avgRGI ?? null,
    avgOcc: m.avgOcc ?? null,
    trend_status: diagnosisLike.trend_status ?? null
  };
}

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
  const raw = getRowValue(row, INGESTION_DATE_KEYS);
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

function parseYmdToUtcDate(ymd) {
  if (!ymd || typeof ymd !== 'string') return null;
  const m = ymd.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return null;
  const y = Number(m[1]);
  const mo = Number(m[2]);
  const d = Number(m[3]);
  const dt = new Date(Date.UTC(y, mo - 1, d));
  return Number.isNaN(dt.getTime()) ? null : dt;
}

// --- PMS pace comparator (internal; no issue engine / UI dependency) ---

function pmsNormHeaderLyNotStly(nk) {
  if (nk.includes('stly') || nk.includes('same time')) return false;
  return nk.includes('last year') || /\bly\b/.test(nk);
}

function pmsNormHeaderIsStly(nk) {
  return nk.includes('stly') || nk.includes('same time last year') || nk.includes('same time ly');
}

function pickPmsNumericByHeader(row, testFn) {
  if (!row || typeof row !== 'object') return { value: null, source_key: null };
  for (const [k, v] of Object.entries(row)) {
    if (k === '_ingestion') continue;
    const nk = normalizeKey(k);
    if (!testFn(nk)) continue;
    const n = toNumber(v);
    if (n !== null) return { value: n, source_key: k };
  }
  return { value: null, source_key: null };
}

function matchPmsRnTyOnBooks(nk) {
  if (!nk.includes('room') || !nk.includes('night')) return false;
  if (nk.includes('forecast')) return false;
  if (pmsNormHeaderIsStly(nk)) return false;
  if (pmsNormHeaderLyNotStly(nk)) return false;
  if (!(nk.includes('on book') || nk.includes('otb'))) return false;
  return nk.includes('ty') || nk.includes('this year');
}

function matchPmsRnLyReference(nk) {
  if (!nk.includes('room') || !nk.includes('night')) return false;
  if (nk.includes('forecast')) return false;
  if (pmsNormHeaderIsStly(nk)) return false;
  return pmsNormHeaderLyNotStly(nk);
}

function matchPmsRnStly(nk) {
  if (!nk.includes('room') || !nk.includes('night')) return false;
  return pmsNormHeaderIsStly(nk);
}

function matchPmsRevTy(nk) {
  if (nk.includes('forecast')) return false;
  if (pmsNormHeaderIsStly(nk)) return false;
  if (pmsNormHeaderLyNotStly(nk)) return false;
  const revish =
    nk.includes('revenue') || nk.includes('booked rev') || (nk.includes('booked') && nk.includes('rev'));
  if (!revish) return false;
  return nk.includes('ty') || nk.includes('this year');
}

function matchPmsRevLy(nk) {
  if (nk.includes('forecast')) return false;
  if (pmsNormHeaderIsStly(nk)) return false;
  const revish =
    nk.includes('revenue') || nk.includes('booked rev') || (nk.includes('booked') && nk.includes('rev'));
  if (!revish) return false;
  return pmsNormHeaderLyNotStly(nk);
}

function matchPmsRevStly(nk) {
  if (nk.includes('forecast')) return false;
  const revish =
    nk.includes('revenue') || nk.includes('booked rev') || (nk.includes('booked') && nk.includes('rev'));
  if (!revish) return false;
  return pmsNormHeaderIsStly(nk);
}

function matchPmsForecastRnTy(nk) {
  if (!nk.includes('forecast')) return false;
  if (!nk.includes('room') || !nk.includes('night')) return false;
  return !pmsNormHeaderLyNotStly(nk) && !pmsNormHeaderIsStly(nk);
}

function matchPmsForecastRevTy(nk) {
  if (!nk.includes('forecast')) return false;
  if (!(nk.includes('revenue') || nk.includes('rev'))) return false;
  return !pmsNormHeaderLyNotStly(nk) && !pmsNormHeaderIsStly(nk);
}

function matchPmsForecastLy(nk) {
  if (!nk.includes('forecast')) return false;
  return pmsNormHeaderLyNotStly(nk);
}

function matchPmsAdrTy(nk) {
  if (nk.includes('forecast')) return false;
  if (pmsNormHeaderIsStly(nk) || pmsNormHeaderLyNotStly(nk)) return false;
  const adrish = nk.includes('adr') || nk.includes('average rate') || nk.includes('avg rate');
  if (!adrish) return false;
  return nk.includes('ty') || nk.includes('this year');
}

function matchPmsAdrLy(nk) {
  if (nk.includes('forecast')) return false;
  if (pmsNormHeaderIsStly(nk)) return false;
  const adrish = nk.includes('adr') || nk.includes('average rate') || nk.includes('avg rate');
  if (!adrish) return false;
  return pmsNormHeaderLyNotStly(nk);
}

function matchPmsAdrStly(nk) {
  if (nk.includes('forecast')) return false;
  const adrish = nk.includes('adr') || nk.includes('average rate') || nk.includes('avg rate');
  if (!adrish) return false;
  return pmsNormHeaderIsStly(nk);
}

function getIsoWeekStayBoundsFromYmd(stayYmd) {
  const date = parseYmdToUtcDate(stayYmd);
  if (!date) {
    return {
      stay_week_key: null,
      stay_week_start_ymd: null,
      stay_week_end_ymd: null
    };
  }
  const stay_week_key = weekBucketKeyFromDate(date);
  const dow = date.getUTCDay();
  const mondayOffset = dow === 0 ? -6 : 1 - dow;
  const mon = new Date(date.getTime());
  mon.setUTCDate(date.getUTCDate() + mondayOffset);
  const sun = new Date(mon.getTime());
  sun.setUTCDate(mon.getUTCDate() + 6);
  return {
    stay_week_key,
    stay_week_start_ymd: formatDateToYMD(mon),
    stay_week_end_ymd: formatDateToYMD(sun)
  };
}

function extractPmsMarketSegmentLabel(row) {
  const raw =
    getRowValue(row, [
      'Market Segment Name',
      'market segment name',
      'Segment',
      'segment',
      'Segment Name',
      'segment name'
    ]) || '';
  return raw.toString().trim();
}

function safeDivideRevByRn(rev, rn) {
  if (rev === null || rn === null) return null;
  if (!Number.isFinite(rev) || !Number.isFinite(rn) || rn === 0) return null;
  return rev / rn;
}

/**
 * One normalized PMS comparator row: TY/LY/STLY metrics + weekly + readiness for future pace engine.
 */
function buildPmsPaceComparatorRow(row, snapshotYmd, rowIndex) {
  const ing = row._ingestion || {};
  const stayYmd = ing.stay_date_ymd || getRowStayDateYmd(row);
  const week = getIsoWeekStayBoundsFromYmd(stayYmd);

  let lead_days_to_stay = null;
  if (stayYmd && snapshotYmd) {
    const sStay = parseYmdToUtcDate(stayYmd);
    const sSnap = parseYmdToUtcDate(snapshotYmd);
    if (sStay && sSnap) {
      lead_days_to_stay = Math.round((sStay.getTime() - sSnap.getTime()) / 86400000);
    }
  }

  const rnTy = pickPmsNumericByHeader(row, matchPmsRnTyOnBooks);
  const rnLy = pickPmsNumericByHeader(row, matchPmsRnLyReference);
  const rnStly = pickPmsNumericByHeader(row, matchPmsRnStly);
  const revTy = pickPmsNumericByHeader(row, matchPmsRevTy);
  const revLy = pickPmsNumericByHeader(row, matchPmsRevLy);
  const revStly = pickPmsNumericByHeader(row, matchPmsRevStly);
  const fcRnTy = pickPmsNumericByHeader(row, matchPmsForecastRnTy);
  const fcRevTy = pickPmsNumericByHeader(row, matchPmsForecastRevTy);
  const fcLy = pickPmsNumericByHeader(row, matchPmsForecastLy);
  const adrTy = pickPmsNumericByHeader(row, matchPmsAdrTy);
  const adrLy = pickPmsNumericByHeader(row, matchPmsAdrLy);
  const adrStly = pickPmsNumericByHeader(row, matchPmsAdrStly);

  const derivedAdrTy = safeDivideRevByRn(revTy.value, rnTy.value);
  const derivedAdrLy = safeDivideRevByRn(revLy.value, rnLy.value);
  const derivedAdrStly = safeDivideRevByRn(revStly.value, rnStly.value);

  const rowPhase = ing.row_phase || 'undated';
  const futureWindow =
    rowPhase === 'future_otb' || rowPhase === 'future_forecast'
      ? 'future_forward'
      : rowPhase === 'actualized'
        ? 'past_actualized'
        : 'undated_or_unknown';

  const hasStay = Boolean(stayYmd);
  const hasTyRn = rnTy.value !== null;
  const hasLyRn = rnLy.value !== null;
  const hasStlyRn = rnStly.value !== null;
  const hasTyRev = revTy.value !== null;
  const hasLyRev = revLy.value !== null;
  const hasStlyRev = revStly.value !== null;

  return {
    row_index: rowIndex,
    date_identity: {
      stay_date_ymd: stayYmd,
      stay_week_key: week.stay_week_key,
      stay_week_start_ymd: week.stay_week_start_ymd,
      stay_week_end_ymd: week.stay_week_end_ymd
    },
    snapshot: {
      snapshot_date_ymd: snapshotYmd,
      lead_days_snapshot_to_stay: lead_days_to_stay
    },
    segment: {
      market_segment_label: extractPmsMarketSegmentLabel(row)
    },
    row_phase: rowPhase,
    future_window_class: futureWindow,
    comparators: {
      room_nights_ty_on_books: { value: rnTy.value, source_key: rnTy.source_key },
      room_nights_ly_reference: { value: rnLy.value, source_key: rnLy.source_key },
      room_nights_stly_on_books: { value: rnStly.value, source_key: rnStly.source_key },
      revenue_ty_booked: { value: revTy.value, source_key: revTy.source_key },
      revenue_ly_booked: { value: revLy.value, source_key: revLy.source_key },
      revenue_stly_booked: { value: revStly.value, source_key: revStly.source_key },
      forecast_room_nights_ty: { value: fcRnTy.value, source_key: fcRnTy.source_key },
      forecast_revenue_ty: { value: fcRevTy.value, source_key: fcRevTy.source_key },
      forecast_ly_slot: { value: fcLy.value, source_key: fcLy.source_key },
      adr_ty: { value: adrTy.value, source_key: adrTy.source_key },
      adr_ly: { value: adrLy.value, source_key: adrLy.source_key },
      adr_stly: { value: adrStly.value, source_key: adrStly.source_key },
      adr_derived_ty_from_rev_rn: derivedAdrTy,
      adr_derived_ly_from_rev_rn: derivedAdrLy,
      adr_derived_stly_from_rev_rn: derivedAdrStly
    },
    readiness: {
      has_stay_date: hasStay,
      has_ty_on_books_rn: hasTyRn,
      has_ly_rn: hasLyRn,
      has_stly_rn: hasStlyRn,
      has_ty_booked_revenue: hasTyRev,
      has_ly_booked_revenue: hasLyRev,
      has_stly_booked_revenue: hasStlyRev,
      same_lead_ty_vs_stly_rn_ready: hasStay && hasTyRn && hasStlyRn,
      same_lead_ty_vs_stly_rev_ready: hasStay && hasTyRev && hasStlyRev,
      weekly_rollup_ready: Boolean(week.stay_week_key && hasStay)
    }
  };
}

function buildPmsPaceComparatorLayer(pmsClassifiedRows, snapshotYmd, stlyTabFlag) {
  const list = Array.isArray(pmsClassifiedRows) ? pmsClassifiedRows : [];
  const pace_rows = list.map((row, i) => buildPmsPaceComparatorRow(row, snapshotYmd, i));

  const countReady = (pred) => pace_rows.filter(pred).length;

  const tyStlyRnReady = countReady((p) => p.readiness.same_lead_ty_vs_stly_rn_ready);
  const tyStlyRevReady = countReady((p) => p.readiness.same_lead_ty_vs_stly_rev_ready);

  const missingHints = [];
  if (!stlyTabFlag) {
    missingHints.push('PMS sheet headers do not indicate LY/STLY columns (no ly / last year / stly in headers).');
  }
  if (stlyTabFlag && tyStlyRnReady === 0) {
    missingHints.push(
      'No row matched both TY on-books room nights and STLY room nights — add or rename columns (e.g. Room Nights On Books TY + Room Nights STLY / same-time-last-year).'
    );
  }
  if (stlyTabFlag && tyStlyRevReady === 0) {
    missingHints.push(
      'No row matched both TY booked revenue and STLY booked revenue — add or rename columns for STLY revenue.'
    );
  }

  return {
    schema_version: 1,
    snapshot_date_ymd: snapshotYmd,
    note:
      'Comparator values are parsed from the current upload only. True same-lead historical pace requires prior snapshots or explicit as-of/STLY columns.',
    summary: {
      pace_row_count: pace_rows.length,
      rows_with_stay_date: countReady((p) => p.readiness.has_stay_date),
      rows_future_forward: countReady((p) => p.future_window_class === 'future_forward'),
      rows_past_actualized: countReady((p) => p.future_window_class === 'past_actualized'),
      ty_stly_rn_ready_row_count: tyStlyRnReady,
      ty_stly_rev_ready_row_count: tyStlyRevReady,
      stly_tab_headers_detected: Boolean(stlyTabFlag),
      template_sufficient_for_ty_vs_stly_rn_at_stay_date: tyStlyRnReady > 0,
      template_sufficient_for_ty_vs_stly_rev_at_stay_date: tyStlyRevReady > 0,
      same_lead_pace_comparison_globally_ready: false,
      missing_or_weak_columns_hint: missingHints
    },
    pace_rows
  };
}

/** Supabase table name for slim PMS pace history (see sql/pms_pace_snapshots.sql). */
const PMS_PACE_SNAPSHOTS_TABLE = 'pms_pace_snapshots';
const DECISION_TRACKING_TABLE = 'decision_tracking';
const DECISION_TARGET_METRICS = {
  pricing_resistance: { metric: 'avg_mpi', direction: 'up' },
  share_loss: { metric: 'avg_mpi', direction: 'up' },
  discount_inefficiency: { metric: 'avg_ari', direction: 'up' },
  compression_mismanagement: { metric: 'avg_rgi', direction: 'up' },
  visibility_gap: { metric: 'avg_mpi', direction: 'up' },
  healthy: { metric: 'avg_rgi', direction: 'hold' },
  forward_pace_risk: { metric: 'rn_on_books_ty', direction: 'up' },
  forecast_gap: { metric: 'rn_on_books_ty', direction: 'up' },
  mix_displacement: { metric: 'avg_ari', direction: 'up' },
  mix_concentration: { metric: 'avg_mpi', direction: 'up' },
  mix_rate_dilution: { metric: 'avg_ari', direction: 'up' },
  weekly_pickup_delta: { metric: 'rn_on_books_ty', direction: 'up' },
  corporate_pace_risk: { metric: 'rn_on_books_ty', direction: 'up' },
  corporate_pace_opportunity: { metric: 'rn_on_books_ty', direction: 'hold' },
  group_pipeline_gap: { metric: 'rn_on_books_ty', direction: 'up' }
};
/** Smaller than default 500 to reduce single-request statement time (PostgREST upsert). */
const PMS_PACE_SNAPSHOT_UPSERT_CHUNK_SIZE = 100;

/**
 * Slim rows for pms_pace_snapshots — only stay-dated PMS comparator rows (skips undated).
 */
function buildPmsPaceSnapshotRowsForPersistence({ hotelCode, snapshotDateYmd, pmsPaceComparator }) {
  const paceRows = pmsPaceComparator?.pace_rows;
  if (!Array.isArray(paceRows) || !paceRows.length || !hotelCode || !snapshotDateYmd) return [];

  const out = [];
  const nowIso = new Date().toISOString();

  for (const pr of paceRows) {
    const stay = pr.date_identity?.stay_date_ymd;
    if (!stay) continue;

    const c = pr.comparators || {};
    const adrTy =
      c.adr_ty?.value != null && Number.isFinite(Number(c.adr_ty.value))
        ? Number(c.adr_ty.value)
        : c.adr_derived_ty_from_rev_rn;
    const adrLy =
      c.adr_ly?.value != null && Number.isFinite(Number(c.adr_ly.value))
        ? Number(c.adr_ly.value)
        : c.adr_derived_ly_from_rev_rn;
    const adrStly =
      c.adr_stly?.value != null && Number.isFinite(Number(c.adr_stly.value))
        ? Number(c.adr_stly.value)
        : c.adr_derived_stly_from_rev_rn;

    out.push({
      hotel_code: hotelCode,
      snapshot_date: snapshotDateYmd,
      stay_date_ymd: stay,
      stay_week_key: pr.date_identity?.stay_week_key || null,
      stay_week_start_ymd: pr.date_identity?.stay_week_start_ymd || null,
      stay_week_end_ymd: pr.date_identity?.stay_week_end_ymd || null,
      market_segment_label: (pr.segment?.market_segment_label || '').trim(),
      source_row_index: Number.isFinite(Number(pr.row_index)) ? Number(pr.row_index) : 0,
      row_phase: pr.row_phase || null,
      future_window_class: pr.future_window_class || null,
      lead_days_snapshot_to_stay:
        pr.snapshot?.lead_days_snapshot_to_stay != null &&
        Number.isFinite(Number(pr.snapshot.lead_days_snapshot_to_stay))
          ? Number(pr.snapshot.lead_days_snapshot_to_stay)
          : null,
      rn_on_books_ty: c.room_nights_ty_on_books?.value ?? null,
      rn_ly_actual: c.room_nights_ly_reference?.value ?? null,
      rn_stly: c.room_nights_stly_on_books?.value ?? null,
      booked_revenue_ty: c.revenue_ty_booked?.value ?? null,
      booked_revenue_ly_actual: c.revenue_ly_booked?.value ?? null,
      booked_revenue_stly: c.revenue_stly_booked?.value ?? null,
      forecast_room_nights_ty: c.forecast_room_nights_ty?.value ?? null,
      forecast_revenue_ty: c.forecast_revenue_ty?.value ?? null,
      adr_ty: adrTy != null && Number.isFinite(adrTy) ? adrTy : null,
      adr_ly_actual: adrLy != null && Number.isFinite(adrLy) ? adrLy : null,
      adr_stly: adrStly != null && Number.isFinite(adrStly) ? adrStly : null,
      ready_ty_stly_rn: Boolean(pr.readiness?.same_lead_ty_vs_stly_rn_ready),
      ready_ty_stly_rev: Boolean(pr.readiness?.same_lead_ty_vs_stly_rev_ready),
      weekly_rollup_ready: Boolean(pr.readiness?.weekly_rollup_ready),
      updated_at: nowIso
    });
  }

  return out;
}

async function persistPmsPaceSnapshots(supabaseClient, rows) {
  if (!rows.length) return { ok: true, written: 0 };

  const maxRowsRaw = process.env.PMS_PACE_SNAPSHOT_UPSERT_MAX_ROWS;
  const hasManualCap =
    maxRowsRaw !== undefined && maxRowsRaw !== '' && Number.isFinite(Number(maxRowsRaw));
  const maxRowsCap = hasManualCap ? Math.max(0, Math.floor(Number(maxRowsRaw))) : null;
  const toWrite = hasManualCap ? rows.slice(0, maxRowsCap) : rows;
  console.log('DEBUG pms_pace_snapshots upsert row cap:', {
    envCapApplied: hasManualCap,
    cap: maxRowsCap,
    inputRowCount: rows.length,
    writingRowCount: toWrite.length
  });

  const chunkSize = PMS_PACE_SNAPSHOT_UPSERT_CHUNK_SIZE;
  let written = 0;
  const totalChunks = Math.ceil(toWrite.length / chunkSize) || 0;

  for (let i = 0; i < toWrite.length; i += chunkSize) {
    const chunkIndex = Math.floor(i / chunkSize);
    const chunk = toWrite.slice(i, i + chunkSize);
    const t0 = Date.now();
    console.log('DEBUG pms_pace_snapshots upsert chunk start:', {
      chunkIndex,
      totalChunks,
      chunkRowCount: chunk.length,
      writtenSoFar: written
    });
    const { error } = await supabaseClient.from(PMS_PACE_SNAPSHOTS_TABLE).upsert(chunk, {
      onConflict: 'hotel_code,snapshot_date,stay_date_ymd,market_segment_label,source_row_index'
    });
    const elapsedMs = Date.now() - t0;

    if (error) {
      console.log('DEBUG pms_pace_snapshots upsert chunk error timing:', { chunkIndex, elapsedMs });
      return { ok: false, written, error };
    }
    written += chunk.length;
    console.log('DEBUG pms_pace_snapshots upsert chunk ok:', {
      chunkIndex,
      chunkRowCount: chunk.length,
      elapsedMs,
      writtenAfterChunk: written
    });
  }

  return { ok: true, written };
}

function toFiniteNumberOrNull(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function pmsStaySegmentKey(stayDateYmd, marketSegmentLabel) {
  return `${stayDateYmd || ''}||${(marketSegmentLabel || '').toString().trim()}`;
}

function summarizeCurrentTyVsStlyFutureRows(currentFutureRows) {
  let comparableCount = 0;
  let tyAboveStlyCount = 0;
  let tyBelowStlyCount = 0;
  let tyEqualStlyCount = 0;
  let missingStlyCount = 0;

  for (const row of currentFutureRows) {
    const ty = toFiniteNumberOrNull(row.rn_on_books_ty);
    const stly = toFiniteNumberOrNull(row.rn_stly);
    if (ty === null || stly === null) {
      missingStlyCount += 1;
      continue;
    }
    comparableCount += 1;
    if (ty > stly) tyAboveStlyCount += 1;
    else if (ty < stly) tyBelowStlyCount += 1;
    else tyEqualStlyCount += 1;
  }

  return {
    comparable_count: comparableCount,
    ty_above_stly_count: tyAboveStlyCount,
    ty_below_stly_count: tyBelowStlyCount,
    ty_equal_stly_count: tyEqualStlyCount,
    missing_stly_count: missingStlyCount
  };
}

function buildPmsSnapshotHistorySummary({ snapshotDateYmd, currentRows, historicalRows }) {
  const currentFutureRows = (currentRows || []).filter(
    (r) => r?.future_window_class === 'future_forward' && r?.stay_date_ymd
  );
  const historicalList = Array.isArray(historicalRows) ? historicalRows : [];

  if (!currentFutureRows.length) {
    return {
      schema_version: 1,
      snapshot_date_ymd: snapshotDateYmd,
      coverage: {
        current_future_rows: 0,
        historical_rows_scanned: historicalList.length,
        comparable_stay_segment_pairs: 0,
        exact_same_lead_matches: 0
      },
      by_stay_date: [],
      by_stay_week: [],
      strongest_gainers: [],
      strongest_decliners: [],
      ty_vs_stly_future_flags: summarizeCurrentTyVsStlyFutureRows(currentFutureRows),
      data_quality_notes: ['No future PMS rows available for snapshot-history comparison.']
    };
  }

  const currentAgg = new Map();
  for (const row of currentFutureRows) {
    const key = pmsStaySegmentKey(row.stay_date_ymd, row.market_segment_label);
    if (!currentAgg.has(key)) {
      currentAgg.set(key, {
        stay_date_ymd: row.stay_date_ymd,
        stay_week_key: row.stay_week_key || null,
        market_segment_label: (row.market_segment_label || '').toString().trim(),
        rn_on_books_ty: 0,
        rn_stly: 0,
        hasTy: false,
        hasStly: false
      });
    }
    const target = currentAgg.get(key);
    const ty = toFiniteNumberOrNull(row.rn_on_books_ty);
    const stly = toFiniteNumberOrNull(row.rn_stly);
    if (ty !== null) {
      target.rn_on_books_ty += ty;
      target.hasTy = true;
    }
    if (stly !== null) {
      target.rn_stly += stly;
      target.hasStly = true;
    }
  }

  const priorByKeyAndSnapshot = new Map();
  for (const row of historicalList) {
    const stay = row?.stay_date_ymd;
    if (!stay) continue;
    const seg = (row.market_segment_label || '').toString().trim();
    const snapshot = row.snapshot_date;
    if (!snapshot || snapshot >= snapshotDateYmd) continue;
    const key = pmsStaySegmentKey(stay, seg);
    const keySnap = `${key}||${snapshot}`;
    if (!priorByKeyAndSnapshot.has(keySnap)) {
      priorByKeyAndSnapshot.set(keySnap, {
        stay_date_ymd: stay,
        stay_week_key: row.stay_week_key || null,
        market_segment_label: seg,
        snapshot_date: snapshot,
        rn_on_books_ty: 0,
        rn_stly: 0,
        hasTy: false,
        hasStly: false
      });
    }
    const target = priorByKeyAndSnapshot.get(keySnap);
    const ty = toFiniteNumberOrNull(row.rn_on_books_ty);
    const stly = toFiniteNumberOrNull(row.rn_stly);
    if (ty !== null) {
      target.rn_on_books_ty += ty;
      target.hasTy = true;
    }
    if (stly !== null) {
      target.rn_stly += stly;
      target.hasStly = true;
    }
  }

  const latestPriorByKey = new Map();
  for (const snapshotAgg of priorByKeyAndSnapshot.values()) {
    const key = pmsStaySegmentKey(snapshotAgg.stay_date_ymd, snapshotAgg.market_segment_label);
    const prev = latestPriorByKey.get(key);
    if (!prev || snapshotAgg.snapshot_date > prev.snapshot_date) {
      latestPriorByKey.set(key, snapshotAgg);
    }
  }

  let comparisons = 0;
  let exactLeadMatchRows = 0;
  let noExactLeadMatchRows = 0;
  const weeklyRollup = new Map();
  const byStayDate = [];

  const histByStaySegLead = new Map();
  for (const row of historicalList) {
    const stay = row?.stay_date_ymd;
    if (!stay) continue;
    const seg = (row.market_segment_label || '').toString().trim();
    const snapshot = row.snapshot_date;
    const lead = toFiniteNumberOrNull(row.lead_days_snapshot_to_stay);
    if (!snapshot || snapshot >= snapshotDateYmd || lead === null) continue;
    const key = `${pmsStaySegmentKey(stay, seg)}||${lead}`;
    const prev = histByStaySegLead.get(key);
    if (!prev || snapshot > prev.snapshot_date) {
      histByStaySegLead.set(key, row);
    }
  }

  for (const row of currentFutureRows) {
    const lead = toFiniteNumberOrNull(row.lead_days_snapshot_to_stay);
    if (lead === null) {
      noExactLeadMatchRows += 1;
    } else {
      const leadKey = `${pmsStaySegmentKey(row.stay_date_ymd, row.market_segment_label)}||${lead}`;
      if (histByStaySegLead.has(leadKey)) exactLeadMatchRows += 1;
      else noExactLeadMatchRows += 1;
    }
  }

  for (const current of currentAgg.values()) {
    if (!current.hasTy) continue;
    const key = pmsStaySegmentKey(current.stay_date_ymd, current.market_segment_label);
    const prior = latestPriorByKey.get(key);
    if (!prior || !prior.hasTy) continue;

    const delta = current.rn_on_books_ty - prior.rn_on_books_ty;
    comparisons += 1;
    byStayDate.push({
      stay_date_ymd: current.stay_date_ymd,
      market_segment_label: current.market_segment_label,
      stay_week_key: current.stay_week_key || null,
      current_snapshot_date: snapshotDateYmd,
      prior_snapshot_date: prior.snapshot_date,
      rn_on_books_ty_current: current.rn_on_books_ty,
      rn_on_books_ty_prior: prior.rn_on_books_ty,
      rn_on_books_ty_delta: delta,
      trend: delta > 0 ? 'up' : delta < 0 ? 'down' : 'flat'
    });

    const weekKey = current.stay_week_key || 'unknown_week';
    if (!weeklyRollup.has(weekKey)) {
      weeklyRollup.set(weekKey, {
        stay_week_key: weekKey,
        compared_rows: 0,
        net_rn_on_books_ty_delta: 0
      });
    }
    const week = weeklyRollup.get(weekKey);
    week.compared_rows += 1;
    week.net_rn_on_books_ty_delta += delta;
  }

  const byStayWeek = Array.from(weeklyRollup.values())
    .map((w) => ({
      ...w,
      trend:
        w.net_rn_on_books_ty_delta > 0
          ? 'strengthening'
          : w.net_rn_on_books_ty_delta < 0
            ? 'weakening'
            : 'flat'
    }))
    .sort((a, b) => Math.abs(b.net_rn_on_books_ty_delta) - Math.abs(a.net_rn_on_books_ty_delta))
    .slice(0, 24);

  const sortedByDelta = [...byStayDate].sort((a, b) => b.rn_on_books_ty_delta - a.rn_on_books_ty_delta);
  const strongestGainers = sortedByDelta.filter((r) => r.rn_on_books_ty_delta > 0).slice(0, 12);
  const strongestDecliners = [...sortedByDelta]
    .reverse()
    .filter((r) => r.rn_on_books_ty_delta < 0)
    .slice(0, 12);
  const byStayDateLimited = sortedByDelta
    .sort((a, b) => {
      if (a.stay_date_ymd === b.stay_date_ymd) return a.market_segment_label.localeCompare(b.market_segment_label);
      return a.stay_date_ymd.localeCompare(b.stay_date_ymd);
    })
    .slice(0, 200);

  const dataQualityNotes = [];
  if (historicalList.length === 0) {
    dataQualityNotes.push('No prior pms_pace_snapshots rows matched current future stay-date window.');
  }
  if (comparisons === 0) {
    dataQualityNotes.push(
      'No stay_date_ymd + market_segment_label pairs had both current TY and prior TY values for comparison.'
    );
  }
  if (noExactLeadMatchRows > exactLeadMatchRows) {
    dataQualityNotes.push('Same-lead exact matches are sparse; most comparisons use latest prior snapshot fallback.');
  }

  return {
    schema_version: 1,
    snapshot_date_ymd: snapshotDateYmd,
    coverage: {
      current_future_rows: currentFutureRows.length,
      historical_rows_scanned: historicalList.length,
      comparable_stay_segment_pairs: comparisons,
      exact_same_lead_matches: exactLeadMatchRows
    },
    by_stay_date: byStayDateLimited,
    by_stay_week: byStayWeek,
    strongest_gainers: strongestGainers,
    strongest_decliners: strongestDecliners,
    ty_vs_stly_future_flags: summarizeCurrentTyVsStlyFutureRows(currentFutureRows),
    data_quality_notes: dataQualityNotes
  };
}

function buildPaceSignalSummaryFromSnapshotHistory(snapshotHistorySummary) {
  const summary = snapshotHistorySummary || {};
  const coverage = summary.coverage || {};
  const byStayDate = Array.isArray(summary.by_stay_date) ? summary.by_stay_date : [];
  const byStayWeek = Array.isArray(summary.by_stay_week) ? summary.by_stay_week : [];
  const tyVsStly = summary.ty_vs_stly_future_flags || {};
  const qualityNotes = Array.isArray(summary.data_quality_notes) ? [...summary.data_quality_notes] : [];

  const comparablePairs = Number(coverage.comparable_stay_segment_pairs || 0);
  const currentFutureRows = Number(coverage.current_future_rows || 0);
  const exactSameLeadMatches = Number(coverage.exact_same_lead_matches || 0);
  const comparableTyVsStly = Number(tyVsStly.comparable_count || 0);
  const aboveCount = Number(tyVsStly.ty_above_stly_count || 0);
  const belowCount = Number(tyVsStly.ty_below_stly_count || 0);

  const hasMinimalCoverage = comparablePairs >= 8;
  const hasStrongCoverage = comparablePairs >= 20;
  const leadCoverageRatio = comparablePairs > 0 ? exactSameLeadMatches / comparablePairs : 0;
  const tyVsStlyCoverageRatio = comparableTyVsStly > 0 ? Math.max(aboveCount, belowCount) / comparableTyVsStly : 0;

  const coverageStatus =
    hasStrongCoverage && leadCoverageRatio >= 0.35
      ? 'strong'
      : hasMinimalCoverage
        ? 'moderate'
        : comparablePairs > 0
          ? 'limited'
          : 'insufficient';

  if (coverageStatus !== 'strong') {
    qualityNotes.push('Pace signal confidence is gated due to partial snapshot-history coverage.');
  }

  const signalAbsThreshold = coverageStatus === 'strong' ? 5 : coverageStatus === 'moderate' ? 8 : 12;
  const weekAbsThreshold = coverageStatus === 'strong' ? 10 : coverageStatus === 'moderate' ? 14 : 20;

  const dateLevelSignals = byStayDate
    .filter((r) => Math.abs(Number(r.rn_on_books_ty_delta || 0)) >= signalAbsThreshold)
    .map((r) => {
      const delta = Number(r.rn_on_books_ty_delta || 0);
      const magnitude = Math.abs(delta);
      const direction = delta > 0 ? 'strengthening' : delta < 0 ? 'weakening' : 'flat';
      const confidence =
        coverageStatus === 'strong'
          ? magnitude >= 10
            ? 'high'
            : 'medium'
          : coverageStatus === 'moderate'
            ? 'medium'
            : 'low';
      return {
        signal_type: direction === 'weakening' ? 'future_stay_date_weakening' : 'future_stay_date_strengthening',
        stay_date_ymd: r.stay_date_ymd,
        market_segment_label: r.market_segment_label,
        stay_week_key: r.stay_week_key || null,
        rn_on_books_ty_delta: delta,
        confidence,
        prior_snapshot_date: r.prior_snapshot_date
      };
    })
    .filter((s) => s.signal_type !== 'future_stay_date_strengthening' || s.rn_on_books_ty_delta > 0)
    .sort((a, b) => Math.abs(b.rn_on_books_ty_delta) - Math.abs(a.rn_on_books_ty_delta))
    .slice(0, 40);

  const weekLevelSignals = byStayWeek
    .filter((w) => Math.abs(Number(w.net_rn_on_books_ty_delta || 0)) >= weekAbsThreshold)
    .map((w) => {
      const delta = Number(w.net_rn_on_books_ty_delta || 0);
      return {
        signal_type: delta >= 0 ? 'future_week_strengthening' : 'future_week_weakening',
        stay_week_key: w.stay_week_key,
        net_rn_on_books_ty_delta: delta,
        compared_rows: Number(w.compared_rows || 0),
        confidence:
          coverageStatus === 'strong' && Number(w.compared_rows || 0) >= 2
            ? 'high'
            : coverageStatus === 'insufficient'
              ? 'low'
              : 'medium'
      };
    })
    .sort((a, b) => Math.abs(b.net_rn_on_books_ty_delta) - Math.abs(a.net_rn_on_books_ty_delta))
    .slice(0, 20);

  const tyVsStlySignals = [];
  if (comparableTyVsStly >= 6) {
    if (belowCount > 0) {
      tyVsStlySignals.push({
        signal_type: 'future_ty_below_stly',
        count: belowCount,
        comparable_count: comparableTyVsStly,
        ratio: comparableTyVsStly > 0 ? belowCount / comparableTyVsStly : 0,
        confidence: tyVsStlyCoverageRatio >= 0.55 && coverageStatus !== 'insufficient' ? 'high' : 'medium'
      });
    }
    if (aboveCount > 0) {
      tyVsStlySignals.push({
        signal_type: 'future_ty_above_stly',
        count: aboveCount,
        comparable_count: comparableTyVsStly,
        ratio: comparableTyVsStly > 0 ? aboveCount / comparableTyVsStly : 0,
        confidence: tyVsStlyCoverageRatio >= 0.55 && coverageStatus !== 'insufficient' ? 'high' : 'medium'
      });
    }
  } else if (currentFutureRows > 0) {
    qualityNotes.push('TY vs STLY comparisons are sparse for future windows; TY/STLY signals are low-confidence.');
  }

  const strongestHiddenRisks = [
    ...dateLevelSignals.filter((s) => s.rn_on_books_ty_delta < 0),
    ...weekLevelSignals.filter((s) => s.net_rn_on_books_ty_delta < 0)
  ]
    .sort(
      (a, b) =>
        Math.abs(
          Number(b.rn_on_books_ty_delta ?? b.net_rn_on_books_ty_delta ?? 0)
        ) - Math.abs(Number(a.rn_on_books_ty_delta ?? a.net_rn_on_books_ty_delta ?? 0))
    )
    .slice(0, 12);

  const strongestHiddenOpportunities = [
    ...dateLevelSignals.filter((s) => s.rn_on_books_ty_delta > 0),
    ...weekLevelSignals.filter((s) => s.net_rn_on_books_ty_delta > 0)
  ]
    .sort(
      (a, b) =>
        Math.abs(
          Number(b.rn_on_books_ty_delta ?? b.net_rn_on_books_ty_delta ?? 0)
        ) - Math.abs(Number(a.rn_on_books_ty_delta ?? a.net_rn_on_books_ty_delta ?? 0))
    )
    .slice(0, 12);

  return {
    schema_version: 1,
    coverage_status: coverageStatus,
    date_level_signals: dateLevelSignals,
    week_level_signals: weekLevelSignals,
    ty_vs_stly_signals: tyVsStlySignals,
    strongest_hidden_risks: strongestHiddenRisks,
    strongest_hidden_opportunities: strongestHiddenOpportunities,
    data_quality_notes: qualityNotes
  };
}

function severityFromRnDelta(absDelta, tier) {
  if (tier === 'date') {
    if (absDelta >= 20) return 'high';
    if (absDelta >= 10) return 'medium';
    return 'low';
  }
  if (absDelta >= 40) return 'high';
  if (absDelta >= 20) return 'medium';
  return 'low';
}

function buildPaceCandidateIssuesFromPaceSignalSummary(paceSignalSummary) {
  const ps = paceSignalSummary || {};
  const coverageStatus = ps.coverage_status || 'insufficient';
  const qualityNotes = Array.isArray(ps.data_quality_notes) ? [...ps.data_quality_notes] : [];

  if (coverageStatus === 'insufficient') {
    qualityNotes.push(
      'pace_candidate_issues: date- and week-level candidates suppressed (coverage_status insufficient).'
    );
  }

  const candidates = [];
  const allowDateWeek = coverageStatus !== 'insufficient';

  if (allowDateWeek) {
    for (const s of ps.date_level_signals || []) {
      const absD = Math.abs(Number(s.rn_on_books_ty_delta || 0));
      if (s.signal_type === 'future_stay_date_weakening') {
        candidates.push({
          issue_family: 'future pace weakening',
          signal_source: 'snapshot_history_date_delta',
          title: `Future pace weakening: ${s.stay_date_ymd} · ${s.market_segment_label || 'segment'}`,
          severity: severityFromRnDelta(absD, 'date'),
          confidence: s.confidence === 'high' ? 'high' : coverageStatus === 'strong' ? 'medium' : 'low',
          scope: {
            kind: 'stay_date_segment',
            stay_date_ymd: s.stay_date_ymd,
            market_segment_label: s.market_segment_label,
            stay_week_key: s.stay_week_key || null
          },
          evidence_summary: `TY on-books room nights vs latest prior snapshot: delta ${Number(s.rn_on_books_ty_delta)} (prior snapshot ${s.prior_snapshot_date}).`
        });
      } else if (s.signal_type === 'future_stay_date_strengthening' && Number(s.rn_on_books_ty_delta) > 0) {
        candidates.push({
          issue_family: 'future pace strengthening',
          signal_source: 'snapshot_history_date_delta',
          title: `Future pace strengthening: ${s.stay_date_ymd} · ${s.market_segment_label || 'segment'}`,
          severity: severityFromRnDelta(absD, 'date'),
          confidence: s.confidence === 'high' ? 'high' : coverageStatus === 'strong' ? 'medium' : 'low',
          scope: {
            kind: 'stay_date_segment',
            stay_date_ymd: s.stay_date_ymd,
            market_segment_label: s.market_segment_label,
            stay_week_key: s.stay_week_key || null
          },
          evidence_summary: `TY on-books room nights vs latest prior snapshot: delta +${Number(s.rn_on_books_ty_delta)} (prior snapshot ${s.prior_snapshot_date}).`
        });
      }
    }

    for (const w of ps.week_level_signals || []) {
      const delta = Number(w.net_rn_on_books_ty_delta || 0);
      const absD = Math.abs(delta);
      if (w.signal_type === 'future_week_weakening') {
        candidates.push({
          issue_family: 'future pace weakening',
          signal_source: 'snapshot_history_week_delta',
          title: `Future week pace weakening: ${w.stay_week_key}`,
          severity: severityFromRnDelta(absD, 'week'),
          confidence: w.confidence === 'high' ? 'high' : coverageStatus === 'strong' ? 'medium' : 'low',
          scope: {
            kind: 'stay_week',
            stay_week_key: w.stay_week_key,
            compared_rows: w.compared_rows
          },
          evidence_summary: `Net TY on-books change vs prior snapshot across week: ${delta} (rows compared: ${w.compared_rows}).`
        });
      } else if (w.signal_type === 'future_week_strengthening' && delta > 0) {
        candidates.push({
          issue_family: 'future pace strengthening',
          signal_source: 'snapshot_history_week_delta',
          title: `Future week pace strengthening: ${w.stay_week_key}`,
          severity: severityFromRnDelta(absD, 'week'),
          confidence: w.confidence === 'high' ? 'high' : coverageStatus === 'strong' ? 'medium' : 'low',
          scope: {
            kind: 'stay_week',
            stay_week_key: w.stay_week_key,
            compared_rows: w.compared_rows
          },
          evidence_summary: `Net TY on-books change vs prior snapshot across week: +${delta} (rows compared: ${w.compared_rows}).`
        });
      }
    }
  }

  for (const t of ps.ty_vs_stly_signals || []) {
    const ratio = Number(t.ratio || 0);
    const comparable = Number(t.comparable_count || 0);
    const count = Number(t.count || 0);
    const material =
      comparable >= 6 && (ratio >= 0.25 || count >= 8 || (ratio >= 0.15 && comparable >= 12));
    if (!material) continue;

    if (t.signal_type === 'future_ty_below_stly') {
      candidates.push({
        issue_family: 'future TY below STLY',
        signal_source: 'current_upload_ty_vs_stly_future_rows',
        title: `Future TY below STLY on ${count} of ${comparable} comparable future rows`,
        severity: ratio >= 0.45 ? 'high' : ratio >= 0.3 ? 'medium' : 'low',
        confidence: t.confidence === 'high' && coverageStatus !== 'limited' ? 'high' : 'medium',
        scope: { kind: 'future_window_aggregate', comparable_future_rows: comparable },
        evidence_summary: `Share of future rows where TY on-books RN < STLY: ${(ratio * 100).toFixed(1)}%.`
      });
    } else if (t.signal_type === 'future_ty_above_stly') {
      candidates.push({
        issue_family: 'future TY above STLY',
        signal_source: 'current_upload_ty_vs_stly_future_rows',
        title: `Future TY above STLY on ${count} of ${comparable} comparable future rows`,
        severity: ratio >= 0.45 ? 'high' : ratio >= 0.3 ? 'medium' : 'low',
        confidence: t.confidence === 'high' && coverageStatus !== 'limited' ? 'high' : 'medium',
        scope: { kind: 'future_window_aggregate', comparable_future_rows: comparable },
        evidence_summary: `Share of future rows where TY on-books RN > STLY: ${(ratio * 100).toFixed(1)}%.`
      });
    }
  }

  return {
    schema_version: 1,
    coverage_status: coverageStatus,
    candidates,
    strongest_hidden_risks: ps.strongest_hidden_risks || [],
    strongest_hidden_opportunities: ps.strongest_hidden_opportunities || [],
    data_quality_notes: qualityNotes
  };
}

function hiddenPaceIssueRankScore(candidate) {
  const severityBase = candidate.severity === 'high' ? 30 : candidate.severity === 'medium' ? 20 : 10;
  const confidenceBonus =
    candidate.confidence === 'high' ? 8 : candidate.confidence === 'medium' ? 4 : 1;
  const evidence = candidate.evidence_summary || '';
  const deltaMatch = evidence.match(/delta\s*\+?(-?\d+(\.\d+)?)/i);
  const ratioMatch = evidence.match(/(\d+(\.\d+)?)%/);
  const absDelta = deltaMatch ? Math.abs(Number(deltaMatch[1])) : 0;
  const ratioPct = ratioMatch ? Number(ratioMatch[1]) : 0;
  return severityBase + confidenceBonus + Math.min(30, absDelta) + Math.min(20, ratioPct / 5);
}

function paceCandidateToHiddenRetailLikeIssue(candidate, idx) {
  const familyToDriver = {
    'future pace weakening': 'visibility',
    'future pace strengthening': 'pricing',
    'future TY below STLY': 'conversion',
    'future TY above STLY': 'pricing'
  };
  const issueFamilyNormalized = (candidate.issue_family || '').toLowerCase();
  const findingKey = `PACE_HIDDEN_${issueFamilyNormalized.replace(/[^a-z0-9]+/gi, '_')}_${idx + 1}`;
  return {
    finding_key: findingKey,
    issue_family: issueFamilyNormalized,
    driver: familyToDriver[candidate.issue_family] || 'pricing',
    segment: 'retail',
    priority: candidate.severity === 'high' ? 'high' : 'medium',
    title: candidate.title,
    finding: candidate.evidence_summary,
    root_cause: candidate.signal_source,
    expected_outcome:
      candidate.issue_family === 'future pace weakening' || candidate.issue_family === 'future TY below STLY'
        ? 'Early hidden warning: future retail pace softness may require corrective pricing/conversion actions.'
        : 'Early hidden opportunity: future retail pace strength may support selective rate/mix optimization.',
    rule_triggered: candidate.issue_family,
    confidence: candidate.confidence || 'low',
    scope: candidate.scope || null,
    signal_source: candidate.signal_source || null,
    hidden_rank_score: hiddenPaceIssueRankScore(candidate)
  };
}

function buildHiddenRankedPaceIssuesFromCandidates(paceCandidateIssues) {
  const pci = paceCandidateIssues || {};
  const coverageStatus = pci.coverage_status || 'insufficient';
  const notes = Array.isArray(pci.data_quality_notes) ? [...pci.data_quality_notes] : [];
  const candidateList = Array.isArray(pci.candidates) ? pci.candidates : [];

  const allowDateWeek = coverageStatus === 'moderate' || coverageStatus === 'strong';
  if (!allowDateWeek) {
    notes.push('Hidden pace date/week ranked issues suppressed until coverage_status reaches moderate.');
  }

  const filtered = candidateList.filter((c) => {
    const fam = c.issue_family;
    const isDateWeek = fam === 'future pace weakening' || fam === 'future pace strengthening';
    if (isDateWeek && !allowDateWeek) return false;
    if (fam === 'future TY below STLY' || fam === 'future TY above STLY') return true;
    return false;
  });

  const hiddenIssues = filtered
    .map((c, i) => paceCandidateToHiddenRetailLikeIssue(c, i))
    .sort((a, b) => Number(b.hidden_rank_score || 0) - Number(a.hidden_rank_score || 0))
    .slice(0, 20);

  return {
    schema_version: 1,
    coverage_status: coverageStatus,
    hidden_ranked_issues: hiddenIssues,
    strongest_hidden_risks: pci.strongest_hidden_risks || [],
    strongest_hidden_opportunities: pci.strongest_hidden_opportunities || [],
    data_quality_notes: notes
  };
}

async function readPmsPaceHistoricalRowsForFutureWindow({
  supabaseClient,
  hotelCode,
  snapshotDateYmd,
  currentRows
}) {
  const currentFutureRows = (currentRows || []).filter(
    (r) => r?.future_window_class === 'future_forward' && r?.stay_date_ymd
  );
  if (!currentFutureRows.length || !hotelCode || !snapshotDateYmd) return [];

  const stayDates = Array.from(new Set(currentFutureRows.map((r) => r.stay_date_ymd))).sort();
  const minStay = stayDates[0];
  const maxStay = stayDates[stayDates.length - 1];

  const pageSize = 1000;
  const out = [];
  let page = 0;
  const maxPages = 50;

  while (page < maxPages) {
    const from = page * pageSize;
    const to = from + pageSize - 1;
    const { data, error } = await supabaseClient
      .from(PMS_PACE_SNAPSHOTS_TABLE)
      .select(
        'snapshot_date,stay_date_ymd,stay_week_key,market_segment_label,lead_days_snapshot_to_stay,rn_on_books_ty,rn_stly'
      )
      .eq('hotel_code', hotelCode)
      .lt('snapshot_date', snapshotDateYmd)
      .gte('stay_date_ymd', minStay)
      .lte('stay_date_ymd', maxStay)
      .order('snapshot_date', { ascending: false })
      .range(from, to);

    if (error) throw error;
    if (!Array.isArray(data) || !data.length) break;

    out.push(...data);
    if (data.length < pageSize) break;
    page += 1;
  }

  const relevantKeySet = new Set(
    currentFutureRows.map((r) => pmsStaySegmentKey(r.stay_date_ymd, r.market_segment_label))
  );
  return out.filter((r) => relevantKeySet.has(pmsStaySegmentKey(r.stay_date_ymd, r.market_segment_label)));
}

function spanDaysInclusive(startYmd, endYmd) {
  const s = parseYmdToUtcDate(startYmd);
  const e = parseYmdToUtcDate(endYmd);
  if (!s || !e) return null;
  return Math.floor((e.getTime() - s.getTime()) / 86400000) + 1;
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

// Phase 2: Groups actualized STR daily rows by calendar month key (YYYY-MM).
// Mirrors groupStrRowsByCalendarWeek conceptually but uses month boundaries instead of ISO weeks.
function groupStrRowsByCalendarMonth(strRows) {
  const byMonth = new Map();
  for (const row of strRows) {
    const ymd = getRowStayDateYmd(row);
    if (!ymd) continue;
    const monthKey = ymd.slice(0, 7); // 'YYYY-MM'
    if (!byMonth.has(monthKey)) byMonth.set(monthKey, []);
    byMonth.get(monthKey).push(row);
  }
  return byMonth;
}

// Phase 2: Main monthly granularity function.
// Slices STR rows by calendar month, computes per-month metrics,
// compares each month to the period average, and generates a card
// only for months where at least 2 metrics breach their divergence threshold.
// Returns an array of monthly issue objects using the same schema as enrichedIssues.
function buildMonthlyIssues(strRows, pmsRows, diagnosis) {
  const byMonth = groupStrRowsByCalendarMonth(strRows);
  const sortedMonthKeys = [...byMonth.keys()].sort();

  // Need at least 2 months to have meaningful divergence comparison.
  if (sortedMonthKeys.length < 2) return [];

  // --- Compute period-level weighted averages for comparison baseline ---
  // Occ weighted by STR day count; simple average for MPI/ARI/RGI per qualifying month.
  let totalDays = 0;
  let sumOcc = 0;
  let sumMPI = 0;
  let sumARI = 0;
  let sumRGI = 0;
  // Per-index month counts: each index is averaged only over months where that index exists (avoids skew if one column is sparse).
  let mpiMonthCount = 0;
  let ariMonthCount = 0;
  let rgiMonthCount = 0;

  const monthMetrics = new Map();

  for (const mk of sortedMonthKeys) {
    const rows = byMonth.get(mk);
    // A month qualifies if it has 20+ days (complete or near-complete month)
    // OR if it is the most recent month in the dataset (MTD — current partial month).
    // The MTD month gets a confidence of 'low' automatically via the existing dayCount check.
    const isMostRecentMonth = mk === sortedMonthKeys[sortedMonthKeys.length - 1];
    const meetsMinDays = rows && rows.length >= MONTHLY_MIN_DAYS;
    if (!rows || (!meetsMinDays && !isMostRecentMonth)) continue;

    const metrics = buildWindowMetricsFromRows(rows);
    monthMetrics.set(mk, { rows, metrics, dayCount: rows.length });

    totalDays += rows.length;
    if (metrics.avgOcc !== null) sumOcc += metrics.avgOcc * rows.length;
    if (metrics.avgMPI !== null) {
      sumMPI += metrics.avgMPI;
      mpiMonthCount += 1;
    }
    if (metrics.avgARI !== null) {
      sumARI += metrics.avgARI;
      ariMonthCount += 1;
    }
    if (metrics.avgRGI !== null) {
      sumRGI += metrics.avgRGI;
      rgiMonthCount += 1;
    }
  }

  if (monthMetrics.size < 2) return [];

  const periodAvgOcc = totalDays > 0 ? sumOcc / totalDays : null;
  const periodAvgMPI = mpiMonthCount > 0 ? sumMPI / mpiMonthCount : null;
  const periodAvgARI = ariMonthCount > 0 ? sumARI / ariMonthCount : null;
  const periodAvgRGI = rgiMonthCount > 0 ? sumRGI / rgiMonthCount : null;

  const monthNames = [
    'January',
    'February',
    'March',
    'April',
    'May',
    'June',
    'July',
    'August',
    'September',
    'October',
    'November',
    'December'
  ];

  const results = [];

  for (const mk of sortedMonthKeys) {
    const entry = monthMetrics.get(mk);
    if (!entry) continue; // month was below MONTHLY_MIN_DAYS — skip silently

    const { rows, metrics, dayCount } = entry;
    const [yearStr, monthStr] = mk.split('-');
    const monthLabel = `${monthNames[parseInt(monthStr, 10) - 1]} ${yearStr}`;

    // --- Count how many metrics breach their threshold ---
    let breachCount = 0;
    const breachDetails = {};

    if (periodAvgOcc !== null && metrics.avgOcc !== null) {
      const delta = Math.abs(metrics.avgOcc - periodAvgOcc);
      if (delta >= MONTHLY_OCC_DIVERGENCE_PT) {
        breachCount += 1;
        breachDetails.occ = delta;
      }
    }
    if (periodAvgMPI !== null && metrics.avgMPI !== null) {
      const delta = Math.abs(metrics.avgMPI - periodAvgMPI);
      if (delta >= MONTHLY_MPI_DIVERGENCE_PT) {
        breachCount += 1;
        breachDetails.mpi = delta;
      }
    }
    if (periodAvgARI !== null && metrics.avgARI !== null) {
      const delta = Math.abs(metrics.avgARI - periodAvgARI);
      if (delta >= MONTHLY_ARI_DIVERGENCE_PT) {
        breachCount += 1;
        breachDetails.ari = delta;
      }
    }
    if (periodAvgRGI !== null && metrics.avgRGI !== null) {
      const delta = Math.abs(metrics.avgRGI - periodAvgRGI);
      if (delta >= MONTHLY_RGI_DIVERGENCE_PT) {
        breachCount += 1;
        breachDetails.rgi = delta;
      }
    }

    // Require at least 2 metric breaches — single-metric divergence may be noise.
    if (breachCount < 2) continue;

    // --- Determine direction: is this month above or below period average? ---
    const occDir =
      metrics.avgOcc !== null && periodAvgOcc !== null
        ? metrics.avgOcc > periodAvgOcc
          ? 'above'
          : 'below'
        : null;
    const rgiDir =
      metrics.avgRGI !== null && periodAvgRGI !== null
        ? metrics.avgRGI > periodAvgRGI
          ? 'above'
          : 'below'
        : null;
    const direction = rgiDir || occDir || 'divergent';

    // --- Find dominant PMS segment for this month if data exists ---
    let dominantSegment = null;
    if (Array.isArray(pmsRows) && pmsRows.length > 0) {
      const monthStart = `${mk}-01`;
      const [y, m] = mk.split('-').map(Number);
      const lastDay = new Date(Date.UTC(y, m, 0)).getUTCDate();
      const monthEnd = `${mk}-${String(lastDay).padStart(2, '0')}`;
      const monthPmsRows = pmsRows.filter((r) => {
        const d = r?._ingestion?.stay_date_ymd;
        return d && d >= monthStart && d <= monthEnd;
      });
      if (monthPmsRows.length > 0) {
        const segRN = {};
        for (const r of monthPmsRows) {
          const name = r['Market Segment Name'] || r['market segment name'] || '';
          const bucket = mapMarketSegmentNameToUsaliBucket(name);
          const displayName = usaliBucketToDisplayName(bucket, name);
          const rn = toNumber(r['Room Nights TY (Actual / OTB)'] || r['Room Nights TY'] || 0) || 0;
          segRN[displayName] = (segRN[displayName] || 0) + rn;
        }
        const topSeg = Object.entries(segRN).sort((a, b) => b[1] - a[1])[0];
        if (topSeg) dominantSegment = topSeg[0];
      }
    }

    // --- Determine confidence ---
    // HIGH: ≥20 days STR + PMS present + 2+ breaches
    // MEDIUM: ≥20 days STR + no PMS, or exactly 2 breaches
    // LOW: 20-27 days STR (borderline complete month)
    let confidence = 'medium';
    if (dayCount >= 28 && dominantSegment !== null && breachCount >= 2) confidence = 'high';
    if (dayCount < 28) confidence = 'low';

    // --- Build narrative ---
    // Concise: month label, direction vs period, key metric deltas, segment if available.
    const occStr = metrics.avgOcc !== null ? `Occ ${metrics.avgOcc.toFixed(1)}%` : null;
    const mpiStr = metrics.avgMPI !== null ? `MPI ${metrics.avgMPI.toFixed(1)}` : null;
    const ariStr = metrics.avgARI !== null ? `ARI ${metrics.avgARI.toFixed(1)}` : null;
    const rgiStr = metrics.avgRGI !== null ? `RGI ${metrics.avgRGI.toFixed(1)}` : null;
    const metricsStr = [occStr, mpiStr, ariStr, rgiStr].filter(Boolean).join(', ');

    const periodOccStr = periodAvgOcc !== null ? `${periodAvgOcc.toFixed(1)}%` : 'period avg';
    const periodRgiStr = periodAvgRGI !== null ? `${periodAvgRGI.toFixed(1)}` : 'period avg';

    const segLine = dominantSegment ? `\n\nDominant segment: ${dominantSegment}.` : '';

    const narrative =
      `${monthLabel} performed ${direction} the period average across ${breachCount} metrics.\n\n` +
      `Month metrics: ${metricsStr}.\n` +
      `Period averages: Occ ${periodOccStr}, RGI ${periodRgiStr}.` +
      segLine;

    // --- Determine issue family from monthly metrics ---
    // Reuse the same diagnosis logic already in the engine.
    const monthDiag = buildDiagnosisFromSTR(rows);
    const monthFamily = monthDiag.diagnosis_type || 'share_loss';
    const monthDriver =
      monthDiag.diagnosis_type === 'pricing_resistance'
        ? 'pricing'
        : monthDiag.diagnosis_type === 'discount_inefficiency'
          ? 'conversion'
          : monthDiag.diagnosis_type === 'healthy'
            ? 'none'
            : 'visibility';

    // --- Decision line ---
    let decisionLine = '';
    if (direction === 'above') {
      decisionLine = `${monthLabel} outperformed the period — identify what drove this and replicate the conditions.`;
    } else {
      decisionLine = `${monthLabel} underperformed the period — isolate the cause before the pattern recurs.`;
    }

    // --- Build execution actions ---
    const executionActions = [];
    if (direction === 'below') {
      if (breachDetails.mpi)
        executionActions.push(
          `Audit share position in ${monthLabel}: MPI deviated ${breachDetails.mpi.toFixed(1)} pts from period average.`
        );
      if (breachDetails.ari)
        executionActions.push(
          `Review rate positioning for ${monthLabel}: ARI deviated ${breachDetails.ari.toFixed(1)} pts from period average.`
        );
      if (dominantSegment)
        executionActions.push(
          `Examine ${dominantSegment} segment performance for ${monthLabel} — it was the dominant demand source.`
        );
    } else {
      executionActions.push(`Document what drove ${monthLabel} outperformance and build a replication framework.`);
      if (dominantSegment)
        executionActions.push(
          `${dominantSegment} was dominant in ${monthLabel} — assess whether this mix can be replicated in equivalent months.`
        );
    }
    if (!executionActions.length)
      executionActions.push(`Review full commercial mix for ${monthLabel} against period benchmarks.`);

    results.push({
      // Stable unique key for this monthly card
      finding_key: `MONTHLY_${mk}_${monthFamily}`,
      // Granularity flag so frontend can distinguish from period cards
      granularity: 'monthly',
      month_key: mk,
      month_label: monthLabel,
      title: `${monthLabel} — ${direction === 'above' ? 'Outperformance' : 'Underperformance'} vs Period`,
      issue_family: monthFamily,
      primary_driver: monthDriver,
      priority: direction === 'below' ? (breachCount >= 4 ? 'high' : 'medium') : 'medium',
      confidence,
      commercial_narrative: narrative,
      enforced_decision_line: decisionLine,
      enforced_execution_actions: executionActions,
      card_metrics: {
        avgMPI: metrics.avgMPI,
        avgARI: metrics.avgARI,
        avgRGI: metrics.avgRGI,
        avgOcc: metrics.avgOcc,
        trend_status: monthDiag.trend_status
      },
      period_comparison: {
        period_avg_occ: periodAvgOcc,
        period_avg_mpi: periodAvgMPI,
        period_avg_ari: periodAvgARI,
        period_avg_rgi: periodAvgRGI,
        breach_count: breachCount,
        breach_details: breachDetails,
        direction
      },
      dominant_segment: dominantSegment,
      day_count: dayCount
    });
  }

  return results;
}

/**
 * Phase 2: Forecast vs OTB Gap analysis.
 * Compares the hotel's own forecast (Forecasted Room Nights TY, Forecasted Revenue TY)
 * against current OTB (Room Nights TY Actual/OTB, Revenue TY Actual/OTB).
 * Produces gap cards (shortfall) and beat cards (ahead of plan) per 30-day window.
 * Entirely separate from buildForwardIssuesFromPmsOtb which compares OTB vs LY.
 *
 * @param {object[]} allPmsRows  - pmsNormalized.all (includes future rows with _ingestion)
 * @param {object[]} strRows     - actualized STR rows (used for capacity inference only)
 * @param {object}   diagnosis   - engine diagnosis object (used for capacity inference)
 * @param {string}   snapshotYmd - current snapshot date YYYY-MM-DD
 */
function buildForecastGapIssues(allPmsRows, strRows, diagnosis, snapshotYmd) {
  // --- Guard: need future rows to proceed ---
  const futureRows = (allPmsRows || []).filter(
    (r) => r?._ingestion?.row_phase === 'future_otb' || r?._ingestion?.row_phase === 'future_forecast'
  );
  if (!futureRows.length) return [];

  // --- Guard: check that forecast columns are actually present ---
  // Sample up to 10 future rows — if none have forecast RN, return silently.
  const sampleRows = futureRows.slice(0, 10);
  const hasForecastColumns = sampleRows.some((r) => {
    const fRN = toNumber(r['Forecasted Room Nights TY'] ?? r['Forecasted Room Nights; TY']);
    return fRN !== null && fRN > 0;
  });
  if (!hasForecastColumns) return [];

  // --- Infer hotel capacity for the coverage floor check ---
  const hotelCapacity = inferHotelCapacityFromContext(strRows, diagnosis) || 100;

  // --- Helper: days from snapshot to a stay date ---
  const leadDays = (stayYmd) => {
    const s0 = parseYmdToUtcDate(snapshotYmd);
    const s1 = parseYmdToUtcDate(stayYmd);
    if (!s0 || !s1) return null;
    return Math.round((s1.getTime() - s0.getTime()) / 86400000);
  };

  // --- Helper: safe number extraction with multiple key fallbacks ---
  const safeGet = (row, ...keys) => {
    for (const k of keys) {
      const v = toNumber(row[k]);
      if (v !== null) return v;
    }
    return null;
  };

  // --- Helper: format date label for narrative ---
  const fmtDate = (ymd) => {
    if (!ymd || !/^\d{4}-\d{2}-\d{2}$/.test(ymd)) return ymd || '';
    const [y, m, d] = ymd.split('-').map(Number);
    const months = [
      'January',
      'February',
      'March',
      'April',
      'May',
      'June',
      'July',
      'August',
      'September',
      'October',
      'November',
      'December'
    ];
    return `${d} ${months[m - 1]} ${y}`;
  };

  // --- Helper: determine gap profile from RN and revenue gaps ---
  // Returns one of: 'double_miss' | 'volume_miss' | 'rate_miss' | 'beat' | 'on_plan'
  const classifyGapProfile = (rnGapPct, revGapPct) => {
    const rnMiss = rnGapPct > FORECAST_RN_GAP_PCT_THRESHOLD;
    const revMiss = revGapPct > FORECAST_REVENUE_GAP_THRESHOLD / 10000; // relative proxy
    const rnBeat = rnGapPct < -FORECAST_BEAT_RN_PCT_THRESHOLD;
    const revBeat = revGapPct < -FORECAST_BEAT_REV_PCT_THRESHOLD;
    if (rnBeat && revBeat) return 'beat';
    if (rnBeat && !revBeat) return 'beat_volume_rate_miss'; // volume ahead, rate diluted
    if (rnMiss && revMiss) return 'double_miss';
    if (rnMiss && !revMiss) return 'volume_miss';
    if (!rnMiss && revMiss) return 'rate_miss';
    return 'on_plan';
  };

  // --- Helper: gap severity for double_miss and volume_miss ---
  const classifyGapSeverity = (rnGapPct, revGap) => {
    if (rnGapPct > 0.25 && revGap > 20000) return 'critical';
    if (rnGapPct > 0.15 || revGap > 10000) return 'high';
    if (rnGapPct > 0.1 || revGap > 5000) return 'moderate';
    return 'low';
  };

  // --- Helper: derive decision line from profile, severity, and window ---
  const buildDecisionLine = (profile, severity, windowLabel, primarySeg, daysRemaining) => {
    const seg = primarySeg || 'the lagging segment';
    if (profile === 'beat') {
      return `Forecast is being exceeded — protect rate integrity and assess whether the forecast should be revised upward.`;
    }
    if (profile === 'beat_volume_rate_miss') {
      return `Room nights ahead of forecast but ADR is diluted — volume strategy is working; now focus on rate quality to convert volume into revenue.`;
    }
    if (profile === 'rate_miss') {
      return `Room night target is on plan but revenue is short — ADR dilution in ${seg} must be addressed before it compounds.`;
    }
    if (profile === 'volume_miss') {
      if (severity === 'critical' && daysRemaining <= 30) {
        return `Critical volume shortfall with ${daysRemaining} days remaining — immediate commercial intervention required on ${seg}.`;
      }
      if (severity === 'high' && daysRemaining <= 30) {
        return `Deploy targeted tactical offers on ${seg} now — ${daysRemaining} days remain and the gap will not self-close.`;
      }
      if (daysRemaining > 60) {
        return `Review forecast validity for ${windowLabel} — a gap of this size at this lead time may indicate a planning assumption error.`;
      }
      return `Activate demand stimulation on ${seg} — ${daysRemaining} days remain in this window.`;
    }
    if (profile === 'double_miss') {
      if (severity === 'critical') {
        return `Both volume and rate are materially below forecast — escalate to full commercial team review immediately.`;
      }
      if (severity === 'high' && daysRemaining <= 30) {
        return `Volume and rate both short with ${daysRemaining} days remaining — deploy tactical offers on ${seg} with rate floor protection.`;
      }
      if (daysRemaining > 60) {
        return `Double miss at long lead time — recalibrate forecast and launch early demand capture on ${seg}.`;
      }
      return `Address both volume and rate gaps on ${seg} — ${daysRemaining} days remain in this window.`;
    }
    return `Monitor ${windowLabel} forecast materialisation weekly.`;
  };

  // --- Helper: build execution actions ---
  const buildExecutionActions = (
    profile,
    severity,
    primarySeg,
    secondarySeg,
    rnGap,
    revGap,
    rnGapPct,
    windowEnd,
    daysRemaining
  ) => {
    const seg = primarySeg || 'primary segment';
    const seg2 = secondarySeg || null;
    const actions = [];

    if (profile === 'beat' || profile === 'beat_volume_rate_miss') {
      actions.push(
        `${seg} is the over-performing segment — validate that rate integrity is being maintained as volume builds.`
      );
      if (profile === 'beat_volume_rate_miss') {
        actions.push(
          `ADR is tracking below forecast despite volume strength — review discount depth and ensure rate floors are enforced.`
        );
      }
      actions.push(`Reassess the forecast for this window — current pace suggests the original plan was conservative.`);
      return actions;
    }

    // Gap cards
    if (rnGap > 0) {
      actions.push(
        `${seg} is the primary volume shortfall driver — ${Math.round(rnGap)} room nights needed by ${fmtDate(windowEnd)} to close the gap.`
      );
    }
    if (seg2) {
      actions.push(`${seg2} is a secondary contributor to the shortfall — include in demand recovery plan.`);
    }
    if (profile === 'double_miss' || profile === 'rate_miss') {
      actions.push(`ADR for this window is below forecast — enforce rate floors on ${seg} and avoid further discount depth.`);
    }
    if (severity === 'critical' || severity === 'high') {
      actions.push(
        `Set a weekly OTB checkpoint: if ${seg} does not recover at least ${Math.round(rnGap / Math.max(1, Math.ceil(daysRemaining / 7)))} room nights per week, escalate intervention.`
      );
    } else {
      actions.push(`Monitor ${seg} pickup weekly — intervene if gap does not reduce by at least 20% within two weeks.`);
    }
    return actions.slice(0, 3); // executive readability cap
  };

  // ─────────────────────────────────────────────────
  // MAIN WINDOW LOOP
  // ─────────────────────────────────────────────────
  const results = [];

  for (const win of FORECAST_WINDOWS) {
    // Filter future rows inside this lead window
    const windowRows = futureRows.filter((r) => {
      const stayYmd = r?._ingestion?.stay_date_ymd;
      if (!stayYmd) return false;
      const ld = leadDays(stayYmd);
      if (ld === null) return false;
      return ld >= win.minLead && ld <= win.maxLead;
    });

    if (!windowRows.length) continue;

    // Determine window date range for labelling
    const stayDates = windowRows
      .map((r) => r._ingestion.stay_date_ymd)
      .filter(Boolean)
      .sort();
    const windowStart = stayDates[0];
    const windowEnd = stayDates[stayDates.length - 1];
    const daysRemaining = win.maxLead - win.minLead + 1;

    // ── Aggregate forecast and OTB totals across all rows in window ──
    let totalForecastRN = 0;
    let totalOtbRN = 0;
    let totalForecastRev = 0;
    let totalOtbRev = 0;
    let totalStlyRN = 0;
    let forecastRNRowCount = 0;

    // Per-segment aggregation for decomposition
    const segData = {};

    for (const row of windowRows) {
      const segName = row['Market Segment Name'] || row['market segment name'] || '';
      const segBucket = mapMarketSegmentNameToUsaliBucket(segName);
      const segDisplay = usaliBucketToDisplayName(segBucket, segName);

      const forecastRN =
        safeGet(row, 'Forecasted Room Nights TY', 'Forecasted Room Nights; TY', 'Forecast Room Nights TY') || 0;
      const otbRN = safeGet(row, 'Room Nights TY (Actual / OTB)', 'Room Nights TY') || 0;
      const forecastRev =
        safeGet(row, 'Forecasted Revenue TY', 'Forecasted Revenue; TY', 'Forecast Revenue TY') || 0;
      const otbRev = safeGet(row, 'Revenue TY (Actual / OTB)', 'Revenue TY') || 0;
      const stlyRN = safeGet(row, 'Room Nights STLY', 'Room Nights LY Actual', 'Room Nights LY') || 0;

      totalForecastRN += forecastRN;
      totalOtbRN += otbRN;
      totalForecastRev += forecastRev;
      totalOtbRev += otbRev;
      totalStlyRN += stlyRN;
      if (forecastRN > 0) forecastRNRowCount += 1;

      // Segment rollup
      if (!segData[segDisplay]) {
        segData[segDisplay] = { forecastRN: 0, otbRN: 0, forecastRev: 0, otbRev: 0 };
      }
      segData[segDisplay].forecastRN += forecastRN;
      segData[segDisplay].otbRN += otbRN;
      segData[segDisplay].forecastRev += forecastRev;
      segData[segDisplay].otbRev += otbRev;
    }

    // ── Coverage floor check ──
    // Suppress if forecast is too thin to be meaningful
    const windowDays = win.maxLead - win.minLead + 1;
    const coverageFloor = Math.max(
      FORECAST_MIN_ABSOLUTE_RN,
      hotelCapacity * windowDays * FORECAST_MIN_WINDOW_COVERAGE_PCT
    );
    if (totalForecastRN < coverageFloor) continue;

    // ── Compute gaps ──
    const rnGap = totalForecastRN - totalOtbRN; // positive = shortfall
    const revGap = totalForecastRev - totalOtbRev; // positive = shortfall
    const rnGapPct = totalForecastRN > 0 ? rnGap / totalForecastRN : 0;
    const revGapPct = totalForecastRev > 0 ? revGap / totalForecastRev : 0;

    // Forecast realism check: compare forecast RN to STLY RN.
    // If forecast is more than 20% above STLY, it may be an aggressive plan.
    // If OTB is within 10% of STLY despite the forecast gap, the hotel is
    // tracking normally vs last year — the gap is a planning issue, not demand failure.
    const forecastVsStlyPct = totalStlyRN > 0 ? (totalForecastRN - totalStlyRN) / totalStlyRN : null;
    const otbVsStlyPct = totalStlyRN > 0 ? (totalOtbRN - totalStlyRN) / totalStlyRN : null;

    // Flag: forecast is aggressive (>20% above LY) AND OTB is close to LY (<10% gap)
    // This means the gap is a forecast error, not a demand collapse.
    const isForecastAggressive = forecastVsStlyPct !== null && forecastVsStlyPct > 0.2;
    const isOtbCloseToStly = otbVsStlyPct !== null && Math.abs(otbVsStlyPct) < 0.1;
    const isForecastGapNotDemandGap = isForecastAggressive && isOtbCloseToStly;

    // ── Derive ADR figures ──
    const forecastADR = totalForecastRN > 0 ? totalForecastRev / totalForecastRN : null;
    const otbADR = totalOtbRN > 0 ? totalOtbRev / totalOtbRN : null;
    const adrGap =
      forecastADR !== null && otbADR !== null ? forecastADR - otbADR : null; // positive = OTB ADR below forecast

    // ── Classify gap profile ──
    const profile = classifyGapProfile(rnGapPct, revGapPct);
    let severity =
      profile !== 'beat' && profile !== 'beat_volume_rate_miss' && profile !== 'on_plan'
        ? classifyGapSeverity(rnGapPct, revGap)
        : null;
    // Downgrade severity when gap is driven by aggressive forecasting, not demand failure.
    // A hotel tracking at LY level should not trigger critical intervention.
    if (isForecastGapNotDemandGap && severity === 'critical') severity = 'moderate';
    if (isForecastGapNotDemandGap && severity === 'high') severity = 'moderate';

    // ── Suppress on_plan windows — no card needed ──
    if (profile === 'on_plan') continue;

    // ── Find primary and secondary gap segments ──
    // Sort segments by absolute RN gap (forecast minus OTB), largest first
    const segEntries = Object.entries(segData)
      .map(([name, d]) => ({
        name,
        rnGap: d.forecastRN - d.otbRN,
        revGap: d.forecastRev - d.otbRev,
        forecastRN: d.forecastRN,
        otbRN: d.otbRN
      }))
      .filter((s) => s.forecastRN > 0); // only segments with a forecast

    // For gap cards: sort by largest shortfall first
    // For beat cards: sort by largest surplus first (most negative rnGap)
    const isBeat = profile === 'beat' || profile === 'beat_volume_rate_miss';
    segEntries.sort((a, b) => (isBeat ? a.rnGap - b.rnGap : b.rnGap - a.rnGap));

    const primarySeg = segEntries[0]?.name || null;
    const secondaryGap = segEntries[1];
    // Secondary segment named only if it contributes >30% of total gap
    const secondarySeg =
      secondaryGap && Math.abs(rnGap) > 0 && Math.abs(secondaryGap.rnGap) / Math.abs(rnGap) > 0.3
        ? secondaryGap.name
        : null;

    // ── STLY implied landing (conditional — only when STLY data is present) ──
    // Derives what the hotel is likely to land at based on last year's final RN
    // and the current OTB position vs STLY OTB at same lead.
    // Only adds a narrative sentence — does not change card type or decision.
    let impliedLandingLine = null;
    if (totalStlyRN > 0 && totalOtbRN > 0) {
      // Implied landing = current OTB + (STLY total − STLY OTB at same lead)
      // We approximate STLY-at-same-lead as totalStlyRN (from STLY column in PMS)
      // This is a directional estimate, not a precise pickup model.
      // Flagged as estimate in narrative — never presented as fact.
      const stlyPickupRemaining = Math.max(0, totalStlyRN - totalOtbRN);
      const impliedLanding = Math.round(totalOtbRN + stlyPickupRemaining);
      const impliedVsForecast = impliedLanding - totalForecastRN;
      if (Math.abs(impliedVsForecast / totalForecastRN) > 0.1) {
        // Only surface when implied landing differs from forecast by >10% — otherwise noise
        const direction = impliedVsForecast > 0 ? 'above' : 'below';
        impliedLandingLine =
          `Based on last year's pickup curve, implied landing is ~${impliedLanding} room nights` +
          ` — ${Math.abs(Math.round(impliedVsForecast))} room nights ${direction} the hotel forecast of ${Math.round(totalForecastRN)}.`;
      }
    }

    // ── Build narrative ──
    const rnGapAbs = Math.abs(Math.round(rnGap));
    const revGapAbs = Math.abs(Math.round(revGap));
    const rnGapPctFmt = `${Math.abs(rnGapPct * 100).toFixed(1)}%`;
    const revFmt = (v) =>
      new Intl.NumberFormat('en-GB', { maximumFractionDigits: 0 }).format(Math.round(v));
    const adrFmt = (v) => (v !== null ? `${v.toFixed(0)}` : 'n/a');

    // Signal paragraph — precise numbers, no editorialising
    let signalPara = '';
    if (isBeat) {
      signalPara =
        `${win.label}: ${fmtDate(windowStart)} → ${fmtDate(windowEnd)}. ` +
        `OTB is ${rnGapAbs} room nights ahead of forecast (${rnGapPctFmt} above plan). ` +
        `Forecast: ${Math.round(totalForecastRN)} RN at ADR ${adrFmt(forecastADR)}. ` +
        `OTB: ${Math.round(totalOtbRN)} RN at ADR ${adrFmt(otbADR)}.`;
    } else {
      signalPara =
        `${win.label}: ${fmtDate(windowStart)} → ${fmtDate(windowEnd)}. ` +
        `OTB is ${rnGapAbs} room nights short of forecast (${rnGapPctFmt} below plan). ` +
        `Forecast: ${Math.round(totalForecastRN)} RN / ${revFmt(totalForecastRev)} revenue. ` +
        `OTB: ${Math.round(totalOtbRN)} RN / ${revFmt(totalOtbRev)} revenue. ` +
        `Revenue gap: ${revFmt(revGapAbs)}.`;
    }

    // Analysis paragraph — segment decomposition + ADR + implied landing
    let analysisPara = '';
    if (primarySeg) {
      const pSegData = segData[primarySeg];
      const pRnGap = Math.round(pSegData.forecastRN - pSegData.otbRN);
      const pRnPct =
        pSegData.forecastRN > 0
          ? (((pSegData.forecastRN - pSegData.otbRN) / pSegData.forecastRN) * 100).toFixed(1)
          : '0.0';
      analysisPara = isBeat
        ? `${primarySeg} is the primary driver of the beat — OTB is ${Math.abs(pRnGap)} room nights ahead of its segment forecast.`
        : `${primarySeg} is the primary shortfall driver — ${Math.abs(pRnGap)} room nights short of its segment forecast (${pRnPct}% below plan).`;
      if (secondarySeg) {
        const sSegData = segData[secondarySeg];
        const sRnGap = Math.round(sSegData.forecastRN - sSegData.otbRN);
        analysisPara += ` ${secondarySeg} is a secondary contributor with ${Math.abs(sRnGap)} room nights of the shortfall.`;
      }
    }

    // ADR paragraph — only when ADR gap is meaningful (>5 currency units)
    let adrPara = '';
    if (adrGap !== null && Math.abs(adrGap) > 5) {
      if (adrGap > 0) {
        adrPara = `ADR is ${adrGap.toFixed(0)} below the forecast rate (OTB: ${adrFmt(otbADR)} vs forecast: ${adrFmt(forecastADR)}) — revenue shortfall is compounded by rate dilution.`;
      } else {
        adrPara = `ADR is ${Math.abs(adrGap).toFixed(0)} above the forecast rate (OTB: ${adrFmt(otbADR)} vs forecast: ${adrFmt(forecastADR)}) — rate is overdelivering; volume is the constraint.`;
      }
    }

    // Assemble full narrative
    // When the gap appears to be a planning issue rather than demand failure,
    // add an explicit forecast challenge paragraph to the analysis.
    let forecastChallengePara = null;
    if (isForecastGapNotDemandGap) {
      const stlyFmt = Math.round(totalStlyRN);
      const fcstFmt = Math.round(totalForecastRN);
      const otbFmt = Math.round(totalOtbRN);
      const pctAboveLY = Math.round(forecastVsStlyPct * 100);
      forecastChallengePara =
        `Forecast challenge: the hotel forecast of ${fcstFmt} room nights is ` +
        `${pctAboveLY}% above last year's ${stlyFmt} room nights. ` +
        `Current OTB of ${otbFmt} room nights is tracking within 10% of last year — ` +
        `the gap vs forecast is primarily a planning assumption, not a demand failure. ` +
        `Validate the forecast basis before triggering demand generation intervention.`;
    }
    const narrativeParts = [
      signalPara,
      analysisPara,
      adrPara,
      forecastChallengePara,
      impliedLandingLine
    ].filter(Boolean);
    const commercialNarrative = narrativeParts.join('\n\n');

    // ── Decision and execution ──
    const decisionLine = buildDecisionLine(profile, severity, win.label, primarySeg, daysRemaining);
    // Override decision framing when the gap is a forecast planning issue.
    const finalDecisionLine = isForecastGapNotDemandGap
      ? `Validate the forecast basis before acting — OTB is tracking close to last year. ` +
        `The gap reflects an aggressive plan, not a demand collapse. ` +
        `Reassess the forecast assumption before committing to demand generation spend.`
      : decisionLine;
    const executionActions = buildExecutionActions(
      profile,
      severity,
      primarySeg,
      secondarySeg,
      rnGap,
      revGap,
      rnGapPct,
      windowEnd,
      daysRemaining
    );

    // ── Priority and confidence ──
    let priority = 'medium';
    if (profile === 'beat' || profile === 'beat_volume_rate_miss') {
      priority = 'low'; // green cards are informational, not urgent
    } else if (severity === 'critical') {
      priority = 'high';
    } else if (severity === 'high') {
      priority = 'high';
    } else if (severity === 'moderate') {
      priority = 'medium';
    } else {
      priority = 'low';
    }

    // Confidence: high when both forecast and OTB columns populated across most rows
    const rowCoverage = forecastRNRowCount / Math.max(1, windowRows.length);
    const confidence = rowCoverage >= 0.8 ? 'high' : rowCoverage >= 0.5 ? 'medium' : 'low';

    // ── Card title ──
    const cardTitle = isBeat
      ? `Forecast beat — ${win.label} OTB is ahead of plan`
      : profile === 'rate_miss'
        ? `Rate shortfall vs forecast — ${win.label} volume on plan, revenue behind`
        : profile === 'volume_miss'
          ? `Volume shortfall vs forecast — ${win.label} room nights behind plan`
          : `Double miss vs forecast — ${win.label} volume and revenue both short`;

    results.push({
      // Stable unique key
      finding_key: `FCG_${win.minLead}_${win.maxLead}_${profile}`,
      // Granularity flag — distinct from forward_issues (LY pace) and monthly_issues
      granularity: 'forecast_gap',
      is_forward_card: true,
      issue_family: profile,
      primary_driver: isBeat ? 'pricing' : profile === 'rate_miss' ? 'pricing' : 'conversion',
      window_label: win.label,
      window_start: windowStart,
      window_end: windowEnd,
      forward_window: FORECAST_WINDOWS.indexOf(win) + 1,
      title: cardTitle,
      priority,
      confidence,
      gap_profile: profile,
      gap_severity: severity,
      commercial_narrative: commercialNarrative,
      enforced_decision_line: finalDecisionLine,
      enforced_execution_actions: executionActions,
      // Metrics for card header display
      card_metrics: {
        avgMPI: null, // STR indices not applicable to forecast gap cards
        avgARI: null,
        avgRGI: null,
        avgOcc:
          totalForecastRN > 0 ? Math.round((totalOtbRN / totalForecastRN) * 100 * 10) / 10 : null // repurposed: OTB as % of forecast
      },
      // Full quantification for downstream use
      quantification: {
        total_forecast_rn: Math.round(totalForecastRN),
        total_otb_rn: Math.round(totalOtbRN),
        total_forecast_rev: Math.round(totalForecastRev),
        total_otb_rev: Math.round(totalOtbRev),
        rn_gap: Math.round(rnGap),
        rn_gap_pct: Math.round(rnGapPct * 1000) / 10,
        rev_gap: Math.round(revGap),
        rev_gap_pct: Math.round(revGapPct * 1000) / 10,
        forecast_adr: forecastADR !== null ? Math.round(forecastADR) : null,
        otb_adr: otbADR !== null ? Math.round(otbADR) : null,
        adr_gap: adrGap !== null ? Math.round(adrGap) : null,
        stly_rn: Math.round(totalStlyRN),
        implied_landing_line: impliedLandingLine || null
      },
      primary_segment: primarySeg,
      secondary_segment: secondarySeg
    });
  }

  return results;
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
        endYmd: c.endYmd,
        segment_attribution_summary: c.segment_attribution_summary != null ? c.segment_attribution_summary : null,
        daily_validation_summary: c.daily_validation_summary != null ? c.daily_validation_summary : null,
        narrative_chain: c.narrative_chain != null ? c.narrative_chain : null,
        performance_story: c.performance_story != null ? c.performance_story : null,
        final_decision_rationale: c.final_decision_rationale != null ? c.final_decision_rationale : null,
        commercial_narrative: c.commercial_narrative != null ? c.commercial_narrative : null
      };
    } else {
      cur.weekKeys.push(c.weekKey);
      cur.weekOrdinals.push(c.weekOrdinal);
      cur.weekPayloads.push(c);
      cur.lastWeekOrdinal = c.weekOrdinal;
      cur.lastMetrics = c.metrics;
      cur.endYmd = c.endYmd;
      if (cur.segment_attribution_summary == null && c.segment_attribution_summary != null) {
        cur.segment_attribution_summary = c.segment_attribution_summary;
      }
      if (cur.daily_validation_summary == null && c.daily_validation_summary != null) {
        cur.daily_validation_summary = c.daily_validation_summary;
      }
      if (cur.narrative_chain == null && c.narrative_chain != null) cur.narrative_chain = c.narrative_chain;
      if (cur.performance_story == null && c.performance_story != null) cur.performance_story = c.performance_story;
      if (cur.final_decision_rationale == null && c.final_decision_rationale != null) {
        cur.final_decision_rationale = c.final_decision_rationale;
      }
      if (cur.commercial_narrative == null && c.commercial_narrative != null) {
        cur.commercial_narrative = c.commercial_narrative;
      }
    }
  }
  if (cur) episodes.push(cur);
  return episodes;
}

/**
 * Family-aware rank score (higher = more severe / more important for cap ordering).
 * Not MPI-only: blends signals that match how each issue family is detected.
 */
function episodeFamilyRankScore(ep) {
  const agg = aggregateEpisodeMetrics(ep.weekPayloads);
  const mpi = Number(agg.avgMPI);
  const ari = Number(agg.avgARI);
  const occ = Number(agg.avgOcc);
  const mpiGap = Number.isFinite(mpi) ? Math.max(0, 100 - mpi) : 0;
  const family = ep.family;

  if (family === 'pricing_resistance' || family === 'mix_constraint') {
    const ariPremiumGap = Number.isFinite(ari) ? Math.max(0, ari - 100) : 0;
    return mpiGap + ariPremiumGap * 0.65;
  }

  if (family === 'discount_inefficiency') {
    const ariUnder = Number.isFinite(ari) ? Math.max(0, 100 - ari) : 0;
    return mpiGap + ariUnder * 0.65;
  }

  if (family === 'missed_pricing_opportunity') {
    const occStrength = Number.isFinite(occ) ? Math.max(0, occ - 78) : 0;
    const ariUnder = Number.isFinite(ari) ? Math.max(0, 100 - ari) : 0;
    return occStrength * 0.85 + ariUnder * 1.1;
  }

  if (family === 'visibility_gap') {
    const worsening = ep.weekPayloads?.some((w) => w.trend === 'worsening') ? 1 : 0;
    return mpiGap + worsening * 14;
  }

  return mpiGap;
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
  const chain = ep.narrative_chain || {};
  const am = aggMetrics;
  const mpiF = toFiniteNumberOrNull(am?.avgMPI);
  const ariF = toFiniteNumberOrNull(am?.avgARI);
  const rgiF = toFiniteNumberOrNull(am?.avgRGI);
  const occF = toFiniteNumberOrNull(am?.avgOcc);
  const tailAriMpiRgi = [
    ariF !== null ? `ARI ${ariF.toFixed(1)}` : null,
    mpiF !== null ? `MPI ${mpiF.toFixed(1)}` : null,
    rgiF !== null ? `RGI ${rgiF.toFixed(1)}` : null
  ]
    .filter(Boolean)
    .join(', ');
  const tailMpiRgi = [mpiF !== null ? `MPI ${mpiF.toFixed(1)}` : null, rgiF !== null ? `RGI ${rgiF.toFixed(1)}` : null]
    .filter(Boolean)
    .join(', ');
  const tailOccAri = [occF !== null ? `Occ ${occF.toFixed(1)}%` : null, ariF !== null ? `ARI ${ariF.toFixed(1)}` : null]
    .filter(Boolean)
    .join(', ');
  const tailAriMpi = [ariF !== null ? `ARI ${ariF.toFixed(1)}` : null, mpiF !== null ? `MPI ${mpiF.toFixed(1)}` : null]
    .filter(Boolean)
    .join(', ');
  let findingFromAgg = '';
  if (ep.family === 'pricing_resistance') {
    findingFromAgg = tailAriMpiRgi
      ? `Rate premium above competitive set with share underperformance — ${tailAriMpiRgi}.`
      : 'Rate premium above competitive set with share underperformance.';
  } else if (ep.family === 'discount_inefficiency') {
    findingFromAgg = tailAriMpiRgi
      ? `Discounting below competitive set without proportional share recovery — ${tailAriMpiRgi}.`
      : 'Discounting below competitive set without proportional share recovery.';
  } else if (ep.family === 'visibility_gap') {
    findingFromAgg = tailMpiRgi
      ? `Share underperformance without a clear rate cause — ${tailMpiRgi}.`
      : 'Share underperformance without a clear rate cause.';
  } else if (ep.family === 'missed_pricing_opportunity') {
    findingFromAgg = tailOccAri
      ? `Occupancy strength not translating into rate capture — ${tailOccAri}.`
      : 'Occupancy strength not translating into rate capture.';
  } else if (ep.family === 'mix_constraint') {
    findingFromAgg = tailAriMpi
      ? `Segment mix limiting rate and share performance simultaneously — ${tailAriMpi}.`
      : 'Segment mix limiting rate and share performance simultaneously.';
  } else {
    findingFromAgg = tailAriMpiRgi
      ? `Commercial underperformance detected — ${tailAriMpiRgi}.`
      : 'Commercial underperformance detected.';
  }
  const findingText = findingFromAgg;
  const rootCauseText = [ep.final_decision_rationale, chain.segment_summary].filter(Boolean).join(' ');
  const expectedOutcomeText = [chain.daily_validation_summary_text, ep.final_decision_rationale].filter(Boolean).join(' ');

  return {
    finding_key,
    issue_family: ep.family,
    driver: ep.primary_driver,
    segment: 'retail',
    priority: pri,
    title: RETAIL_ISSUE_TITLES[ep.family] || RETAIL_ISSUE_TITLES.share_loss_fallback,
    finding: findingText || retailIssueFindingText(ep.family, windowDiagnosis, focus),
    root_cause: rootCauseText || RETAIL_ISSUE_ROOT_CAUSES[ep.family] || RETAIL_ISSUE_ROOT_CAUSES.share_loss_fallback,
    expected_outcome:
      expectedOutcomeText || RETAIL_ISSUE_EXPECTED_OUTCOMES[ep.family] || RETAIL_ISSUE_EXPECTED_OUTCOMES.share_loss_fallback,
    rule_triggered: ep.family,
    _library_actions: cappedLib,
    episode_week_start: ep.startYmd,
    episode_week_end: ep.endYmd,
    episode_week_keys: ep.weekKeys,
    episode_week_count: ep.weekKeys.length,
    window_label: `${ep.startYmd} → ${ep.endYmd}`,
    temporal_layer: 'weekly_episode',
    performance_story: ep.performance_story || null,
    segment_attribution_summary: ep.segment_attribution_summary || null,
    daily_validation_summary: ep.daily_validation_summary || null,
    final_decision_rationale: ep.final_decision_rationale || null,
    narrative_chain: ep.narrative_chain || null,
    card_metrics: {
      ...snapshotCardMetricsFromDiagnosisLike(windowDiagnosis),
      trend_status: lastTrend
    }
  };
}

function classifyEpisodeTypeForFamily(ep) {
  const agg = aggregateEpisodeMetrics(ep?.weekPayloads || []);
  const mpi = Number(agg?.avgMPI);
  const ari = Number(agg?.avgARI);
  const occ = Number(agg?.avgOcc);
  const trend = ep?.weekPayloads?.[ep.weekPayloads.length - 1]?.trend || 'stable';
  const family = ep?.family || 'unknown';

  if (family === 'pricing_resistance' || family === 'mix_constraint') {
    if (Number.isFinite(ari) && ari >= 108) return 'high_premium_rejection';
    if (Number.isFinite(ari) && ari >= 103) return 'moderate_premium_rejection';
    return 'marginal_premium_rejection';
  }

  if (family === 'discount_inefficiency') {
    if (Number.isFinite(ari) && ari <= 95 && Number.isFinite(mpi) && mpi <= 95) return 'deep_discount_low_share';
    if (Number.isFinite(ari) && ari <= 98) return 'discount_inefficiency_core';
    return 'conversion_led_inefficiency';
  }

  if (family === 'visibility_gap') {
    if (trend === 'worsening') return 'visibility_erosion';
    return 'visibility_plateau';
  }

  if (family === 'missed_pricing_opportunity') {
    if (Number.isFinite(occ) && occ >= 84) return 'high_occ_rate_gap';
    return 'occupancy_supported_rate_gap';
  }

  return 'generic_pattern';
}

function classifyConsolidatedTemporalPattern({ episodes = [], weekCount = 0, spanDays = null, isOngoingMost = false }) {
  const episodeCount = episodes.length;
  if (episodeCount <= 0) return 'isolated';

  const typeSet = new Set(
    episodes.map((ep) => classifyEpisodeTypeForFamily(ep)).filter(Boolean)
  );

  // Regime shift precedence: same family, materially different episode signatures over time.
  if (episodeCount >= 2 && typeSet.size >= 2) return 'regime_shift';

  // Persistent: one long continuous run, or dominates most of observed period.
  if ((episodeCount === 1 && weekCount >= 3) || isOngoingMost || (spanDays !== null && spanDays >= 21)) {
    return 'persistent';
  }

  if (episodeCount >= 2 || weekCount >= 3) return 'recurring';

  return 'isolated';
}

function temporalRankBonusByRecurrenceType(recurrenceType) {
  switch (recurrenceType) {
    case 'persistent':
      return 2;
    case 'regime_shift':
      return 1.5;
    case 'recurring':
      return 1;
    case 'isolated':
    default:
      return 0;
  }
}

/**
 * One executive top-level issue per retail issue family: same stable finding_key as non-weekly retail (RET_ISSUE_*).
 * Representative copy (title/finding/root/outcome/actions) comes from the highest-severity episode in the family.
 * Per-episode boundaries stay in executive_synthesis for drilldown.
 */
function consolidateRetailEpisodesToExecutiveIssues(episodes, focus, temporalContext = {}) {
  if (!episodes.length) return [];

  const byFam = new Map();
  for (const ep of episodes) {
    const fam = ep.family || 'unknown';
    if (!byFam.has(fam)) byFam.set(fam, []);
    byFam.get(fam).push(ep);
  }

  const out = [];
  for (const family of [...byFam.keys()].sort()) {
    const eps = byFam.get(family);
    const scored = eps.map((ep) => ({ ep, score: episodeFamilyRankScore(ep) }));
    scored.sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      const rec = (b.ep.endYmd || '').localeCompare(a.ep.endYmd || '');
      if (rec !== 0) return rec;
      return (b.ep.weekKeys?.length || 0) - (a.ep.weekKeys?.length || 0);
    });

    const rep = scored[0].ep;
    const repWithContext =
      scored.find(
        ({ ep }) => ep?.segment_attribution_summary != null && ep?.daily_validation_summary != null
      )?.ep || rep;
    let minStart = null;
    let maxEnd = null;
    const weekKeySet = new Set();
    let maxRank = 0;
    const episodeSummaries = [];

    for (const { ep, score } of scored) {
      maxRank = Math.max(maxRank, score);
      for (const wk of ep.weekKeys || []) weekKeySet.add(wk);
      const s = ep.startYmd || '';
      const e = ep.endYmd || '';
      if (!minStart || s < minStart) minStart = ep.startYmd;
      if (!maxEnd || e > maxEnd) maxEnd = ep.endYmd;
      episodeSummaries.push({
        episode_finding_key: episodeFindingKey(ep.family, ep.startYmd, ep.endYmd),
        start_ymd: ep.startYmd,
        end_ymd: ep.endYmd,
        week_keys: [...(ep.weekKeys || [])],
        week_count: ep.weekKeys?.length || 0,
        episode_rank_score: score
      });
    }
    episodeSummaries.sort((a, b) => (a.start_ymd || '').localeCompare(b.start_ymd || ''));
    const weekKeysSorted = [...weekKeySet].sort();
    const spanDays = spanDaysInclusive(minStart, maxEnd);
    const observedWeekCount = Number(temporalContext?.observed_week_count || 0);
    const issueCoverageRatio = observedWeekCount > 0 ? (weekKeysSorted.length / observedWeekCount) : 0;
    const isOngoingMost = issueCoverageRatio >= 0.6;
    const recurrenceType = classifyConsolidatedTemporalPattern({
      episodes: eps,
      weekCount: weekKeysSorted.length,
      spanDays,
      isOngoingMost
    });
    const representativeEpisodeType = classifyEpisodeTypeForFamily(rep);
    const rankBonus = temporalRankBonusByRecurrenceType(recurrenceType);

    const materialized = materializeRetailEpisode(rep, focus);
    const weeklySorted = Array.isArray(temporalContext?.weekly_windows)
      ? [...temporalContext.weekly_windows].sort((a, b) =>
          String(b.week_key || '').localeCompare(String(a.week_key || ''))
        )
      : [];
    const latestWin = weeklySorted[0];
    const wr = latestWin?.weekly_reasoning;
    const fdRecent = wr?.final_decision;
    const recentFam = fdRecent?.issue_family || null;
    const recentConf = wr?.segment_attribution?.attribution_confidence || null;
    const recentPriDriver = fdRecent?.primary_driver || null;
    let execIssue = { ...materialized };
    if (
      recentFam &&
      recentFam !== family &&
      recentConf === 'high' &&
      recentFam !== 'unknown'
    ) {
      const mappedDriver =
        recentPriDriver ||
        mapRetailIssueFamilyToDriver(recentFam, null, []);
      const ivrTitle =
        'Retail volume strategy is rebuilding share while ADR runs below last year — monitor rate dilution risk';
      const ivrRoot =
        'Recent weeks show deliberate volume-led recovery: MPI and pickup are improving versus last year while ARI is discounted; the priority is to protect margin as pace closes.';
      const ivrOutcome =
        'Maintain the volume trajectory while tightening rate floors on forward peak dates so share gains are not funded by uncapped ADR erosion.';
      execIssue = {
        ...execIssue,
        issue_family: recentFam,
        driver: mappedDriver,
        primary_driver: mappedDriver,
        title:
          RETAIL_ISSUE_TITLES[recentFam] ||
          (recentFam === 'intentional_volume_recovery' ? ivrTitle : RETAIL_ISSUE_TITLES.share_loss_fallback),
        root_cause:
          RETAIL_ISSUE_ROOT_CAUSES[recentFam] ||
          (recentFam === 'intentional_volume_recovery' ? ivrRoot : RETAIL_ISSUE_ROOT_CAUSES.share_loss_fallback),
        expected_outcome:
          RETAIL_ISSUE_EXPECTED_OUTCOMES[recentFam] ||
          (recentFam === 'intentional_volume_recovery'
            ? ivrOutcome
            : RETAIL_ISSUE_EXPECTED_OUTCOMES.share_loss_fallback),
        finding:
          retailIssueFindingText(
            recentFam,
            {
              metrics: execIssue.card_metrics || {},
              trend_status: execIssue.card_metrics?.trend_status || null
            },
            focus
          ) || execIssue.finding,
        rule_triggered: recentFam
      };
    }
    out.push({
      ...execIssue,
      finding_key: retailIssueFindingKey(family),
      window_label: `${minStart} → ${maxEnd}`,
      episode_week_start: minStart,
      episode_week_end: maxEnd,
      episode_week_keys: weekKeysSorted,
      episode_week_count: weekKeysSorted.length,
      temporal_layer: 'weekly_episode_executive',
      recurrence_type: recurrenceType,
      contributing_episode_count: eps.length,
      contributing_week_count: weekKeysSorted.length,
      first_seen_date: minStart,
      last_seen_date: maxEnd,
      span_days: spanDays,
      representative_episode_type: representativeEpisodeType,
      ongoing_across_most_uploaded_period: isOngoingMost,
      performance_story: rep.performance_story || null,
      segment_attribution_summary: repWithContext.segment_attribution_summary || null,
      daily_validation_summary: repWithContext.daily_validation_summary || null,
      final_decision_rationale: rep.final_decision_rationale || null,
      narrative_chain: rep.narrative_chain || null,
      executive_synthesis: {
        contributing_episode_count: eps.length,
        contributing_week_keys: weekKeysSorted,
        date_range: { earliest_start: minStart, latest_end: maxEnd },
        representative_episode_finding_key: episodeFindingKey(rep.family, rep.startYmd, rep.endYmd),
        max_episode_rank_score: maxRank,
        temporal_pattern: {
          recurrence_type: recurrenceType,
          contributing_episode_count: eps.length,
          contributing_week_count: weekKeysSorted.length,
          first_seen_date: minStart,
          last_seen_date: maxEnd,
          span_days: spanDays,
          representative_episode_type: representativeEpisodeType,
          ongoing_across_most_uploaded_period: isOngoingMost,
          issue_week_coverage_ratio: Number(issueCoverageRatio.toFixed(3)),
          rank_bonus: rankBonus
        },
        episodes: episodeSummaries
      }
    });
  }
  return out;
}

function rankAndCapExecutiveRetailIssues(consolidatedIssues, maxCount) {
  return [...consolidatedIssues]
    .sort((a, b) => {
      const sbBase = Number(b.executive_synthesis?.max_episode_rank_score ?? 0);
      const saBase = Number(a.executive_synthesis?.max_episode_rank_score ?? 0);
      const sb = sbBase + Number(b.executive_synthesis?.temporal_pattern?.rank_bonus ?? 0);
      const sa = saBase + Number(a.executive_synthesis?.temporal_pattern?.rank_bonus ?? 0);
      if (sb !== sa) return sb - sa;
      const rec = (b.episode_week_end || '').localeCompare(a.episode_week_end || '');
      if (rec !== 0) return rec;
      return (b.episode_week_count || 0) - (a.episode_week_count || 0);
    })
    .slice(0, maxCount);
}

function buildWeeklyMacroContext(strWeekRows, pmsRows) {
  const metrics = buildWindowMetricsFromRows(strWeekRows || []);
  const rgiVar = averageMetric(strWeekRows || [], ['RGI % Change', 'RGI %']);
  const mpiVar = averageMetric(strWeekRows || [], ['MPI % Change', 'MPI %']);
  const ariVar = averageMetric(strWeekRows || [], ['ARI % Change', 'ARI %']);
  return {
    metrics,
    variance_vs_ly: {
      rgi_variance_vs_ly: rgiVar,
      mpi_variance_vs_ly: mpiVar,
      ari_variance_vs_ly: ariVar
    },
    pms_rows_scanned: Array.isArray(pmsRows) ? pmsRows.length : 0
  };
}

function classifyWeeklyPositionAndDirection(context) {
  const m = context?.metrics || {};
  const v = context?.variance_vs_ly || {};
  const rgi = toFiniteNumberOrNull(m.avgRGI);
  const mpi = toFiniteNumberOrNull(m.avgMPI);
  const ari = toFiniteNumberOrNull(m.avgARI);
  const rgiVar = toFiniteNumberOrNull(v.rgi_variance_vs_ly);
  const mpiVar = toFiniteNumberOrNull(v.mpi_variance_vs_ly);
  const ariVar = toFiniteNumberOrNull(v.ari_variance_vs_ly);

  const position = rgi !== null && rgi > 100 ? 'outperforming' : rgi !== null ? 'underperforming' : 'unknown';
  const direction =
    rgiVar === null
      ? 'unknown'
      : rgiVar > 0
        ? 'improving'
        : rgiVar < 0
          ? 'deteriorating'
          : 'stable';

  let source_of_movement = 'mixed';
  if (mpiVar !== null || ariVar !== null) {
    const a = Math.abs(Number(mpiVar || 0));
    const b = Math.abs(Number(ariVar || 0));
    if (a > b * 1.2) source_of_movement = 'MPI-led';
    else if (b > a * 1.2) source_of_movement = 'ARI-led';
  }

  return { position, direction, source_of_movement, rgi, mpi, ari, rgiVar, mpiVar, ariVar };
}

function buildWeeklyPerformanceStory(context) {
  const cls = classifyWeeklyPositionAndDirection(context);
  let story = 'mixed_or_unclear';
  if (cls.position === 'outperforming') {
    if (cls.direction === 'improving') story = 'improving_outperformance';
    else if (cls.direction === 'deteriorating') story = 'deteriorating_outperformance';
    else if (cls.source_of_movement === 'MPI-led') story = 'mpi_led_outperformance';
    else if (cls.source_of_movement === 'ARI-led') story = 'ari_led_outperformance';
    else story = 'stable_outperformance';
  } else if (cls.position === 'underperforming') {
    // RGI-first hard pricing gate: premium with weak share should not be flattened.
    if ((cls.ari || 0) > 100 && (cls.mpi || 0) < 100) {
      story = 'pricing_resistance_underperformance';
    } else
    if (
      cls.direction === 'improving' &&
      (cls.mpiVar || 0) > 0 &&
      (cls.ariVar || 0) < 0
    ) {
      story = 'volume_led_recovery';
    } else if (cls.source_of_movement === 'ARI-led' && (cls.ari || 0) > 100 && (cls.mpi || 0) < 100) {
      story = 'pricing_resistance_underperformance';
    } else if ((cls.ari || 0) < 100 && (cls.mpi || 0) <= 100) {
      story = 'discount_or_conversion_drag';
    } else {
      story = 'share_softness_underperformance';
    }
  }
  return { ...cls, performance_story: story };
}

function rankReasoningCandidatePriority(candidate) {
  const fam = candidate?.family || 'unknown';
  const perf = candidate?.performance_story || '';
  const m = candidate?.metrics || {};
  const rgi = toFiniteNumberOrNull(m.avgRGI) ?? 100;
  const mpi = toFiniteNumberOrNull(m.avgMPI) ?? 100;
  const ari = toFiniteNumberOrNull(m.avgARI) ?? 100;

  if (fam === 'pricing_resistance' && rgi < 100 && ari > 100 && mpi < 100) return 100;
  if (fam === 'mix_constraint' && perf === 'volume_led_recovery') return 95;
  if (fam === 'pricing_resistance') return 85;
  if (fam === 'mix_constraint') return 80;
  if (fam === 'discount_inefficiency') return 70;
  if (fam === 'missed_pricing_opportunity') return 60;
  if (fam === 'visibility_gap' && rgi < 100 && ari > 100 && mpi < 100) return 20;
  if (fam === 'visibility_gap') return 50;
  return 40;
}

function enforceReasoningCandidateHierarchy(candidates) {
  const list = Array.isArray(candidates) ? candidates : [];
  const byWeek = new Map();
  for (const c of list) {
    const key = Number(c.weekOrdinal);
    if (!byWeek.has(key)) byWeek.set(key, []);
    byWeek.get(key).push(c);
  }
  const out = [];
  for (const weekList of byWeek.values()) {
    const hasPricingOrMixTruth = weekList.some(
      (c) =>
        c.family === 'pricing_resistance' ||
        c.family === 'mix_constraint' ||
        c.performance_story === 'volume_led_recovery'
    );
    const filtered = hasPricingOrMixTruth
      ? weekList.filter((c) => c.family !== 'visibility_gap')
      : weekList;
    out.push(...filtered);
  }
  return out;
}

function attributePrimarySegmentDriver(pmsRows, context) {
  const list = Array.isArray(pmsRows) ? pmsRows : [];
  if (!list.length) {
    return {
      primary_segment: 'unknown',
      segment_scores: [],
      attribution_confidence: 'low',
      primary_segment_story: 'no_segment_data',
      rn_delta: null,
      adr_delta: null,
      revenue_delta: null
    };
  }

  const bucket = new Map();

  for (const row of list) {
    const segName =
      row['market segment name'] ||
      row['Market Segment Name'] ||
      row.segment ||
      row.Segment ||
      '';
    const seg = mapMarketSegmentNameToUsaliBucket(segName);
    const rnTy = toFiniteNumberOrNull(row['Room Nights TY (Actual / OTB)'] || row.rn) || 0;
    const rnLy = toFiniteNumberOrNull(row['Room Nights LY Actual']) || 0;
    const revTy = toFiniteNumberOrNull(row['Revenue TY (Actual / OTB)']);
    const revLy = toFiniteNumberOrNull(row['Revenue LY Actual']);
    const adrTy = toFiniteNumberOrNull(
      row.adr_ty || row['ADR TY'] || row.adr || row.ADR || row['Average Rate']
    );
    const adrLy = toFiniteNumberOrNull(
      row.adr_ly_actual || row['ADR LY'] || row['Average Rate LY']
    );

    const rnDelta = rnTy - rnLy;
    const revDelta = revTy !== null && revLy !== null ? revTy - revLy : null;
    let adrDelta = null;
    if (adrTy !== null && adrLy !== null) adrDelta = adrTy - adrLy;
    else if (revTy !== null && revLy !== null && rnTy > 0 && rnLy > 0) adrDelta = revTy / rnTy - revLy / rnLy;

    if (!bucket.has(seg)) {
      bucket.set(seg, {
        segment: seg,
        rn_ty: 0,
        rn_ly: 0,
        rn_delta: 0,
        rev_ty: 0,
        rev_ly: 0,
        revenue_delta: 0,
        adr_delta_sum: 0,
        adr_delta_count: 0,
        score: 0,
        count: 0
      });
    }
    const b = bucket.get(seg);
    b.rn_ty += rnTy;
    b.rn_ly += rnLy;
    b.rn_delta += rnDelta;
    if (revTy !== null) b.rev_ty += revTy;
    if (revLy !== null) b.rev_ly += revLy;
    if (revDelta !== null) b.revenue_delta += revDelta;
    if (adrDelta !== null) {
      b.adr_delta_sum += adrDelta;
      b.adr_delta_count += 1;
    }
    b.count += 1;
  }
  const perfStory = context?.performance_story || 'mixed_or_unclear';
  const segmentScores = Array.from(bucket.values())
    .map((b) => {
      const adr_delta = b.adr_delta_count > 0 ? b.adr_delta_sum / b.adr_delta_count : null;
      let score = Math.abs(b.rn_delta);
      if (perfStory === 'volume_led_recovery') {
        // Favor volume gain with ADR softness (MPI up, ARI down pattern).
        score += b.rn_delta > 0 ? Math.abs(b.rn_delta) * 0.7 : 0;
        score += adr_delta !== null && adr_delta < 0 ? Math.abs(adr_delta) * 10 : 0;
      } else if (perfStory === 'pricing_resistance_underperformance') {
        // Favor premium-ish ADR with weak/negative volume response.
        score += adr_delta !== null && adr_delta > 0 ? Math.abs(adr_delta) * 12 : 0;
        score += b.rn_delta <= 0 ? Math.abs(b.rn_delta) * 0.8 : 0;
      } else {
        score += Math.abs(b.revenue_delta) * 0.05;
      }
      return {
        segment: b.segment,
        rn_ty: b.rn_ty,
        rn_ly: b.rn_ly,
        rn_delta: b.rn_delta,
        rev_ty: b.rev_ty || null,
        rev_ly: b.rev_ly || null,
        revenue_delta: b.revenue_delta || null,
        adr_delta,
        score: Number(score.toFixed(3)),
        count: b.count
      };
    })
    .sort((a, b) => Math.abs(b.score) - Math.abs(a.score));
  const primary = segmentScores[0]?.segment || 'unknown';
  const primaryRow = segmentScores[0] || null;
  const confidence =
    primaryRow && primaryRow.count >= 2
      ? primaryRow.adr_delta !== null || primaryRow.revenue_delta !== null
        ? 'high'
        : 'medium'
      : segmentScores.length
        ? 'medium'
        : 'low';
  const primaryStory =
    primaryRow === null
      ? 'no_segment_data'
      : perfStory === 'volume_led_recovery'
        ? 'volume_led_segment_with_adr_softness_check'
        : perfStory === 'pricing_resistance_underperformance'
          ? 'premium_pricing_with_weak_volume_response'
          : 'mixed_segment_driver';

  return {
    primary_segment: primary,
    segment_scores: segmentScores.slice(0, 5),
    attribution_confidence: confidence,
    primary_segment_story: primaryStory,
    rn_delta: primaryRow?.rn_delta ?? null,
    adr_delta: primaryRow?.adr_delta ?? null,
    revenue_delta: primaryRow?.revenue_delta ?? null
  };
}

function buildDailyValidationLayer(strWeekRows, pmsRows, context, segmentAttribution) {
  const rows = Array.isArray(strWeekRows) ? strWeekRows : [];
  if (!rows.length) return { displacement_risk: 'unknown', validation_notes: ['no_daily_rows'] };
  const dayClass = { peak: 0, shoulder: 0, low_demand: 0 };
  const dayTypeDates = { peak: new Set(), shoulder: new Set(), low_demand: new Set() };
  for (const r of rows) {
    const occ = toFiniteNumberOrNull(getMetricFromRow(r, ['Occupancy %', 'Hotel Occupancy %']));
    const d = getStrRowDate(r);
    const ymd = d ? formatDateToYMD(d) : null;
    if (occ === null) continue;
    if (occ >= 80) {
      dayClass.peak += 1;
      if (ymd) dayTypeDates.peak.add(ymd);
    } else if (occ >= 65) {
      dayClass.shoulder += 1;
      if (ymd) dayTypeDates.shoulder.add(ymd);
    } else {
      dayClass.low_demand += 1;
      if (ymd) dayTypeDates.low_demand.add(ymd);
    }
  }

  const seg = segmentAttribution?.primary_segment || 'unknown';

  const validatedPeak = [];
  const validatedShoulder = [];
  const validatedLowDemand = [];
  const primaryRows = (Array.isArray(pmsRows) ? pmsRows : []).filter((row) => {
    const segName =
      row['market segment name'] ||
      row['Market Segment Name'] ||
      row.segment ||
      row.Segment ||
      '';
    return mapMarketSegmentNameToUsaliBucket(segName) === seg;
  });

  for (const row of primaryRows) {
    const ymd = row?._ingestion?.stay_date_ymd || getRowStayDateYmd(row);
    if (!ymd) continue;
    const rnTy = toFiniteNumberOrNull(row['Room Nights TY (Actual / OTB)'] || row.rn);
    const rnLy = toFiniteNumberOrNull(row['Room Nights LY Actual']);
    if (rnTy === null || rnLy === null) continue;
    const delta = rnTy - rnLy;
    if (delta <= 0) continue;
    if (dayTypeDates.peak.has(ymd)) validatedPeak.push(ymd);
    if (dayTypeDates.shoulder.has(ymd)) validatedShoulder.push(ymd);
    if (dayTypeDates.low_demand.has(ymd)) validatedLowDemand.push(ymd);
  }

  let displacementRisk = 'unknown';
  if (seg === 'unknown' || primaryRows.length === 0) {
    displacementRisk = 'unknown';
  } else if (validatedPeak.length > 0) {
    displacementRisk = 'high';
  } else if (validatedShoulder.length > 0 || validatedLowDemand.length > 0) {
    displacementRisk = 'low';
  } else {
    displacementRisk = 'unknown';
  }

  return {
    displacement_risk: displacementRisk,
    day_mix: dayClass,
    validated_peak_growth_dates: [...new Set(validatedPeak)].sort(),
    validated_shoulder_growth_dates: [...new Set(validatedShoulder)].sort(),
    validated_low_demand_growth_dates: [...new Set(validatedLowDemand)].sort(),
    validation_notes: [
      `daily_peak_days=${dayClass.peak}`,
      `daily_shoulder_days=${dayClass.shoulder}`,
      `daily_low_demand_days=${dayClass.low_demand}`,
      `primary_segment=${seg}`,
      `primary_segment_rows_scanned=${primaryRows.length}`,
      `validated_peak_growth_count=${validatedPeak.length}`,
      `validated_shoulder_growth_count=${validatedShoulder.length}`,
      `validated_low_demand_growth_count=${validatedLowDemand.length}`
    ]
  };
}

function buildRetailCommercialDecision(context, performanceStory, segmentAttribution, dailyValidation) {
  const cls = performanceStory || {};
  const seg = segmentAttribution?.primary_segment || 'unknown';
  const risk = dailyValidation?.displacement_risk || 'unknown';

  // Hard commercial KPI override: premium + weak share under market = pricing truth.
  if (
    Number(context?.metrics?.avgRGI) < 100 &&
    Number(context?.metrics?.avgARI) > 100 &&
    Number(context?.metrics?.avgMPI) < 100
  ) {
    return {
      issue_family: 'pricing_resistance',
      primary_driver: 'pricing',
      rationale: 'Rate premium is limiting share capture below fair market level.'
    };
  }

  let family = 'visibility_gap';
  let primary_driver = 'visibility';
  let rationale = 'Share softness requires closer commercial visibility and capture discipline.';

  if (cls.position === 'outperforming') {
    if (cls.direction === 'deteriorating') {
      family = 'visibility_gap';
      primary_driver = 'visibility';
      rationale = 'Outperformance is deteriorating; preserve share momentum before slippage compounds.';
    } else {
      return { issue_family: null, primary_driver: null, rationale: 'healthy_outperformance_no_issue' };
    }
  } else if (cls.performance_story === 'pricing_resistance_underperformance') {
    family = 'pricing_resistance';
    primary_driver = 'pricing';
    rationale = 'ARI premium with MPI weakness indicates pricing friction against demand capture.';
  } else if (cls.performance_story === 'discount_or_conversion_drag') {
    family = 'discount_inefficiency';
    primary_driver = 'conversion';
    rationale = 'ARI/MPI weakness indicates discount or conversion inefficiency; discounting remains subordinate to core strategy.';
  } else if (cls.performance_story === 'volume_led_recovery') {
    family = risk === 'high' ? 'mix_constraint' : 'pricing_resistance';
    primary_driver = risk === 'high' ? 'mix_strategy' : 'pricing';
    rationale =
      risk === 'high'
        ? 'Volume-led recovery is concentrated in potentially displacing day types; protect value on peak windows.'
        : 'Volume-led recovery appears controlled in non-peak day types; continue selective pricing/mix strategy while rebuilding fair share.';
  } else if (cls.position === 'underperforming' && (cls.ari || 0) > 100 && (cls.mpi || 0) < 100) {
    family = 'pricing_resistance';
    primary_driver = 'pricing';
    rationale = 'Below-fair-share performance with ARI premium and MPI weakness indicates pricing-led share rejection.';
  }

  if (family === 'visibility_gap' && Number(context?.metrics?.avgRGI) >= 100) {
    return {
      issue_family: null,
      primary_driver: null,
      rationale: 'RGI_above_fair_share_visibility_gap_suppressed'
    };
  }

  return {
    issue_family: family,
    primary_driver,
    rationale,
    segment_driver: seg,
    displacement_risk: risk
  };
}

function buildRetailReasoningIssue({ context, performanceStory, segmentAttribution, dailyValidation, finalDecision }) {
  if (!finalDecision?.issue_family) return null;
  const diagnosis = context?.diagnosis || {};
  const pmsRows = Array.isArray(context?.pmsRows) ? context.pmsRows : [];
  const paceSignalSummary = context?.paceSignalSummary || null;
  const positionSummary =
    performanceStory?.rgi != null
      ? `RGI ${Number(performanceStory.rgi).toFixed(1)} (${performanceStory.position || 'unknown'}).`
      : 'RGI position unavailable.';
  const rgiV = toFiniteNumberOrNull(diagnosis?.rgiVar);
  const mpiV = toFiniteNumberOrNull(diagnosis?.mpiVar);
  const ariV = toFiniteNumberOrNull(diagnosis?.ariVar);
  const fmtVarPt = (v) => (v === null ? null : `${v >= 0 ? '+' : ''}${v.toFixed(1)}`);
  const varBits = [
    rgiV !== null ? `RGI ${fmtVarPt(rgiV)} vs LY` : null,
    mpiV !== null ? `MPI ${fmtVarPt(mpiV)} vs LY` : null,
    ariV !== null ? `ARI ${fmtVarPt(ariV)} vs LY` : null
  ].filter(Boolean);
  const varianceSummary = varBits.length ? `${varBits.join('; ')}.` : null;
  const sourceSummary = `Source of movement: ${performanceStory?.source_of_movement || 'mixed'}.`;
  const rnDeltaRaw = segmentAttribution?.rn_delta;
  const adrDeltaRaw = segmentAttribution?.adr_delta;
  const rnDeltaStr =
    rnDeltaRaw == null || !Number.isFinite(Number(rnDeltaRaw)) ? 'n/a' : Number(rnDeltaRaw).toFixed(1);
  const adrDeltaStr =
    adrDeltaRaw == null || !Number.isFinite(Number(adrDeltaRaw)) ? 'n/a' : Number(adrDeltaRaw).toFixed(1);
  const segmentSummary =
    `Segment driver: ${segmentAttribution?.primary_segment || 'unknown'} ` +
    `(RN delta ${rnDeltaStr}, ADR delta ${adrDeltaStr}).`;
  const dailySummary =
    dailyValidation?.displacement_risk === 'high'
      ? 'Daily validation: low-rated growth appeared on peak dates, creating displacement risk.'
      : dailyValidation?.displacement_risk === 'low'
        ? 'Daily validation: growth concentrated on shoulder/low-demand dates, limiting displacement risk.'
        : 'Daily validation inconclusive; maintain caution before escalation.';
  const { narrative: commercialNarrative, decisionLine: narrativeDecisionLine } =
    buildCommercialNarrative(
      { issue_family: finalDecision?.issue_family },
      diagnosis,
      segmentAttribution,
      dailyValidation,
      pmsRows,
      paceSignalSummary
    ) || {};
  return {
    family: finalDecision.issue_family,
    primary_driver: finalDecision.primary_driver,
    performance_story: performanceStory?.performance_story || null,
    segment_attribution_summary: {
      primary_segment: segmentAttribution?.primary_segment || 'unknown',
      attribution_confidence: segmentAttribution?.attribution_confidence || 'low',
      primary_segment_story: segmentAttribution?.primary_segment_story || null,
      rn_delta: segmentAttribution?.rn_delta ?? null,
      adr_delta: segmentAttribution?.adr_delta ?? null,
      revenue_delta: segmentAttribution?.revenue_delta ?? null
    },
    daily_validation_summary: {
      displacement_risk: dailyValidation?.displacement_risk || 'unknown',
      validated_peak_growth_dates: dailyValidation?.validated_peak_growth_dates || [],
      validated_shoulder_growth_dates: dailyValidation?.validated_shoulder_growth_dates || [],
      validated_low_demand_growth_dates: dailyValidation?.validated_low_demand_growth_dates || []
    },
    final_decision_rationale: finalDecision?.rationale || null,
    commercial_narrative: commercialNarrative || null,
    enforced_decision_line: narrativeDecisionLine || null,
    narrative_chain: {
      position_summary: positionSummary,
      variance_summary: varianceSummary,
      source_summary: sourceSummary,
      segment_summary: segmentSummary,
      daily_validation_summary_text: dailySummary
    },
    reasoning: {
      position: {
        rgi_current: performanceStory?.rgi ?? null,
        mpi_current: performanceStory?.mpi ?? null,
        ari_current: performanceStory?.ari ?? null
      },
      variance: {
        rgi_variance_vs_ly: performanceStory?.rgiVar ?? null,
        mpi_variance_vs_ly: performanceStory?.mpiVar ?? null,
        ari_variance_vs_ly: performanceStory?.ariVar ?? null
      },
      performance_story: performanceStory?.performance_story || null,
      segment_attribution: segmentAttribution || null,
      daily_truth_validation: dailyValidation || null,
      final_decision: finalDecision || null
    }
  };
}

/**
 * Internal weekly temporal pipeline: windows -> weekly specs -> episodes -> raw issue objects.
 * Returns { rawIssues, temporal_meta } or { rawIssues: [], temporal_meta } if unusable.
 */
function buildRetailIssuesFromWeeklyTemporal(strRows, focus, driver, pmsRows = []) {
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

    const expandYmdByDays = (ymd, delta) => {
      if (!ymd || !/^\d{4}-\d{2}-\d{2}$/.test(String(ymd))) return null;
      const [y, m, d] = String(ymd).split('-').map(Number);
      const dt = new Date(Date.UTC(y, m - 1, d));
      dt.setUTCDate(dt.getUTCDate() + delta);
      return formatDateToYMD(dt);
    };
    const windowStart = (minYmd && expandYmdByDays(minYmd, -3)) || minYmd;
    const windowEnd = (maxYmd && expandYmdByDays(maxYmd, 3)) || maxYmd;

    const pmsWeekRows = (Array.isArray(pmsRows) ? pmsRows : []).filter((row) => {
      let ymd = row?._ingestion?.stay_date_ymd;
      if (ymd == null || String(ymd).trim() === '') ymd = getRowStayDateYmd(row);
      return ymd && windowStart && windowEnd ? ymd >= windowStart && ymd <= windowEnd : false;
    });
    console.log('DEBUG pmsWeekRows weekly filter', {
      weekKey,
      minYmd,
      maxYmd,
      'pmsWeekRows.length': pmsWeekRows.length
    });
    const macroContext = buildWeeklyMacroContext(rows, pmsWeekRows);
    const perfStory = buildWeeklyPerformanceStory(macroContext);
    const segAttr = attributePrimarySegmentDriver(pmsWeekRows, perfStory);
    const dailyValidation = buildDailyValidationLayer(rows, pmsWeekRows, perfStory, segAttr);
    const finalDecision = buildRetailCommercialDecision(macroContext, perfStory, segAttr, dailyValidation);
    const reasoningIssue = buildRetailReasoningIssue({
      context: macroContext,
      performanceStory: perfStory,
      segmentAttribution: segAttr,
      dailyValidation,
      finalDecision
    });

    temporal_meta.weekly_windows[temporal_meta.weekly_windows.length - 1].weekly_reasoning = reasoningIssue?.reasoning || null;

    if (reasoningIssue) {
      candidates.push({
        family: reasoningIssue.family,
        primary_driver: reasoningIssue.primary_driver,
        reasoning_priority_score: rankReasoningCandidatePriority({
          family: reasoningIssue.family,
          performance_story: reasoningIssue.performance_story,
          metrics
        }),
        performance_story: reasoningIssue.performance_story,
        segment_attribution_summary: reasoningIssue.segment_attribution_summary,
        daily_validation_summary: reasoningIssue.daily_validation_summary,
        final_decision_rationale: reasoningIssue.final_decision_rationale,
        narrative_chain: reasoningIssue.narrative_chain,
        commercial_narrative: reasoningIssue.commercial_narrative || null,
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

  const arbitratedCandidates = enforceReasoningCandidateHierarchy(candidates);
  arbitratedCandidates.sort((a, b) => {
    const pa = Number(a.reasoning_priority_score || 0);
    const pb = Number(b.reasoning_priority_score || 0);
    if (pb !== pa) return pb - pa;
    if (a.weekOrdinal !== b.weekOrdinal) return a.weekOrdinal - b.weekOrdinal;
    const ra = a.family || '';
    const rb = b.family || '';
    return ra.localeCompare(rb);
  });

  const byFamily = new Map();
  for (const c of arbitratedCandidates) {
    if (!byFamily.has(c.family)) byFamily.set(c.family, []);
    byFamily.get(c.family).push(c);
  }

  let episodes = [];
  for (const famList of byFamily.values()) {
    famList.sort((a, b) => a.weekOrdinal - b.weekOrdinal);
    episodes = episodes.concat(mergeWeeklyCandidatesIntoEpisodes(famList));
  }

  temporal_meta.episode_count = episodes.length;
  temporal_meta.episodes_pre_executive = episodes.map((ep) => ({
    family: ep.family,
    start_ymd: ep.startYmd,
    end_ymd: ep.endYmd,
    week_keys: [...(ep.weekKeys || [])],
    episode_finding_key: episodeFindingKey(ep.family, ep.startYmd, ep.endYmd)
  }));

  const consolidated = consolidateRetailEpisodesToExecutiveIssues(episodes, focus, {
    observed_week_count: temporal_meta.weekly_windows.length,
    weekly_windows: temporal_meta.weekly_windows
  });
  temporal_meta.executive_family_count = consolidated.length;

  const rankedIssues = rankAndCapExecutiveRetailIssues(consolidated, MAX_RETAIL_ISSUES_PER_RUN);

  for (const issue of rankedIssues) {
    const episodeKeys = issue?.episode_week_keys;
    if (!Array.isArray(episodeKeys) || !episodeKeys.length || !issue.card_metrics) continue;
    const keySet = new Set(episodeKeys);
    let bestWindow = null;
    let bestOrdinal = -Infinity;
    for (const win of temporal_meta.weekly_windows || []) {
      const wk = win?.week_key;
      if (wk == null || !keySet.has(wk)) continue;
      let ord = weekOrdinalMap.get(wk);
      if (ord === undefined) ord = sortedWeekKeys.indexOf(wk);
      if (ord < 0) continue;
      if (ord > bestOrdinal) {
        bestOrdinal = ord;
        bestWindow = win;
      }
    }
    if (bestWindow && bestWindow.trend_status != null) {
      issue.card_metrics.trend_status = bestWindow.trend_status;
    }
  }

  // FINAL deterministic commercial override (post consolidation/ranking):
  // if pricing truth exists (RGI<100, ARI>100, MPI<100), force it primary and downgrade visibility/distribution narratives.
  const pricingTruthIssue = rankedIssues.find((issue) => {
    const driver = issue?.primary_driver || issue?.driver;
    const m = issue?.card_metrics || {};
    const ari = Number(m.avgARI);
    const mpi = Number(m.avgMPI);
    const rgi = Number(m.avgRGI);
    return driver === 'pricing' && Number.isFinite(ari) && Number.isFinite(mpi) && Number.isFinite(rgi) && ari > 100 && mpi < 100 && rgi < 100;
  });

  let rawIssues = rankedIssues;
  if (pricingTruthIssue) {
    rawIssues = [
      {
        ...pricingTruthIssue,
        arbitration_role: 'primary'
      },
      ...rankedIssues
        .filter((issue) => issue.finding_key !== pricingTruthIssue.finding_key)
        .map((issue) => {
          const driver = issue?.primary_driver || issue?.driver;
          const family = issue?.issue_family || '';
          const isVisibilityOrDistribution =
            family === 'visibility_gap' || driver === 'visibility' || driver === 'distribution';
          return isVisibilityOrDistribution
            ? { ...issue, arbitration_role: 'supporting' }
            : issue;
        })
    ];
  }

  temporal_meta.executive_capped_count = rawIssues.length;

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

  if (
    Number.isFinite(avgMPI) &&
    ((avgMPI < 95 && trend === 'worsening') || avgMPI < 92) &&
    !(Number.isFinite(avgRGI) && avgRGI >= 100)
  ) {
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
    _library_actions: cappedLib,
    card_metrics: snapshotCardMetricsFromDiagnosisLike(diagnosis),
    segment_attribution_summary: spec.segment_attribution_summary || null,
    daily_validation_summary: spec.daily_validation_summary || null,
    narrative_chain: spec.narrative_chain || null,
    performance_story: spec.performance_story || null,
    commercial_narrative: spec.commercial_narrative || null,
    final_decision_rationale: spec.final_decision_rationale || null
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
      })),
      card_metrics: snapshotCardMetricsFromDiagnosisLike(diagnosis)
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

function fmtYmdForExecutionBullet(ymd) {
  const s = ymd != null ? String(ymd).trim() : '';
  const core = /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : null;
  if (!core) return null;
  const [y, mth, d] = core.split('-').map(Number);
  const monthNames = [
    'January',
    'February',
    'March',
    'April',
    'May',
    'June',
    'July',
    'August',
    'September',
    'October',
    'November',
    'December'
  ];
  const mn = monthNames[(mth || 1) - 1];
  if (!mn || !d) return null;
  return `${d} ${mn}`;
}

function isSafeExecutionBulletText(s) {
  if (!s || typeof s !== 'string') return false;
  const t = s.trim();
  if (!t) return false;
  if (/\bnull\b|\bundefined\b|\bn\/a\b/i.test(t)) return false;
  return true;
}

function fmtPctSignedForExecution(v) {
  if (v === null || !Number.isFinite(v)) return null;
  return `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`;
}

function aggregateSegmentRollupsFromPms(pmsRows) {
  const rows = Array.isArray(pmsRows) ? pmsRows : [];
  const bySeg = new Map();
  for (const row of rows) {
    const raw = String(row?.['Market Segment Name'] || '').trim();
    const seg = mapMarketSegmentNameToUsaliBucket(raw);
    if (!bySeg.has(seg)) {
      bySeg.set(seg, {
        seg,
        sampleName: raw,
        rnTy: 0,
        rnLy: 0,
        revTy: 0,
        revLy: 0,
        seenTy: false,
        seenLy: false,
        seenRTy: false,
        seenRLy: false
      });
    }
    const a = bySeg.get(seg);
    if (raw && !a.sampleName) a.sampleName = raw;
    const nt = toFiniteNumberOrNull(row['Room Nights TY (Actual / OTB)'] || row.rn);
    const nl = toFiniteNumberOrNull(row['Room Nights LY Actual']);
    const rt = toFiniteNumberOrNull(row['Revenue TY (Actual / OTB)']);
    const rl = toFiniteNumberOrNull(row['Revenue LY Actual']);
    if (nt !== null) {
      a.rnTy += nt;
      a.seenTy = true;
    }
    if (nl !== null) {
      a.rnLy += nl;
      a.seenLy = true;
    }
    if (rt !== null) {
      a.revTy += rt;
      a.seenRTy = true;
    }
    if (rl !== null) {
      a.revLy += rl;
      a.seenRLy = true;
    }
  }
  return bySeg;
}

function buildNarrativeEnforcedExecutionActions(issue, pmsRows) {
  const dl = (issue.enforced_decision_line || '').toString();
  const fam = (issue.issue_family || '').toString();
  const attr = issue.segment_attribution_summary;
  const daily = issue.daily_validation_summary;
  const primarySeg =
    attr?.primary_segment != null && attr.primary_segment !== '' && attr.primary_segment !== 'unknown'
      ? attr.primary_segment
      : null;

  const bySeg = aggregateSegmentRollupsFromPms(pmsRows);
  const primAgg = primarySeg ? bySeg.get(primarySeg) : null;
  const adrLyFromSummary = toFiniteNumberOrNull(attr?.adr_ly ?? attr?.adrLY ?? attr?.adr_ly_actual);
  const adrLyPrimarySegment =
    adrLyFromSummary !== null && Number.isFinite(adrLyFromSummary)
      ? adrLyFromSummary
      : primAgg && primAgg.rnLy > 0 && primAgg.seenRLy && Number.isFinite(primAgg.revLy / primAgg.rnLy)
        ? primAgg.revLy / primAgg.rnLy
        : null;
  const fmtEur0 = (v) =>
    v === null || !Number.isFinite(v)
      ? null
      : new Intl.NumberFormat('en-IE', {
          style: 'currency',
          currency: 'EUR',
          maximumFractionDigits: 0
        }).format(Math.round(v));
  const adrLyFormatted = fmtEur0(adrLyPrimarySegment);
  let sampleForPrimary = primAgg?.sampleName ? String(primAgg.sampleName).trim() : '';
  if (!sampleForPrimary && primarySeg) {
    const rows = Array.isArray(pmsRows) ? pmsRows : [];
    for (const row of rows) {
      const raw = String(row?.['Market Segment Name'] || '').trim();
      if (!raw) continue;
      if (mapMarketSegmentNameToUsaliBucket(raw) === primarySeg) {
        sampleForPrimary = raw;
        break;
      }
    }
  }
  const primaryDisplay = primarySeg ? usaliBucketToDisplayName(primarySeg, sampleForPrimary) : null;

  let rnPct = null;
  let adrPct = null;
  if (primAgg && primAgg.seenLy && primAgg.rnLy > 0) {
    rnPct = ((primAgg.rnTy - primAgg.rnLy) / primAgg.rnLy) * 100;
  }
  if (
    primAgg &&
    primAgg.rnTy > 0 &&
    primAgg.rnLy > 0 &&
    primAgg.seenRTy &&
    primAgg.seenRLy &&
    primAgg.revLy > 0
  ) {
    const adrTy = primAgg.revTy / primAgg.rnTy;
    const adrLy = primAgg.revLy / primAgg.rnLy;
    adrPct = ((adrTy - adrLy) / adrLy) * 100;
  }
  const fmtRN = fmtPctSignedForExecution(rnPct);
  const fmtADR = fmtPctSignedForExecution(adrPct);
  const xAdrRecover =
    adrPct != null && Number.isFinite(adrPct) ? Math.round(Math.abs(adrPct)) : null;

  const secondVol = [...bySeg.values()]
    .filter((a) => a.seg !== primarySeg && a.rnTy > 0)
    .sort((a, b) => b.rnTy - a.rnTy)[0];
  const secondVolLabel = secondVol
    ? usaliBucketToDisplayName(secondVol.seg, secondVol.sampleName || '')
    : null;

  const peakDates = Array.isArray(daily?.validated_peak_growth_dates)
    ? daily.validated_peak_growth_dates.filter(Boolean)
    : [];
  const shoulderDates = Array.isArray(daily?.validated_shoulder_growth_dates)
    ? daily.validated_shoulder_growth_dates.filter(Boolean)
    : [];
  const peakShoulderYmds = [...new Set([...shoulderDates, ...peakDates].map((d) => String(d).trim()))].filter(Boolean).sort();
  const peakNamed = peakDates.map((d) => fmtYmdForExecutionBullet(d)).filter(Boolean).slice(0, 10);
  const peakJoin = peakNamed.length ? peakNamed.join(', ') : null;
  const volGrowthDayMonth = peakShoulderYmds
    .map((d) => fmtYmdForExecutionBullet(d))
    .filter(Boolean)
    .slice(0, 2);
  const volGrowthJoin = volGrowthDayMonth.length ? volGrowthDayMonth.join(', ') : null;

  const pushSafe = (arr, s) => {
    if (isSafeExecutionBulletText(s)) arr.push(s.trim());
  };

  const bullets = [];

  const isPricingHold =
    fam === 'pricing_resistance' &&
    (dl.includes('Hold current rate positioning') || dl.includes('narrowing on its own'));

  const isIvrStop = dl.includes('Stop accepting');
  const isIvrByLine =
    isIvrStop ||
    dl.includes('Volume strategy is working') ||
    dl.includes('Give it ') ||
    dl.includes('Do not unwind the full discount') ||
    dl.includes('taper it');

  if (fam === 'pricing_resistance') {
    if (isPricingHold) {
      if (primaryDisplay && fmtRN && fmtADR) {
        pushSafe(
          bullets,
          `${primaryDisplay} room nights ${fmtRN} vs LY at ADR ${fmtADR} vs LY — monitor this segment's pickup weekly before reconsidering rate.`
        );
      }
      pushSafe(
        bullets,
        'Do not apply BAR reductions — the current trend is self-correcting. Any intervention risks resetting the recovery trajectory.'
      );
      if (peakJoin && primaryDisplay) {
        pushSafe(
          bullets,
          `Exception: review allocation on ${peakJoin} where ${primaryDisplay} grew at below-LY rates on peak occupancy days.`
        );
      }
    } else {
      if (primaryDisplay && fmtRN && fmtADR) {
        pushSafe(
          bullets,
          `${primaryDisplay} is the share-loss driver — ${fmtRN} room nights vs LY despite ADR ${fmtADR} vs LY. This segment is rejecting the current rate.`
        );
      }
      if (peakJoin) {
        pushSafe(
          bullets,
          `Apply targeted BAR correction on ${peakJoin} only. Hold rate on all other dates.`
        );
      } else {
        const weakRef = primaryDisplay || 'the primary segment';
        pushSafe(
          bullets,
          `Apply targeted BAR correction on shoulder dates where ${weakRef} conversion is weakest. Hold rate on peak dates.`
        );
      }
      pushSafe(
        bullets,
        'Measure MPI response within 10 days. If share does not recover, escalate correction to a second tier of dates.'
      );
    }
  } else if (fam === 'intentional_volume_recovery' || (fam !== 'pricing_resistance' && isIvrByLine)) {
    if (isIvrStop) {
      if (primaryDisplay && peakJoin) {
        pushSafe(
          bullets,
          `${primaryDisplay} is growing on peak dates at below-LY rates — this is value destruction, not recovery. Stop accepting this segment at current rates on ${peakJoin}.`
        );
      }
      if (primaryDisplay) {
        pushSafe(
          bullets,
          `Restore minimum rate floor for ${primaryDisplay} on peak dates to ADR LY level immediately.`
        );
      }
      if (secondVolLabel) {
        pushSafe(bullets, `Redirect inventory on those dates toward ${secondVolLabel} at BAR.`);
      }
    } else {
      if (primaryDisplay && fmtRN && fmtADR && rnPct !== null && rnPct > 0) {
        pushSafe(
          bullets,
          `${primaryDisplay} drove ${fmtRN} room night growth at ADR ${fmtADR} vs LY — the volume strategy is delivering. Do not disrupt it.`
        );
      }
      const occThreshold = 80;
      let rateRestoreBullet = null;
      if (adrLyFormatted && primaryDisplay) {
        rateRestoreBullet = `Begin rate restoration toward ${adrLyFormatted} — last year's achieved rate on ${primaryDisplay} — on forward peak dates first, not across all inventory.`;
      }
      if (volGrowthJoin && primaryDisplay) {
        const dateTail = `${volGrowthJoin} showed ${primaryDisplay} volume growth on days above ${occThreshold}% occupancy — these are the confirmed dates where rate correction applies without displacement risk.`;
        rateRestoreBullet = rateRestoreBullet ? `${rateRestoreBullet} ${dateTail}` : dateTail;
      }
      if (!rateRestoreBullet) {
        rateRestoreBullet =
          'Begin rate restoration selectively — raise BAR on the highest-occupancy forward dates first while maintaining current rates on need periods.';
      }
      pushSafe(bullets, rateRestoreBullet);
      if (
        xAdrRecover != null &&
        Number.isFinite(xAdrRecover) &&
        primaryDisplay &&
        adrPct != null &&
        Number.isFinite(adrPct)
      ) {
        pushSafe(
          bullets,
          `Target ADR recovery of ${xAdrRecover}% — the exact rate concession made versus last year on ${primaryDisplay} — progressively over the next booking cycle. This is not an arbitrary target: it is the precise gap between this year's ADR and last year's on the segment driving the current strategy.`
        );
      }
    }
  } else if (fam === 'discount_inefficiency') {
    if (primaryDisplay && fmtRN && fmtADR) {
      pushSafe(
        bullets,
        `${primaryDisplay} room nights ${fmtRN} vs LY at ADR ${fmtADR} vs LY — discounting is not generating proportional volume. The problem is not price.`
      );
    }
    if (primaryDisplay) {
      pushSafe(
        bullets,
        `Audit the booking path for ${primaryDisplay} — check OTA content quality, rate parity, and booking friction before adjusting rate further.`
      );
      pushSafe(
        bullets,
        `Freeze further rate reductions for ${primaryDisplay} for 14 days and measure whether conversion improves without additional price movement.`
      );
    }
  } else if (fam === 'visibility_gap') {
    if (primaryDisplay && fmtRN) {
      pushSafe(
        bullets,
        `${primaryDisplay} shows the weakest pickup — ${fmtRN} room nights vs LY. This segment is not reaching the booking stage.`
      );
    }
    if (primaryDisplay) {
      pushSafe(
        bullets,
        `Activate demand generation targeting ${primaryDisplay} specifically — paid search, metasearch visibility, and direct campaign exposure.`
      );
    }
    if (primaryDisplay) {
      pushSafe(
        bullets,
        'Do not adjust rate for this segment. The block is upstream awareness, not price sensitivity.'
      );
    }
  }

  return bullets.filter(isSafeExecutionBulletText).slice(0, 3);
}

function enrichRetailIssue(issue, ctx) {
  const { diagnosis, focus, detection, pmsRows, strRows, period_start, period_end } = ctx;
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
        strRows,
        period_start,
        period_end
      })
    };
  });

  const { _library_actions, ...rest } = issue;
  issue = { ...rest, actions };
  const mergedDiagnosis = {
    ...(ctx?.diagnosis || {}),
    metrics: {
      ...((ctx?.diagnosis && ctx.diagnosis.metrics) || {}),
      ...((issue && issue.card_metrics) || {})
    }
  };
  const { narrative: commercialNarrative, decisionLine: narrativeDecisionLine } =
    buildCommercialNarrative(
      { issue_family: issue.issue_family },
      mergedDiagnosis,
      issue.segment_attribution_summary,
      issue.daily_validation_summary,
      ctx?.pmsRows || [],
      ctx?.paceSignalSummary || null
    ) || {};
  issue.commercial_narrative = commercialNarrative || issue.commercial_narrative || null;
  issue.enforced_decision_line = narrativeDecisionLine || issue.enforced_decision_line || null;
  if (issue.commercial_narrative) {
    issue.enforced_execution_actions = buildNarrativeEnforcedExecutionActions(issue, ctx?.pmsRows || []);
  }
  return issue;
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

function buildFinancialImpact({
  driver,
  diagnosis,
  action,
  detection,
  pmsRows = [],
  strRows = [],
  period_start = null,
  period_end = null
}) {
  const periodStart = period_start || null;
  const periodEnd = period_end || null;
  const rows = (Array.isArray(pmsRows) ? pmsRows : []).filter((row) => {
    if (periodStart && periodEnd) {
      const ymd = row?._ingestion?.stay_date_ymd;
      if (!ymd) return true;
      return ymd >= periodStart && ymd <= periodEnd;
    }
    return true;
  });
  const str = Array.isArray(strRows) ? strRows : [];

  const adrValues = rows
    .map((row) => {
      let adr = toFiniteNumberOrNull(row['ADR TY']);
      if (adr === null || adr === 0) {
        const rev = toFiniteNumberOrNull(row['Revenue TY (Actual / OTB)']);
        const rn = toFiniteNumberOrNull(row['Room Nights TY (Actual / OTB)']);
        if (rev !== null && rn !== null && rn > 0) adr = rev / rn;
      }
      return adr;
    })
    .filter((v) => v !== null && v > 0);

  const rnValues = rows
    .map((row) => toFiniteNumberOrNull(row['Room Nights TY (Actual / OTB)']))
    .filter((v) => v !== null && v > 0);

  const avgMPI = averageMetric(str, ['MPI', 'MPI (Index)', 'Occupancy Index']);

  const avgADR =
    adrValues.length > 0
      ? adrValues.reduce((a, b) => a + b, 0) / adrValues.length
      : null;

  const totalRN =
    rnValues.length > 0
      ? rnValues.reduce((a, b) => a + b, 0)
      : null;

  if (avgADR == null || totalRN == null || totalRN <= 0 || avgMPI == null) {
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

function estimateRetailIssueImpact(issue, contextData) {
  const issueFamily = (issue?.issue_family || '').toString();
  const diagnosis = contextData?.diagnosis || {};
  const pmsRows = Array.isArray(contextData?.pmsRows) ? contextData.pmsRows : [];
  const periodStart = contextData?.period_start || null;
  const periodEnd = contextData?.period_end || null;

  const actualizedPmsRows = pmsRows.filter((row) => {
    const phase = row?._ingestion?.row_phase;
    if (phase !== 'actualized' && phase !== 'undated') return false;
    if (periodStart && periodEnd) {
      const ymd = row?._ingestion?.stay_date_ymd;
      if (!ymd) return true;
      return ymd >= periodStart && ymd <= periodEnd;
    }
    return true;
  });
  const pmsPaceRows = Array.isArray(contextData?.pmsPaceRows) ? contextData.pmsPaceRows : [];

  const cardMetrics = issue?.card_metrics || {};
  const avgMPI = toFiniteNumberOrNull(cardMetrics.avgMPI ?? diagnosis?.metrics?.avgMPI);
  const avgARI = toFiniteNumberOrNull(cardMetrics.avgARI ?? diagnosis?.metrics?.avgARI);
  const avgRGI = toFiniteNumberOrNull(cardMetrics.avgRGI ?? diagnosis?.metrics?.avgRGI);
  const avgOcc = toFiniteNumberOrNull(cardMetrics.avgOcc ?? diagnosis?.metrics?.avgOcc);

  const adrFromRow = (row) => {
    let adr = toFiniteNumberOrNull(row['ADR TY']);
    if (adr === null || adr === 0) {
      const rev = toFiniteNumberOrNull(row['Revenue TY (Actual / OTB)']);
      const rn = toFiniteNumberOrNull(row['Room Nights TY (Actual / OTB)']);
      if (rev !== null && rn !== null && rn > 0) adr = rev / rn;
    }
    return adr;
  };
  const adrValuesRaw = actualizedPmsRows.map((row) => adrFromRow(row)).filter((v) => v !== null && v > 0);
  const rnValuesRaw = actualizedPmsRows
    .map((row) => toFiniteNumberOrNull(row['Room Nights TY (Actual / OTB)']))
    .filter((v) => v !== null && v > 0);
  const revenueValuesRaw = actualizedPmsRows
    .map((row) => toFiniteNumberOrNull(row['Revenue TY (Actual / OTB)']))
    .filter((v) => v !== null && v > 0);

  const actualizedAdrValues = adrValuesRaw;
  const actualizedRnValues = rnValuesRaw;
  const actualizedTotalRN = actualizedRnValues.length
    ? actualizedRnValues.reduce((a, b) => a + b, 0)
    : null;
  const actualizedEffectiveADR = actualizedAdrValues.length
    ? actualizedAdrValues.reduce((a, b) => a + b, 0) / actualizedAdrValues.length
    : null;
  const adrLyVals = actualizedPmsRows
    .map((row) => {
      const ly = toFiniteNumberOrNull(row['ADR LY']);
      if (ly !== null) return ly;
      return toFiniteNumberOrNull(row['ADR STLY']);
    })
    .filter((v) => v !== null && Number.isFinite(v));
  const adrLY =
    adrLyVals.length > 0 ? adrLyVals.reduce((a, b) => a + b, 0) / adrLyVals.length : null;

  const paceRowsFuture = pmsPaceRows.filter((r) => (r?.future_window_class || '') === 'future_forward');
  const paceRowsForQuant = paceRowsFuture.length ? paceRowsFuture : pmsPaceRows;

  const adrValuesPace = paceRowsForQuant
    .map((row) => toFiniteNumberOrNull(row.adr_ty))
    .filter((v) => v !== null && v > 0);
  const rnValuesPace = paceRowsForQuant
    .map((row) => toFiniteNumberOrNull(row.rn_on_books_ty))
    .filter((v) => v !== null && v > 0);
  const revenueValuesPace = paceRowsForQuant
    .map((row) => toFiniteNumberOrNull(row.booked_revenue_ty))
    .filter((v) => v !== null && v > 0);

  const adrValues = adrValuesPace.length ? adrValuesPace : adrValuesRaw;
  const rnValues = rnValuesPace.length ? rnValuesPace : rnValuesRaw;
  const revenueValues = revenueValuesPace.length ? revenueValuesPace : revenueValuesRaw;

  const avgADR =
    adrValues.length > 0
      ? adrValues.reduce((a, b) => a + b, 0) / adrValues.length
      : null;
  const totalRN =
    rnValues.length > 0
      ? rnValues.reduce((a, b) => a + b, 0)
      : null;
  const totalRevenueTY =
    revenueValues.length > 0
      ? revenueValues.reduce((a, b) => a + b, 0)
      : null;

  const effectiveADR =
    avgADR !== null
      ? avgADR
      : totalRevenueTY !== null && totalRN !== null && totalRN > 0
        ? totalRevenueTY / totalRN
        : null;

  // Conservative proxy fallback: use existing issue action impact max as a floor signal,
  // then back-derive RN proxy only when ADR is available.
  const maxActionImpact =
    (issue?.actions || []).reduce((m, a) => Math.max(m, Number(a?.financial_impact?.impact_range?.high || 0)), 0) || 0;

  const indexGap =
    avgMPI !== null
      ? Math.max(0, 100 - avgMPI)
      : avgRGI !== null
        ? Math.max(0, 100 - avgRGI)
        : null;
  const adrGap =
    avgARI !== null
      ? Math.max(0, 100 - avgARI)
      : null;
  const occupancyGap =
    avgOcc !== null
      ? Math.max(0, 80 - avgOcc)
      : null;

  let roomNightsAtRisk = null;
  let revenueRange = null;
  let confidence = 'low';

  if (
    issueFamily === 'pricing_resistance' &&
    actualizedTotalRN !== null &&
    actualizedEffectiveADR !== null &&
    avgMPI !== null &&
    avgMPI > 0
  ) {
    const fairShareRN = actualizedTotalRN * (100 / avgMPI);
    const lostRN = Math.max(0, fairShareRN - actualizedTotalRN);
    const revenueAtRisk = lostRN * actualizedEffectiveADR;
    roomNightsAtRisk = Math.round(lostRN);
    revenueRange = {
      min: Math.round(revenueAtRisk * 0.6),
      max: Math.round(revenueAtRisk * 0.9)
    };
    confidence = avgMPI !== null && actualizedEffectiveADR !== null ? 'high' : 'medium';
  } else if (
    issueFamily === 'discount_inefficiency' &&
    actualizedTotalRN !== null &&
    actualizedEffectiveADR !== null &&
    avgARI !== null &&
    avgARI > 0
  ) {
    const fairShareADR = actualizedEffectiveADR * (100 / avgARI);
    const adrShortfall = Math.max(0, fairShareADR - actualizedEffectiveADR);
    const revenueAtRisk = actualizedTotalRN * adrShortfall;
    roomNightsAtRisk = Math.round(actualizedTotalRN);
    revenueRange = {
      min: Math.round(revenueAtRisk * 0.6),
      max: Math.round(revenueAtRisk * 0.9)
    };
    confidence = avgARI !== null && actualizedEffectiveADR !== null ? 'high' : 'medium';
  } else if (issueFamily === 'intentional_volume_recovery' && actualizedTotalRN !== null && actualizedEffectiveADR !== null) {
    roomNightsAtRisk = Math.round(actualizedTotalRN);
    if (adrLY === null) {
      revenueRange = null;
      confidence = 'medium';
    } else {
      const rateConcession = Math.max(0, adrLY - actualizedEffectiveADR);
      const revenueAtRisk = actualizedTotalRN * rateConcession;
      revenueRange = {
        min: Math.round(revenueAtRisk * 0.6),
        max: Math.round(revenueAtRisk * 0.9)
      };
      if (rateConcession <= 0) {
        confidence = 'low';
      } else {
        confidence = 'high';
      }
    }
  } else if (
    issueFamily === 'missed_pricing_opportunity' &&
    actualizedTotalRN !== null &&
    actualizedEffectiveADR !== null &&
    avgARI !== null &&
    avgARI > 0
  ) {
    const impliedFairADR = actualizedEffectiveADR * (100 / avgARI);
    const upliftPerRoom = Math.max(0, impliedFairADR - actualizedEffectiveADR);
    const revenueAtRisk = actualizedTotalRN * upliftPerRoom;
    roomNightsAtRisk = Math.round(actualizedTotalRN * 0.3);
    revenueRange = {
      min: Math.round(revenueAtRisk * 0.3),
      max: Math.round(revenueAtRisk * 0.6)
    };
    confidence = 'medium';
  } else {
    let issueWeight = 0.08;
    if (issueFamily === 'visibility_gap') issueWeight = 0.10;
    if (issueFamily === 'discount_inefficiency') issueWeight = 0.09;
    if (issueFamily === 'pricing_resistance') issueWeight = 0.07;
    if (issueFamily === 'mix_constraint') issueWeight = 0.06;
    if (issueFamily === 'missed_pricing_opportunity') issueWeight = 0.05;

    const gapAmplifier =
      indexGap !== null
        ? Math.max(0.6, Math.min(1.6, indexGap / 8))
        : adrGap !== null
          ? Math.max(0.6, Math.min(1.4, adrGap / 10))
          : 1;

    roomNightsAtRisk =
      totalRN !== null
        ? Math.round(totalRN * issueWeight * gapAmplifier)
        : null;
    if (roomNightsAtRisk === null && effectiveADR !== null && maxActionImpact > 0) {
      roomNightsAtRisk = Math.max(1, Math.round((maxActionImpact * 0.55) / effectiveADR));
    }

    revenueRange = null;
    if (roomNightsAtRisk !== null && roomNightsAtRisk > 0 && effectiveADR !== null) {
      const min = Math.round(roomNightsAtRisk * effectiveADR * 0.55);
      const max = Math.round(roomNightsAtRisk * effectiveADR * 0.95);
      revenueRange = { min, max };
    } else if (maxActionImpact > 0) {
      revenueRange = {
        min: Math.round(maxActionImpact * 0.45),
        max: Math.round(maxActionImpact * 0.75)
      };
    }

    const dataPoints = [avgMPI, avgARI, avgRGI, avgOcc, effectiveADR, totalRN].filter((v) => v !== null).length;
    confidence = 'low';
    if (dataPoints >= 5 && revenueRange) confidence = 'high';
    else if (dataPoints >= 3) confidence = 'medium';
  }

  if (revenueRange !== null && revenueRange.max > 2000000) {
    revenueRange = null;
    confidence = 'low';
  }

  let impactBand = 'low';
  const maxRevenue = revenueRange?.max ?? 0;
  const occGapForBand = occupancyGap ?? 0;
  const idxGapForBand = indexGap ?? 0;
  if (
    maxRevenue >= 15000 ||
    (roomNightsAtRisk ?? 0) >= 120 ||
    occGapForBand >= 12 ||
    idxGapForBand >= 12
  ) {
    impactBand = 'high';
  } else if (
    maxRevenue >= 5000 ||
    (roomNightsAtRisk ?? 0) >= 40 ||
    occGapForBand >= 6 ||
    idxGapForBand >= 6
  ) {
    impactBand = 'medium';
  }

  if (confidence === 'low' && impactBand === 'high') {
    // Keep conservative posture under low data confidence.
    impactBand = 'medium';
  }

  return {
    impact_band: impactBand,
    quantified_signals: {
      room_nights_at_risk: roomNightsAtRisk,
      adr_gap: adrGap,
      revenue_range: revenueRange,
      index_gap: indexGap,
      occupancy_gap: occupancyGap
    },
    confidence
  };
}

function interpretCommercialImpact(issue, quantifiedSignals) {
  const family = (issue?.issue_family || '').toString();
  const signals = quantifiedSignals || {};
  const band = signals.impact_band || 'low';
  const qs = signals.quantified_signals || {};
  const rev = qs.revenue_range;
  const rnRisk = qs.room_nights_at_risk;
  const idxGap = qs.index_gap;
  const occGap = qs.occupancy_gap;
  const adrGap = qs.adr_gap;
  const revText = rev ? `estimated revenue range ${rev.min}-${rev.max}` : 'revenue range not robust';
  const dominantSignal =
    rnRisk != null && rnRisk > 0
      ? `${rnRisk} room nights at risk`
      : idxGap != null && idxGap > 0
        ? `index gap ${idxGap.toFixed(1)}`
        : occGap != null && occGap > 0
          ? `occupancy gap ${occGap.toFixed(1)} pts`
          : adrGap != null && adrGap > 0
            ? `ADR index gap ${adrGap.toFixed(1)}`
            : 'directional evidence only';

  if (!rev && band === 'low') {
    return `Financial impact appears contained, but repeated inefficiency may compound over time (${dominantSignal}).`;
  }

  if (family === 'visibility_gap' || family === 'discount_inefficiency') {
    if (band === 'high') return `Material leakage signal: ${dominantSignal}; ${revText}.`;
    if (band === 'medium') return `Moderate share-loss signal: ${dominantSignal}; ${revText}.`;
    return `Leakage appears limited now (${dominantSignal}), but persistent conversion drag can scale over time.`;
  }

  if (family === 'pricing_resistance' || family === 'mix_constraint') {
    if (band === 'high') return `Rate/mix friction looks material (${dominantSignal}); ${revText}.`;
    if (band === 'medium') return `Commercial drag appears moderate from rate/mix imbalance (${dominantSignal}).`;
    return `Pricing/mix pressure is currently contained (${dominantSignal}) but should be monitored.`;
  }

  if (family === 'missed_pricing_opportunity') {
    if (band === 'high') return `Material upside signal for pricing optimization (${dominantSignal}); ${revText}.`;
    if (band === 'medium') return `Moderate pricing upside indicated (${dominantSignal}); ${revText}.`;
    return `Upside is limited at current signal strength (${dominantSignal}) but remains directionally positive.`;
  }

  if (band === 'high') return `Material commercial impact signal (${dominantSignal}); ${revText}.`;
  if (band === 'medium') return `Moderate commercial impact with actionable upside (${dominantSignal}).`;
  return `Financial impact appears contained at current levels (${dominantSignal}).`;
}

function buildFinancialQuantificationSummary(issues, contextData) {
  const list = (Array.isArray(issues) ? issues : []).filter(
    (issue) => (issue?.segment || 'retail') === 'retail'
  );
  const out = [];
  const notes = [];

  for (const issue of list) {
    const est = estimateRetailIssueImpact(issue, contextData);
    out.push({
      finding_key: issue?.finding_key || null,
      issue_family: issue?.issue_family || null,
      impact_band: est.impact_band,
      quantified_signals: est.quantified_signals,
      commercial_interpretation: interpretCommercialImpact(issue, est),
      confidence: est.confidence
    });
  }

  const withRevenue = out.filter((r) => r.quantified_signals?.revenue_range?.max != null);
  const withRoomNights = out.filter((r) => r.quantified_signals?.room_nights_at_risk != null);
  const withIndexGap = out.filter((r) => r.quantified_signals?.index_gap != null);
  const withOccGap = out.filter((r) => r.quantified_signals?.occupancy_gap != null);
  if (!withRevenue.length && out.length > 0) {
    notes.push('Revenue ranges are unavailable for most issues due to partial PMS/ADR/RN coverage.');
  } else if (withRevenue.length > 0 && withRevenue.length < out.length) {
    notes.push('Revenue ranges were estimated for some issues; others lacked enough financial fields for conservative quantification.');
  }
  if (out.length > 0 && withRoomNights.length < Math.ceil(out.length / 2)) {
    notes.push('Room-nights-at-risk coverage is limited; PMS room-night fields are sparse or inconsistent.');
  }
  if (out.length > 0 && withIndexGap.length < Math.ceil(out.length / 2)) {
    notes.push('Index-gap coverage is limited; MPI/RGI metrics are missing for several issues.');
  }
  if (out.length > 0 && withOccGap.length < Math.ceil(out.length / 2)) {
    notes.push('Occupancy-gap coverage is limited; STR occupancy metrics are partially missing.');
  }
  if (!out.length) {
    notes.push('No retail issues were available for financial quantification.');
  }

  const strongestHiddenLosses = [...out]
    .filter((r) => (r.issue_family || '') !== 'missed_pricing_opportunity')
    .sort(
      (a, b) => {
        const d =
          Number(b.quantified_signals?.revenue_range?.max || 0) -
          Number(a.quantified_signals?.revenue_range?.max || 0);
        if (d !== 0) return d;
        return String(a.finding_key || '').localeCompare(String(b.finding_key || ''));
      }
    )
    .slice(0, 8)
    .map((r) => ({
      finding_key: r.finding_key,
      issue_family: r.issue_family,
      impact_band: r.impact_band,
      revenue_range: r.quantified_signals?.revenue_range || null,
      confidence: r.confidence
    }));

  const strongestHiddenOpportunities = [...out]
    .filter((r) => (r.issue_family || '') === 'missed_pricing_opportunity')
    .sort(
      (a, b) => {
        const d =
          Number(b.quantified_signals?.revenue_range?.max || 0) -
          Number(a.quantified_signals?.revenue_range?.max || 0);
        if (d !== 0) return d;
        return String(a.finding_key || '').localeCompare(String(b.finding_key || ''));
      }
    )
    .slice(0, 8)
    .map((r) => ({
      finding_key: r.finding_key,
      issue_family: r.issue_family,
      impact_band: r.impact_band,
      revenue_range: r.quantified_signals?.revenue_range || null,
      confidence: r.confidence
    }));

  const highConfidenceCount = out.filter((r) => r.confidence === 'high').length;
  const mediumOrHighCount = out.filter((r) => r.confidence !== 'low').length;
  let coverageStatus = 'low';
  if (highConfidenceCount >= 2 || (out.length > 0 && highConfidenceCount === out.length)) coverageStatus = 'high';
  else if (mediumOrHighCount >= Math.ceil(Math.max(1, out.length) / 2)) coverageStatus = 'medium';

  return {
    schema_version: '1.0',
    coverage_status: coverageStatus,
    issue_level_quantification: out,
    strongest_hidden_losses: strongestHiddenLosses,
    strongest_hidden_opportunities: strongestHiddenOpportunities,
    data_quality_notes: notes
  };
}

function detectRetailCommercialContext(issue, contextData) {
  const family = (issue?.issue_family || '').toString();
  const diagnosis = contextData?.diagnosis || {};
  const q = contextData?.quantification || {};
  const qs = q.quantified_signals || {};
  const flags = [];

  const avgMPI = toFiniteNumberOrNull(issue?.card_metrics?.avgMPI ?? diagnosis?.metrics?.avgMPI);
  const avgARI = toFiniteNumberOrNull(issue?.card_metrics?.avgARI ?? diagnosis?.metrics?.avgARI);
  const avgRGI = toFiniteNumberOrNull(issue?.card_metrics?.avgRGI ?? diagnosis?.metrics?.avgRGI);
  const avgOcc = toFiniteNumberOrNull(issue?.card_metrics?.avgOcc ?? diagnosis?.metrics?.avgOcc);

  const idxGap = toFiniteNumberOrNull(qs.index_gap);
  const occGap = toFiniteNumberOrNull(qs.occupancy_gap);
  const adrGap = toFiniteNumberOrNull(qs.adr_gap);

  if (avgOcc !== null && avgOcc >= 82) flags.push('inventory_pressure');
  if (avgOcc !== null && avgOcc >= 88) flags.push('possible_compression');
  if (avgOcc !== null && avgOcc < 75) flags.push('occupancy_softness');
  if (idxGap !== null && idxGap >= 6) flags.push('share_softness');
  if (family === 'pricing_resistance') flags.push('pricing_resistance');
  if (family === 'discount_inefficiency') flags.push('discount_leakage');
  if (family === 'mix_constraint') flags.push('mix_inefficiency');
  if (avgARI !== null && avgARI < 100) flags.push('limited_rate_headroom');
  if (avgMPI === null && avgRGI === null) flags.push('demand_uncertain');

  let commercialSituation = 'mixed_or_unclear_context';
  if (flags.includes('possible_compression')) commercialSituation = 'constrained_inventory_context';
  else if (flags.includes('inventory_pressure')) commercialSituation = 'inventory_pressure_context';
  else if (flags.includes('pricing_resistance')) commercialSituation = 'pricing_friction_context';
  else if (flags.includes('discount_leakage')) commercialSituation = 'discount_inefficiency_context';
  else if (flags.includes('share_softness') || flags.includes('occupancy_softness')) {
    commercialSituation = 'demand_or_share_softness_context';
  }

  let dominantSignal = null;
  if (idxGap !== null && idxGap > 0) dominantSignal = `index_gap_${idxGap.toFixed(1)}`;
  else if (occGap !== null && occGap > 0) dominantSignal = `occupancy_gap_${occGap.toFixed(1)}`;
  else if (adrGap !== null && adrGap > 0) dominantSignal = `adr_gap_${adrGap.toFixed(1)}`;

  let constraintStatus = 'unclear';
  if (avgOcc !== null) {
    if (avgOcc >= 88) constraintStatus = 'high';
    else if (avgOcc >= 82) constraintStatus = 'medium';
    else constraintStatus = 'low';
  }

  const points = [avgMPI, avgARI, avgRGI, avgOcc, idxGap, occGap, adrGap].filter((v) => v !== null).length;
  const confidence = points >= 5 ? 'high' : points >= 3 ? 'medium' : 'low';

  return {
    context_flags: Array.from(new Set(flags)),
    commercial_situation: commercialSituation,
    dominant_signal: dominantSignal,
    constraint_status: constraintStatus,
    confidence
  };
}

/**
 * Forward OTB / forecast issue cards: windowed pace, ADR, and compression signals (separate from historical retail issues).
 * @param {unknown[]} strRows STR rows (reserved for capacity / context extensions)
 * @param {object} diagnosis Diagnosis object (reserved for cross-layer context)
 */
function buildForwardIssuesFromPmsOtb(pmsRows, strRows, diagnosis, snapshotYmd) {
  const capacityFallback = inferHotelCapacityFromContext(strRows, diagnosis);
  const FORWARD_REV_CAP = 5000000;
  const forwardSegDisplayName = (segObj) => {
    if (segObj == null) return null;
    const dn = segObj.displayName != null ? String(segObj.displayName).trim() : '';
    if (dn) return dn;
    const nm = segObj.name != null ? String(segObj.name).trim() : '';
    if (nm) return nm;
    const segKey = segObj.segmentKey != null ? String(segObj.segmentKey).trim() : '';
    if (segKey) return usaliBucketToDisplayName(segKey, segObj.sampleName || '');
    const sk = segObj.seg != null ? String(segObj.seg).trim() : '';
    if (sk) return usaliBucketToDisplayName(sk, segObj.sampleName || '');
    const sn = segObj.sampleName != null ? String(segObj.sampleName).trim() : '';
    return sn || null;
  };

  const fwdNum = (row, keys) => {
    for (const k of keys) {
      const v = toFiniteNumberOrNull(row?.[k]);
      if (v !== null) return v;
    }
    return null;
  };

  const leadDaysToStay = (stayYmd) => {
    const s0 = parseYmdToUtcDate(snapshotYmd);
    const s1 = parseYmdToUtcDate(stayYmd);
    if (!s0 || !s1) return null;
    return Math.round((s1.getTime() - s0.getTime()) / 86400000);
  };

  const addDaysYmd = (ymd, days) => {
    const d = parseYmdToUtcDate(ymd);
    if (!d || !Number.isFinite(days)) return null;
    return formatDateToYMD(new Date(d.getTime() + days * 86400000));
  };

  const fmtDayMonthYear = (ymd) => {
    if (!ymd || !/^\d{4}-\d{2}-\d{2}$/.test(ymd)) return null;
    const [y, mth, d] = ymd.split('-').map(Number);
    const monthNames = [
      'January',
      'February',
      'March',
      'April',
      'May',
      'June',
      'July',
      'August',
      'September',
      'October',
      'November',
      'December'
    ];
    const mn = monthNames[(mth || 1) - 1];
    if (!mn || !d) return null;
    return `${d} ${mn} ${y}`;
  };

  const fmtPct1 = (v) => (v === null || !Number.isFinite(v) ? null : `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`);
  const fmtMoney0 = (v) =>
    v === null || !Number.isFinite(v)
      ? null
      : new Intl.NumberFormat('en-GB', { maximumFractionDigits: 0 }).format(Math.round(v));

  const windowLabelFromBounds = (loLead, hiLead, startYmd, endYmd) => {
    const a = fmtDayMonthYear(startYmd);
    const b = fmtDayMonthYear(endYmd);
    if (!a || !b) return `Forward window (${loLead}–${hiLead} days ahead)`;
    if (loLead === 1 && hiLead === 30) return `Next 30 days: ${a} → ${b}`;
    return `Days ${loLead}–${hiLead} ahead: ${a} → ${b}`;
  };

  const windowBounds = (win) => {
    if (win === 1) return { lo: 1, hi: 30, endLead: 30 };
    if (win === 2) return { lo: 31, hi: 60, endLead: 60 };
    if (win === 3) return { lo: 61, hi: 90, endLead: 90 };
    return { lo: 91, hi: null, endLead: null };
  };

  const allRows = Array.isArray(pmsRows) ? pmsRows : [];
  const forwardRows = allRows.filter((row) => {
    const ph = row?._ingestion?.row_phase;
    return ph === 'future_otb' || ph === 'future_forecast';
  });
  if (!forwardRows.length) return [];

  const window1Start = addDaysYmd(snapshotYmd, 1);
  const window1End = addDaysYmd(snapshotYmd, 30);
  const window2Start = addDaysYmd(snapshotYmd, 31);
  const window2End = addDaysYmd(snapshotYmd, 60);
  const window3Start = addDaysYmd(snapshotYmd, 61);
  const window3End = addDaysYmd(snapshotYmd, 90);
  const window4Start = addDaysYmd(snapshotYmd, 91);

  const byWindow = [[], [], [], []];
  for (const row of forwardRows) {
    const rawStay = row?._ingestion?.stay_date_ymd;
    const stay = rawStay != null ? String(rawStay).trim() : '';
    if (!stay || !/^\d{4}-\d{2}-\d{2}$/.test(stay)) continue;
    if (
      window1Start &&
      window1End &&
      stay >= window1Start &&
      stay <= window1End
    ) {
      byWindow[0].push(row);
    } else if (
      window2Start &&
      window2End &&
      stay >= window2Start &&
      stay <= window2End
    ) {
      byWindow[1].push(row);
    } else if (
      window3Start &&
      window3End &&
      stay >= window3Start &&
      stay <= window3End
    ) {
      byWindow[2].push(row);
    } else if (window4Start && stay >= window4Start) {
      byWindow[3].push(row);
    }
  }

  console.log('DEBUG forward window rows', {
    window: 1,
    startDays: 1,
    endDays: 30,
    windowStartDate: window1Start,
    windowEndDate: window1End,
    rowCount: byWindow[0].length,
    snapshotYmd
  });
  console.log('DEBUG forward window rows', {
    window: 2,
    startDays: 31,
    endDays: 60,
    windowStartDate: window2Start,
    windowEndDate: window2End,
    rowCount: byWindow[1].length,
    snapshotYmd
  });
  console.log('DEBUG forward window rows', {
    window: 3,
    startDays: 61,
    endDays: 90,
    windowStartDate: window3Start,
    windowEndDate: window3End,
    rowCount: byWindow[2].length,
    snapshotYmd
  });
  console.log('DEBUG forward window rows', {
    window: 4,
    startDays: 91,
    endDays: null,
    windowStartDate: window4Start,
    windowEndDate: null,
    rowCount: byWindow[3].length,
    snapshotYmd
  });

  const buildWindowMetrics = (rows, win) => {
    const b = windowBounds(win);
    const startYmd = addDaysYmd(snapshotYmd, b.lo);
    let endYmd = null;
    if (win === 4) {
      let maxLead = 91;
      for (const row of rows) {
        const st = row?._ingestion?.stay_date_ymd || getRowStayDateYmd(row);
        const ld = leadDaysToStay(st);
        if (ld !== null && ld > maxLead) maxLead = ld;
      }
      b.endLead = maxLead;
      endYmd = addDaysYmd(snapshotYmd, maxLead);
    } else {
      endYmd = addDaysYmd(snapshotYmd, b.hi);
    }

    let totalRnTy = 0;
    let totalRnStly = 0;
    let totalRnLy = 0;
    let totalRevTy = 0;
    let totalRevStly = 0;
    let totalRevLy = 0;

    const segMap = new Map();
    const dateRn = new Map();

    for (const row of rows) {
      const stay = row?._ingestion?.stay_date_ymd || getRowStayDateYmd(row);
      const rnTy = fwdNum(row, ['Room Nights TY (Actual / OTB)']) || 0;
      const rnStly = fwdNum(row, ['Room Nights STLY']) || 0;
      const rnLy = fwdNum(row, ['Room Nights LY Actual']) || 0;
      const revTy = fwdNum(row, ['Revenue TY (Actual / OTB)']) || 0;
      const revStly = fwdNum(row, ['Revenue STLY']) || 0;
      const revLy = fwdNum(row, ['Revenue LY Actual']) || 0;

      totalRnTy += rnTy;
      totalRnStly += rnStly;
      totalRnLy += rnLy;
      totalRevTy += revTy;
      totalRevStly += revStly;
      totalRevLy += revLy;

      if (stay) {
        dateRn.set(stay, (dateRn.get(stay) || 0) + rnTy);
      }

      const rawSeg = String(row?.['Market Segment Name'] || '').trim();
      const seg = mapMarketSegmentNameToUsaliBucket(rawSeg);
      if (!segMap.has(seg)) {
        segMap.set(seg, {
          seg,
          sampleName: rawSeg || null,
          rnTy: 0,
          rnStly: 0,
          rnLy: 0,
          revTy: 0,
          revStly: 0,
          revLy: 0
        });
      } else if (rawSeg && !segMap.get(seg).sampleName) {
        segMap.get(seg).sampleName = rawSeg;
      }
      const a = segMap.get(seg);
      a.rnTy += rnTy;
      a.rnStly += rnStly;
      a.rnLy += rnLy;
      a.revTy += revTy;
      a.revStly += revStly;
      a.revLy += revLy;
    }

    const referenceRn = totalRnStly > 0 ? totalRnStly : totalRnLy;
    console.log('DEBUG window signal inputs', {
      win,
      totalRnTy,
      totalRnStly,
      totalRnLy,
      referenceRn,
      paceGapPct: referenceRn > 0 ? ((totalRnTy - referenceRn) / referenceRn) * 100 : null
    });
    const referenceRev = totalRevStly > 0 ? totalRevStly : totalRevLy;
    const paceGapPct =
      referenceRn > 0 ? ((totalRnTy - referenceRn) / referenceRn) * 100 : null;
    const revenueGapPct =
      referenceRev > 0 ? ((totalRevTy - referenceRev) / referenceRev) * 100 : null;
    const blendedAdrTy = totalRnTy > 0 ? totalRevTy / totalRnTy : null;
    const referenceAdr = referenceRn > 0 ? referenceRev / referenceRn : null;
    const adrGapPct =
      blendedAdrTy !== null && referenceAdr !== null && referenceAdr > 0
        ? ((blendedAdrTy - referenceAdr) / referenceAdr) * 100
        : null;

    const segStats = [...segMap.values()].map((s) => {
      const refRnS = s.rnStly > 0 ? s.rnStly : s.rnLy;
      const refRevS = s.revStly > 0 ? s.revStly : s.revLy;
      const adrTyS = s.rnTy > 0 ? s.revTy / s.rnTy : null;
      const adrRefS = refRnS > 0 ? refRevS / refRnS : null;
      const rnDelta = refRnS > 0 ? s.rnTy - refRnS : null;
      const rnDeltaPct = refRnS > 0 ? ((s.rnTy - refRnS) / refRnS) * 100 : null;
      const adrDeltaPct =
        adrTyS !== null && adrRefS !== null && adrRefS > 0
          ? ((adrTyS - adrRefS) / adrRefS) * 100
          : null;
      return {
        ...s,
        refRnS,
        refRevS,
        adrTyS,
        adrRefS,
        rnDelta,
        rnDeltaPct,
        adrDeltaPct,
        segmentKey: s.seg,
        displayName: usaliBucketToDisplayName(s.seg, s.sampleName || '')
      };
    });

    let topGrowthSegment = null;
    let topDeclineSegment = null;
    let adrDilutionSegment = null;
    let bestGrow = null;
    let worstDecl = null;
    let worstAdrDil = null;

    for (const s of segStats) {
      if (s.rnDelta !== null && s.rnDelta > 0) {
        if (bestGrow === null || s.rnDelta > bestGrow) {
          bestGrow = s.rnDelta;
          topGrowthSegment = s;
        }
      }
      if (s.rnDelta !== null && s.rnDelta < 0) {
        if (worstDecl === null || s.rnDelta < worstDecl) {
          worstDecl = s.rnDelta;
          topDeclineSegment = s;
        }
      }
      if (s.rnDelta !== null && s.rnDelta > 0 && s.adrDeltaPct !== null && s.adrDeltaPct < 0) {
        if (worstAdrDil === null || s.adrDeltaPct < worstAdrDil) {
          worstAdrDil = s.adrDeltaPct;
          adrDilutionSegment = s;
        }
      }
    }

    const cap = capacityFallback.capacity;
    const capacityIsEstimate = capacityFallback.isEstimate;
    let hasHighOccDate = false;
    for (const tot of dateRn.values()) {
      if (cap > 0 && tot / cap > 0.8) {
        hasHighOccDate = true;
        break;
      }
    }

    const dilutionMeetsPeak =
      adrDilutionSegment != null &&
      (adrDilutionSegment?.adrTyS ?? null) != null &&
      (adrDilutionSegment?.adrRefS ?? null) != null &&
      (adrDilutionSegment?.adrTyS ?? 0) < (adrDilutionSegment?.adrRefS ?? 0);

    const paceGapThreshold = win === 1 ? -1 : win === 2 ? -2 : win === 3 ? -3 : -5;
    const adrErosionAdrThreshold = win === 1 ? -0.5 : win === 2 ? -1 : win === 3 ? -2 : -3;

    let signal = null;
    if (hasHighOccDate && dilutionMeetsPeak) {
      signal = 'peak_exposure';
    } else if (
      paceGapPct !== null &&
      adrGapPct !== null &&
      paceGapPct > 0 &&
      adrGapPct < adrErosionAdrThreshold
    ) {
      signal = 'adr_erosion';
    } else if (paceGapPct !== null && paceGapPct < paceGapThreshold) {
      signal = 'pace_gap';
    } else if (paceGapPct !== null && adrGapPct !== null && paceGapPct > 5 && adrGapPct > 0) {
      signal = 'pace_ahead_clean';
    }

    const snapD = parseYmdToUtcDate(snapshotYmd);
    const winStartD = parseYmdToUtcDate(startYmd);
    const winEndD = parseYmdToUtcDate(endYmd);
    let totalDaysInWindow = 0;
    if (winStartD && winEndD) {
      totalDaysInWindow = Math.max(
        1,
        Math.round((winEndD.getTime() - winStartD.getTime()) / 86400000) + 1
      );
    }
    const daysElapsedInWindow = 0;
    let remainingDays = 0;
    if (snapD && winEndD) {
      remainingDays = Math.max(0, Math.round((winEndD.getTime() - snapD.getTime()) / 86400000));
    }

    const dilutionRnOnPeakDates = (() => {
      if (!adrDilutionSegment) return 0;
      let acc = 0;
      for (const row of rows) {
        const rawSeg = String(row?.['Market Segment Name'] || '').trim();
        const seg = mapMarketSegmentNameToUsaliBucket(rawSeg);
        if (seg !== (adrDilutionSegment?.seg ?? null)) continue;
        const stay = row?._ingestion?.stay_date_ymd || getRowStayDateYmd(row);
        const tot = stay ? dateRn.get(stay) : 0;
        if (stay && cap > 0 && tot / cap > 0.8) {
          acc += fwdNum(row, ['Room Nights TY (Actual / OTB)']) || 0;
        }
      }
      return acc;
    })();

    const highOccStayDates = [...dateRn.entries()]
      .filter(([, tot]) => cap > 0 && Number.isFinite(tot) && tot / cap > 0.8)
      .map(([ymd]) => ymd)
      .filter(Boolean)
      .sort();
    const peakHighOccLabels = highOccStayDates
      .slice(0, 3)
      .map((ymd) => fmtYmdForExecutionBullet(ymd))
      .filter(Boolean);

    const winLab =
      startYmd && endYmd ? windowLabelFromBounds(b.lo, b.hi || b.endLead || 91, startYmd, endYmd) : `Window ${win}`;

    return {
      forward_window: win,
      window_label: winLab,
      window_start_ymd: startYmd || null,
      window_end_ymd: endYmd || null,
      peak_high_occ_labels: peakHighOccLabels,
      rows,
      totalRnTy,
      referenceRn,
      referenceRev,
      totalRevTy,
      paceGapPct,
      revenueGapPct,
      blendedAdrTy,
      referenceAdr,
      adrGapPct,
      topGrowthSegment,
      topDeclineSegment,
      adrDilutionSegment,
      signal,
      daysElapsedInWindow,
      totalDaysInWindow,
      remainingDays,
      endLead: b.endLead || b.hi || 30,
      dilutionRnOnPeakDates,
      capacityIsEstimate,
      cap,
      avgLead: 0,
      segStats
    };
  };

  const windowCandidates = [];
  for (let w = 0; w < 4; w += 1) {
    const chunk = byWindow[w];
    if (!chunk.length) continue;
    const m = buildWindowMetrics(chunk, w + 1);
    if (!m.signal) continue;
    windowCandidates.push(m);
  }

  const pri = { peak_exposure: 0, adr_erosion: 1, pace_gap: 2, pace_ahead_clean: 3 };
  windowCandidates.sort((a, b) => {
    const pa = pri[a.signal] ?? 99;
    const pb = pri[b.signal] ?? 99;
    if (pa !== pb) return pa - pb;
    return (a.forward_window ?? 99) - (b.forward_window ?? 99);
  });

  const picked = windowCandidates.slice(0, 3);

  const fwdRnCountForNarrative = (v) => {
    const r = Math.round(Math.abs(v));
    if (!Number.isFinite(r)) return '0';
    if (r > 100000) return 'more than 100,000';
    return String(r);
  };

  const bulletsFor = (signal, payload) => {
    const out = [];
    const push = (s) => {
      if (isSafeExecutionBulletText(s)) out.push(s.trim());
    };
    if (signal === 'peak_exposure') {
      const lbls = Array.isArray(payload.peakHighOccLabels) ? payload.peakHighOccLabels.filter(Boolean) : [];
      const adrDil = payload.dilAdrDeltaPctRaw;
      const adrDilAbs =
        adrDil !== null && Number.isFinite(adrDil) ? Math.abs(adrDil) : null;
      if (lbls.length && payload.segLabel && adrDilAbs !== null) {
        const open =
          lbls.length === 1
            ? `${lbls[0]} is already above 80% on books`
            : `${lbls.slice(0, 3).join(', ')} are already above 80% on books`;
        push(
          `${open} — ${payload.segLabel} holds disproportionate inventory at ADR ${adrDilAbs.toFixed(
            1
          )}% below last year. Close ${payload.segLabel} availability on these specific dates now.`
        );
      } else {
        push(
          `Close ${payload.segLabel} inventory on dates above 80% on-the-books occupancy immediately; treat capacity as ${
            payload.capacityIsEstimate ? 'an estimated 100-room proxy' : 'reported capacity'
          } until validated.`
        );
      }
      push(
        `Reopen ${payload.segLabel} only if occupancy on those stay dates falls below 70% inside the 10 days before arrival.`
      );
      push('Shift freed peak nights toward transient retail BAR in the RMS before accepting new discounted blocks.');
    } else if (signal === 'adr_erosion') {
      const revGp = payload.revenueGapPctRaw;
      const rnD = payload.dilRnDeltaPctRaw;
      const adrD = payload.dilAdrDeltaPctRaw;
      const rnTxt = fmtPct1(rnD);
      const adrTxt = fmtPct1(adrD);
      const revTxt = fmtPct1(revGp);
      const seg = payload.segLabel;
      const volOk = rnTxt && adrTxt && revTxt && seg;
      if (volOk && revGp !== null && Number.isFinite(revGp)) {
        const tail =
          revGp > 0
            ? 'The volume gain is currently offsetting the rate cost — set a rate floor to protect the margin.'
            : 'The volume gain is not offsetting the rate cost — the arithmetic is negative and requires immediate rate correction.';
        push(
          `${seg} volume is ${rnTxt} ahead but ADR is ${adrTxt} below last year. Net revenue versus last year: ${revTxt}. ${tail}`
        );
      }
      if (payload.revenueGapPctRaw !== null && Number.isFinite(payload.revenueGapPctRaw) && payload.revenueGapPctRaw > 0) {
        if (!volOk || revGp === null || !Number.isFinite(revGp)) {
          push(
            `Hold the current volume posture for this forward window but institute a hard ADR floor for ${payload.segLabel} at last year blended ADR (${payload.refAdrTxt}).`
          );
        }
        push('Review BAR ladders daily — any further erosion beyond the floor triggers an immediate yield meeting.');
      } else {
        if (!volOk || revGp === null || !Number.isFinite(revGp)) {
          push(
            `Stop accepting ${payload.segLabel} at dilutive rates for this window; the revenue arithmetic is negative versus last year.`
          );
        }
        push('Redirect availability to segments holding rate integrity before adding promotional depth.');
      }
      push('Reconcile segment-level OTB daily with finance to confirm net ADR after overrides.');
    } else if (signal === 'pace_gap') {
      const drag = payload.declLabel;
      const rnDeltaPct = payload.declRnDeltaPct;
      const segRnDelta = payload.declRnDelta;
      const totalRnGap = payload.totalRnGap;
      const winEnd = payload.windowEndReadable;
      const remDays = payload.remainingDays;
      const pctBehind =
        rnDeltaPct !== null && Number.isFinite(rnDeltaPct) ? Math.abs(rnDeltaPct) : null;
      const absSegRn = segRnDelta !== null && Number.isFinite(segRnDelta) ? Math.round(Math.abs(segRnDelta)) : null;
      const absTotalGap =
        totalRnGap !== null && Number.isFinite(totalRnGap) ? Math.abs(totalRnGap) : null;
      const shareOfGap =
        absSegRn !== null &&
        absTotalGap !== null &&
        absTotalGap > 0 &&
        Number.isFinite(absSegRn / absTotalGap)
          ? Math.round((absSegRn / absTotalGap) * 100)
          : null;
      const paceOpenOk =
        drag &&
        pctBehind !== null &&
        absSegRn !== null &&
        shareOfGap !== null &&
        winEnd &&
        segRnDelta !== null &&
        Number.isFinite(segRnDelta) &&
        totalRnGap !== null &&
        Number.isFinite(totalRnGap) &&
        totalRnGap < 0;
      if (paceOpenOk) {
        push(
          `${drag} is ${pctBehind.toFixed(1)}% behind last year — ${absSegRn} room nights short, representing ${shareOfGap}% of the total window shortfall. This segment needs ${absSegRn} additional room nights by ${winEnd} to close the gap.`
        );
      }
      const weeksChunk = remDays !== null && Number.isFinite(remDays) ? Math.max(1, Math.round(remDays / 7)) : null;
      const perWeekRn =
        absSegRn !== null && weeksChunk !== null && weeksChunk > 0
          ? Math.round(absSegRn / weeksChunk)
          : null;
      const weeklyOk = drag && perWeekRn !== null && winEnd;
      if (weeklyOk) {
        push(
          `Weekly OTB checkpoint threshold: if ${drag} does not recover at least ${perWeekRn} room nights per week, escalate to direct sales intervention before ${winEnd}.`
        );
      }
      if (payload.gapRecoverable) {
        if (!paceOpenOk) {
          push(`Let ${payload.growLabel} run without incremental discount; monitor pickup weekly on the same dashboard cut.`);
        }
        if (!weeklyOk) {
          push('Hold group and OTA releases steady — avoid last-minute panic cuts that collapse ADR.');
        }
      } else {
        if (!paceOpenOk) {
          push(
            `Launch targeted demand generation for ${payload.declLabel} now with explicit pickup targets; ${payload.growLabel} cannot close the RN gap alone.`
          );
        }
        push('Brief sales on deficit dates inside this window with minimum acceptable rate floors.');
        if (!weeklyOk) {
          push('Weekly steering meeting until OTB materially improves versus reference or the window is formally written off.');
        }
      }
    } else if (signal === 'pace_ahead_clean') {
      push('Freeze promotional rate plans for this window unless occupancy on a stay date drops below 65% inside 14 days of arrival.');
      push(`Protect ${payload.growLabel} rate integrity on the books — do not auto-match competitor tactical discounts.`);
      push('Brief revenue meetings that this window is confirmation-grade performance, not a discounting opportunity.');
    }
    return out.filter(isSafeExecutionBulletText).slice(0, 3);
  };

  const cards = [];
  for (const m of picked) {
    const sig = m.signal;
    const winLab = m.window_label;
    const segDil = forwardSegDisplayName(m.adrDilutionSegment);
    const growLab = forwardSegDisplayName(m.topGrowthSegment);
    const declLab = forwardSegDisplayName(m.topDeclineSegment);
    const growLabBullet = growLab || 'segments outpacing reference pace';
    const declLabBullet = declLab || 'segments trailing reference pace';
    const segDilBullet = segDil || 'discount-led segments';

    let title = 'Forward commercial signal';
    let narrative = '';
    let priority = 'medium';

    const refAdrTxt =
      m.referenceAdr !== null && Number.isFinite(m.referenceAdr) ? m.referenceAdr.toFixed(0) : 'last year';

    if (sig === 'peak_exposure') {
      title = 'Forward peak: compression with rate dilution';
      priority = 'high';
      const occNote = m.capacityIsEstimate
        ? ' (occupancy estimated using a 100-room capacity proxy — validate against actual keys)'
        : '';
      const dilAdrDeltaPct = m.adrDilutionSegment?.adrDeltaPct ?? null;
      const dilAdrTyS = m.adrDilutionSegment?.adrTyS ?? null;
      let peakP1Suffix = 'Forward nights on the books show peak-level compression with rate dilution risk in this window.';
      if (segDil) {
        peakP1Suffix =
          dilAdrDeltaPct !== null && Number.isFinite(dilAdrDeltaPct)
            ? `${segDil} holds the largest share of inventory at ADR ${Math.abs(dilAdrDeltaPct).toFixed(1)}% below last year.`
            : `${segDil} holds the largest share of compressed inventory in this window.`;
      }
      const estRn = Math.max(0, Math.round(m.dilutionRnOnPeakDates));
      let revRec =
        m.adrDilutionSegment != null &&
        dilAdrTyS !== null &&
        m.referenceAdr !== null &&
        estRn > 0
          ? estRn * Math.max(0, m.referenceAdr - dilAdrTyS)
          : null;
      let peakRevCapped = false;
      if (revRec !== null && Number.isFinite(revRec) && Math.abs(revRec) > FORWARD_REV_CAP) {
        revRec = null;
        peakRevCapped = true;
      }
      const revRecTxt = revRec !== null && Number.isFinite(revRec) ? fmtMoney0(revRec) : null;
      let peakP2 = null;
      if (peakRevCapped) {
        peakP2 =
          'Revenue impact not quantified — data coverage across this window is insufficient for a reliable estimate.';
      } else if (estRn > 0 && segDil && revRecTxt) {
        peakP2 = `Replacing an estimated ${fwdRnCountForNarrative(estRn)} room nights of ${segDil} allocation with transient retail at BAR would recover an estimated ${revRecTxt} in that window. The window to act is ${m.remainingDays} days.`;
      } else if (estRn > 0 && segDil) {
        peakP2 = `Replacing an estimated ${fwdRnCountForNarrative(estRn)} room nights of ${segDil} allocation with transient retail at BAR could recover revenue; timing to act is ${m.remainingDays} days.`;
      }
      const peakP3 = segDil
        ? `Close ${segDil} availability on dates above 80% OTB immediately. Reopen only if occupancy drops below 70% in the 10 days before arrival.`
        : 'Close dilutive segment availability on dates above 80% OTB immediately. Reopen only if occupancy drops below 70% in the 10 days before arrival.';
      narrative = [`${winLab} is tracking above 80% occupancy on books${occNote}. ${peakP1Suffix}`, peakP2, peakP3]
        .filter(Boolean)
        .join('\n\n');
    } else if (sig === 'adr_erosion') {
      title = 'Forward ADR erosion despite pace strength';
      const revPos = m.revenueGapPct !== null && m.revenueGapPct > 0;
      priority = revPos ? 'medium' : 'high';
      const dil = m.adrDilutionSegment;
      const dilRnPct = dil?.rnDeltaPct ?? null;
      const dilAdrPct = dil?.adrDeltaPct ?? null;
      const dilRn = fmtPct1(dilRnPct);
      const dilAdr = fmtPct1(dilAdrPct);
      const revGp = m.revenueGapPct !== null ? fmtPct1(m.revenueGapPct) : null;
      const marginTxt =
        m.revenueGapPct !== null && m.adrGapPct !== null
          ? (m.revenueGapPct - Math.abs(m.adrGapPct)).toFixed(1)
          : '0';
      const dilNm = segDil || 'discount-led segments';
      let adrErosionSegLine = '';
      if (dil) {
        const drv = [];
        if (dilRn) drv.push(`room nights ${dilRn} ahead versus last year`);
        if (dilAdr) drv.push(`ADR ${dilAdr} versus last year`);
        adrErosionSegLine = drv.length
          ? `${dilNm} is the primary driver — ${drv.join(' but ')}. Net revenue impact versus last year: ${revGp || 'unavailable'}.`
          : `${dilNm} is the primary driver of the blended ADR gap. Net revenue impact versus last year: ${revGp || 'unavailable'}.`;
      } else {
        adrErosionSegLine = `Segment-level dilution is mixed; net revenue impact versus last year: ${revGp || 'unavailable'}.`;
      }
      narrative = [
        `${winLab} is ${fmtPct1(m.paceGapPct)} ahead on room nights versus last year but blended ADR is ${fmtPct1(
          m.adrGapPct
        )} below. Volume is building but at a rate cost.`,
        adrErosionSegLine,
        revPos
          ? `Despite the ADR dilution, total revenue is ahead of last year. The volume gain is currently outweighing the rate cost — but the net pricing margin is only ${marginTxt} percentage points. Any further ADR erosion will turn this negative.`
          : `Total revenue is behind last year despite volume growth. The rate cost is already outweighing the volume gain. This window is losing money versus last year in revenue terms.`,
        revPos
          ? `Hold current volume strategy but set a rate floor for ${dilNm} at ${refAdrTxt} to prevent further dilution.`
          : `Stop accepting ${dilNm} at current rates for this window. The revenue arithmetic does not support the discount.`
      ].join('\n\n');
    } else if (sig === 'pace_gap') {
      title = 'Forward pace shortfall versus last year';
      priority = 'medium';

      const totalDaysInWindow = m.totalDaysInWindow ?? 0;
      const projectedGapVsReference = m.totalRnTy - m.referenceRn;
      const gapRecoverable =
        projectedGapVsReference >= 0 ||
        Boolean(
          m.topGrowthSegment != null &&
            (m.topGrowthSegment.rnDelta ?? 0) >= Math.abs(Math.min(0, projectedGapVsReference))
        );

      const declRnDeltaPct = m.topDeclineSegment?.rnDeltaPct ?? null;
      const growRnDeltaPct = m.topGrowthSegment?.rnDeltaPct ?? null;
      const declPct = fmtPct1(declRnDeltaPct);
      const growPct = fmtPct1(growRnDeltaPct);

      const pcg = m.paceGapPct;
      const paceIntro =
        pcg !== null && pcg < 0
          ? `${winLab} is ${Math.abs(pcg).toFixed(1)}% behind last year on room nights.`
          : pcg !== null && pcg > 0
            ? `${winLab} is ${pcg.toFixed(1)}% ahead of last year on room nights.`
            : `${winLab} is in line with last year on room nights.`;

      const absGapDisp = fwdRnCountForNarrative(projectedGapVsReference);
      let trajectoryPart = '';
      if (projectedGapVsReference < 0) {
        trajectoryPart = `Currently ${absGapDisp} room nights behind reference with ${totalDaysInWindow} day${
          totalDaysInWindow === 1 ? '' : 's'
        } remaining in the window.`;
      } else if (projectedGapVsReference > 0) {
        trajectoryPart = `Currently ${absGapDisp} room nights ahead of reference with ${totalDaysInWindow} day${
          totalDaysInWindow === 1 ? '' : 's'
        } remaining in the window.`;
      } else {
        trajectoryPart = `Currently aligned with reference on room nights with ${totalDaysInWindow} day${
          totalDaysInWindow === 1 ? '' : 's'
        } remaining in the window.`;
      }

      const revSuffix =
        ' Forward revenue trajectory from OTB alone is not projected here — snapshot history would be needed to infer pickup pace over time.';

      const paceSegParts = [];
      if (declPct && declLab) {
        paceSegParts.push(`${declLab} is the largest drag — ${declPct} behind last year.`);
      } else if (declPct) {
        paceSegParts.push(`The largest segment drag shows room nights ${declPct} behind last year.`);
      } else if (declLab) {
        paceSegParts.push(`${declLab} is the largest drag versus last year's reference pace.`);
      } else {
        paceSegParts.push("Room nights trail last year's reference pace across this window.");
      }
      const trackTail = gapRecoverable
        ? 'which may support coordinated recovery plays versus reference.'
        : 'which by itself does not offset the window-wide shortfall versus reference.';
      if (growPct && growLab) {
        paceSegParts.push(`${growLab} is the strongest performer at ${growPct} ahead, ${trackTail}`);
      } else if (growPct) {
        paceSegParts.push(
          `Fastest-recovering segment mix is ${growPct} ahead of reference on room nights, ${trackTail}`
        );
      } else if (growLab) {
        paceSegParts.push(`${growLab} is the strongest performer in this window, ${trackTail}`);
      }
      const paceSegLine = paceSegParts.join(' ');

      const para3 = gapRecoverable
        ? growLab
          ? `${growLab} shows constructive momentum versus last year. No emergency discounting — review OTB composition weekly.`
          : 'Segment mix shows constructive pockets versus last year. No emergency discounting — review OTB composition weekly.'
        : growLab
          ? `Prioritize demand generation for weaker segments${declLab ? ` including ${declLab}` : ''}. ${growLab} alone cannot close the overall OTB shortfall — ${totalDaysInWindow} day${
              totalDaysInWindow === 1 ? '' : 's'
            } remain in this window.`
          : `Prioritize demand generation for weaker segments${declLab ? ` including ${declLab}` : ''}. ${totalDaysInWindow} day${
              totalDaysInWindow === 1 ? '' : 's'
            } remain in this window.`;

      const para4 = gapRecoverable
        ? growLab
          ? `Keep yielding discipline: reinforce ${growLab} where it holds rate integrity; audit OTB weekly.`
          : 'Keep yielding discipline and audit OTB weekly until the window closes.'
        : declLab
          ? `Escalate demand generation and sales intervention for ${declLab}; set weekly OTB checkpoints until the window closes.`
          : 'Escalate demand generation and sales intervention; set weekly OTB checkpoints until the window closes.';

      narrative = [`${paceIntro} ${trajectoryPart}${revSuffix}`, paceSegLine, para3, para4].join('\n\n');
    } else if (sig === 'pace_ahead_clean') {
      title = 'Forward pace and rate confirmation';
      priority = 'low';
      const leadSeg = m.topGrowthSegment;
      const leadRnPct = leadSeg?.rnDeltaPct ?? null;
      const leadAdrPct = leadSeg?.adrDeltaPct ?? null;
      const leadRnFmt = fmtPct1(leadRnPct);
      const leadAdrFmt = fmtPct1(leadAdrPct);
      let leadTxt = 'Pickup for this window is outperforming reference pace on a volume basis.';
      if (growLab) {
        leadTxt = `${growLab} leads contribution in this window on a volume basis.`;
      }
      if (growLab && leadSeg != null && leadRnFmt && leadAdrFmt) {
        leadTxt = `${growLab} leads the window with room nights ${leadRnFmt} ahead versus last year at ADR ${leadAdrFmt}.`;
      } else if (growLab && leadSeg != null && leadRnFmt) {
        leadTxt = `${growLab} leads the window with room nights ${leadRnFmt} ahead versus last year.`;
      } else if (growLab && leadSeg != null && leadAdrFmt) {
        leadTxt = `${growLab} leads the window with ADR ${leadAdrFmt} versus last year.`;
      }
      narrative = [
        `${winLab} is ${fmtPct1(m.paceGapPct)} ahead on room nights with ADR ${fmtPct1(m.adrGapPct)} above last year. Both volume and rate are outperforming.`,
        leadTxt,
        `Protect this window from last-minute discounting. The position is strong — do not erode it with unnecessary promotional activity in the ${m.remainingDays} days before arrival.`
      ].join('\n\n');
    }

    const paragraphs = narrative.split('\n\n').map((s) => s.trim()).filter(Boolean);
    const enforcedDecisionLine = paragraphs.length ? paragraphs[paragraphs.length - 1] : '';

    let gapRecoverablePace = false;
    if (sig === 'pace_gap') {
      const currentRnGapP = m.totalRnTy - m.referenceRn;
      gapRecoverablePace =
        currentRnGapP >= 0 ||
        Boolean(
          m.topGrowthSegment != null &&
            (m.topGrowthSegment.rnDelta ?? 0) >= Math.abs(Math.min(0, currentRnGapP))
        );
    }

    const bulletCtx = {
      segLabel: segDilBullet,
      growLabel: growLabBullet,
      declLabel: declLabBullet,
      revenueGapPct: m.revenueGapPct ?? 0,
      refAdrTxt,
      gapRecoverable: gapRecoverablePace,
      capacityIsEstimate: m.capacityIsEstimate,
      declRnDeltaPct: m.topDeclineSegment?.rnDeltaPct ?? null,
      declRnDelta: m.topDeclineSegment?.rnDelta ?? null,
      totalRnGap: m.totalRnTy - m.referenceRn,
      remainingDays: m.remainingDays ?? null,
      windowEndReadable: fmtDayMonthYear(m.window_end_ymd),
      peakHighOccLabels: Array.isArray(m.peak_high_occ_labels) ? m.peak_high_occ_labels : [],
      dilAdrDeltaPctRaw: m.adrDilutionSegment?.adrDeltaPct ?? null,
      dilRnDeltaPctRaw: m.adrDilutionSegment?.rnDeltaPct ?? null,
      revenueGapPctRaw: m.revenueGapPct
    };

    const enforced_execution_actions = bulletsFor(sig, bulletCtx);

    cards.push({
      finding_key: `FWD_${sig}_${m.forward_window}`,
      issue_family: sig,
      segment: 'retail',
      priority,
      title,
      commercial_narrative: narrative,
      enforced_decision_line: enforcedDecisionLine,
      enforced_execution_actions,
      card_metrics: { avgMPI: null, avgARI: null, avgRGI: null, avgOcc: null },
      temporal_layer: 'forward_otb',
      window_label: winLab,
      forward_window: m.forward_window,
      pace_data: {
        otb_rn: Math.round(m.totalRnTy),
        reference_rn: Math.round(m.referenceRn),
        pace_gap_pct: m.paceGapPct !== null ? Math.round(m.paceGapPct * 10) / 10 : null,
        otb_rev: m.totalRevTy !== null ? Math.round(m.totalRevTy) : null,
        reference_rev: m.referenceRev !== null ? Math.round(m.referenceRev) : null,
        revenue_gap_pct: m.revenueGapPct !== null ? Math.round(m.revenueGapPct * 10) / 10 : null,
        otb_adr: m.blendedAdrTy !== null ? Math.round(m.blendedAdrTy) : null,
        reference_adr: m.referenceAdr !== null ? Math.round(m.referenceAdr) : null,
        window_label: winLab,
        window_start_ymd: m.window_start_ymd || null,
        window_end_ymd: m.window_end_ymd || null,
        remaining_days: m.remainingDays ?? null
      },
      is_forward_card: true
    });
  }

  return cards;
}

function inferHotelCapacityFromContext(strRows, diagnosis) {
  const rows = Array.isArray(strRows) ? strRows : [];
  if (!diagnosis && !rows.length) return { capacity: 100, isEstimate: true };
  for (const row of rows) {
    for (const [k, v] of Object.entries(row)) {
      if (k === '_ingestion') continue;
      const nk = normalizeKey(k);
      if (!/\broom\b/.test(nk) || !/\b(count|inventory|keys|supply)\b/.test(nk)) continue;
      const n = toFiniteNumberOrNull(v);
      if (n !== null && n >= 10 && n <= 5000) return { capacity: n, isEstimate: false };
    }
  }
  return { capacity: 100, isEstimate: true };
}

function buildCommercialNarrative(issue, diagnosis, segmentAttribution, dailyValidation, pmsRows, paceSignalSummary) {
  const m = diagnosis?.metrics || {};
  const avgMPI = toFiniteNumberOrNull(m.avgMPI);
  const avgARI = toFiniteNumberOrNull(m.avgARI);
  const avgRGI = toFiniteNumberOrNull(m.avgRGI);
  const avgOcc = toFiniteNumberOrNull(m.avgOcc);
  const mpiVar = toFiniteNumberOrNull(diagnosis?.mpiVar);
  const ariVar = toFiniteNumberOrNull(diagnosis?.ariVar);
  const rgiVar = toFiniteNumberOrNull(diagnosis?.rgiVar);
  const family = (issue?.issue_family || '').toString();
  const rows = Array.isArray(pmsRows) ? pmsRows : [];

  const segLabel = (bucket, sampleOriginalName = '') => usaliBucketToDisplayName(bucket, sampleOriginalName);
  const pct = (v, d = 1) => (v === null || !Number.isFinite(v) ? null : `${v >= 0 ? '+' : ''}${v.toFixed(d)}%`);
  const dirPoints = (v) => (v === null ? null : `${v >= 0 ? 'up' : 'down'} ${Math.abs(v).toFixed(1)} points vs LY`);
  const parseYmd = (row) => row?.['Date'] || null;
  const toYmd = (raw) => {
    if (!raw) return null;
    const s = String(raw).trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
    const d = parseExcelDate(s);
    return d ? formatDateToYMD(d) : null;
  };
  const fmtDate = (ymd) => {
    if (!ymd || !/^\d{4}-\d{2}-\d{2}$/.test(ymd)) return null;
    const [y, mth, d] = ymd.split('-').map(Number);
    const monthNames = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
    const mn = monthNames[(mth || 1) - 1];
    if (!mn || !d) return null;
    return `${d} ${mn}`;
  };
  const num = (row, keys) => {
    for (const k of keys) {
      const v = toFiniteNumberOrNull(row?.[k]);
      if (v !== null) return v;
    }
    return null;
  };
  const adrTyFromRow = (row) => {
    const direct = num(row, ['ADR TY']);
    if (direct !== null && direct !== 0) return direct;
    const revTy = num(row, ['Revenue TY (Actual / OTB)']);
    const rnTy = num(row, ['Room Nights TY (Actual / OTB)']);
    return revTy !== null && rnTy !== null && rnTy > 0 ? revTy / rnTy : null;
  };
  const adrLyFromRow = (row) => {
    const direct = num(row, ['ADR LY']);
    if (direct !== null && direct !== 0) return direct;
    const revLy = num(row, ['Revenue LY Actual']);
    const rnLy = num(row, ['Room Nights LY Actual']);
    return revLy !== null && rnLy !== null && rnLy > 0 ? revLy / rnLy : null;
  };

  const segAgg = new Map();
  const segDateAgg = new Map();
  for (const row of rows) {
    const rawSegName = String(row?.['Market Segment Name'] || '').trim();
    const seg = mapMarketSegmentNameToUsaliBucket(rawSegName);
    const rnTy = num(row, ['Room Nights TY (Actual / OTB)']);
    const rnLy = num(row, ['Room Nights LY Actual']);
    const adrTy = adrTyFromRow(row);
    const adrLy = adrLyFromRow(row);
    const revTy = (rnTy !== null && adrTy !== null) ? rnTy * adrTy : null;
    const revLy = (rnLy !== null && adrLy !== null) ? rnLy * adrLy : null;
    const ymd = toYmd(parseYmd(row));

    if (!segAgg.has(seg)) {
      segAgg.set(seg, {
        seg,
        sampleName: rawSegName || null,
        rnTy: 0,
        rnLy: 0,
        revTy: 0,
        revLy: 0,
        rnTySeen: false,
        rnLySeen: false,
        revTySeen: false,
        revLySeen: false
      });
    } else {
      const ex = segAgg.get(seg);
      if (!ex.sampleName && rawSegName) ex.sampleName = rawSegName;
    }
    const a = segAgg.get(seg);
    if (rnTy !== null) { a.rnTy += rnTy; a.rnTySeen = true; }
    if (rnLy !== null) { a.rnLy += rnLy; a.rnLySeen = true; }
    if (revTy !== null) { a.revTy += revTy; a.revTySeen = true; }
    if (revLy !== null) { a.revLy += revLy; a.revLySeen = true; }

    if (ymd) {
      const k = `${seg}|${ymd}`;
      if (!segDateAgg.has(k)) segDateAgg.set(k, { seg, ymd, rnTy: 0, rnLy: 0, revTy: 0, revLy: 0, rnTySeen: false, rnLySeen: false, revTySeen: false, revLySeen: false });
      const d = segDateAgg.get(k);
      if (rnTy !== null) { d.rnTy += rnTy; d.rnTySeen = true; }
      if (rnLy !== null) { d.rnLy += rnLy; d.rnLySeen = true; }
      if (revTy !== null) { d.revTy += revTy; d.revTySeen = true; }
      if (revLy !== null) { d.revLy += revLy; d.revLySeen = true; }
    }
  }

  const segStats = [...segAgg.values()].map((x) => {
    const rnDelta = (x.rnTySeen && x.rnLySeen) ? (x.rnTy - x.rnLy) : null;
    const rnPct = (rnDelta !== null && x.rnLy > 0) ? (rnDelta / x.rnLy) * 100 : null;
    const adrTy = (x.revTySeen && x.rnTy > 0) ? (x.revTy / x.rnTy) : null;
    const adrLy = (x.revLySeen && x.rnLy > 0) ? (x.revLy / x.rnLy) : null;
    const adrPct = (adrTy !== null && adrLy !== null && adrLy > 0) ? ((adrTy - adrLy) / adrLy) * 100 : null;
    return { ...x, rnDelta, rnPct, adrTy, adrLy, adrPct, absDelta: rnDelta === null ? 0 : Math.abs(rnDelta) };
  }).sort((a, b) => b.absDelta - a.absDelta);

  const topGrower = segStats.find((s) => s.rnDelta !== null && s.rnDelta > 0) || null;
  const topDecliner = segStats.find((s) => s.rnDelta !== null && s.rnDelta < 0) || null;

  const peakDatesFromValidation = Array.isArray(dailyValidation?.validated_peak_growth_dates)
    ? [...new Set(dailyValidation.validated_peak_growth_dates.map((d) => toYmd(d)).filter(Boolean))].sort()
    : [];
  const peakDates = peakDatesFromValidation.slice(0, 5);
  const peakDateSet = new Set(peakDates);

  let peakGrowthDates = [];
  let confirmedDisplacementDates = [];
  if (topGrower) {
    const segDateRows = [...segDateAgg.values()].filter((x) => x.seg === topGrower.seg);
    peakGrowthDates = segDateRows
      .filter((x) => x.rnTySeen && x.rnLySeen && x.rnTy > x.rnLy && peakDateSet.has(x.ymd))
      .map((x) => x.ymd)
      .sort();
    confirmedDisplacementDates = segDateRows
      .filter((x) => {
        if (!(x.rnTySeen && x.rnLySeen && x.rnTy > x.rnLy && peakDateSet.has(x.ymd))) return false;
        const adrTy = x.rnTy > 0 && x.revTySeen ? x.revTy / x.rnTy : null;
        const adrLy = x.rnLy > 0 && x.revLySeen ? x.revLy / x.rnLy : null;
        return adrTy !== null && adrLy !== null && adrTy < adrLy;
      })
      .map((x) => x.ymd)
      .sort();
  }

  const totalRnTy = rows.reduce((s, row) => s + (num(row, ['Room Nights TY (Actual / OTB)']) || 0), 0);
  const totalRnStly = rows.reduce((s, row) => s + (num(row, ['Room Nights STLY']) || 0), 0);
  const totalRnLy = rows.reduce((s, row) => s + (num(row, ['Room Nights LY Actual']) || 0), 0);
  const paceBase = totalRnStly > 0 ? totalRnStly : totalRnLy > 0 ? totalRnLy : null;
  const paceGapPct = paceBase ? ((totalRnTy - paceBase) / paceBase) * 100 : null;

  const fmtPeakDates = (dates) => dates.map((d) => fmtDate(d)).filter(Boolean).slice(0, 5).join(', ');
  const topGrowerLine = topGrower && (topGrower.rnPct !== null || topGrower.adrPct !== null)
    ? `${segLabel(topGrower.seg, topGrower.sampleName)} grew room nights ${pct(topGrower.rnPct) || ''}${topGrower.adrPct !== null ? ` with ADR ${pct(topGrower.adrPct)}` : ''}.`.replace(/\s+/g, ' ').trim()
    : null;
  const topDeclinerLine = topDecliner && (topDecliner.rnPct !== null || topDecliner.adrPct !== null)
    ? `${segLabel(topDecliner.seg, topDecliner.sampleName)} declined room nights ${pct(topDecliner.rnPct) || ''}${topDecliner.adrPct !== null ? ` with ADR ${pct(topDecliner.adrPct)}` : ''}.`.replace(/\s+/g, ' ').trim()
    : null;

  const lines = [];
  const pushLine = (t) => { if (t && t.trim()) lines.push(t.trim()); };
  const packNarrative = (fullText) => {
    if (fullText == null || String(fullText).trim() === '') {
      return { narrative: null, decisionLine: null };
    }
    const paragraphs = String(fullText)
      .split('\n\n')
      .map((s) => s.trim())
      .filter(Boolean);
    if (!paragraphs.length) return { narrative: null, decisionLine: null };
    const lastParagraph = paragraphs[paragraphs.length - 1];
    const paragraphsExceptLast = paragraphs.slice(0, -1);
    return {
      narrative: paragraphsExceptLast.length ? paragraphsExceptLast.join('\n\n') : null,
      decisionLine: lastParagraph
    };
  };
  const trendStr = [
    mpiVar !== null ? `MPI ${dirPoints(mpiVar)}` : null,
    ariVar !== null ? `ARI ${dirPoints(ariVar)}` : null,
    rgiVar !== null ? `RGI ${dirPoints(rgiVar)}` : null
  ].filter(Boolean).join('; ');

  const primaryBucket = (segmentAttribution?.primary_segment || '').toString();
  let sampleNameForPrimary = '';
  for (const row of rows) {
    const raw = String(row?.['Market Segment Name'] || '').trim();
    if (!raw) continue;
    if (mapMarketSegmentNameToUsaliBucket(raw) === primaryBucket) {
      sampleNameForPrimary = raw;
      break;
    }
  }
  const primarySegDisplay = usaliBucketToDisplayName(primaryBucket || 'other', sampleNameForPrimary);

  const stayYmds = rows
    .map((r) => getRowStayDateYmd(r))
    .filter((y) => y && /^\d{4}-\d{2}-\d{2}$/.test(y))
    .sort();
  let pmsHorizonWithin30 = false;
  if (stayYmds.length >= 2) {
    const dayDiff = (Date.parse(stayYmds[stayYmds.length - 1]) - Date.parse(stayYmds[0])) / 86400000;
    pmsHorizonWithin30 = dayDiff <= 30;
  }
  const paceWaitDays = pmsHorizonWithin30 ? 14 : 7;

  let primaryRnTyStly = { ty: 0, stly: 0, hasStly: false };
  if (primaryBucket && primaryBucket !== 'unknown') {
    for (const row of rows) {
      const raw = String(row?.['Market Segment Name'] || '').trim();
      if (mapMarketSegmentNameToUsaliBucket(raw) !== primaryBucket) continue;
      const rnt = num(row, ['Room Nights TY (Actual / OTB)']);
      const rns = num(row, ['Room Nights STLY']);
      if (rnt !== null) primaryRnTyStly.ty += rnt;
      if (rns !== null) {
        primaryRnTyStly.stly += rns;
        primaryRnTyStly.hasStly = true;
      }
    }
  }
  const primaryPickupBelowStly =
    primaryRnTyStly.hasStly && primaryRnTyStly.stly > 0 && primaryRnTyStly.ty < primaryRnTyStly.stly;

  const primarySegStat =
    primaryBucket && primaryBucket !== 'unknown' ? segStats.find((s) => s.seg === primaryBucket) || null : null;
  const segAdrDeltaPct = primarySegStat?.adrPct ?? null;
  const primaryRnPctNeg =
    primarySegStat !== null && primarySegStat.rnPct !== null && primarySegStat.rnPct < 0;

  const secondVolumeSeg = [...segStats]
    .filter((s) => s.seg !== primaryBucket && s.rnTy > 0)
    .sort((a, b) => b.rnTy - a.rnTy)[0] || null;
  const secondVolLabel = secondVolumeSeg ? segLabel(secondVolumeSeg.seg, secondVolumeSeg.sampleName) : null;

  let varianceStory = '';
  {
    const M = mpiVar;
    const A = ariVar;
    const R = rgiVar;
    const hasM = M !== null;
    const hasA = A !== null;
    const hasR = R !== null;
    if (hasM && hasA && hasR) {
      if (R > 0 && M > 0 && A > 0) {
        varianceStory =
          'All three indices are improving versus last year — rate, share, and total revenue performance are moving in the right direction simultaneously.';
      } else if (R > 0 && M > 0 && A <= 0) {
        const pts = Math.abs(A).toFixed(1);
        let exchange = '';
        if (A !== 0 && Number.isFinite(M) && Number.isFinite(A)) {
          exchange = ` The exchange rate is ${Math.abs(M / A).toFixed(1)} points of share per point of rate concession.`;
        } else if (A === 0) {
          exchange =
            ' ARI is unchanged versus last year on a points basis, so a share-for-rate exchange ratio is not defined.';
        }
        varianceStory = `RGI and MPI are both improving versus last year but ARI has declined ${pts} points — share recovery is being bought with rate.${exchange}`;
      } else if (R > 0 && M <= 0 && A > 0) {
        varianceStory = `RGI is improving and ARI is strengthening but MPI has softened ${Math.abs(M).toFixed(
          1
        )} points versus last year — rate-led growth is coming at a share cost. Monitor whether MPI continues to slide or stabilises.`;
      } else if (R > 0 && M <= 0 && A <= 0) {
        varianceStory =
          'RGI is improving despite both MPI and ARI declining versus last year. This suggests an external factor — competitor supply reduction or demand concentration — rather than commercial strategy execution. This outperformance is fragile and should not be treated as earned.';
      } else if (R <= 0 && M > 0 && A <= 0) {
        varianceStory = `Share is recovering — MPI up ${M.toFixed(1)} points versus last year — but rate dilution is outpacing the volume gain. RGI has declined ${Math.abs(
          R
        ).toFixed(1)} points despite the share improvement. The strategy is gaining volume but losing revenue quality.`;
      } else if (R <= 0 && M <= 0 && A > 0) {
        varianceStory =
          'This is the most commercially dangerous variance pattern: rate premium is strengthening but both share and total revenue performance are declining versus last year. The market is rejecting the rate position and moving to competitors. Tactical rate correction is not optional — it is overdue.';
      } else if (R <= 0 && M <= 0 && A <= 0) {
        varianceStory =
          'All three indices are declining versus last year — rate, share, and total revenue performance are deteriorating simultaneously. No single lever is working. This requires a strategic reset, not a tactical adjustment.';
      } else if (R <= 0 && M > 0 && A > 0) {
        varianceStory =
          'Both MPI and ARI are above last year but RGI is declining — this is a contradictory signal. Validate comp set composition and data quality before acting. If the data is clean, the issue is occupancy mix or length-of-stay dilution.';
      }
    } else if (hasM || hasA || hasR) {
      const parts = [];
      if (hasR) parts.push(R > 0 ? 'RGI is improving versus last year' : 'RGI is declining versus last year');
      if (hasM) parts.push(M > 0 ? 'MPI is improving versus last year' : 'MPI is declining versus last year');
      if (hasA) parts.push(A > 0 ? 'ARI is strengthening versus last year' : 'ARI has declined versus last year');
      varianceStory = `${parts.join(
        '; '
      )}. The full three-index variance matrix is incomplete in this snapshot — interpret alongside segment OTB before acting.`;
    }
  }

  if (family === 'pricing_resistance') {
    pushLine([
      avgARI !== null ? `ARI is ${avgARI.toFixed(1)}` : null,
      avgMPI !== null ? `MPI is ${avgMPI.toFixed(1)}` : null,
      avgRGI !== null ? `RGI is ${avgRGI.toFixed(1)}` : null
    ].filter(Boolean).join(', ') + (trendStr ? `. ${trendStr}.` : '.'));
    if (varianceStory) pushLine(varianceStory);
    pushLine([topGrowerLine, topDeclinerLine].filter(Boolean).join(' ') || null);
    if (confirmedDisplacementDates.length) {
      pushLine(`On ${fmtPeakDates(confirmedDisplacementDates)} the ${segLabel(topGrower.seg, topGrower.sampleName)} segment grew volume at below-last-year ADR on peak occupancy days at or above 80%, confirming displacement risk.`);
    } else if (peakGrowthDates.length) {
      pushLine(`Peak-date checks on ${fmtPeakDates(peakGrowthDates)} show growth on high-occupancy days without confirmed below-last-year ADR displacement.`);
    } else if (peakDates.length) {
      pushLine(`Peak-date checks on ${fmtPeakDates(peakDates)} are clean for confirmed low-rated displacement in the lead growth segment.`);
    }
    if (paceGapPct !== null) pushLine(`Forward room-night pace is ${pct(paceGapPct)} versus reference pace, so rate decisions now directly determine whether the share gap closes or widens.`);
    let pricingDirective = '';
    if (confirmedDisplacementDates.length) {
      pricingDirective = `Rate correction is needed on ${fmtPeakDates(confirmedDisplacementDates)}. These are peak days where the current premium is provably costing room nights. Protect rate on all other dates and correct selectively on these.`;
    } else if (paceGapPct !== null && paceGapPct < 0 && avgRGI !== null && avgRGI < 100) {
      pricingDirective =
        'The rate premium is not being absorbed and forward pace is soft. Reduce BAR on shoulder dates by enough to close the share gap — target MPI recovery to 100 without surrendering the full ARI premium.';
    } else if ((paceGapPct === null || paceGapPct >= 0) && rgiVar !== null && rgiVar > 0) {
      pricingDirective =
        'The gap is narrowing on its own. Hold current rate positioning and do not correct — intervening now risks disrupting a trend that is already moving in the right direction.';
    } else {
      pricingDirective =
        'Correct rate on the specific dates and segments where share loss is confirmed. Do not apply a blanket reduction — surgical correction preserves positioning while recovering share.';
    }
    pushLine(pricingDirective);
    return packNarrative(lines.slice(0, 8).join('\n\n'));
  }

  if (
    family === 'intentional_volume_recovery' ||
    (avgARI !== null && avgMPI !== null && avgARI < 100 && avgMPI > 95 && mpiVar !== null && mpiVar > 0 && ariVar !== null && ariVar < 0)
  ) {
    pushLine(`This is a deliberate volume-recovery posture: ${avgARI !== null ? `ARI ${avgARI.toFixed(1)}` : ''}${avgMPI !== null ? `, MPI ${avgMPI.toFixed(1)}` : ''}${avgRGI !== null ? `, RGI ${avgRGI.toFixed(1)}` : ''}. ${trendStr}`.trim());
    if (varianceStory) pushLine(varianceStory);
    pushLine(topGrowerLine || null);
    if (confirmedDisplacementDates.length) {
      pushLine(`On ${fmtPeakDates(confirmedDisplacementDates)} the recovery segment is growing on peak dates at below-last-year ADR, which indicates value destruction rather than need-period fill.`);
    } else if (peakGrowthDates.length) {
      pushLine(`Growth dates on peaks are ${fmtPeakDates(peakGrowthDates)} and do not show confirmed below-last-year ADR displacement.`);
    } else if (topGrower) {
      pushLine(`Current growth in ${segLabel(topGrower.seg, topGrower.sampleName)} is concentrated away from confirmed peak-displacement signals, consistent with need-period filling.`);
    }
    if (paceGapPct !== null) pushLine(`Pace is ${pct(paceGapPct)} versus reference, which indicates whether discount cost is producing the expected room-night response.`);
    let volDirective = '';
    if (paceGapPct !== null && paceGapPct < 0 && confirmedDisplacementDates.length) {
      volDirective = `The volume strategy is not delivering and it is displacing value on peak dates. Stop accepting ${primarySegDisplay} at below-STLY rates on ${fmtPeakDates(confirmedDisplacementDates)} immediately. The discount has not earned its cost.`;
    } else if (paceGapPct !== null && paceGapPct < 0) {
      volDirective = `Pace has not closed despite the rate concession. Give it ${paceWaitDays} days before extending the discount further. If pace does not respond, begin rate restoration on the highest-demand dates first.`;
    } else {
      volDirective =
        'Volume strategy is working. Begin selective rate restoration on peak dates to recover ADR without losing the share gains. Do not unwind the full discount — taper it.';
    }
    pushLine(volDirective);
    return packNarrative(lines.slice(0, 8).join('\n\n'));
  }

  if (family === 'discount_inefficiency') {
    pushLine([
      avgARI !== null ? `ARI is ${avgARI.toFixed(1)}` : null,
      avgMPI !== null ? `MPI is ${avgMPI.toFixed(1)}` : null,
      avgRGI !== null ? `RGI is ${avgRGI.toFixed(1)}` : null
    ].filter(Boolean).join(', ') + '.');
    if (varianceStory) pushLine(varianceStory);
    if (topGrower && topGrower.adrPct !== null && topGrower.rnPct !== null) {
      const dispro = Math.abs(topGrower.adrPct) > Math.abs(topGrower.rnPct);
      pushLine(`${segLabel(topGrower.seg, topGrower.sampleName)} is discounting most aggressively (ADR ${pct(topGrower.adrPct)}) while room nights moved ${pct(topGrower.rnPct)}${dispro ? ', which is less than proportional to the rate cut' : ''}.`);
    } else {
      pushLine([topGrowerLine, topDeclinerLine].filter(Boolean).join(' ') || null);
    }
    if (peakGrowthDates.length) {
      pushLine(`Discount-led growth appears on peak dates ${fmtPeakDates(peakGrowthDates)} where discount dependence should be minimal.`);
    } else if (peakDates.length) {
      pushLine(`Peak-date review on ${fmtPeakDates(peakDates)} does not show material discount-led volume growth.`);
    }
    let discDirective = '';
    if (
      primaryBucket &&
      primaryBucket !== 'unknown' &&
      segAdrDeltaPct !== null &&
      segAdrDeltaPct < 0 &&
      primaryRnPctNeg &&
      secondVolLabel
    ) {
      discDirective = `${primarySegDisplay} is declining in both volume and rate. This segment is not responding to discounting. Stop discounting to this segment and redirect inventory to ${secondVolLabel}.`;
    } else {
      discDirective = `Discounting is not converting. Do not cut further. Audit the booking path for ${primarySegDisplay} — the block is not price, it is friction.`;
    }
    pushLine(discDirective);
    return packNarrative(lines.slice(0, 8).join('\n\n'));
  }

  if (family === 'visibility_gap') {
    const hasMetrics = avgMPI !== null || avgARI !== null || avgRGI !== null;
    if (!hasMetrics) {
      pushLine(`This issue is generated from a lighter signal path and points to demand not reliably reaching the hotel before booking decisions are made.`);
    } else {
      pushLine([
        avgMPI !== null ? `MPI is ${avgMPI.toFixed(1)}` : null,
        avgARI !== null ? `ARI is ${avgARI.toFixed(1)}` : null,
        avgRGI !== null ? `RGI is ${avgRGI.toFixed(1)}` : null
      ].filter(Boolean).join(', ') + (trendStr ? `. ${trendStr}.` : '.'));
    }
    if (varianceStory) pushLine(varianceStory);
    const weakestPickup = segStats
      .filter((s) => s.rnPct !== null)
      .sort((a, b) => a.rnPct - b.rnPct)[0];
    if (weakestPickup) {
      pushLine(`${segLabel(weakestPickup.seg, weakestPickup.sampleName)} shows the weakest pickup versus last year with room nights ${pct(weakestPickup.rnPct)}${weakestPickup.adrPct !== null ? ` and ADR ${pct(weakestPickup.adrPct)}` : ''}.`);
    }
    let visDirective = '';
    if (primaryPickupBelowStly && primaryBucket && primaryBucket !== 'unknown') {
      visDirective = `${primarySegDisplay} pickup is the weakest it has been versus last year. Activate targeted demand generation for this segment before the booking window closes.`;
    } else {
      visDirective =
        'Demand is not reaching the hotel before booking decisions are made. Prioritise top-of-funnel activation over rate or conversion changes — fixing downstream friction will not help if upstream demand is absent.';
    }
    pushLine(visDirective);
    return packNarrative(lines.slice(0, 8).join('\n\n'));
  }

  if (avgRGI !== null && avgRGI >= 100) {
    pushLine(`RGI is ${avgRGI.toFixed(1)}, confirming outperformance versus the competitive set${rgiVar !== null ? ` with RGI ${dirPoints(rgiVar)}` : ''}.`);
    if (varianceStory) pushLine(varianceStory);
    if (topGrower) {
      const healthy = topGrower.adrPct === null || topGrower.adrPct >= 0;
      pushLine(`${segLabel(topGrower.seg, topGrower.sampleName)} is the primary performance driver with room nights ${pct(topGrower.rnPct)}${topGrower.adrPct !== null ? ` and ADR ${pct(topGrower.adrPct)}` : ''}, which is ${healthy ? 'quality-accretive' : 'potentially fragile due to rate dilution'}.`);
    }
    pushLine(
      'Hold outperformance discipline: protect mix quality and rate integrity on compression peaks; add volume only where demand is truly incremental and dilution risk is low.'
    );
    return packNarrative(lines.slice(0, 8).join('\n\n'));
  }

  return { narrative: null, decisionLine: null };
}

function buildRetailIssueNarrative(issue, quantification, commercialContext) {
  const family = (issue?.issue_family || '').toString();
  const q = quantification || {};
  const ctx = commercialContext || {};
  const qs = q.quantified_signals || {};
  const rev = qs.revenue_range;
  const rnRisk = qs.room_nights_at_risk;
  const impactBand = q.impact_band || 'low';
  const situation = ctx.commercial_situation || 'mixed_or_unclear_context';

  const scaleText = rev
    ? `estimated revenue at risk ${rev.min}-${rev.max}`
    : rnRisk != null
      ? `${rnRisk} room nights exposed`
      : 'limited quantified scale';

  let whyThisMatters = `This issue can erode retail performance through ${scaleText}.`;
  if (impactBand === 'high') whyThisMatters = `This appears commercially material with ${scaleText}.`;
  else if (impactBand === 'medium') whyThisMatters = `This looks moderately material with ${scaleText}.`;

  let whatThisLikelyMeans = `Pattern is consistent with ${situation.replace(/_/g, ' ')}.`;
  if (family === 'visibility_gap') {
    whatThisLikelyMeans = `Signals suggest demand capture weakness and share softness under current market conditions (${situation.replace(/_/g, ' ')}).`;
  } else if (family === 'discount_inefficiency') {
    whatThisLikelyMeans = `Signals suggest discounting is not translating into proportional volume gains (${situation.replace(/_/g, ' ')}).`;
  } else if (family === 'pricing_resistance') {
    whatThisLikelyMeans = `Signals suggest rate position may be suppressing conversion more than expected (${situation.replace(/_/g, ' ')}).`;
  } else if (family === 'mix_constraint') {
    whatThisLikelyMeans = `Signals suggest business-mix friction is limiting performance despite available demand (${situation.replace(/_/g, ' ')}).`;
  } else if (family === 'missed_pricing_opportunity') {
    whatThisLikelyMeans = `Signals suggest unrealized pricing upside in periods where demand appears resilient (${situation.replace(/_/g, ' ')}).`;
  }

  let commercialWatchout =
    'Watch for repeated underperformance across upcoming periods; compounding effects can emerge even from moderate gaps.';
  if (ctx.constraint_status === 'high') {
    commercialWatchout =
      'High inventory pressure/compression context means execution errors can destroy value quickly; protect mix quality.';
  } else if (ctx.constraint_status === 'low' && impactBand !== 'low') {
    commercialWatchout =
      'With limited inventory pressure, persistent weakness likely reflects execution friction rather than pure capacity constraint.';
  }

  return {
    why_this_matters: whyThisMatters,
    what_this_likely_means: whatThisLikelyMeans,
    commercial_watchout: commercialWatchout
  };
}

function buildCommercialContextSummary(issues, financialQuantificationSummary, contextData) {
  const retailIssues = (Array.isArray(issues) ? issues : []).filter(
    (issue) => (issue?.segment || 'retail') === 'retail'
  );
  const quantRows = Array.isArray(financialQuantificationSummary?.issue_level_quantification)
    ? financialQuantificationSummary.issue_level_quantification
    : [];
  const quantByKey = new Map(quantRows.map((q) => [q.finding_key, q]));
  const notes = Array.isArray(financialQuantificationSummary?.data_quality_notes)
    ? [...financialQuantificationSummary.data_quality_notes]
    : [];

  const issueLevelContext = retailIssues.map((issue) => {
    const q = quantByKey.get(issue.finding_key) || {
      impact_band: 'low',
      quantified_signals: {
        room_nights_at_risk: null,
        adr_gap: null,
        revenue_range: null,
        index_gap: null,
        occupancy_gap: null
      },
      confidence: 'low'
    };
    const ctx = detectRetailCommercialContext(issue, { ...contextData, quantification: q });
    const narrative = buildRetailIssueNarrative(issue, q, ctx);
    return {
      finding_key: issue.finding_key,
      issue_family: issue.issue_family,
      commercial_situation: ctx.commercial_situation,
      context_flags: ctx.context_flags,
      dominant_signal: ctx.dominant_signal,
      constraint_status: ctx.constraint_status,
      confidence:
        ctx.confidence === 'low' || q.confidence === 'low'
          ? 'low'
          : ctx.confidence === 'high' && q.confidence === 'high'
            ? 'high'
            : 'medium',
      narrative
    };
  });

  const flagCounts = {};
  for (const row of issueLevelContext) {
    for (const f of row.context_flags || []) flagCounts[f] = (flagCounts[f] || 0) + 1;
  }
  const topFlag = Object.entries(flagCounts).sort((a, b) => b[1] - a[1])[0]?.[0] || null;

  let overallMarketContext = 'mixed_or_unclear_context';
  if (topFlag === 'share_softness' || topFlag === 'occupancy_softness') {
    overallMarketContext = 'demand_or_share_softness_context';
  } else if (topFlag === 'pricing_resistance') {
    overallMarketContext = 'pricing_friction_context';
  } else if (topFlag === 'discount_leakage') {
    overallMarketContext = 'discount_inefficiency_context';
  } else if (topFlag === 'possible_compression' || topFlag === 'inventory_pressure') {
    overallMarketContext = 'inventory_pressure_context';
  }

  const highConstraint = issueLevelContext.filter((r) => r.constraint_status === 'high').length;
  const mediumConstraint = issueLevelContext.filter((r) => r.constraint_status === 'medium').length;
  let overallConstraintContext = 'unclear';
  if (highConstraint > 0) overallConstraintContext = 'high';
  else if (mediumConstraint > 0) overallConstraintContext = 'medium';
  else if (issueLevelContext.length > 0) overallConstraintContext = 'low';

  const highConf = issueLevelContext.filter((r) => r.confidence === 'high').length;
  const medOrHigh = issueLevelContext.filter((r) => r.confidence !== 'low').length;
  let contextConfidence = 'low';
  if (issueLevelContext.length > 0 && highConf >= Math.ceil(issueLevelContext.length / 2)) contextConfidence = 'high';
  else if (medOrHigh >= Math.ceil(Math.max(1, issueLevelContext.length) / 2)) contextConfidence = 'medium';

  const portfolioWatchouts = [];
  if (flagCounts.share_softness || flagCounts.occupancy_softness) {
    portfolioWatchouts.push('Share/occupancy softness appears across multiple retail issue signals.');
  }
  if (flagCounts.pricing_resistance) {
    portfolioWatchouts.push('Pricing resistance context appears repeatedly; watch conversion sensitivity.');
  }
  if (flagCounts.discount_leakage) {
    portfolioWatchouts.push('Discount leakage signals suggest potential margin-for-volume inefficiency.');
  }
  if (highConstraint > 0) {
    portfolioWatchouts.push('Some issues occur under high inventory pressure/compression-like conditions.');
  }
  if (!issueLevelContext.length) {
    notes.push('No visible retail issues available for commercial context classification.');
  }
  if (contextConfidence === 'low' && issueLevelContext.length > 0) {
    notes.push('Commercial context confidence is low due to limited reliable context signals.');
  }

  return {
    schema_version: '1.0',
    overall_market_context: overallMarketContext,
    overall_constraint_context: overallConstraintContext,
    context_confidence: contextConfidence,
    issue_level_context: issueLevelContext,
    portfolio_watchouts: portfolioWatchouts,
    data_quality_notes: notes
  };
}

function classifyRetailDecisionType(issue, quantification, context) {
  const family = (issue?.issue_family || '').toString();
  const q = quantification || {};
  const c = context || {};
  const flags = Array.isArray(c.context_flags) ? c.context_flags : [];
  const idxGap = toFiniteNumberOrNull(q?.quantified_signals?.index_gap);
  const occGap = toFiniteNumberOrNull(q?.quantified_signals?.occupancy_gap);

  let decisionType = 'monitoring_only';
  let primaryLever = 'performance_monitoring';
  let decisionIntent = 'Validate whether current signals persist before committing major commercial changes.';

  if (family === 'pricing_resistance') {
    decisionType = 'pricing_adjustment';
    primaryLever = 'rate_positioning';
    decisionIntent = 'Re-evaluate price positioning versus demand response to reduce conversion drag.';
  } else if (family === 'discount_inefficiency') {
    decisionType = 'discount_strategy_review';
    primaryLever = 'discount_architecture';
    decisionIntent = 'Review discount depth/structure where volume response appears insufficient.';
  } else if (family === 'mix_constraint') {
    decisionType = 'mix_rebalancing';
    primaryLever = 'channel_mix';
    decisionIntent = 'Rebalance business mix priorities where current composition suppresses performance quality.';
  } else if (family === 'missed_pricing_opportunity') {
    decisionType = 'pricing_adjustment';
    primaryLever = 'rate_capture';
    decisionIntent = 'Assess selective pricing uplift where demand appears resilient.';
  } else if (family === 'visibility_gap') {
    if (flags.includes('occupancy_softness') || (occGap !== null && occGap >= 6) || (idxGap !== null && idxGap >= 6)) {
      decisionType = 'demand_generation';
      primaryLever = 'demand_capture';
      decisionIntent = 'Prioritize demand capture and conversion readiness in periods showing softness.';
    } else {
      decisionType = 'conversion_optimization';
      primaryLever = 'conversion_funnel';
      decisionIntent = 'Tighten conversion execution where visibility appears to underperform relative to demand.';
    }
  } else if (flags.includes('possible_compression') || flags.includes('inventory_pressure')) {
    decisionType = 'mix_rebalancing';
    primaryLever = 'inventory_allocation';
    decisionIntent = 'Protect value quality under constrained inventory conditions.';
  }

  const signalPoints = [
    q?.impact_band,
    q?.quantified_signals?.room_nights_at_risk,
    q?.quantified_signals?.revenue_range?.max,
    c?.dominant_signal
  ].filter((v) => v !== null && v !== undefined).length;
  const confidence =
    signalPoints >= 4 && (q?.confidence === 'high' || c?.confidence === 'high')
      ? 'high'
      : signalPoints >= 2
        ? 'medium'
        : 'low';

  return {
    decision_type: decisionType,
    primary_lever: primaryLever,
    decision_intent: decisionIntent,
    confidence
  };
}

function assessDecisionUrgency(issue, quantification, context) {
  const q = quantification || {};
  const c = context || {};
  const maxRev = Number(q?.quantified_signals?.revenue_range?.max || 0);
  const rnRisk = Number(q?.quantified_signals?.room_nights_at_risk || 0);
  const impactBand = q?.impact_band || 'low';
  const isConstraintHigh = c?.constraint_status === 'high';
  const family = (issue?.issue_family || '').toString();

  let urgencyLevel = 'low';
  let urgencyReason = 'Current quantified exposure appears limited; monitor for persistence.';

  if (maxRev >= 18000 || rnRisk >= 140 || (impactBand === 'high' && isConstraintHigh)) {
    urgencyLevel = 'high';
    urgencyReason = 'Material near-term exposure under current market context indicates a priority decision need.';
  } else if (
    maxRev >= 6000 ||
    rnRisk >= 50 ||
    impactBand === 'medium' ||
    family === 'discount_inefficiency' ||
    family === 'pricing_resistance'
  ) {
    urgencyLevel = 'medium';
    urgencyReason = 'Commercial impact is meaningful and should be addressed before inefficiency compounds.';
  }

  return {
    urgency_level: urgencyLevel,
    urgency_reason: urgencyReason
  };
}

function buildDecisionFramingSummary(issues, quantSummary, contextSummary) {
  const retailIssues = (Array.isArray(issues) ? issues : []).filter(
    (issue) => (issue?.segment || 'retail') === 'retail'
  );
  const quantRows = Array.isArray(quantSummary?.issue_level_quantification)
    ? quantSummary.issue_level_quantification
    : [];
  const contextRows = Array.isArray(contextSummary?.issue_level_context)
    ? contextSummary.issue_level_context
    : [];
  const quantByKey = new Map(quantRows.map((q) => [q.finding_key, q]));
  const contextByKey = new Map(contextRows.map((r) => [r.finding_key, r]));
  const notes = [
    ...(Array.isArray(quantSummary?.data_quality_notes) ? quantSummary.data_quality_notes : []),
    ...(Array.isArray(contextSummary?.data_quality_notes) ? contextSummary.data_quality_notes : [])
  ];

  const issueLevelDecisions = retailIssues.map((issue) => {
    const q = quantByKey.get(issue.finding_key) || {
      impact_band: 'low',
      quantified_signals: {
        room_nights_at_risk: null,
        adr_gap: null,
        revenue_range: null,
        index_gap: null,
        occupancy_gap: null
      },
      confidence: 'low'
    };
    const c = contextByKey.get(issue.finding_key) || {
      commercial_situation: 'mixed_or_unclear_context',
      context_flags: [],
      dominant_signal: null,
      constraint_status: 'unclear',
      confidence: 'low'
    };

    const cls = classifyRetailDecisionType(issue, q, c);
    const urg = assessDecisionUrgency(issue, q, c);
    const confidence =
      cls.confidence === 'high' && q.confidence === 'high' && c.confidence === 'high'
        ? 'high'
        : cls.confidence === 'low' || q.confidence === 'low' || c.confidence === 'low'
          ? 'low'
          : 'medium';

    return {
      finding_key: issue.finding_key,
      issue_family: issue.issue_family,
      decision_type: cls.decision_type,
      primary_lever: cls.primary_lever,
      decision_intent: cls.decision_intent,
      urgency_level: urg.urgency_level,
      urgency_reason: urg.urgency_reason,
      confidence
    };
  });

  const portfolioPriorities = [];
  if (issueLevelDecisions.some((d) => d.urgency_level === 'high')) {
    portfolioPriorities.push('Prioritize high-urgency retail decisions where near-term exposure is material.');
  }
  const decisionCounts = {};
  for (const d of issueLevelDecisions) decisionCounts[d.decision_type] = (decisionCounts[d.decision_type] || 0) + 1;
  const topDecisionType = Object.entries(decisionCounts).sort((a, b) => b[1] - a[1])[0]?.[0];
  if (topDecisionType) {
    portfolioPriorities.push(`Primary decision pattern this run: ${topDecisionType}.`);
  }
  if (contextSummary?.overall_constraint_context === 'high') {
    portfolioPriorities.push('Balance value capture versus volume under high inventory pressure.');
  }

  const portfolioRisks = [];
  if (issueLevelDecisions.some((d) => d.decision_type === 'discount_strategy_review')) {
    portfolioRisks.push('Discount leakage risk: margin loss may continue without proportional volume gain.');
  }
  if (issueLevelDecisions.some((d) => d.decision_type === 'pricing_adjustment')) {
    portfolioRisks.push('Pricing execution risk: delayed adjustment can widen share and revenue gaps.');
  }
  if (issueLevelDecisions.some((d) => d.decision_type === 'demand_generation')) {
    portfolioRisks.push('Demand softness risk: unresolved capture weakness can compound into forward periods.');
  }

  let overallDecisionEnvironment = 'mixed_decision_environment';
  if (issueLevelDecisions.every((d) => d.decision_type === 'monitoring_only') && issueLevelDecisions.length > 0) {
    overallDecisionEnvironment = 'monitoring_environment';
  } else if (issueLevelDecisions.some((d) => d.urgency_level === 'high')) {
    overallDecisionEnvironment = 'active_intervention_environment';
  } else if (issueLevelDecisions.length > 0) {
    overallDecisionEnvironment = 'managed_adjustment_environment';
  }

  if (!issueLevelDecisions.length) {
    notes.push('No visible retail issues available for decision framing.');
  }

  return {
    schema_version: '1.0',
    overall_decision_environment: overallDecisionEnvironment,
    portfolio_priorities: portfolioPriorities,
    issue_level_decisions: issueLevelDecisions,
    portfolio_risks: portfolioRisks,
    data_quality_notes: Array.from(new Set(notes))
  };
}

function suggestRetailActionFocus(issue, decision, context, quantification) {
  const family = (issue?.issue_family || '').toString();
  const dType = decision?.decision_type || 'monitoring_only';
  const c = context || {};
  const q = quantification || {};
  const flags = Array.isArray(c.context_flags) ? c.context_flags : [];
  const impactBand = q?.impact_band || 'low';

  let actionFocus = 'monitoring';
  let supportingLevers = ['signal_tracking'];
  let actionIntent = 'Track signal consistency before changing commercial execution.';

  if (dType === 'pricing_adjustment') {
    actionFocus = 'rate_positioning';
    supportingLevers = ['price_ladder', 'fence_integrity', 'value_communication'];
    actionIntent = 'Refine relative rate posture to improve conversion without eroding value quality.';
  } else if (dType === 'discount_strategy_review') {
    actionFocus = 'discount_structure';
    supportingLevers = ['offer_design', 'discount_fences', 'promo_eligibility'];
    actionIntent = 'Reassess discount architecture where incentive depth is not yielding efficient demand capture.';
  } else if (dType === 'conversion_optimization') {
    actionFocus = 'conversion_path';
    supportingLevers = ['booking_friction', 'content_strength', 'channel_experience'];
    actionIntent = 'Improve conversion path quality where visibility is not translating to bookings.';
  } else if (dType === 'demand_generation') {
    actionFocus = 'demand_stimulation';
    supportingLevers = ['demand_activation', 'campaign_timing', 'market_visibility'];
    actionIntent = 'Strengthen demand capture posture in softer periods while preserving pricing discipline.';
  } else if (dType === 'mix_rebalancing') {
    if (flags.includes('possible_compression') || c?.constraint_status === 'high') {
      actionFocus = 'inventory_control';
      supportingLevers = ['inventory_protection', 'segment_priority', 'stay_pattern_control'];
      actionIntent = 'Protect value under constrained conditions and avoid low-quality demand displacement.';
    } else {
      actionFocus = 'channel_mix';
      supportingLevers = ['segment_mix', 'channel_weighting', 'value_quality'];
      actionIntent = 'Rebalance demand sources toward healthier contribution mix.';
    }
  }

  let confidence = 'low';
  if ((decision?.confidence === 'high' || context?.confidence === 'high') && impactBand !== 'low') confidence = 'high';
  else if (decision?.confidence !== 'low' || context?.confidence !== 'low') confidence = 'medium';

  // Family-level safety override for weak signals.
  if (family === 'visibility_gap' && impactBand === 'low' && context?.confidence === 'low') {
    actionFocus = 'monitoring';
    supportingLevers = ['signal_tracking', 'diagnostic_validation'];
    actionIntent = 'Validate whether demand softness signal persists before activating broader interventions.';
    confidence = 'low';
  }

  return {
    action_focus: actionFocus,
    supporting_levers: supportingLevers,
    action_intent: actionIntent,
    confidence
  };
}

function identifyActionConstraints(issue, context, quantification) {
  const family = (issue?.issue_family || '').toString();
  const c = context || {};
  const q = quantification || {};
  const qs = q?.quantified_signals || {};
  const flags = Array.isArray(c.context_flags) ? c.context_flags : [];
  const constraints = [];

  const occGap = toFiniteNumberOrNull(qs.occupancy_gap);
  const adrGap = toFiniteNumberOrNull(qs.adr_gap);
  const idxGap = toFiniteNumberOrNull(qs.index_gap);

  if (c?.constraint_status === 'high' || flags.includes('possible_compression')) constraints.push('limited_inventory');
  if (flags.includes('occupancy_softness') || (occGap !== null && occGap >= 6)) constraints.push('low_demand_environment');
  if (family === 'pricing_resistance' || (adrGap !== null && adrGap > 0)) constraints.push('price_sensitivity');
  if (family === 'discount_inefficiency') constraints.push('rate_integrity_risk');
  if (family === 'mix_constraint' || flags.includes('mix_inefficiency')) constraints.push('channel_dependency');
  if (flags.includes('demand_uncertain') || c?.confidence === 'low') constraints.push('uncertain_signal');
  if (q?.confidence === 'low' || idxGap === null) constraints.push('data_limitations');

  let constraintSeverity = 'low';
  if (constraints.includes('limited_inventory') || constraints.includes('rate_integrity_risk')) {
    constraintSeverity = 'high';
  } else if (constraints.length >= 2) {
    constraintSeverity = 'medium';
  }

  return {
    constraints: Array.from(new Set(constraints)),
    constraint_severity: constraintSeverity
  };
}

function buildActionGuidance(issue, actionFocus, constraints, decision, context) {
  const family = (issue?.issue_family || '').toString();
  const focus = actionFocus?.action_focus || 'monitoring';
  const leverText = (actionFocus?.supporting_levers || []).join(', ') || 'signal tracking';
  const cons = Array.isArray(constraints?.constraints) ? constraints.constraints : [];

  let recommendedDirection = `Use a ${focus.replace(/_/g, ' ')} posture while preserving commercial discipline.`;
  if (focus === 'rate_positioning') {
    recommendedDirection = 'Reassess price positioning framework against demand response and value perception.';
  } else if (focus === 'discount_structure') {
    recommendedDirection = 'Tighten discount governance and evaluate whether current incentive architecture is efficient.';
  } else if (focus === 'conversion_path') {
    recommendedDirection = 'Prioritize conversion-path quality improvements before broad pricing changes.';
  } else if (focus === 'demand_stimulation') {
    recommendedDirection = 'Strengthen demand stimulation posture while protecting baseline rate integrity.';
  } else if (focus === 'inventory_control') {
    recommendedDirection = 'Protect inventory quality and avoid low-value demand under constrained conditions.';
  } else if (focus === 'channel_mix') {
    recommendedDirection = 'Rebalance channel/segment mix toward demand sources with stronger value contribution.';
  }

  const keyConsiderations = [
    `Primary levers to evaluate: ${leverText}.`,
    `Decision framing alignment: ${(decision?.decision_type || 'monitoring_only').replace(/_/g, ' ')}.`
  ];
  if (context?.commercial_situation) {
    keyConsiderations.push(`Current commercial situation: ${context.commercial_situation.replace(/_/g, ' ')}.`);
  }
  if (cons.includes('limited_inventory')) keyConsiderations.push('Guard inventory quality before adding demand pressure.');
  if (cons.includes('low_demand_environment')) keyConsiderations.push('In low-demand context, prioritize conversion efficiency over pure rate movement.');
  if (cons.includes('price_sensitivity')) keyConsiderations.push('Assess sensitivity risk before tightening price posture.');

  const riskWatchouts = [];
  if (cons.includes('rate_integrity_risk')) riskWatchouts.push('Further discounting may dilute rate integrity without proportional volume lift.');
  if (cons.includes('channel_dependency')) riskWatchouts.push('Over-reliance on narrow channels can weaken mix resilience.');
  if (cons.includes('limited_inventory')) riskWatchouts.push('Demand-push tactics under tight inventory can displace higher-value demand.');
  if (cons.includes('uncertain_signal') || cons.includes('data_limitations')) {
    riskWatchouts.push('Signal confidence is limited; avoid over-rotating strategy on incomplete evidence.');
  }
  if (!riskWatchouts.length) {
    riskWatchouts.push('Execution drift can reduce impact even when decision direction is sound.');
  }
  if (family === 'visibility_gap' && focus === 'demand_stimulation') {
    riskWatchouts.push('Demand activation without conversion readiness can increase cost without proportional booking gain.');
  }

  return {
    recommended_direction: recommendedDirection,
    key_considerations: keyConsiderations.slice(0, 5),
    risk_watchouts: riskWatchouts.slice(0, 5)
  };
}

function buildActionIntelligenceSummary(issues, decisionSummary, contextSummary, quantSummary) {
  const retailIssues = (Array.isArray(issues) ? issues : []).filter(
    (issue) => (issue?.segment || 'retail') === 'retail'
  );
  const decisionRows = Array.isArray(decisionSummary?.issue_level_decisions)
    ? decisionSummary.issue_level_decisions
    : [];
  const contextRows = Array.isArray(contextSummary?.issue_level_context)
    ? contextSummary.issue_level_context
    : [];
  const quantRows = Array.isArray(quantSummary?.issue_level_quantification)
    ? quantSummary.issue_level_quantification
    : [];

  const decisionByKey = new Map(decisionRows.map((d) => [d.finding_key, d]));
  const contextByKey = new Map(contextRows.map((c) => [c.finding_key, c]));
  const quantByKey = new Map(quantRows.map((q) => [q.finding_key, q]));

  const notes = [
    ...(Array.isArray(decisionSummary?.data_quality_notes) ? decisionSummary.data_quality_notes : []),
    ...(Array.isArray(contextSummary?.data_quality_notes) ? contextSummary.data_quality_notes : []),
    ...(Array.isArray(quantSummary?.data_quality_notes) ? quantSummary.data_quality_notes : [])
  ];

  const issueLevelActions = retailIssues.map((issue) => {
    const d = decisionByKey.get(issue.finding_key) || {
      decision_type: 'monitoring_only',
      confidence: 'low',
      urgency_level: 'low'
    };
    const c = contextByKey.get(issue.finding_key) || {
      commercial_situation: 'mixed_or_unclear_context',
      context_flags: [],
      constraint_status: 'unclear',
      confidence: 'low'
    };
    const q = quantByKey.get(issue.finding_key) || {
      impact_band: 'low',
      quantified_signals: {
        room_nights_at_risk: null,
        adr_gap: null,
        revenue_range: null,
        index_gap: null,
        occupancy_gap: null
      },
      confidence: 'low'
    };

    const focus = suggestRetailActionFocus(issue, d, c, q);
    const cons = identifyActionConstraints(issue, c, q);
    const guidance = buildActionGuidance(issue, focus, cons, d, c);
    const confidence =
      focus.confidence === 'high' && d.confidence === 'high' ? 'high' : focus.confidence === 'low' ? 'low' : 'medium';

    return {
      finding_key: issue.finding_key,
      issue_family: issue.issue_family,
      action_focus: focus.action_focus,
      supporting_levers: focus.supporting_levers,
      action_intent: focus.action_intent,
      constraints: cons.constraints,
      constraint_severity: cons.constraint_severity,
      recommended_direction: guidance.recommended_direction,
      key_considerations: guidance.key_considerations,
      risk_watchouts: guidance.risk_watchouts,
      confidence
    };
  });

  let overallActionPosture = 'balanced_guided_posture';
  if (issueLevelActions.every((a) => a.action_focus === 'monitoring') && issueLevelActions.length > 0) {
    overallActionPosture = 'monitoring_posture';
  } else if (issueLevelActions.some((a) => a.constraint_severity === 'high')) {
    overallActionPosture = 'constraint_aware_posture';
  } else if (issueLevelActions.length > 0) {
    overallActionPosture = 'active_guided_posture';
  }

  const portfolioActionPriorities = [];
  const highConstraintCount = issueLevelActions.filter((a) => a.constraint_severity === 'high').length;
  if (highConstraintCount > 0) {
    portfolioActionPriorities.push('Prioritize constraint-aware execution where inventory/rate integrity risk is elevated.');
  }
  const focusCounts = {};
  for (const row of issueLevelActions) focusCounts[row.action_focus] = (focusCounts[row.action_focus] || 0) + 1;
  const topFocus = Object.entries(focusCounts).sort((a, b) => b[1] - a[1])[0]?.[0];
  if (topFocus) {
    portfolioActionPriorities.push(`Primary action focus this run: ${topFocus.replace(/_/g, ' ')}.`);
  }

  const globalRiskFlags = [];
  if (issueLevelActions.some((a) => a.constraints.includes('limited_inventory'))) globalRiskFlags.push('limited_inventory');
  if (issueLevelActions.some((a) => a.constraints.includes('rate_integrity_risk'))) globalRiskFlags.push('rate_integrity_risk');
  if (issueLevelActions.some((a) => a.constraints.includes('uncertain_signal'))) globalRiskFlags.push('uncertain_signal');
  if (issueLevelActions.some((a) => a.constraints.includes('data_limitations'))) globalRiskFlags.push('data_limitations');

  if (!issueLevelActions.length) {
    notes.push('No visible retail issues available for action intelligence framing.');
  }

  return {
    schema_version: '1.0',
    overall_action_posture: overallActionPosture,
    portfolio_action_priorities: portfolioActionPriorities,
    issue_level_actions: issueLevelActions,
    global_risk_flags: Array.from(new Set(globalRiskFlags)),
    data_quality_notes: Array.from(new Set(notes))
  };
}

function buildControlledAction(issue, actionIntel, decision, context, quant) {
  const family = (issue?.issue_family || '').toString();
  const ai = actionIntel || {};
  const d = decision || {};
  const c = context || {};
  const q = quant || {};
  const qs = q.quantified_signals || {};

  const titleByFocus = {
    rate_positioning: 'Recalibrate transient rate positioning',
    discount_structure: 'Tighten discount structure',
    conversion_path: 'Strengthen conversion efficiency',
    demand_stimulation: 'Strengthen demand capture posture',
    channel_mix: 'Rebalance channel mix quality',
    inventory_control: 'Protect inventory value quality',
    monitoring: 'Monitor retail signal progression'
  };
  const actionTitle = titleByFocus[ai.action_focus] || 'Refine commercial execution focus';

  const summaryByFocus = {
    rate_positioning:
      'Refine pricing posture to better align value perception with observed demand response.',
    discount_structure:
      'Reassess discount architecture to improve volume efficiency without unnecessary rate dilution.',
    conversion_path:
      'Reduce conversion friction across core retail journeys to improve demand-to-booking capture.',
    demand_stimulation:
      'Prioritize demand stimulation with disciplined execution in softer retail windows.',
    channel_mix:
      'Rebalance channel and segment mix toward stronger contribution quality.',
    inventory_control:
      'Protect inventory value under constrained conditions while preserving commercial flexibility.',
    monitoring:
      'Maintain close monitoring while validating whether current signals persist.'
  };
  const actionSummary = summaryByFocus[ai.action_focus] || 'Use a controlled commercial adjustment posture.';

  const rev = qs.revenue_range;
  const rn = qs.room_nights_at_risk;
  const quantText = rev
    ? `Estimated revenue-at-risk range is ${rev.min}-${rev.max}.`
    : rn != null
      ? `Estimated room nights at risk are ~${rn}.`
      : 'Quantified scale is limited; directional signals should still be monitored.';
  const contextText = c?.commercial_situation
    ? `Context indicates ${String(c.commercial_situation).replace(/_/g, ' ')}.`
    : 'Context remains mixed.';
  const decisionText = d?.decision_type
    ? `Decision framing points to ${String(d.decision_type).replace(/_/g, ' ')}.`
    : 'Decision framing remains monitoring-oriented.';
  const actionRationale = '';

  const maxRev = Number(rev?.max || 0);
  const rnRisk = Number(rn || 0);
  const impactBand = q?.impact_band || 'low';
  const urgency = d?.urgency_level || 'low';
  let priority = 'low';
  if ((impactBand === 'high' && urgency !== 'low') || maxRev >= 15000 || rnRisk >= 120) priority = 'high';
  else if (impactBand === 'medium' || urgency === 'medium' || maxRev >= 5000 || rnRisk >= 40) priority = 'medium';

  const confParts = [q?.confidence || 'low', c?.confidence || 'low', d?.confidence || 'low', ai?.confidence || 'low'];
  const highCount = confParts.filter((x) => x === 'high').length;
  const lowCount = confParts.filter((x) => x === 'low').length;
  let confidence = 'medium';
  if (highCount >= 3) confidence = 'high';
  else if (lowCount >= 2) confidence = 'low';

  // Conservative downgrade in weak-signal situations.
  if (
    ai?.action_focus === 'monitoring' ||
    (family === 'visibility_gap' && confidence === 'low' && impactBand === 'low')
  ) {
    priority = 'low';
  }

  return {
    action_title: actionTitle,
    action_summary: actionSummary,
    action_rationale: actionRationale,
    internal_context_summary: `${quantText} ${contextText} ${decisionText}`,
    priority,
    confidence
  };
}

function applyControlledActionsToRetailIssues(
  issues,
  actionIntelligenceSummary,
  decisionFramingSummary,
  commercialContextSummary,
  financialQuantificationSummary
) {
  const list = Array.isArray(issues) ? issues : [];
  const actionRows = Array.isArray(actionIntelligenceSummary?.issue_level_actions)
    ? actionIntelligenceSummary.issue_level_actions
    : [];
  const decisionRows = Array.isArray(decisionFramingSummary?.issue_level_decisions)
    ? decisionFramingSummary.issue_level_decisions
    : [];
  const contextRows = Array.isArray(commercialContextSummary?.issue_level_context)
    ? commercialContextSummary.issue_level_context
    : [];
  const quantRows = Array.isArray(financialQuantificationSummary?.issue_level_quantification)
    ? financialQuantificationSummary.issue_level_quantification
    : [];

  const actionByKey = new Map(actionRows.map((x) => [x.finding_key, x]));
  const decisionByKey = new Map(decisionRows.map((x) => [x.finding_key, x]));
  const contextByKey = new Map(contextRows.map((x) => [x.finding_key, x]));
  const quantByKey = new Map(quantRows.map((x) => [x.finding_key, x]));

  return list.map((issue) => {
    if ((issue?.segment || 'retail') !== 'retail') return issue;
    const actionIntel = actionByKey.get(issue.finding_key);
    const decision = decisionByKey.get(issue.finding_key);
    const context = contextByKey.get(issue.finding_key);
    const quant = quantByKey.get(issue.finding_key);
    if (!actionIntel && !decision && !context && !quant) return issue;

    const controlled = buildControlledAction(issue, actionIntel, decision, context, quant);
    const rawActions = (issue.actions || []);
    const seen = new Set();
    const updatedActions = rawActions.map((act) => {
      const desc = `${controlled.action_summary} ${controlled.action_rationale}`;
      return {
        ...act,
        title: controlled.action_title,
        description: desc,
        priority: controlled.priority,
        confidence: controlled.confidence
      };
    }).filter((act) => {
      const key = act.description?.trim().toLowerCase().slice(0, 80);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });

    return {
      ...issue,
      priority: controlled.priority,
      expected_outcome: controlled.action_summary,
      root_cause: controlled.action_rationale,
      actions: updatedActions
    };
  });
}

function mapRetailIssueFamilyToDriver(issueFamily, decisionType, contextFlags = []) {
  const fam = (issueFamily || '').toString();
  const dec = (decisionType || '').toString();
  const flags = Array.isArray(contextFlags) ? contextFlags : [];

  if (fam === 'pricing_resistance' || fam === 'missed_pricing_opportunity') return 'pricing';
  if (fam === 'discount_inefficiency') return 'conversion';
  if (fam === 'mix_constraint') return flags.includes('channel_dependency') ? 'distribution' : 'pricing';
  if (fam === 'visibility_gap') {
    if (dec === 'conversion_optimization') return 'conversion';
    if (dec === 'demand_generation') return 'distribution';
    return 'conversion';
  }

  if (dec === 'pricing_adjustment') return 'pricing';
  if (dec === 'discount_strategy_review') return 'discounting';
  if (dec === 'conversion_optimization') return 'conversion';
  if (dec === 'demand_generation' || dec === 'mix_rebalancing') return 'distribution';
  return 'monitoring';
}

function buildRetailDriverScorecard(
  issues,
  quantSummary,
  contextSummary,
  decisionSummary,
  actionSummary
) {
  const retailIssues = (Array.isArray(issues) ? issues : []).filter(
    (issue) => (issue?.segment || 'retail') === 'retail'
  );
  const quantByKey = new Map(
    (Array.isArray(quantSummary?.issue_level_quantification) ? quantSummary.issue_level_quantification : [])
      .map((q) => [q.finding_key, q])
  );
  const contextByKey = new Map(
    (Array.isArray(contextSummary?.issue_level_context) ? contextSummary.issue_level_context : [])
      .map((c) => [c.finding_key, c])
  );
  const decisionByKey = new Map(
    (Array.isArray(decisionSummary?.issue_level_decisions) ? decisionSummary.issue_level_decisions : [])
      .map((d) => [d.finding_key, d])
  );
  const actionByKey = new Map(
    (Array.isArray(actionSummary?.issue_level_actions) ? actionSummary.issue_level_actions : [])
      .map((a) => [a.finding_key, a])
  );

  const driverStats = new Map();
  const confScore = { low: 1, medium: 2, high: 3 };
  const scoreImpact = { low: 1.0, medium: 2.0, high: 3.0 };
  const scoreUrgency = { low: 1.0, medium: 1.8, high: 2.6 };

  for (const issue of retailIssues) {
    const q = quantByKey.get(issue.finding_key) || {};
    const c = contextByKey.get(issue.finding_key) || {};
    const d = decisionByKey.get(issue.finding_key) || {};
    const a = actionByKey.get(issue.finding_key) || {};

    const driver = mapRetailIssueFamilyToDriver(issue.issue_family, d.decision_type, c.context_flags);
    if (!driverStats.has(driver)) {
      driverStats.set(driver, {
        driver,
        weighted_score: 0,
        issue_count: 0,
        total_estimated_impact: 0,
        conf_acc: 0
      });
    }
    const bucket = driverStats.get(driver);
    const maxRev = Number(q?.quantified_signals?.revenue_range?.max || 0);
    const rnRisk = Number(q?.quantified_signals?.room_nights_at_risk || 0);
    const impactProxy = maxRev > 0 ? maxRev : rnRisk * 120;
    const impactBand = q?.impact_band || 'low';
    const urgency = d?.urgency_level || 'low';

    const blendedConfidence = Math.max(
      confScore[q?.confidence || 'low'],
      confScore[c?.confidence || 'low'],
      confScore[d?.confidence || 'low'],
      confScore[a?.confidence || 'low']
    );

    const weighted =
      scoreImpact[impactBand] * scoreUrgency[urgency] * (0.9 + blendedConfidence * 0.2) +
      Math.min(4, impactProxy / 10000);

    bucket.weighted_score += weighted;
    bucket.issue_count += 1;
    bucket.total_estimated_impact += impactProxy > 0 ? impactProxy : 0;
    bucket.conf_acc += blendedConfidence;
  }

  return Array.from(driverStats.values())
    .map((row) => ({
      driver: row.driver,
      weighted_score: Number(row.weighted_score.toFixed(2)),
      issue_count: row.issue_count,
      total_estimated_impact: row.total_estimated_impact > 0 ? Math.round(row.total_estimated_impact) : null,
      average_confidence:
        row.issue_count > 0
          ? row.conf_acc / row.issue_count >= 2.5
            ? 'high'
            : row.conf_acc / row.issue_count >= 1.7
              ? 'medium'
              : 'low'
          : 'low'
    }))
    .sort((a, b) => {
      if (b.weighted_score !== a.weighted_score) return b.weighted_score - a.weighted_score;
      const bi = Number(b.total_estimated_impact || 0);
      const ai = Number(a.total_estimated_impact || 0);
      if (bi !== ai) return bi - ai;
      const rank = { high: 3, medium: 2, low: 1 };
      const bc = rank[b.average_confidence] || 1;
      const ac = rank[a.average_confidence] || 1;
      if (bc !== ac) return bc - ac;
      if (b.issue_count !== a.issue_count) return b.issue_count - a.issue_count;
      return String(a.driver).localeCompare(String(b.driver));
    });
}

function selectPrimaryRetailDriver(driverScorecard) {
  const list = Array.isArray(driverScorecard) ? driverScorecard : [];
  if (!list.length) return null;
  return list[0].driver || null;
}

function deriveAdaptedActionPosture(issue, primaryDriver) {
  const fam = (issue?.issue_family || '').toString();
  if (!primaryDriver) return 'monitor this issue while primary strategy is clarified';
  if (primaryDriver === 'pricing') {
    if (fam === 'discount_inefficiency') return 'remove public discount leakage after rate correction';
    if (fam === 'visibility_gap') return 'support pricing correction with conversion readiness, avoid broad discount expansion';
    return 'align execution to pricing-first strategy';
  }
  if (primaryDriver === 'discounting') {
    if (fam === 'pricing_resistance') return 'hold broad rate moves until discount architecture is stabilized';
    return 'prioritize discount discipline and keep other levers secondary';
  }
  if (primaryDriver === 'conversion') {
    if (fam === 'pricing_resistance') return 'hold broad pricing moves until conversion friction is addressed';
    return 'prioritize conversion repair before secondary rate/discount shifts';
  }
  if (primaryDriver === 'distribution') {
    return 'prioritize mix/distribution alignment before broad pricing/discount changes';
  }
  return 'monitor this issue while primary strategy is executed';
}

function resolveRetailDriverConflict(issue, primaryDriver, contextData) {
  const issueDriver = contextData?.issueDriver || 'monitoring';
  const decisionType = contextData?.decisionType || 'monitoring_only';
  if (!primaryDriver) {
    return {
      arbitration_role: 'monitor',
      suppression_reason: 'no_primary_driver_selected',
      adapted_action_posture: 'monitor this issue while strategy hierarchy is established'
    };
  }

  if (issueDriver === primaryDriver) {
    return {
      arbitration_role: 'primary',
      suppression_reason: null,
      adapted_action_posture: deriveAdaptedActionPosture(issue, primaryDriver)
    };
  }

  // Controlled conflict rules
  if (primaryDriver === 'pricing' && issueDriver === 'discounting') {
    return {
      arbitration_role: 'supporting',
      suppression_reason: 'avoid independent discount expansion while pricing correction is primary',
      adapted_action_posture: 'remove public discount leakage after rate correction'
    };
  }
  if (primaryDriver === 'discounting' && issueDriver === 'pricing') {
    return {
      arbitration_role: 'supporting',
      suppression_reason: 'avoid simultaneous broad rate and discount shifts in same recovery window',
      adapted_action_posture: 'support discount discipline first; keep pricing tactical'
    };
  }
  if (primaryDriver === 'conversion' && (issueDriver === 'pricing' || issueDriver === 'discounting')) {
    return {
      arbitration_role: 'suppressed',
      suppression_reason: 'conversion friction is dominant; defer broad pricing/discount shifts',
      adapted_action_posture: deriveAdaptedActionPosture(issue, primaryDriver)
    };
  }
  if (primaryDriver === 'distribution' && (issueDriver === 'pricing' || issueDriver === 'discounting')) {
    return {
      arbitration_role: 'supporting',
      suppression_reason: 'distribution/mix posture leads this cycle; pricing/discounting are secondary',
      adapted_action_posture: deriveAdaptedActionPosture(issue, primaryDriver)
    };
  }

  if (decisionType === 'monitoring_only') {
    return {
      arbitration_role: 'monitor',
      suppression_reason: 'signal_strength_insufficient_for_primary_execution_role',
      adapted_action_posture: 'monitor this issue while primary strategy is executed'
    };
  }

  return {
    arbitration_role: 'supporting',
    suppression_reason: null,
    adapted_action_posture: deriveAdaptedActionPosture(issue, primaryDriver)
  };
}

function arbitrateRetailIssuesAgainstPrimaryDriver(issues, primaryDriver, contextData) {
  const retailIssues = (Array.isArray(issues) ? issues : []).filter(
    (issue) => (issue?.segment || 'retail') === 'retail'
  );
  const decisionByKey = contextData?.decisionByKey || new Map();
  const contextByKey = contextData?.contextByKey || new Map();
  const quantByKey = contextData?.quantByKey || new Map();

  return retailIssues.map((issue) => {
    const d = decisionByKey.get(issue.finding_key) || {};
    const c = contextByKey.get(issue.finding_key) || {};
    const q = quantByKey.get(issue.finding_key) || {};
    const issueDriver = mapRetailIssueFamilyToDriver(issue.issue_family, d.decision_type, c.context_flags);
    const resolved = resolveRetailDriverConflict(issue, primaryDriver, {
      issueDriver,
      decisionType: d.decision_type,
      context: c,
      quantification: q
    });
    const confRank = { low: 1, medium: 2, high: 3 };
    const maxRank = Math.max(confRank[d.confidence || 'low'], confRank[c.confidence || 'low'], confRank[q.confidence || 'low']);
    const confidence = maxRank >= 3 ? 'high' : maxRank >= 2 ? 'medium' : 'low';
    return {
      finding_key: issue.finding_key,
      issue_family: issue.issue_family,
      original_driver: issue.driver || 'unknown',
      primary_driver: issueDriver,
      arbitration_role: resolved.arbitration_role,
      suppression_reason: resolved.suppression_reason,
      adapted_action_posture: resolved.adapted_action_posture,
      confidence
    };
  });
}

function buildRetailGlobalStrategyStatement(primaryDriver, driverScorecard, issues) {
  const issueCount = Array.isArray(issues) ? issues.length : 0;
  if (!primaryDriver) {
    return {
      global_strategy_statement:
        'Current cycle lacks a clear dominant retail driver; maintain monitoring posture until stronger signal hierarchy emerges.',
      global_execution_principles: [
        'One primary commercial lever should lead each cycle when evidence is sufficient.',
        'Avoid conflicting tactical moves while signal confidence is low.',
        'Escalate from monitoring only when impact and confidence strengthen.'
      ]
    };
  }
  const top = (Array.isArray(driverScorecard) ? driverScorecard : []).find((d) => d.driver === primaryDriver);
  const impactText = top?.total_estimated_impact ? `estimated impact ~${top.total_estimated_impact}` : 'impact evidence available';

  const strategyByDriver = {
    pricing: 'Current cycle should prioritize pricing correction before secondary discount or conversion adjustments.',
    discounting: 'Current cycle should prioritize discount discipline before broader pricing or distribution shifts.',
    conversion: 'Current cycle should prioritize conversion repair before broader rate or discount intervention.',
    distribution: 'Current cycle should prioritize channel/mix alignment before broad pricing and discount moves.',
    monitoring: 'Current cycle should prioritize monitoring while preserving strategic flexibility.'
  };

  return {
    global_strategy_statement:
      `${strategyByDriver[primaryDriver] || strategyByDriver.monitoring} ` +
      `Driver strength is led by ${primaryDriver} across ${issueCount} visible issues (${impactText}).`,
    global_execution_principles: [
      'One primary commercial lever should lead this cycle.',
      'Secondary tactics must align to the primary strategy.',
      'Conflicting rate and discount moves should be avoided in the same recovery window.'
    ]
  };
}

function buildDecisionArbitrationSummary(
  issues,
  financialQuantificationSummary,
  commercialContextSummary,
  decisionFramingSummary,
  actionIntelligenceSummary
) {
  const retailIssues = (Array.isArray(issues) ? issues : []).filter(
    (issue) => (issue?.segment || 'retail') === 'retail'
  );
  const quantRows = Array.isArray(financialQuantificationSummary?.issue_level_quantification)
    ? financialQuantificationSummary.issue_level_quantification
    : [];
  const contextRows = Array.isArray(commercialContextSummary?.issue_level_context)
    ? commercialContextSummary.issue_level_context
    : [];
  const decisionRows = Array.isArray(decisionFramingSummary?.issue_level_decisions)
    ? decisionFramingSummary.issue_level_decisions
    : [];
  const actionRows = Array.isArray(actionIntelligenceSummary?.issue_level_actions)
    ? actionIntelligenceSummary.issue_level_actions
    : [];

  const quantByKey = new Map(quantRows.map((x) => [x.finding_key, x]));
  const contextByKey = new Map(contextRows.map((x) => [x.finding_key, x]));
  const decisionByKey = new Map(decisionRows.map((x) => [x.finding_key, x]));
  const actionByKey = new Map(actionRows.map((x) => [x.finding_key, x]));

  const driverScorecard = buildRetailDriverScorecard(
    retailIssues,
    financialQuantificationSummary,
    commercialContextSummary,
    decisionFramingSummary,
    actionIntelligenceSummary
  );
  let portfolioPrimaryDriver = selectPrimaryRetailDriver(driverScorecard);
  const pricingTruthExists = retailIssues.some(issue => {
    const m = issue?.card_metrics || {};
    const ari = Number(m.avgARI);
    const mpi = Number(m.avgMPI);
    const rgi = Number(m.avgRGI);
    return Number.isFinite(ari) && Number.isFinite(mpi) && Number.isFinite(rgi)
      && ari > 100 && mpi < 100 && rgi < 100;
  });

  if (pricingTruthExists && portfolioPrimaryDriver !== 'pricing') {
    portfolioPrimaryDriver = 'pricing';
  }
  const issueLevelArbitration = arbitrateRetailIssuesAgainstPrimaryDriver(
    retailIssues,
    portfolioPrimaryDriver,
    { decisionByKey, contextByKey, quantByKey, actionByKey }
  );

  const suppressedConflicts = issueLevelArbitration
    .filter((row) => row.arbitration_role === 'suppressed')
    .map((row) => ({
      finding_key: row.finding_key,
      original_driver: row.original_driver,
      suppression_reason: row.suppression_reason || 'conflict_with_primary_driver'
    }));

  const { global_strategy_statement, global_execution_principles } =
    buildRetailGlobalStrategyStatement(portfolioPrimaryDriver, driverScorecard, retailIssues);

  const notes = [];
  if (!driverScorecard.length) notes.push('No retail driver scorecard rows were produced for arbitration.');
  if (!portfolioPrimaryDriver) notes.push('Primary driver could not be selected deterministically.');
  if (!issueLevelArbitration.length) notes.push('No retail issues available for arbitration layer.');

  return {
    schema_version: '1.0',
    portfolio_primary_driver: portfolioPrimaryDriver,
    portfolio_driver_scores: driverScorecard,
    issue_level_arbitration: issueLevelArbitration,
    global_strategy_statement,
    global_execution_principles,
    suppressed_conflicts: suppressedConflicts,
    data_quality_notes: notes
  };
}

function toSentence(text, fallback) {
  const v = (text || fallback || '').toString().trim();
  if (!v) return fallback || '';
  return /[.!?]$/.test(v) ? v : `${v}.`;
}

function buildEnforcedDecisionLine(issue, arbitration, portfolioPrimaryDriver) {
  const role = arbitration?.arbitration_role || 'monitor';
  const adapted = toSentence(arbitration?.adapted_action_posture, null);
  const primary = (portfolioPrimaryDriver || arbitration?.primary_driver || 'primary strategy').toString();

  if (role === 'primary') {
    const strong =
      toSentence(issue?.enforced_decision_line, null) ||
      toSentence(issue?.expected_outcome, null) ||
      toSentence(issue?.root_cause, null) ||
      adapted;
    return strong || 'Execute the primary strategy for this cycle.';
  }
  if (role === 'supporting') {
    if (primary === 'pricing') {
      return 'Align this lever with pricing correction and avoid independent discount expansion.';
    }
    if (primary === 'discounting') {
      return 'Support discount discipline without introducing conflicting pricing changes.';
    }
    if (primary === 'conversion') {
      return 'Defer pricing and discount changes until conversion performance stabilizes.';
    }
    return 'Support the primary commercial strategy without introducing conflicting actions.';
  }
  if (role === 'suppressed') {
    return 'Defer active changes on this lever until the primary strategy is evaluated.';
  }
  const monitorLine = 'Monitor this issue during the current execution window.';
  return monitorLine || 'Monitor this issue during the current execution window.';
}

function buildEnforcedExecutionActions(issue, arbitration, portfolioPrimaryDriver) {
  const role = arbitration?.arbitration_role || 'monitor';
  const primary = portfolioPrimaryDriver || arbitration?.primary_driver || 'primary strategy';
  const adapted = arbitration?.adapted_action_posture || '';

  let actions = [];
  if (role === 'primary') {
    actions = (issue?.actions || [])
      .map((a) => toSentence(a?.description, 'Execute primary strategy steps'))
      .filter(Boolean);
    if (!actions.length) {
      actions = ['Execute primary strategy steps.', 'Track primary-lever response across the current cycle.'];
    }
    return actions;
  }
  if (role === 'supporting') {
    actions = [
      toSentence(adapted, `Keep this lever tactical while ${primary} remains primary`),
      toSentence(`Avoid independent moves that conflict with ${primary}`, 'Avoid conflicting secondary moves'),
      toSentence(`Reassess this lever after ${primary} response is observed`, 'Reassess after primary response')
    ];
  } else if (role === 'suppressed') {
    actions = [
      toSentence(`Defer active changes on this lever during the ${primary} reset window`, 'Defer active changes'),
      toSentence(`Hold broad tactical shifts until ${primary} impact is measured`, 'Hold broad tactical shifts'),
      toSentence('Revisit once primary-cycle evidence is available', 'Revisit after primary-cycle evidence')
    ];
  } else {
    actions = [
      'Track pickup and share response during the current strategy window.',
      `Revisit this issue after the ${primary} adjustment window.`
    ];
  }

  const cleaned = actions.filter((x) => typeof x === 'string' && x.trim() !== '');
  if (cleaned.length) return cleaned;

  if (role === 'supporting') {
    return [
      'Avoid independent tactical changes on this lever.',
      'Align adjustments with the primary strategy.',
      'Reassess after the next performance cycle.'
    ];
  }
  if (role === 'suppressed') {
    return [
      'Hold current settings on this lever.',
      'Re-evaluate after primary strategy impact is observed.'
    ];
  }
  if (role === 'monitor') {
    return [
      'Track performance evolution on this issue.',
      'Revisit if performance does not improve.'
    ];
  }
  return ['Monitor this issue during the current execution window.'];
}

function applyArbitrationOverlayToRetailIssues(issues, decisionArbitrationSummary) {
  const list = Array.isArray(issues) ? issues : [];
  const arbRows = Array.isArray(decisionArbitrationSummary?.issue_level_arbitration)
    ? decisionArbitrationSummary.issue_level_arbitration
    : [];
  const arbByKey = new Map(arbRows.map((x) => [x.finding_key, x]));
  const portfolioPrimaryDriver = decisionArbitrationSummary?.portfolio_primary_driver || null;

  // Final arbitration-role correction from the final selected set (post ranking/selection).
  const finalPricingTruthIssue = list.find((issue) => {
    if ((issue?.segment || 'retail') !== 'retail') return false;
    const driver = issue?.primary_driver || issue?.driver;
    const m = issue?.card_metrics || {};
    const ari = Number(m.avgARI);
    const mpi = Number(m.avgMPI);
    const rgi = Number(m.avgRGI);
    return driver === 'pricing' && Number.isFinite(ari) && Number.isFinite(mpi) && Number.isFinite(rgi) && ari > 100 && mpi < 100 && rgi < 100;
  });

  return list.map((issue) => {
    let arb = arbByKey.get(issue.finding_key);
    if (!arb || (issue?.segment || 'retail') !== 'retail') return issue;

    if (finalPricingTruthIssue) {
      const isWinner = issue.finding_key === finalPricingTruthIssue.finding_key;
      const issueDriver = issue?.primary_driver || issue?.driver;
      const issueFamily = issue?.issue_family || '';
      if (isWinner) {
        arb = { ...arb, arbitration_role: 'primary', primary_driver: 'pricing' };
      } else if (issueDriver === 'visibility' || issueDriver === 'distribution' || issueFamily === 'visibility_gap') {
        arb = { ...arb, arbitration_role: 'supporting' };
      }
    }

    let enforcedDecisionLine = null;
    if (issue.enforced_decision_line && String(issue.enforced_decision_line).trim()) {
      enforcedDecisionLine = issue.enforced_decision_line;
    } else if (
      (arb.arbitration_role === 'primary' || arb.arbitration_role === 'supporting') &&
      issue?.commercial_narrative
    ) {
      const paragraphs = String(issue.commercial_narrative).split('\n\n').map((s) => s.trim()).filter(Boolean);
      const lastParagraph = paragraphs.length ? paragraphs[paragraphs.length - 1] : '';
      if (lastParagraph) enforcedDecisionLine = lastParagraph;
    }
    if (!enforcedDecisionLine) {
      enforcedDecisionLine = buildEnforcedDecisionLine(issue, arb, portfolioPrimaryDriver);
    }
    let enforcedExecutionActions = buildEnforcedExecutionActions(issue, arb, portfolioPrimaryDriver);
    if (Array.isArray(issue.enforced_execution_actions) && issue.enforced_execution_actions.length > 0) {
      enforcedExecutionActions = issue.enforced_execution_actions;
    }
    const enforcementReason =
      arb.suppression_reason ||
      (arb.arbitration_role === 'primary'
        ? 'aligned_with_primary_driver'
        : `aligned_to_${portfolioPrimaryDriver || arb.primary_driver || 'portfolio_strategy'}`);

    const existingActions = issue.actions || [];
    const targetLen = Math.max(1, Math.min(3, enforcedExecutionActions.length || existingActions.length || 1));
    const normalized = [];
    for (let i = 0; i < targetLen; i += 1) {
      const base = existingActions[i] || existingActions[0] || {};
      normalized.push({
        ...base,
        title:
          arb.arbitration_role === 'monitor'
            ? 'Monitor this issue'
            : arb.arbitration_role === 'suppressed'
              ? 'Defer conflicting execution'
              : arb.arbitration_role === 'supporting'
                ? 'Execute supporting posture'
                : base.title || issue.title,
        description: enforcedExecutionActions[i] || enforcedExecutionActions[enforcedExecutionActions.length - 1],
        priority:
          arb.arbitration_role === 'suppressed' || arb.arbitration_role === 'monitor'
            ? 'low'
            : issue.priority || base.priority || 'medium',
        confidence: arb.confidence || base.confidence || 'medium'
      });
    }

    const updatedIssue = {
      ...issue,
      arbitration_role: arb.arbitration_role,
      primary_driver: arb.primary_driver,
      adapted_action_posture: arb.adapted_action_posture,
      enforced_decision_line: enforcedDecisionLine,
      enforced_execution_actions: enforcedExecutionActions,
      enforcement_reason: enforcementReason,
      root_cause: enforcedDecisionLine,
      expected_outcome: enforcedDecisionLine,
      actions: normalized
    };
    console.log("ARBITRATION_ENFORCED", {
      finding_key: updatedIssue.finding_key,
      title: updatedIssue.title,
      arbitration_role: updatedIssue.arbitration_role,
      enforced_decision_line: updatedIssue.enforced_decision_line,
      enforced_execution_actions: updatedIssue.enforced_execution_actions
    });
    return updatedIssue;
  });
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

function generateDecisionId(hotelCode, findingKey, snapshotYmd) {
  return `${hotelCode}__${findingKey}__${snapshotYmd}`;
}

function buildDecisionTrackingRows({ hotelCode, snapshotYmd, allCards }) {
  const rows = [];
  if (!hotelCode || !snapshotYmd || !Array.isArray(allCards)) return rows;

  for (const card of allCards) {
    const findingKey = card?.finding_key;
    if (findingKey == null || findingKey === '') continue;

    const fromCard =
      card?.card_type || card?.type || card?.issue_type || null;
    let cardType = fromCard;
    if (!cardType) {
      const fk = String(findingKey);
      if (fk.startsWith('MIX_')) cardType = 'mix_shift';
      else if (fk.startsWith('FCG_')) cardType = 'forecast_gap';
      else if (fk.startsWith('MONTHLY_')) cardType = 'monthly';
      else if (fk.startsWith('FWD_')) cardType = 'forward_pace';
      else if (fk.startsWith('WPD_')) cardType = 'weekly_pickup_delta';
      else if (fk.startsWith('CORP_')) cardType = 'corporate_pace';
      else if (fk.startsWith('GRP_')) cardType = 'group_pipeline';
      else cardType = 'retail';
    }

    const decisionSummary =
      card?.decision ||
      card?.gm_decision ||
      card?.enforcement?.decision_line ||
      card?.commercial_decision ||
      card?.analysis?.decision ||
      null;

    const diagnosisKey =
      card?.diagnosis_type || card?.analysis?.diagnosis_type || cardType;
    const targetConfig = DECISION_TARGET_METRICS[diagnosisKey] || null;

    rows.push({
      hotel_code: hotelCode,
      finding_key: findingKey,
      card_type: cardType,
      snapshot_date: snapshotYmd,
      decision_summary: decisionSummary,
      target_metric: targetConfig ? targetConfig.metric : null,
      target_direction: targetConfig ? targetConfig.direction : null,
      validation_snapshot: null,
      outcome_status: 'pending',
      outcome_delta: null,
      outcome_notes: null,
      created_at: new Date().toISOString()
    });
  }

  return rows;
}

async function validatePriorDecisions({
  supabaseClient,
  hotelCode,
  snapshotYmd,
  currentDiagnosis,
  currentPaceRows
}) {
  try {
    const { data: pendingRows, error: fetchError } = await supabaseClient
      .from(DECISION_TRACKING_TABLE)
      .select('*')
      .eq('hotel_code', hotelCode)
      .eq('outcome_status', 'pending')
      .lt('snapshot_date', snapshotYmd);

    if (fetchError) {
      console.log('decision_tracking validatePriorDecisions fetch error:', fetchError);
      return;
    }
    if (!Array.isArray(pendingRows) || pendingRows.length === 0) {
      console.log('decision_tracking validatePriorDecisions: no pending rows');
      return;
    }

    let rnSum = 0;
    if (Array.isArray(currentPaceRows)) {
      for (const pr of currentPaceRows) {
        const v = pr?.rn_on_books_ty;
        const n = Number(v);
        if (Number.isFinite(n)) rnSum += n;
      }
    }

    const currentMetrics = {
      avg_mpi: toFiniteNumberOrNull(currentDiagnosis?.metrics?.avgMPI),
      avg_ari: toFiniteNumberOrNull(currentDiagnosis?.metrics?.avgARI),
      avg_rgi: toFiniteNumberOrNull(currentDiagnosis?.metrics?.avgRGI),
      rn_on_books_ty: Number.isFinite(rnSum) ? rnSum : null
    };

    for (const row of pendingRows) {
      const metricKey = row.target_metric;
      const direction = row.target_direction;
      const cur =
        metricKey != null && Object.prototype.hasOwnProperty.call(currentMetrics, metricKey)
          ? currentMetrics[metricKey]
          : null;

      let outcomeStatus = 'pending';
      let outcomeDelta = null;
      let outcomeNotes = null;
      let validationSnapshot = null;

      if (direction === 'hold') {
        outcomeStatus = 'correct';
        outcomeDelta = cur;
        outcomeNotes = null;
        validationSnapshot = snapshotYmd;
      } else if (
        direction === 'up' &&
        (metricKey === 'avg_mpi' || metricKey === 'avg_ari' || metricKey === 'avg_rgi')
      ) {
        if (cur == null || !Number.isFinite(Number(cur))) {
          outcomeNotes = 'Unable to validate: missing or non-finite metric';
        } else {
          const n = Number(cur);
          outcomeDelta = n;
          if (n >= 100) outcomeStatus = 'correct';
          else if (n >= 95) outcomeStatus = 'partial';
          else outcomeStatus = 'incorrect';
          validationSnapshot = snapshotYmd;
        }
      } else if (direction === 'up' && metricKey === 'rn_on_books_ty') {
        outcomeNotes = 'rn_on_books_ty direction=up: comparison rules not defined; left pending';
      } else if (cur == null || !Number.isFinite(Number(cur))) {
        outcomeNotes = 'Unable to validate: missing or non-finite metric';
      } else {
        outcomeNotes = 'Unable to validate: unsupported target_metric/direction pair';
      }

      const patch = {
        outcome_status: outcomeStatus,
        outcome_delta: outcomeDelta,
        outcome_notes: outcomeNotes,
        validation_snapshot: validationSnapshot
      };

      const { error: updateError } = await supabaseClient
        .from(DECISION_TRACKING_TABLE)
        .update(patch)
        .eq('hotel_code', row.hotel_code)
        .eq('finding_key', row.finding_key)
        .eq('snapshot_date', row.snapshot_date);

      if (updateError) {
        console.log('decision_tracking validatePriorDecisions update error:', updateError);
      }
    }
  } catch (e) {
    console.log('decision_tracking validatePriorDecisions non-fatal error:', e);
  }
}

async function persistDecisionTracking(supabaseClient, rows) {
  try {
    if (!Array.isArray(rows) || rows.length === 0) return;

    const { error } = await supabaseClient.from(DECISION_TRACKING_TABLE).upsert(rows, {
      onConflict: 'hotel_code,finding_key,snapshot_date'
    });

    if (error) {
      console.log('decision_tracking persistDecisionTracking upsert error:', error);
    }
  } catch (e) {
    console.log('decision_tracking persistDecisionTracking non-fatal error:', e);
  }
}

function wpdBucketForLeadDays(leadDays) {
  if (leadDays == null || !Number.isFinite(Number(leadDays))) return null;
  const ld = Number(leadDays);
  if (ld >= 1 && ld <= 30) return 'window_1';
  if (ld >= 31 && ld <= 60) return 'window_2';
  if (ld >= 61 && ld <= 90) return 'window_3';
  return null;
}

function wpdWindowLabel(windowClass) {
  if (windowClass === 'window_1') return '1–30';
  if (windowClass === 'window_2') return '31–60';
  if (windowClass === 'window_3') return '61–90';
  return windowClass || '';
}

function wpdMaxLeadForWindowClass(windowClass) {
  if (windowClass === 'window_1') return 30;
  if (windowClass === 'window_2') return 60;
  if (windowClass === 'window_3') return 90;
  return 0;
}

function buildWeeklyPickupDeltaIssues({ currentPaceRows, historicalPmsPaceRows, snapshotYmd, diagnosis }) {
  try {
    const current = Array.isArray(currentPaceRows) ? currentPaceRows : [];
    const historical = Array.isArray(historicalPmsPaceRows) ? historicalPmsPaceRows : [];
    if (!snapshotYmd || !current.length) return [];

    let priorSnapshotDate = null;
    for (const h of historical) {
      const sd = h?.snapshot_date;
      if (!sd || typeof sd !== 'string') continue;
      if (sd >= snapshotYmd) continue;
      if (priorSnapshotDate == null || sd > priorSnapshotDate) priorSnapshotDate = sd;
    }
    if (!priorSnapshotDate) return [];

    const priorRows = historical.filter((r) => r?.snapshot_date === priorSnapshotDate);

    const sumRn = (rows) => {
      let s = 0;
      for (const r of rows) {
        const n = toFiniteNumberOrNull(r?.rn_on_books_ty);
        if (n !== null) s += n;
      }
      return s;
    };

    const sumForecastRn = (rows) => {
      let s = 0;
      for (const r of rows) {
        const n = toFiniteNumberOrNull(r?.forecast_room_nights_ty);
        if (n !== null) s += n;
      }
      return s;
    };

    const forwardCurrent = current.filter(
      (r) => (r?.future_window_class || '') === 'future_forward' && r?.stay_date_ymd
    );
    const forwardPrior = priorRows.filter((r) => r?.stay_date_ymd);

    const cards = [];
    const windowOrder = ['window_1', 'window_2', 'window_3'];

    for (const windowClass of windowOrder) {
      const curWin = forwardCurrent.filter((r) => wpdBucketForLeadDays(r?.lead_days_snapshot_to_stay) === windowClass);
      const priorWin = forwardPrior.filter((r) => wpdBucketForLeadDays(r?.lead_days_snapshot_to_stay) === windowClass);

      const current_rn_otb = sumRn(curWin);
      const prior_rn_otb = sumRn(priorWin);
      const forecast_rn = sumForecastRn(curWin);

      const pickup_this_week = current_rn_otb - prior_rn_otb;
      const gap_to_forecast = forecast_rn - current_rn_otb;
      const maxLead = wpdMaxLeadForWindowClass(windowClass);
      const weeks_remaining = maxLead > 0 ? Math.ceil(maxLead / 7) : 0;
      const required_weekly_pickup =
        weeks_remaining > 0 && Number.isFinite(gap_to_forecast) ? gap_to_forecast / weeks_remaining : null;
      const pickup_vs_required =
        required_weekly_pickup != null &&
        Number.isFinite(required_weekly_pickup) &&
        required_weekly_pickup > 0 &&
        Number.isFinite(pickup_this_week)
          ? pickup_this_week / required_weekly_pickup
          : null;

      let gap_direction = 'widening';
      if (
        required_weekly_pickup != null &&
        Number.isFinite(required_weekly_pickup) &&
        required_weekly_pickup > 0
      ) {
        gap_direction = pickup_this_week >= required_weekly_pickup ? 'closing' : 'widening';
      } else if (pickup_this_week > 0) {
        gap_direction = 'closing';
      }

      const priorOk = priorWin.length > 0 && Number.isFinite(prior_rn_otb);
      if (!priorOk) continue;
      if (!(gap_to_forecast > 0)) continue;
      if (!(Math.abs(pickup_this_week) > 0)) continue;

      const windowLabel = wpdWindowLabel(windowClass);
      const reqStr =
        required_weekly_pickup != null && Number.isFinite(required_weekly_pickup)
          ? required_weekly_pickup.toFixed(0)
          : 'n/a';

      cards.push({
        finding_key: `WPD_${windowClass}_${snapshotYmd}`,
        card_type: 'weekly_pickup_delta',
        window_class: windowClass,
        snapshot_date: snapshotYmd,
        signal: {
          current_rn_otb,
          prior_rn_otb,
          pickup_this_week,
          required_weekly_pickup,
          pickup_vs_required,
          gap_to_forecast,
          gap_direction,
          weeks_remaining
        },
        situation: `Days ${windowLabel}: picked up ${pickup_this_week} RN this week. Required ${reqStr} RN/week to close forecast gap. Gap is ${gap_direction}.`,
        diagnosis:
          gap_direction === 'widening'
            ? `At current pace you will be ${Math.round(gap_to_forecast)} RN short at window close.`
            : `Pickup rate is sufficient to close the gap if sustained.`,
        decision:
          gap_direction === 'widening'
            ? `Accelerate pickup in Days ${windowLabel}. Review rate positioning and open distribution. Do not wait until window tightens.`
            : `Maintain current pace. Monitor weekly to confirm trend holds.`,
        urgency:
          windowClass === 'window_1' ? 'critical' : windowClass === 'window_2' ? 'high' : 'medium',
        str_validation: {
          mpi: diagnosis?.metrics?.avgMPI,
          ari: diagnosis?.metrics?.avgARI,
          rgi: diagnosis?.metrics?.avgRGI
        }
      });
    }

    return cards;
  } catch (e) {
    console.log('buildWeeklyPickupDeltaIssues non-fatal error:', e);
    return [];
  }
}

function corpParseVarianceFraction(raw) {
  const n = toNumber(raw);
  if (n === null || !Number.isFinite(n)) return null;
  if (Math.abs(n) > 1 && Math.abs(n) <= 100) return n / 100;
  return n;
}

function buildCorporateAccountPaceIssues(corporateNormalized, snapshotYmd) {
  try {
    const rows = corporateNormalized?.all;
    if (!snapshotYmd || !Array.isArray(rows) || !rows.length) return [];

    const companyKeys = [
      'Company Name',
      'company name',
      'Company',
      'Account Name',
      'Corporate Account'
    ];
    const fyLyRnKeys = ['2025 FY Room Nights', '2025 FY RN', 'FY 2025 Room Nights', 'FY LY Room Nights'];
    const fullProjKeys = [
      '2026 Full Year RN (Actual + OTB)',
      '2026 Full Year RN',
      'Full Year RN',
      'Projected FY RN'
    ];
    const varianceKeys = [
      'Projected RN vs 2025',
      'Projected RN vs LY',
      'Variance vs 2025',
      'RN Variance vs LY',
      'Variance %'
    ];
    const ytdAdrKeys = ['2026 Actual ADR YTD', 'Actual ADR YTD', 'ADR YTD'];
    const mgrKeys = ['Account Manager', 'account manager', 'Manager'];
    const sectorKeys = ['Sector', 'sector', 'Industry'];
    const commentsKeys = ['Comments / Account Intelligence', 'Comments', 'Account Intelligence', 'Notes'];

    const accounts = [];
    for (const row of rows) {
      if (!row || typeof row !== 'object') continue;
      const company_name = getRowValue(row, companyKeys);
      if (company_name == null || `${company_name}`.trim() === '') continue;

      const fy_ly_rn = toNumber(getRowValue(row, fyLyRnKeys));
      const full_year_projected_rn = toNumber(getRowValue(row, fullProjKeys));
      const variance_vs_ly = corpParseVarianceFraction(getRowValue(row, varianceKeys));
      if (variance_vs_ly === null || !Number.isFinite(variance_vs_ly)) continue;

      const actual_ytd_adr = toNumber(getRowValue(row, ytdAdrKeys));
      const account_manager = getRowValue(row, mgrKeys) ?? null;
      const sector = getRowValue(row, sectorKeys) ?? null;
      const comments = getRowValue(row, commentsKeys) ?? null;

      let variance_rn = null;
      if (
        fy_ly_rn !== null &&
        full_year_projected_rn !== null &&
        Number.isFinite(fy_ly_rn) &&
        Number.isFinite(full_year_projected_rn)
      ) {
        variance_rn = full_year_projected_rn - fy_ly_rn;
      }

      const variance_pct = variance_vs_ly * 100;

      accounts.push({
        company_name: `${company_name}`.trim(),
        fy_ly_rn,
        full_year_projected_rn,
        variance_vs_ly,
        variance_pct,
        variance_rn,
        actual_ytd_adr,
        account_manager: account_manager != null ? `${account_manager}`.trim() : null,
        sector: sector != null ? `${sector}`.trim() : null,
        comments: comments != null ? `${comments}`.trim() : null
      });
    }

    if (!accounts.length) return [];

    const pace_risk = accounts.filter((a) => a.variance_vs_ly < -0.1).sort((a, b) => a.variance_vs_ly - b.variance_vs_ly);
    const pace_opportunity = accounts
      .filter((a) => a.variance_vs_ly > 0.1)
      .sort((a, b) => b.variance_vs_ly - a.variance_vs_ly);

    const out = [];

    if (pace_risk.length) {
      const accounts_at_risk = pace_risk.map((a) => ({
        company_name: a.company_name,
        fy_ly_rn: a.fy_ly_rn,
        full_year_projected_rn: a.full_year_projected_rn,
        variance_pct: a.variance_pct,
        variance_rn: a.variance_rn,
        account_manager: a.account_manager,
        sector: a.sector,
        comments: a.comments
      }));

      let total_rn_at_risk = 0;
      for (const a of pace_risk) {
        if (a.variance_rn != null && Number.isFinite(a.variance_rn) && a.variance_rn < 0) {
          total_rn_at_risk += a.variance_rn;
        }
      }

      const adrVals = pace_risk.map((a) => a.actual_ytd_adr).filter((v) => v != null && Number.isFinite(v));
      const avgAdr =
        adrVals.length > 0 ? adrVals.reduce((s, v) => s + v, 0) / adrVals.length : null;
      const total_revenue_at_risk =
        avgAdr != null && Number.isFinite(avgAdr) && Number.isFinite(total_rn_at_risk)
          ? Math.abs(total_rn_at_risk) * avgAdr
          : 0;

      const revStr = Number.isFinite(total_revenue_at_risk)
        ? Math.round(total_revenue_at_risk).toLocaleString()
        : '0';

      out.push({
        finding_key: `CORP_PACE_RISK_${snapshotYmd}`,
        card_type: 'corporate_pace_risk',
        snapshot_date: snapshotYmd,
        accounts_at_risk,
        total_rn_at_risk,
        total_revenue_at_risk,
        situation: `${accounts_at_risk.length} corporate accounts are tracking behind 2025 pace. Combined RN shortfall: ${total_rn_at_risk} RN. Estimated revenue at risk: €${revStr}.`,
        diagnosis: `Accounts behind pace may indicate lost RFP, reduced travel budgets, or competitor displacement. Each account requires individual investigation before taking rate or restriction action.`,
        decision: `Contact account managers for the ${Math.min(3, accounts_at_risk.length)} most at-risk accounts this week. Identify whether shortfall is recoverable or requires replacement volume from alternative segments.`,
        urgency: total_rn_at_risk < -200 ? 'critical' : 'high'
      });
    }

    if (pace_opportunity.length) {
      const accounts_ahead = pace_opportunity.map((a) => ({
        company_name: a.company_name,
        fy_ly_rn: a.fy_ly_rn,
        full_year_projected_rn: a.full_year_projected_rn,
        variance_pct: a.variance_pct,
        variance_rn: a.variance_rn,
        account_manager: a.account_manager,
        sector: a.sector,
        comments: a.comments
      }));

      out.push({
        finding_key: `CORP_PACE_OPP_${snapshotYmd}`,
        card_type: 'corporate_pace_opportunity',
        snapshot_date: snapshotYmd,
        accounts_ahead,
        situation: `${accounts_ahead.length} corporate accounts are tracking ahead of 2025 pace.`,
        diagnosis: `Strong corporate pace from these accounts provides rate support. Protect their preferred dates from discount dilution.`,
        decision: `Ensure preferred rate agreements are honoured on compression dates. Do not displace these accounts with lower-rated group or wholesale business.`,
        urgency: 'medium'
      });
    }

    return out;
  } catch (e) {
    console.log('buildCorporateAccountPaceIssues non-fatal error:', e);
    return [];
  }
}

function grpArrivalYmdFromRow(row) {
  const arrivalKeys = [
    'Arrival Date',
    'arrival date',
    'Group Arrival',
    'Start Date',
    'Event Start'
  ];
  const raw = getRowValue(row, arrivalKeys);
  if (raw == null || raw === '') return null;
  if (typeof raw === 'string' && /^\d{4}-\d{2}-\d{2}/.test(raw.trim())) return raw.trim().slice(0, 10);
  const d = parseExcelDate(raw);
  return d ? formatDateToYMD(d) : null;
}

function grpLeadBucketFromArrival(snapshotYmd, arrivalYmd) {
  const sSnap = parseYmdToUtcDate(snapshotYmd);
  const sArr = parseYmdToUtcDate(arrivalYmd);
  if (!sSnap || !sArr) return null;
  const lead = Math.round((sArr.getTime() - sSnap.getTime()) / 86400000);
  if (lead < 1) return null;
  if (lead >= 1 && lead <= 30) return 'window_1';
  if (lead >= 31 && lead <= 60) return 'window_2';
  if (lead >= 61 && lead <= 90) return 'window_3';
  if (lead >= 91) return 'beyond';
  return null;
}

function grpForecastGapRn(issue) {
  const q = issue?.quantification;
  const v =
    q?.gap_rn ??
    q?.rn_gap ??
    issue?.gap_rn ??
    (typeof q?.rn_gap === 'number' ? q.rn_gap : null) ??
    0;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function grpMatchForecastIssue(forecastGapIssues, minLead, maxLead) {
  const list = Array.isArray(forecastGapIssues) ? forecastGapIssues : [];
  for (const issue of list) {
    const fk = String(issue?.finding_key || '');
    const m = fk.match(/^FCG_(\d+)_(\d+)_/);
    if (m) {
      const a = Number(m[1]);
      const b = Number(m[2]);
      if (a === minLead && b === maxLead) return issue;
    }
  }
  return null;
}

function buildGroupPipelineIssues(delphiNormalized, forecastGapIssues, snapshotYmd) {
  try {
    const rows = delphiNormalized?.all;
    if (!snapshotYmd || !Array.isArray(rows) || !rows.length) return [];

    const nameKeys = [
      'Account: Account Name',
      'Account Name',
      'Group Name',
      'Account',
      'Name'
    ];
    const statusKeys = ['Status', 'status', 'Booking Status'];
    const rnKeys = ['Blended Roomnights', 'Blended Room Nights', 'Room Nights', 'RN'];
    const roomRevKeys = [
      'Blended Guestroom Revenue Total',
      'Guestroom Revenue',
      'Room Revenue',
      'Blended Revenue'
    ];
    const totalRevKeys = ['Blended Revenue Total', 'Total Revenue', 'Revenue Total'];
    const segKeys = ['Market Segment', 'Segment', 'market segment'];
    const ownerKeys = ['Booking: Owner Name', 'Owner Name', 'Sales Manager', 'Owner'];

    const byWindow = new Map();
    const windowOrder = ['window_1', 'window_2', 'window_3'];

    for (const w of windowOrder) byWindow.set(w, []);

    for (const row of rows) {
      if (!row || typeof row !== 'object') continue;
      const statusRaw = getRowValue(row, statusKeys);
      const status = statusRaw != null ? `${statusRaw}`.trim() : '';
      const sl = status.toLowerCase();
      if (sl !== 'tentative' && sl !== 'prospect') continue;

      const arrivalYmd = grpArrivalYmdFromRow(row);
      if (!arrivalYmd) continue;
      const windowClass = grpLeadBucketFromArrival(snapshotYmd, arrivalYmd);
      if (!windowClass || windowClass === 'beyond' || !byWindow.has(windowClass)) continue;

      const group_name = getRowValue(row, nameKeys);
      const room_nights = toNumber(getRowValue(row, rnKeys));
      const room_revenue = toNumber(getRowValue(row, roomRevKeys));
      const total_revenue = toNumber(getRowValue(row, totalRevKeys));
      const market_segment = getRowValue(row, segKeys);
      const owner_name = getRowValue(row, ownerKeys);

      const is_event_only = room_nights === null || room_nights === 0;

      byWindow.get(windowClass).push({
        group_name: group_name != null ? `${group_name}`.trim() : null,
        status,
        arrival_date: arrivalYmd,
        room_nights,
        room_revenue,
        total_revenue,
        market_segment: market_segment != null ? `${market_segment}`.trim() : null,
        owner_name: owner_name != null ? `${owner_name}`.trim() : null,
        is_event_only
      });
    }

    const cards = [];
    const winParams = [
      { windowClass: 'window_1', minLead: 1, maxLead: 30, label: '1–30' },
      { windowClass: 'window_2', minLead: 31, maxLead: 60, label: '31–60' },
      { windowClass: 'window_3', minLead: 61, maxLead: 90, label: '61–90' }
    ];

    for (const wp of winParams) {
      const groupsRaw = byWindow.get(wp.windowClass) || [];
      if (!groupsRaw.length) continue;

      const fgIssue = grpMatchForecastIssue(forecastGapIssues, wp.minLead, wp.maxLead);
      if (!fgIssue) continue;

      const forecast_gap_rn = grpForecastGapRn(fgIssue);

      const groups = [...groupsRaw].sort((a, b) => {
        const ra = a.room_revenue != null && Number.isFinite(a.room_revenue) ? a.room_revenue : -Infinity;
        const rb = b.room_revenue != null && Number.isFinite(b.room_revenue) ? b.room_revenue : -Infinity;
        return rb - ra;
      });

      let tentative_rn = 0;
      let prospect_rn = 0;
      let has_event_only_groups = false;

      for (const g of groups) {
        if (g.is_event_only) has_event_only_groups = true;
        const addRn = !g.is_event_only && g.room_nights != null && Number.isFinite(g.room_nights) ? g.room_nights : 0;
        const st = `${g.status}`.toLowerCase();
        if (st === 'tentative') tentative_rn += addRn;
        else if (st === 'prospect') prospect_rn += addRn;
      }

      const total_pipeline_rn = tentative_rn + prospect_rn;
      const gap_closure_pct =
        forecast_gap_rn > 0 ? (tentative_rn + prospect_rn) / forecast_gap_rn : 0;

      const top = groups[0];
      const topRevStr =
        top?.room_revenue != null && Number.isFinite(top.room_revenue)
          ? Math.round(top.room_revenue).toLocaleString()
          : '';
      const topRnStr =
        top?.room_nights != null && Number.isFinite(top.room_nights) ? `${top.room_nights}` : '';
      const priorityName = top?.group_name ? `${top.group_name}` : '';

      cards.push({
        finding_key: `GRP_PIPELINE_${wp.windowClass}_${snapshotYmd}`,
        card_type: 'group_pipeline_gap',
        window_class: wp.windowClass,
        snapshot_date: snapshotYmd,
        groups,
        tentative_rn,
        prospect_rn,
        total_pipeline_rn,
        forecast_gap_rn,
        gap_closure_pct,
        has_event_only_groups,
        situation: `Days ${wp.label}: ${groups.length} groups in pipeline (${tentative_rn} RN Tentative, ${prospect_rn} RN Prospect). Pipeline covers ${(gap_closure_pct * 100).toFixed(0)}% of forecast gap.`,
        diagnosis:
          gap_closure_pct >= 0.5
            ? `Confirming pipeline groups would close the majority of the forecast gap for this window. Priority: convert Tentative to Definite.`
            : `Pipeline groups are insufficient to close the forecast window gap alone. Group conversion must be combined with transient pace acceleration.`,
        decision: `Focus on converting the highest-revenue Tentative groups first. ${
          priorityName
            ? `Priority: ${priorityName} (${topRnStr} RN, €${topRevStr}).`
            : ''
        }`,
        urgency:
          wp.windowClass === 'window_1' ? 'critical' : wp.windowClass === 'window_2' ? 'high' : 'medium'
      });
    }

    return cards;
  } catch (e) {
    console.log('buildGroupPipelineIssues non-fatal error:', e);
    return [];
  }
}

// --------------------
// MAIN HANDLER
// --------------------
async function handler(req, res) {
  try {
    const hotelCode = (req.body?.hotelCode || 'Unknown Hotel').toString().trim();
    const workbook = await getWorkbookFromRequest(req);

    const snapshotInstant = new Date();
    const snapshotYmd = formatDateToYMD(snapshotInstant);

    const strSheetName = findSheetByAliases(workbook, SHEET_ALIASES_STR);
    const strRowsRaw = getSheetRows(workbook, SHEET_ALIASES_STR);
    if (!strRowsRaw.length) {
      return res.status(400).json({ error: 'STR sheet not found or empty' });
    }

    const strRows = filterStrRowsActualizedThroughSnapshot(strRowsRaw, snapshotYmd);
    if (!strRows.length) {
      return res.status(400).json({
        error:
          'No STR rows on or before snapshot date — STR tab must contain actualized history only through the upload snapshot (future market rows are excluded).'
      });
    }

    const pmsRowsRaw = getPmsSheetRows(workbook);

    const DEBUG_PMS_RAW_KEYS_N = 3;
    for (let pri = 0; pri < Math.min(DEBUG_PMS_RAW_KEYS_N, pmsRowsRaw.length); pri += 1) {
      const row = pmsRowsRaw[pri];
      const keys = Object.keys(row);
      const maxKeysInSample = pri === 0 ? 50 : 25;
      const sampleRowTruncated = {};
      for (const k of keys) {
        if (Object.keys(sampleRowTruncated).length >= maxKeysInSample) break;
        const v = row[k];
        if (v === null || v === undefined) sampleRowTruncated[k] = v;
        else if (typeof v === 'string' && v.length > 120) sampleRowTruncated[k] = `${v.slice(0, 120)}…`;
        else sampleRowTruncated[k] = v;
      }
      console.log('DEBUG PMS raw row after sheet load', {
        rowIndex: pri,
        keys,
        sampleRowTruncated
      });
    }

    const pmsNormalized = normalizePmsRowsForIngestion(pmsRowsRaw, snapshotYmd);
    const pmsRows = pmsNormalized.rowsForEngine;

    const corporateRaw = getSheetRowsTabular(workbook, SHEET_ALIASES_CORPORATE);
    const delphiRaw = getSheetRowsTabular(workbook, SHEET_ALIASES_DELPHI);
    const corporateNormalized = normalizeCorporateRowsForIngestion(corporateRaw, snapshotYmd);
    const delphiNormalized = normalizeDelphiRowsForIngestion(delphiRaw, snapshotYmd);

    const pmsPaceComparator = buildPmsPaceComparatorLayer(
      pmsNormalized.all,
      snapshotYmd,
      pmsNormalized.stly_supported_tab
    );

    const workbookIngestion = buildWorkbookIngestionModel({
      snapshotYmd,
      strSheetName,
      strRowsRaw,
      strRowsActualized: strRows,
      pmsNormalized,
      corporateNormalized,
      delphiNormalized,
      pmsPaceComparator
    });

    const detection = detectDataContext(workbook);
    const diagnosis = buildDiagnosisFromSTR(strRows);
    const focus = buildFocusFromPMS(pmsRows, diagnosis);
    const isTransientFocus = ['retail', 'negotiated', 'discount', 'qualified', 'wholesale'].includes(
      focus?.focus_segment || ''
    );
    const driver = buildDriverFromDiagnosis(diagnosis, focus, strRows, pmsRows);
    const periodMeta = extractPeriodMetadata(strRows, snapshotYmd);

    let enrichedIssues = [];
    let enrichedActions;

    let retailTemporalMeta = null;

    if (isTransientFocus) {
      const weekly = buildRetailIssuesFromWeeklyTemporal(strRows, focus, driver, pmsRows);
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

      const enrichCtx = {
        diagnosis,
        focus,
        detection,
        pmsRows,
        strRows,
        period_start: periodMeta.period_start,
        period_end: periodMeta.period_end
      };
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
          strRows,
          period_start: periodMeta.period_start,
          period_end: periodMeta.period_end
        })
      }));
    }

    const totalOpportunity = buildTotalOpportunity(enrichedActions);
    const pmsPaceSnapshotRowsForAnalysis = buildPmsPaceSnapshotRowsForPersistence({
      hotelCode,
      snapshotDateYmd: periodMeta.snapshot_date,
      pmsPaceComparator
    });
    await validatePriorDecisions({
      supabaseClient: supabase,
      hotelCode,
      snapshotYmd: periodMeta.snapshot_date,
      currentDiagnosis: diagnosis,
      currentPaceRows: pmsPaceSnapshotRowsForAnalysis
    });
    const historicalPmsPaceRows = await readPmsPaceHistoricalRowsForFutureWindow({
      supabaseClient: supabase,
      hotelCode,
      snapshotDateYmd: periodMeta.snapshot_date,
      currentRows: pmsPaceSnapshotRowsForAnalysis
    });
    const snapshotHistorySummary = buildPmsSnapshotHistorySummary({
      snapshotDateYmd: periodMeta.snapshot_date,
      currentRows: pmsPaceSnapshotRowsForAnalysis,
      historicalRows: historicalPmsPaceRows
    });
    const paceSignalSummary = buildPaceSignalSummaryFromSnapshotHistory(snapshotHistorySummary);
    const paceCandidateIssues = buildPaceCandidateIssuesFromPaceSignalSummary(paceSignalSummary);
    const hiddenRankedPaceIssues = buildHiddenRankedPaceIssuesFromCandidates(paceCandidateIssues);
    const financialQuantificationSummary = buildFinancialQuantificationSummary(enrichedIssues, {
      diagnosis,
      focus,
      driver,
      detection,
      pmsRows,
      pmsPaceRows: pmsPaceSnapshotRowsForAnalysis,
      strRows,
      period_start: periodMeta.period_start,
      period_end: periodMeta.period_end
    });
    const commercialContextSummary = buildCommercialContextSummary(
      enrichedIssues,
      financialQuantificationSummary,
      {
        diagnosis,
        focus,
        driver,
        detection,
        pmsRows,
        strRows
      }
    );
    const decisionFramingSummary = buildDecisionFramingSummary(
      enrichedIssues,
      financialQuantificationSummary,
      commercialContextSummary
    );
    const actionIntelligenceSummary = buildActionIntelligenceSummary(
      enrichedIssues,
      decisionFramingSummary,
      commercialContextSummary,
      financialQuantificationSummary
    );
    const decisionArbitrationSummary = buildDecisionArbitrationSummary(
      enrichedIssues,
      financialQuantificationSummary,
      commercialContextSummary,
      decisionFramingSummary,
      actionIntelligenceSummary
    );
    if (isTransientFocus && enrichedIssues.length > 0) {
      enrichedIssues = applyControlledActionsToRetailIssues(
        enrichedIssues,
        actionIntelligenceSummary,
        decisionFramingSummary,
        commercialContextSummary,
        financialQuantificationSummary
      );
      enrichedIssues = applyArbitrationOverlayToRetailIssues(
        enrichedIssues,
        decisionArbitrationSummary
      );
      enrichedActions = flattenIssuesToLegacyActions(enrichedIssues);
    }

    const forwardIssues = buildForwardIssuesFromPmsOtb(
      workbookIngestion.rows.pms,
      strRows,
      diagnosis,
      snapshotYmd
    );

    // Phase 2: Forecast vs OTB gap — runs after forward pace issues.
    // Uses allPmsRows (includes future rows) not just rowsForEngine.
    const forecastGapIssues = buildForecastGapIssues(
      workbookIngestion.rows.pms,
      strRows,
      diagnosis,
      snapshotYmd
    );

    const weeklyPickupDeltaIssues = buildWeeklyPickupDeltaIssues({
      currentPaceRows: pmsPaceSnapshotRowsForAnalysis,
      historicalPmsPaceRows,
      snapshotYmd: periodMeta.snapshot_date,
      diagnosis
    });

    const corporateAccountPaceIssues = buildCorporateAccountPaceIssues(
      corporateNormalized,
      periodMeta.snapshot_date
    );

    const groupPipelineIssues = buildGroupPipelineIssues(
      delphiNormalized,
      forecastGapIssues,
      periodMeta.snapshot_date
    );

    // Phase 2: Segment mix shift detection — runs after forecast gap.
    // Uses actualized pmsRows only. Does not modify any existing output.
    const mixShiftIssues = buildMixShiftIssues(
      pmsRows,
      snapshotYmd,
      periodMeta.period_start,
      periodMeta.period_end
    );

    // Phase 2: Monthly granularity — run after the main weekly pipeline is complete.
    // Uses the same strRows and pmsRows already in scope. Does not modify any existing output.
    const monthlyIssues = buildMonthlyIssues(strRows, pmsNormalized.rowsForEngine, diagnosis);

    const str_weekly_series = [];
    if (Array.isArray(strRows) && strRows.length) {
      const byWeek = new Map();
      for (const row of strRows) {
        const ymd = getRowStayDateYmd(row);
        if (!ymd) continue;
        const d = parseYmdToUtcDate(ymd);
        if (!d) continue;
        const weekKey = weekBucketKeyFromDate(d);
        if (!weekKey) continue;
        if (!byWeek.has(weekKey)) byWeek.set(weekKey, []);
        byWeek.get(weekKey).push(row);
      }
      const weeklyParts = [...byWeek.entries()]
        .filter(([, rows]) => rows.length >= MIN_STR_DAYS_PER_WEEK)
        .map(([week_key, rows]) => {
          const sampleYmd = getRowStayDateYmd(rows[0]);
          const bounds = getIsoWeekStayBoundsFromYmd(sampleYmd);
          return {
            week_key,
            week_start_ymd: bounds.stay_week_start_ymd,
            week_end_ymd: bounds.stay_week_end_ymd,
            avg_mpi: averageMetric(rows, ['MPI', 'MPI Index']),
            avg_ari: averageMetric(rows, ['ARI', 'ARI Index']),
            avg_rgi: averageMetric(rows, ['RGI', 'RGI Index']),
            avg_occ: averageMetric(rows, ['Hotel Occupancy %', 'Occupancy %']),
            day_count: rows.length
          };
        })
        .sort((a, b) => (a.week_key < b.week_key ? -1 : a.week_key > b.week_key ? 1 : 0));
      str_weekly_series.push(...weeklyParts);
    }

    const enginePayload = {
      success: true,
      detection,
      diagnosis,
      focus,
      driver,
      issues: isTransientFocus ? enrichedIssues : [],
      forward_issues: forwardIssues,
      forecast_gap_issues: forecastGapIssues,
      weekly_pickup_delta_issues: weeklyPickupDeltaIssues,
      corporate_account_pace_issues: corporateAccountPaceIssues,
      group_pipeline_issues: groupPipelineIssues,
      mix_shift_issues: mixShiftIssues,
      monthly_issues: monthlyIssues,
      // ISO-week STR KPI series for engine_json consumers (additive; does not replace any legacy field).
      str_weekly_series,
      retail_temporal:
        isTransientFocus && retailTemporalMeta ? retailTemporalMeta : null,
      total_opportunity: totalOpportunity,
      /** Normalized workbook views + row classification (pace engine consumes later). */
      workbook_ingestion: workbookIngestion,
      /** Hidden backend-only same-lead pace history summary (not yet used for issue cards/actions). */
      snapshot_history_summary: snapshotHistorySummary,
      /** Hidden backend-only pace signal candidates from snapshot history (not yet user-visible). */
      pace_signal_summary: paceSignalSummary,
      /** Hidden gated pace issue candidates — not merged into retail issues/recommendations/actions. */
      pace_candidate_issues: paceCandidateIssues,
      /** Hidden ranked pace issues in retail-like structure (backend-only; excluded from visible outputs). */
      hidden_ranked_pace_issues: hiddenRankedPaceIssues,
      /** Hidden financial quantification for visible retail issues (backend-only, not UI-exposed yet). */
      financial_quantification_summary: financialQuantificationSummary,
      /** Hidden issue-level commercial context and narrative framing (backend-only). */
      commercial_context_summary: commercialContextSummary,
      /** Hidden decision framing layer (what type of decision is needed, not execution). */
      decision_framing_summary: decisionFramingSummary,
      /** Hidden action intelligence layer (guided, non-prescriptive, backend-only). */
      action_intelligence_summary: actionIntelligenceSummary,
      /** Hidden decision arbitration layer to resolve cross-issue strategic conflicts. */
      decision_arbitration_summary: decisionArbitrationSummary,
      // Back-compat: flattened per-action rows; each carries issue finding_key for joins.
      actions: enrichedActions
    };

    enginePayload.active_segment_signals = focus?.active_signals || [];
    enginePayload.segment_overview = {
      total_rn_ty: focus?.total_rn_ty ?? null,
      total_rn_ly: focus?.total_rn_ly ?? null,
      overall_adr_ty: focus?.overall_adr_ty ?? null,
      overall_adr_ly: focus?.overall_adr_ly ?? null,
      overall_adr_variance: focus?.overall_adr_variance ?? null
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

    const allCardsForTracking = [
      ...(Array.isArray(enrichedIssues) ? enrichedIssues : []),
      ...(Array.isArray(forwardIssues) ? forwardIssues : []),
      ...(Array.isArray(forecastGapIssues) ? forecastGapIssues : []),
      ...(Array.isArray(weeklyPickupDeltaIssues) ? weeklyPickupDeltaIssues : []),
      ...(Array.isArray(corporateAccountPaceIssues) ? corporateAccountPaceIssues : []),
      ...(Array.isArray(groupPipelineIssues) ? groupPipelineIssues : []),
      ...(Array.isArray(mixShiftIssues) ? mixShiftIssues : []),
      ...(Array.isArray(monthlyIssues) ? monthlyIssues : [])
    ];
    const decisionTrackingRows = buildDecisionTrackingRows({
      hotelCode,
      snapshotYmd: periodMeta.snapshot_date,
      allCards: allCardsForTracking
    });
    await persistDecisionTracking(supabase, decisionTrackingRows);
    enginePayload.decision_tracking_count = decisionTrackingRows.length;

    console.log('DEBUG reached post-engine_outputs stage');

    console.log('DEBUG building pms pace snapshot rows', {
      hotelCode,
      snapshotDateYmd: periodMeta.snapshot_date,
      hasPaceComparator: !!pmsPaceComparator,
      paceRowCount: Array.isArray(pmsPaceComparator?.pace_rows) ? pmsPaceComparator.pace_rows.length : 0
    });

    const pmsPaceSnapshotRows = pmsPaceSnapshotRowsForAnalysis;

    console.log('DEBUG built pms pace snapshot rows:', {
      builtRowCount: pmsPaceSnapshotRows.length,
      first3RowsSample: pmsPaceSnapshotRows.slice(0, 3)
    });

    console.log('DEBUG calling persistPmsPaceSnapshots');

    const paceSnapResult = await persistPmsPaceSnapshots(supabase, pmsPaceSnapshotRows);
    if (!paceSnapResult.ok) {
      console.error(
        'pms_pace_snapshots upsert failed (analyze continues):',
        paceSnapResult.error?.message || paceSnapResult.error
      );
    } else {
      console.log('DEBUG pms_pace_snapshots rows upserted:', paceSnapResult.written);
    }

    const recommendationsPayload =
      isTransientFocus && enrichedIssues.length > 0
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
      isTransientFocus && enrichedIssues.length > 0
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
      actions_count: actionsPayload.length,
      decision_tracking_count: decisionTrackingRows.length
    });
  } catch (error) {
    console.error('Analyze handler error:', error);
    return res.status(500).json({
      error: error.message || 'Processing failed'
    });
  }
}

module.exports = handler;
