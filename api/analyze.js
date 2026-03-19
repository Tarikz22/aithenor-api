const ALLOWED_FINDING_IDS = [
  // Revenue
  "REV_OCC_DROP",
  "REV_ADR_UNDERPERFORM",
  "REV_REVPAR_GAP",
  "REV_SEGMENT_IMBALANCE",
  "REV_WEAK_WEEKDAY_BASE",
  "REV_WEAK_WEEKEND_BASE",
  "REV_LOW_PACING",

  // Sales
  "SALES_LOW_CORPORATE_PRODUCTION",
  "SALES_WEAK_ACCOUNT_BASE",
  "SALES_POOR_CONVERSION",
  "SALES_GROUP_PIPELINE_GAP",
  "SALES_RFP_UNDERPERFORMANCE",

  // Marketing
  "MKT_LOW_CAMPAIGN_RETURN",
  "MKT_LOW_DIRECT_TRAFFIC",
  "MKT_WEAK_BRAND_VISIBILITY",
  "MKT_POOR_LEAD_QUALITY",

  // Distribution
  "DIST_HIGH_OTA_DEPENDENCY",
  "DIST_RATE_PARITY_ISSUE",
  "DIST_CHANNEL_MIX_ISSUE",
  "DIST_LOW_DIRECT_SHARE",

  // Finance
  "FIN_COST_OVERSPEND",
  "FIN_LOW_FLOWTHROUGH",
  "FIN_MARGIN_DILUTION",
  "FIN_POOR_EXPENSE_CONTROL",

  // Operations
  "OPS_LOW_GUEST_SATISFACTION",
  "OPS_HIGH_CANCELLATION",
  "OPS_HIGH_REFUND_OR_COMP",
  "OPS_SERVICE_DELIVERY_GAP",
  "OPS_LOW_UPSELL_CAPTURE",

  // Ownership / Strategy
  "OWN_FORECAST_RISK",
  "OWN_BUDGET_GAP",
  "OWN_CAPEX_RETURN_CONCERN",
  "OWN_STRATEGIC_EXECUTION_GAP"
];

const ALLOWED_STRATEGIC_ANGLES = [
  "pricing",
  "segmentation",
  "distribution",
  "sales",
  "marketing",
  "operations",
  "cost_optimization",
  "forecasting",
  "owner_strategy"
];

const ALLOWED_DEPARTMENTS = [
  "Revenue",
  "Sales",
  "Marketing",
  "Distribution",
  "Finance",
  "Operations",
  "Ownership"
];

const XLSX = require('xlsx');

async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const fileUrl = req.body.fileUrl || req.body.fileurl;
    const hotelCode = req.body.hotelCode || req.body.hotelcode;
    const context = req.body.context || '';

    if (!fileUrl) {
      return res.status(400).json({ error: 'Missing fileUrl' });
    }

    const fileResponse = await fetch(fileUrl);
    if (!fileResponse.ok) {
      return res
        .status(400)
        .json({ error: `Failed to download file: ${fileResponse.status}` });
    }

    const arrayBuffer = await fileResponse.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);
    const workbook = XLSX.read(buffer, { type: 'buffer' });

    let allData = '';

    workbook.SheetNames.forEach((sheetName, index) => {
      const sheet = workbook.Sheets[sheetName];
      const csv = XLSX.utils.sheet_to_csv(sheet);

      const lines = csv
        .split('\n')
        .filter((line) => line.replace(/,/g, '').trim() !== '');

      const dataLines = lines.slice(2);

      allData += `\n=== TAB ${index + 1}: ${sheetName} ===\n`;
      allData += dataLines.join('\n');
      allData += '\n\n';
    });

    const prompt = `You are Aithenor, the world's most accomplished hotel commercial strategist. You combine the analytical precision of a McKinsey partner, the revenue intuition of a 30-year luxury hotel operator, and the diagnostic rigour of a forensic accountant. You never give generic advice. Every recommendation you make is specific, data-driven, and immediately actionable.

Hotel Code: ${hotelCode}
Additional Context: ${context}

Below is the complete data from all 5 tabs of this hotel's Excel file in CSV format. The first row of each tab is the header row with column names:

${allData.substring(0, 20000)}

DIAGNOSTIC MODULE: MPI < 100 | ARI > 100

STEP 1 - TRIGGER CONFIRMATION
From Tab 1 STR Daily Report, the columns are: Date, Day of Week, Hotel Occupancy %, Comp Set Occupancy %, Occ % Change vs LY, Occ % Change Comp Set, MPI (Index), MPI % Change, Hotel ADR, Comp Set ADR, ADR % Change vs LY, ADR % Change Comp Set, ARI (Index), ARI % Change, Hotel RevPAR, Comp Set RevPAR, RevPAR % Change vs LY, RevPAR % Change Comp Set, RGI (Index), RGI % Change, Occ Rank, ADR Rank, RevPAR Rank.
Column 7 is MPI (Index) and Column 13 is ARI (Index).
If the majority of data rows show MPI < 100 AND ARI > 100, the trigger is met. Continue with full analysis.
If trigger not met, output: {"trigger_met": false, "findings": [], "actions": []} and stop.

STEP 2 - MARKET VALIDATION
IF market occupancy UP and hotel occupancy DOWN: market healthy, hotel not capturing fair share, continue.
IF market DOWN and hotel DOWN: market softness, downgrade severity, continue with caution.

STEP 3 - SEGMENT GAP IDENTIFICATION
From PMS tab: identify segments where hotel RN TY < hotel RN LY by more than 10%.
From Corporate tab: identify accounts below target or showing YOY decline.
From Delphi tab: check pipeline vs LY and lost business patterns.

STEP 4 - ROOT CAUSE - select ONLY from this list:
OTA rank position decline / OTA Preferred Partner status lapsed / OTA TravelAds budget insufficient / OTA review score below comp set / Rate parity violation / Brand.com digital ads underinvested / GDS rate not loaded correctly / Negotiated account below contracted volume / New accounts pipeline insufficient / Groups pipeline below LY / Lost business rate objection / Lost business competitor preference / Pricing gap vs comp set unjustified / Segment mix distortion / Direct channel underdeveloped / Corporate account office relocation

STEP 5 - GOP IMPACT
Formula: Identified Gap (RN) x Hotel ADR x Probability Coefficient x 65% flow-through
Coefficients: OTA 0.65 / Pricing 0.55 / Negotiated 0.50 / Groups 0.45 / Brand.com 0.60 / GDS 0.50
Show full calculation. Annual = monthly x 12.

OUTPUT RULES:

Return exactly one valid JSON object with this structure:

{
  "hotel_name": "string",
  "period": "string",
  "findings": [
    {
      "finding_title": "string",
      "priority": "Critical | High | Medium | Low",
      "department": "Revenue Management | Sales | Marketing | Distribution | General Management",
      "module": "string",
      "trigger_metric": "string",
      "situation": "string",
      "diagnosis": "string",
      "cause": "string"
    }
  ],
  "actions": [
    {
      "finding_title_reference": "string",
      "action_title": "string",
      "action_detail": "string",
      "owner_department": "Revenue Management | Sales | Marketing | Distribution | General Management",
      "owner_role": "string",
      "urgency": "Critical | High | Medium | Low",
      "expected_outcome": "string",
      "estimated_revenue_upside": number,
      "estimated_gop_saving": "string",
      "due_date": "YYYY-MM-DD"
    }
  ]
}

RULES FOR FINDINGS:
- finding_title: max 8 words, specific channel or segment, never generic
- module: identify the analytical area, for example STR / PMS / Corporate / Delphi / Distribution
- trigger_metric: specific metric for this finding, max 15 characters
- situation: 2-3 sentences using actual numbers from the data
- diagnosis: 1-2 sentences, specific operational failure with actual numbers
- cause: choose ONE root cause only from the approved list

RULES FOR ACTIONS:
- Create at least one action for each finding
- finding_title_reference MUST be copied EXACTLY from the finding_title with zero modification.
- Do NOT rephrase, shorten, or reinterpret the title.
- The value must be a strict character-by-character match.
- If mismatch occurs, the output is considered invalid.
- action_title: concise and operational
- action_detail: 2-4 concrete actions with department and timeframe
- owner_role: assign the most relevant owner such as DOSM, Director of Revenue, Director of Marketing, Distribution Manager, General Manager
- expected_outcome: specific metric improvement using actual numbers where possible
- estimated_revenue_upside: numeric only, no currency symbol
- estimated_gop_saving: show full calculation transparently using actual ADR and room night figures from the data
- due_date: choose a realistic due date within the next 30 days

OUTPUT QUALITY RULES (STRICT):

- Each finding MUST include multiple quantified metrics (€, %, ADR, occupancy, index).
- Findings without numerical evidence are invalid.
- Generic statements are forbidden (e.g., “underperformance”, “opportunity”, “suboptimal” without numbers).

- Each finding must clearly state:
  1. What is happening (with data)
  2. Why it is happening (root cause)
  3. What it impacts financially (revenue and/or GOP)

- Each finding must include an estimated financial impact expressed in revenue and, when possible, translated into GOP impact using a clear flow-through logic.

- Findings must be distinct and non-overlapping.
- Do not repeat the same issue using different wording.
- Each finding must address a unique problem or opportunity.

- Each action must:
  - Be specific and operational (who does what)
  - Reference real levers (pricing, segment mix, channels, accounts)
  - Avoid generic recommendations like “optimize”, “improve”, “review”

- If output is vague or not data-backed, it is invalid.

ADVANCED ANALYSIS RULES (CRITICAL):

- The system must not only identify issues, but challenge commercial decisions.

- For each finding, the analysis must include cross-checks across:
  - segment mix (Transient, Group, Corporate, etc.)
  - pricing vs mix (ADR vs contribution)
  - historical trends (same period last year or typical pattern)
  - market benchmarks (MPI, ARI, competitors)

- The system must explicitly detect and highlight trade-offs, such as:
  - high occupancy driven by low-rated segments
  - displacement of higher-value demand
  - mix distortion impacting ADR or GOP

- When relevant, the system must:
  - challenge the decision taken
  - identify displaced higher-value demand
  - quantify the financial impact of the decision

- Findings must go beyond description and include:
  - decision critique
  - missed opportunity estimation
  - alternative strategy suggestion

- The system must identify pacing gaps:
  - segment underperformance vs last year
  - segment underperformance vs market
  - forward-looking risks when data is available

- If no cross-check or decision logic is applied, the output is considered incomplete.

Generate 3 to 6 findings supported by the data.
Do not invent findings not present in the data.
Use actual numbers from the Excel in every finding and every action outcome where possible.

Respond ONLY with one valid JSON object.
No text before or after.
No markdown.
No code blocks.
The response must start with { and end with }.
`;

    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 4000,
        messages: [{ role: 'user', content: prompt }]
      })
    });

    const data = await response.json();
    console.log('Anthropic status:', response.status);
    console.log('Anthropic response:', JSON.stringify(data));

    if (!data.content || !data.content[0] || !data.content[0].text) {
      return res.status(500).json({
        error: 'Anthropic response invalid',
        anthropic_status: response.status,
        anthropic_data: data
      });
    }

    const text = data.content[0].text;

    const jsonMatch = text.match(/\{[\s\S]*\}/);
    const result = jsonMatch ? jsonMatch[0] : text;

    return res.status(200).json({ result });
  } catch (error) {
    console.error('Handler error:', error);
    return res.status(500).json({ error: error.message });
  }
}

module.exports = { default: handler };
