// ===== AITHENOR TAXONOMY =====
const ALLOWED_FINDING_IDS = [
  "REV_OCC_DROP","REV_ADR_UNDERPERFORM","REV_REVPAR_GAP","REV_SEGMENT_IMBALANCE",
  "REV_WEAK_WEEKDAY_BASE","REV_WEAK_WEEKEND_BASE","REV_LOW_PACING",
  "SALES_LOW_CORPORATE_PRODUCTION","SALES_WEAK_ACCOUNT_BASE","SALES_POOR_CONVERSION",
  "SALES_GROUP_PIPELINE_GAP","SALES_RFP_UNDERPERFORMANCE",
  "MKT_LOW_CAMPAIGN_RETURN","MKT_LOW_DIRECT_TRAFFIC","MKT_WEAK_BRAND_VISIBILITY",
  "MKT_POOR_LEAD_QUALITY",
  "DIST_HIGH_OTA_DEPENDENCY","DIST_RATE_PARITY_ISSUE","DIST_CHANNEL_MIX_ISSUE",
  "DIST_LOW_DIRECT_SHARE",
  "FIN_COST_OVERSPEND","FIN_LOW_FLOWTHROUGH","FIN_MARGIN_DILUTION","FIN_POOR_EXPENSE_CONTROL",
  "OPS_LOW_GUEST_SATISFACTION","OPS_HIGH_CANCELLATION","OPS_HIGH_REFUND_OR_COMP",
  "OPS_SERVICE_DELIVERY_GAP","OPS_LOW_UPSELL_CAPTURE",
  "OWN_FORECAST_RISK","OWN_BUDGET_GAP","OWN_CAPEX_RETURN_CONCERN","OWN_STRATEGIC_EXECUTION_GAP"
];

const ALLOWED_STRATEGIC_ANGLES = [
  "pricing","segmentation","distribution","sales","marketing",
  "operations","cost_optimization","forecasting","owner_strategy"
];

const ALLOWED_DEPARTMENTS = [
  "Revenue","Sales","Marketing","Distribution","Finance","Operations","Ownership"
];

// ===== IMPORTS =====
const XLSX = require('xlsx');
const { createClient } = require('@supabase/supabase-js');

// ===== SUPABASE INIT (FIXED) =====
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

// ===== MEMORY FUNCTION =====
async function getHotelMemory(supabase, hotelId) {
  const { data: recentFindings } = await supabase
    .from("findings")
    .select("finding_id, department, strategic_angle, title")
    .eq("hotel_id", hotelId)
    .order("created_at", { ascending: false })
    .limit(10);

  const { data: recentActions } = await supabase
    .from("recommended_actions")
    .select("action_id, finding_id, action_text")
    .eq("hotel_id", hotelId)
    .order("created_at", { ascending: false })
    .limit(10);

  const { data: openIssues } = await supabase
    .from("issue_memory")
    .select("finding_id, times_flagged, last_strategic_angle, status")
    .eq("hotel_id", hotelId)
    .in("status", ["open", "recurring"])
    .limit(10);

  return {
    recentFindings: recentFindings || [],
    recentActions: recentActions || [],
    openIssues: openIssues || []
  };
}

// ===== MAIN HANDLER =====
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

    // ✅ MEMORY FETCH (CORRECT POSITION)
    const memory = await getHotelMemory(supabase, hotelCode);

    const fileResponse = await fetch(fileUrl);
    if (!fileResponse.ok) {
      return res.status(400).json({ error: `Failed to download file: ${fileResponse.status}` });
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

const prompt = `
You are Aithenor, a hotel-wide decision intelligence engine.

You analyze full hotel performance across:
Revenue, Sales, Marketing, Distribution, Finance, Operations, Ownership.

You do NOT describe data.
You identify issues, challenge decisions, and recommend measurable actions.

--------------------------------
CURRENT HOTEL: ${hotelCode}
--------------------------------

DATA:
${allData.substring(0, 20000)}

--------------------------------
MEMORY — PREVIOUS FINDINGS:
${JSON.stringify(memory.recentFindings, null, 2)}

MEMORY — PREVIOUS ACTIONS:
${JSON.stringify(memory.recentActions, null, 2)}

MEMORY — OPEN ISSUES:
${JSON.stringify(memory.openIssues, null, 2)}
--------------------------------

RULES:

1. Do NOT repeat the same issue unless unresolved.
2. If repeated → explain why situation persists or worsens.
3. Rotate strategic angle (pricing, sales, marketing, cost, operations).
4. Every finding MUST include numbers (ADR, Occ, %, revenue).
5. Every action MUST be concrete and executable.
6. No generic wording (no "improve", "optimize", etc).
7. Cover multiple departments.

--------------------------------

OUTPUT:

Return ONLY valid JSON:

{
  "findings": [
    {
      "title": "short title",
      "department": "Revenue | Sales | Marketing | Distribution | Finance | Operations | Ownership",
      "finding": "data-based explanation",
      "impact_value": number,
      "impact_type": "revenue | cost | gop",
      "is_repeat": true/false,
      "action": {
        "action_text": "clear action",
        "expected_impact_value": number
      }
    }
  ]
}

--------------------------------

QUALITY:

- 3 to 6 findings max
- Must be different issues
- Must be financially relevant
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

    if (!data.content || !data.content[0]?.text) {
      return res.status(500).json({ error: 'Anthropic response invalid', data });
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
