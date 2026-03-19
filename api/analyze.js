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

const XLSX = require('xlsx');
const { createClient } = require('@supabase/supabase-js');

// ===== INIT =====
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// ===== MEMORY =====
async function getHotelMemory(hotelId) {
  const { data: openIssues } = await supabase
    .from("issue_memory")
    .select("*")
    .eq("hotel_id", hotelId)
    .in("status", ["open", "recurring"]);

  return {
    openIssues: openIssues || []
  };
}

// ===== SAVE =====
async function storeResults(hotelId, findings) {
  for (const item of findings) {
    // FINDINGS
    await supabase.from("findings").insert({
      hotel_id: hotelId,
      title: item.title,
      department: item.department,
      finding_text: item.finding,
      impact_value: item.impact_value,
      impact_type: item.impact_type
    });

    // ACTIONS
    await supabase.from("recommended_actions").insert({
      hotel_id: hotelId,
      action_text: item.action.action_text,
      expected_impact_value: item.action.expected_impact_value,
      status: "pending"
    });

    // MEMORY UPSERT
    const { data: existing } = await supabase
      .from("issue_memory")
      .select("*")
      .eq("hotel_id", hotelId)
      .eq("finding_id", item.title) // temp mapping
      .maybeSingle();

    if (!existing) {
      await supabase.from("issue_memory").insert({
        hotel_id: hotelId,
        finding_id: item.title,
        status: "open",
        times_flagged: 1
      });
    } else {
      await supabase
        .from("issue_memory")
        .update({
          times_flagged: existing.times_flagged + 1,
          status: "recurring"
        })
        .eq("id", existing.id);
    }
  }
}

// ===== HANDLER =====
async function handler(req, res) {
  try {
const fileUrl = req.body.fileUrl || req.body.fileurl;
const hotelId = req.body.hotel_id || req.body.hotelId || req.body.hotelCode || req.body.hotelcode || '';
let hotelName = '';

if (!fileUrl) {
  return res.status(400).json({ error: 'Missing fileUrl' });
}

if (!hotelId) {
  return res.status(400).json({ error: 'Missing hotel code' });
}

const { data: hotelRow, error: hotelLookupError } = await supabase
  .from('hotels')
  .select('hotel_name, hotel_code')
  .eq('hotel_code', hotelId)
  .maybeSingle();

if (hotelLookupError) {
  return res.status(500).json({ error: 'Hotel lookup failed', details: hotelLookupError.message });
}

if (!hotelRow) {
  return res.status(400).json({ error: 'Hotel code does not exist, please review.' });
}

hotelName = hotelRow.hotel_name || '';
    const context = req.body.context || '';
    

    const memory = await getHotelMemory(hotelId);

    const fileResponse = await fetch(fileUrl);
    const buffer = Buffer.from(await fileResponse.arrayBuffer());
    const workbook = XLSX.read(buffer, { type: 'buffer' });

    let allData = '';
    workbook.SheetNames.forEach((sheetName) => {
      const sheet = workbook.Sheets[sheetName];
      allData += XLSX.utils.sheet_to_csv(sheet);
    });

    // ===== CLAUDE PROMPT =====
    const prompt = `
You are Aithenor.

Analyze hotel data and detect high-impact issues.

DATA:
${allData.substring(0, 15000)}

OPEN ISSUES:
${JSON.stringify(memory.openIssues)}

RULES:
- Do not repeat same issue unless worsening
- If repeat → mark is_repeat = true
- Give measurable financial impact
- Actions must be concrete

OUTPUT JSON:
{
  "findings": [
    {
      "title": "",
      "department": "",
      "finding": "",
      "impact_value": 0,
      "impact_type": "revenue",
      "is_repeat": false,
      "action": {
        "action_text": "",
        "expected_impact_value": 0
      }
    }
  ]
}
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
        max_tokens: 2000,
        messages: [{ role: 'user', content: prompt }]
      })
    });

    const data = await response.json();
    const text = data.content?.[0]?.text || "{}";
    const json = JSON.parse(text.match(/\{[\s\S]*\}/)[0]);

    // ===== STORE =====
    await storeResults(hotelId, json.findings);

return res.json({
  hotel_id: hotelId,
  hotel_name: hotelName,
  period: "2026-03",
  findings: json.findings
});

  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: e.message });
  }
}

module.exports = { default: handler };
