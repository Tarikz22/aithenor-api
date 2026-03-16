const XLSX = require('xlsx');

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const { fileData, hotelCode, context } = req.body;

    const buffer = Buffer.from(fileData, 'base64');
    const workbook = XLSX.read(buffer, { type: 'buffer' });

    let allData = '';
    workbook.SheetNames.forEach((sheetName, index) => {
      const sheet = workbook.Sheets[sheetName];
      const csv = XLSX.utils.sheet_to_csv(sheet);
      allData += `TAB ${index + 1} - ${sheetName}:\n${csv}\n\n`;
    });

    const prompt = `You are Aithenor, the world's most accomplished hotel commercial strategist. You combine the analytical precision of a McKinsey partner, the revenue intuition of a 30-year luxury hotel operator, and the diagnostic rigour of a forensic accountant. You never give generic advice. Every recommendation you make is specific, data-driven, and immediately actionable.

Hotel Code: ${hotelCode}
Additional Context: ${context}

Below is the complete data from all 5 tabs of this hotel's Excel file:

${allData.substring(0, 12000)}

DIAGNOSTIC MODULE: MPI < 100 | ARI > 100

STEP 1 - TRIGGER CONFIRMATION
Confirm MPI < 100 AND ARI > 100 from the STR tab. If trigger not met, output: [{"trigger_met": false}] and stop.

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
- finding_title: Max 8 words. Specific channel or segment. Never generic.
- priority: Critical / High / Medium / Low only
- department: Revenue Management / Sales / Marketing / Distribution / General Management only
- situation: 2-3 sentences. Use ACTUAL numbers from the data. Quote specific metrics, percentages, and values from the Excel.
- diagnosis: 1-2 sentences. Specific operational failure with actual numbers. Never use: potential, may, possibly, could.
- cause: ONE root cause from approved list only.
- action: 3-5 bullets. Department + timeframe on each.
- expected_outcome: Specific metric improvement with numbers from the data.
- gop_saving: Full calculation shown transparently using actual ADR and room night figures from the data.
- trigger_metric: Specific metric for THIS finding. Max 15 characters.

Generate 3 to 6 findings supported by the data. Do not invent findings not in the data. Use the actual numbers from the Excel in every finding.

Respond ONLY with a valid JSON array. No text before or after. No markdown. No code blocks. Just raw JSON starting with [ and ending with ].`;

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
    return res.status(200).json({ result: data.content[0].text });

  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
}
