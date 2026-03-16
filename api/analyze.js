const XLSX = require('xlsx');

async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const fileUrl = req.body.fileUrl || req.body.fileurl;
    const hotelCode = req.body.hotelCode || req.body.hotelcode;
    const context = req.body.context;

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
      
      // Skip first 2 rows (title and instructions) and use the rest
      const lines = csv.split('\n').filter(line => line.replace(/,/g, '').trim() !== '');
      const dataLines = lines.slice(2); // Skip title and instructions rows
      
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
If trigger not met, output: [{"trigger_met": false}] and stop.

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
    const jsonMatch = text.match(/\[[\s\S]*\]/);
    const result = jsonMatch ? jsonMatch[0] : text;
    return res.status(200).json({ result });

  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
}

module.exports = { default: handler };
