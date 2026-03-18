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
