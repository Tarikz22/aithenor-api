const XLSX = require('xlsx');

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
const fileData = req.body.fileData || req.body.filedata;
const hotelCode = req.body.hotelCode || req.body.hotelcode;
const context = req.body.context;

    const buffer = Buffer.from(fileData, 'base64');
    const workbook = XLSX.read(buffer, { type: 'buffer' });
    if (!workbook.SheetNames.length) return res.status(400).json({ error: 'Could not parse Excel file' });

    let allData = '';
    workbook.SheetNames.forEach((sheetName, index) => {
      const sheet = workbook.Sheets[sheetName];
      const csv = XLSX.utils.sheet_to_csv(sheet);
      allData += `TAB ${index + 1} - ${sheetName}:\n${csv}\n\n`;
    });

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
        messages: [{
          role: 'user',
          content: `You are Aithenor the best hotel commercial strategist. Hotel Code: ${hotelCode}. Context: ${context}. Data from all 5 tabs of the hotel Excel file:\n\n${allData.substring(0, 12000)}\n\nIf MPI < 100 and ARI > 100, output 3 to 6 findings as a valid JSON array only. Each finding must have: finding_title, priority, department, situation, diagnosis, cause, action, expected_outcome, gop_saving, trigger_metric. No markdown, just raw JSON starting with [ and ending with ].`
        }]
      })
    });

    const data = await response.json();
    return res.status(200).json({ result: data.content[0].text });

  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
}
