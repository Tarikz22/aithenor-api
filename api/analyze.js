const XLSX = require('xlsx');

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const fileUrl = req.body.fileUrl || req.body.fileurl;
    const hotelCode = req.body.hotelCode || req.body.hotelcode;
    const context = req.body.context;

    // Download the file from Tally URL
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
      const rows = XLSX.utils.sheet_to_json(sheet, { defval: '' });
      allData += `\n=== TAB ${index + 1}: ${sheetName} ===\n`;
      rows.forEach((row, rowIndex) => {
        const entries = Object.entries(row).filter(([k, v]) => v !== '' && v !== null);
        if (entries.length > 0) {
          allData += `Row ${rowIndex + 1}: `;
          allData += entries.map(([k, v]) => `${k}: ${v}`).join(' | ');
          allData += '\n';
        }
      });
      allData += '\n';
    });

    // Debug first
    return res.status(200).json({ debug: allData.substring(0, 3000) });

  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
}
