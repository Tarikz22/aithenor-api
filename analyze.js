const XLSX = require('xlsx');

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const fileData = req.body.fileData || req.body.filedata;
    const hotelCode = req.body.hotelCode || req.body.hotelcode;
    const context = req.body.context;

    // Try reading the workbook directly from the raw string
    let workbook;
    try {
      workbook = XLSX.read(fileData, { type: 'base64' });
    } catch(e1) {
      try {
        const buffer = Buffer.from(fileData, 'base64');
        workbook = XLSX.read(buffer, { type: 'buffer' });
      } catch(e2) {
        workbook = XLSX.read(fileData, { type: 'binary' });
      }
    }

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

    // Debug: return the extracted data
    return res.status(200).json({ debug: allData.substring(0, 5000) });

  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
}
