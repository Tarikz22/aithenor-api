const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const XLSX = require('xlsx');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

module.exports = async function analyzeHandler(req, res) {
  try {
    const { hotelCode, fileUrl, context } = req.body;

    const normalizedHotelCode = (hotelCode || "").trim().toUpperCase();

    // 1. Download file
    const response = await axios.get(fileUrl, { responseType: 'arraybuffer' });

    // 2. Read Excel
    const workbook = XLSX.read(response.data, { type: 'buffer' });
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];
    const data = XLSX.utils.sheet_to_json(sheet);

    console.log("Rows parsed:", data.length);

    // 3. Build recommendations
    const recommendationsToInsert = data.map((row) => ({
      hotel_code: normalizedHotelCode,
      category: row.category || "Revenue",
      insight: row.insight || JSON.stringify(row),
      value: row.value || 0,
      period: row.period || null
    }));

    // 4. Build actions
    const actionsToInsert = data.map((row) => ({
      hotel_code: normalizedHotelCode,
      action_text: row.action || "Review item",
      status: "open",
      period: row.period || null
    }));

    // 5. Insert Recommendations (IMPORTANT: capital R)
    const { error: recError } = await supabase
      .from("Recommendations")
      .insert(recommendationsToInsert);

    if (recError) {
      console.error("Recommendations insert error:", recError);
      return res.status(500).json({ error: recError.message });
    }

    // 6. Insert actions (lowercase a)
    const { error: actError } = await supabase
      .from("actions")
      .insert(actionsToInsert);

    if (actError) {
      console.error("Actions insert error:", actError);
      return res.status(500).json({ error: actError.message });
    }

    // 7. Return success
    return res.json({
      success: true,
      recommendationsInserted: recommendationsToInsert.length,
      actionsInserted: actionsToInsert.length
    });

  } catch (error) {
    console.error("Analyze error:", error);
    return res.status(500).json({ error: error.message });
  }
};
