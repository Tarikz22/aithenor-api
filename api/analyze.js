const XLSX = require('xlsx');

module.exports = async function analyzeHandler(req, res) {
  try {
    const { fileUrl, hotelCode } = req.body;

    const response = await fetch(fileUrl);
    const arrayBuffer = await response.arrayBuffer();
    const workbook = XLSX.read(Buffer.from(arrayBuffer), { type: 'buffer' });
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const rawData = XLSX.utils.sheet_to_json(sheet);

    let totalMPI = 0;
    let totalARI = 0;
    let totalRGI = 0;
    let count = 0;

    rawData.forEach((row) => {
      const mpi = parseFloat(row["__EMPTY_5"]);
      const ari = parseFloat(row["__EMPTY_11"]);
      const rgi = parseFloat(row["__EMPTY_17"]);

      if (!isNaN(mpi) && !isNaN(ari) && !isNaN(rgi)) {
        totalMPI += mpi;
        totalARI += ari;
        totalRGI += rgi;
        count++;
      }
    });

    if (count === 0) {
      return res.status(200).json({
        success: false,
        message: "No valid STR rows found"
      });
    }

    const avgMPI = totalMPI / count;
    const avgARI = totalARI / count;
    const avgRGI = totalRGI / count;

    const triggerMet = avgMPI < 100 && avgARI > 100;

    if (!triggerMet) {
      return res.status(200).json({
        success: true,
        message: "No issue detected",
        avgMPI,
        avgARI,
        avgRGI
      });
    }

    const supabaseUrl = process.env.SUPABASE_URL;
    const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

    const recommendation = {
      hotel_name: hotelCode,
      title: "MPI underperformance with strong ADR positioning",
      department: "Commercial",
      finding: "Hotel is priced above market but failing to capture enough demand versus the comp set.",
      hotel_id: hotelCode,
      impact_value: Math.round((100 - avgMPI) * 100),
      impact_type: "EUR",
      is_repeat: false,
      expected_impact_value: Math.round((100 - avgMPI) * 100),
      status: "open",
      period: "2026"
    };

    const recRes = await fetch(`${supabaseUrl}/rest/v1/Recommendations`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "apikey": supabaseKey,
        "Authorization": `Bearer ${supabaseKey}`,
        "Prefer": "return=representation"
      },
      body: JSON.stringify(recommendation)
    });

    if (!recRes.ok) {
      const recError = await recRes.text();
      throw new Error(`Recommendation insert failed: ${recError}`);
    }

    const actions = [
      {
        hotel_name: hotelCode,
        title: "Revenue action",
        action_text: "Review BAR and fenced pricing on low-MPI dates to reduce unnecessary ADR premium.",
        hotel_id: hotelCode,
        expected_impact_value: 0,
        status: "open",
        period: "2026"
      },
      {
        hotel_name: hotelCode,
        title: "Marketing action",
        action_text: "Activate short-lead campaigns on high-conversion channels to stimulate incremental demand.",
        hotel_id: hotelCode,
        expected_impact_value: 0,
        status: "open",
        period: "2026"
      },
      {
        hotel_name: hotelCode,
        title: "Sales action",
        action_text: "Target priority accounts and local demand segments to reinforce base occupancy on need dates.",
        hotel_id: hotelCode,
        expected_impact_value: 0,
        status: "open",
        period: "2026"
      }
    ];

    for (const action of actions) {
      const actRes = await fetch(`${supabaseUrl}/rest/v1/actions`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "apikey": supabaseKey,
          "Authorization": `Bearer ${supabaseKey}`
        },
        body: JSON.stringify(action)
      });

      if (!actRes.ok) {
        const actError = await actRes.text();
        throw new Error(`Action insert failed: ${actError}`);
      }
    }

    return res.status(200).json({
      success: true,
      message: "COM-001 executed",
      avgMPI,
      avgARI,
      avgRGI
    });
  } catch (error) {
    console.error("Analyze error:", error);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
};
