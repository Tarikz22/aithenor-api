const XLSX = require('xlsx');

module.exports = async function analyzeHandler(req, res) {
  try {
    const { fileUrl, hotelCode } = req.body;

    const response = await fetch(fileUrl);
    const arrayBuffer = await response.arrayBuffer();
    const workbook = XLSX.read(Buffer.from(arrayBuffer), { type: 'buffer' });
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const rawData = XLSX.utils.sheet_to_json(sheet);

    // ===== PERIOD EXTRACTION =====
const dates = rawData
  .map(row => row["AITHENOR — STR DAILY REPORT TEMPLATE"])
  .filter(val => typeof val === "string" && val.includes("/"))
  .map(d => {
    const [day, month, year] = d.split("/");
    return new Date(`${year}-${month}-${day}`);
  })
  .filter(d => !isNaN(d));

let period = "unknown";

if (dates.length > 0) {
  const minDate = new Date(Math.min(...dates));
  const maxDate = new Date(Math.max(...dates));

  const format = (d) =>
    `${String(d.getDate()).padStart(2, "0")}/${String(d.getMonth()+1).padStart(2, "0")}/${d.getFullYear()}`;

  period =
    minDate.getTime() === maxDate.getTime()
      ? format(minDate)
      : `${format(minDate)} → ${format(maxDate)}`;
}

let totalMPI = 0;
let totalARI = 0;
let totalRGI = 0;
let totalCompOcc = 0;
let count = 0;

    rawData.forEach((row) => {
      const mpi = parseFloat(row["__EMPTY_5"]);
      const ari = parseFloat(row["__EMPTY_11"]);
      const rgi = parseFloat(row["__EMPTY_17"]);
      const compOcc = parseFloat(row["__EMPTY_2"]);

      if (!isNaN(mpi) && !isNaN(ari) && !isNaN(rgi) && !isNaN(compOcc)) {
        totalMPI += mpi;
        totalARI += ari;
        totalRGI += rgi;
        totalCompOcc += compOcc;
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
    const avgCompOcc = totalCompOcc / count;

    // ===== SEVERITY =====
let severity = "low";

if (avgMPI < 90) severity = "critical";
else if (avgMPI < 95) severity = "high";
else severity = "medium";

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
    // ===== SCENARIO =====
let scenario = "unknown";

if (avgCompOcc < 60) {
  scenario = "market_down";
} else {
  scenario = "market_up";
}

    const supabaseUrl = process.env.SUPABASE_URL;
    const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

const recommendation = {
  hotel_name: hotelCode,
  title:
    scenario === "market_down"
      ? `Market softness detected (Comp Occ ${Math.round(avgCompOcc)}%)`
      : `Hotel underperformance in strong market`,
  department: "Commercial",
  finding:
    scenario === "market_down"
      ? `Market demand is weak (Comp Occ ${Math.round(avgCompOcc)}%), impacting occupancy. MPI (${Math.round(avgMPI)}) decline is driven by external demand conditions.`
      : `Market demand is strong (Comp Occ ${Math.round(avgCompOcc)}%) but hotel under-indexes (MPI ${Math.round(avgMPI)}), indicating internal commercial inefficiencies.`,
  hotel_id: hotelCode,
  impact_value: Math.round((100 - avgMPI) * 120),
  impact_type: "EUR",
  is_repeat: false,
  expected_impact_value: Math.round((100 - avgMPI) * 120),
  status: "open",
  period: period
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

let actions = [];

if (scenario === "market_down") {
  actions = [
    "Focus on demand stimulation rather than price reductions",
    "Activate local and short-lead segments to capture limited demand",
    "Optimize channel mix to maximize visibility in low-demand periods"
  ];
} else {
  actions = [
    "Adjust pricing strategy on low MPI dates to improve competitiveness",
    "Strengthen conversion strategy across direct and OTA channels",
    "Engage sales teams to reinforce base business and account production"
  ];
}

for (const text of actions) {
  await fetch(`${supabaseUrl}/rest/v1/actions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "apikey": supabaseKey,
      "Authorization": `Bearer ${supabaseKey}`
    },
    body: JSON.stringify({
      hotel_name: hotelCode,
      action_text: text,
      status: "open",
      period: period
    })
  });
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
