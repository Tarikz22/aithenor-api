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

    const supabaseUrl = process.env.SUPABASE_URL;
    const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

const recommendation = {
  hotel_name: hotelCode,
  title: `MPI underperformance (${Math.round(avgMPI)}) vs ARI (${Math.round(avgARI)})`,
  department: "Commercial",
  finding: `Hotel is priced above market (ARI ${Math.round(avgARI)}) but under-indexing on demand (MPI ${Math.round(avgMPI)}), resulting in weak RGI (${Math.round(avgRGI)}).`,
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

if (severity === "critical") {
  actions = [
    "Immediate pricing correction on all need dates (remove ADR premium)",
    "Activate all demand channels (OTA, direct, partners) with tactical offers",
    "Deploy sales blitz on top accounts to rebuild base occupancy"
  ];
} else if (severity === "high") {
  actions = [
    "Adjust pricing on low pickup dates and monitor elasticity",
    "Launch short-term marketing campaigns targeting conversion segments",
    "Engage key accounts to support weekday base demand"
  ];
} else {
  actions = [
    "Fine-tune pricing strategy on specific low-MPI dates",
    "Monitor pace and adjust distribution exposure",
    "Support demand through light tactical campaigns"
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
