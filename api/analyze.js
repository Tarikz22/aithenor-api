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
        `${String(d.getDate()).padStart(2, "0")}/${String(d.getMonth() + 1).padStart(2, "0")}/${d.getFullYear()}`;

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

let performancePosition = "";

if (avgMPI >= 100) {
  performancePosition = "outperforming";
} else {
  performancePosition = "underperforming";
}

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

    // ===== STR v2 — SEGMENTATION ENTRY LAYER =====
    let segmentFocus = "Retail";
    let segmentReason = "Transient demand appears to be the most likely source of underperformance at this stage.";

    if (avgCompOcc < 50 && performancePosition === "underperforming") {
      segmentFocus = "Retail";
      segmentReason = "In a soft market where the hotel is underperforming, the most likely first pressure point is transient retail demand, including pricing, visibility, conversion, or channel mix.";
    } else if (avgCompOcc < 50 && performancePosition === "outperforming") {
      segmentFocus = "Groups";
      segmentReason = "In a soft market where the hotel is outperforming, stronger base business support is the most likely explanation, typically from group or negotiated demand.";
    } else if (scenario === "market_down") {
      segmentFocus = "Retail";
      segmentReason = "When the market is soft overall, transient retail is the first segment to validate because it reacts fastest to weak demand conditions.";
    } else {
      segmentFocus = "Negotiated";
      segmentReason = "When the market is holding but the hotel under-indexes, the issue is more likely structural and linked to negotiated accounts, account production, or contracted base demand.";
    }

    const supabaseUrl = process.env.SUPABASE_URL;
    const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

// ===== v2-final — CLEANER OUTPUT STRUCTURE =====

let diagnosisText = `MPI is ${Math.round(avgMPI)}, ARI is ${Math.round(avgARI)}, and RGI is ${Math.round(avgRGI)}. Market demand is ${scenario === 'market_down' ? 'weak' : 'strong'} (Comp Occ ${Math.round(avgCompOcc)}%), and the hotel is ${performancePosition} versus the comp set.`;

let rootCauseText = segmentReason;

let expectedOutcomeText = `Addressing ${segmentFocus.toLowerCase()} performance gaps should improve occupancy penetration, strengthen market share, and support short-term revenue recovery.`;

let recommendationTitle = `${segmentFocus} underperformance in a ${scenario === 'market_down' ? 'soft' : 'strong'} market`;
let recommendationFinding = `${diagnosisText} ${rootCauseText}`;

    const recommendation = {
      hotel_name: hotelCode,
      title: recommendationTitle,
      department: "Commercial",
      finding: diagnosisText,
      hotel_id: hotelCode,
      impact_value: Math.round((100 - avgMPI) * 120),
      impact_type: "EUR",
      is_repeat: false,
      expected_impact_value: Math.round((100 - avgMPI) * 120),
      status: "open",
      period: period
    };

console.log('STR v2 segment focus:', segmentFocus);
console.log('STR v2 segment reason:', segmentReason);
console.log('STR v2 diagnosis:', diagnosisText);
console.log('STR v2 root cause:', rootCauseText);
console.log('STR v2 expected outcome:', expectedOutcomeText);

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

    if (segmentFocus === "Retail") {
      actions = [
        "Review retail pricing position, OTA visibility, and digital conversion versus the comp set",
        "Validate whether transient demand erosion is coming from price competitiveness, channel mix, or weaker pace",
        "Launch a short-cycle retail recovery plan across revenue, marketing, and distribution teams"
      ];
    } else if (segmentFocus === "Negotiated") {
      actions = [
        "Review negotiated account production, contracted rate positioning, and displaced account opportunities",
        "Validate whether the hotel is missing structural base demand from key corporate or government accounts",
        "Build an account-recovery plan with sales leadership focused on top production gaps and dormant accounts"
      ];
    } else if (segmentFocus === "Groups") {
      actions = [
        "Review group base contribution, pipeline strength, and pace of conversion for upcoming need periods",
        "Validate whether group support is protecting occupancy or whether reliance on group business is masking transient weakness",
        "Align sales and revenue on a group optimization plan focused on need dates, conversion, and displacement quality"
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
          title: recommendationTitle,
          action_text: text,
          status: "open",
          expected_impact_value: Math.round((100 - avgMPI) * 120),
          period: period
        })
      });
    }

    return res.status(200).json({
      success: true,
      message: "COM-001 STR v2 executed",
      avgMPI,
      avgARI,
      avgRGI,
      segmentFocus
    });
  } catch (error) {
    console.error("Analyze error:", error);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
};
