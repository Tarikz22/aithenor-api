import * as XLSX from 'xlsx';
import fetch from 'node-fetch';

export default async function handler(req, res) {
  try {
    const { fileUrl, hotelCode } = req.body;

    // 1. Download file
    const response = await fetch(fileUrl);
    const buffer = await response.arrayBuffer();
    const workbook = XLSX.read(buffer, { type: 'buffer' });
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const rawData = XLSX.utils.sheet_to_json(sheet);

    // 2. Extract STR metrics
    let totalMPI = 0;
    let totalARI = 0;
    let totalRGI = 0;
    let count = 0;

    rawData.forEach(row => {
      const mpi = parseFloat(row["__EMPTY_5"]); // MPI
      const ari = parseFloat(row["__EMPTY_11"]); // ARI
      const rgi = parseFloat(row["__EMPTY_17"]); // RGI

      if (!isNaN(mpi) && !isNaN(ari) && !isNaN(rgi)) {
        totalMPI += mpi;
        totalARI += ari;
        totalRGI += rgi;
        count++;
      }
    });

    const avgMPI = totalMPI / count;
    const avgARI = totalARI / count;
    const avgRGI = totalRGI / count;

    // 3. TRIGGER (COM-001)
    let triggerMet = avgMPI < 100 && avgARI > 100;

    if (!triggerMet) {
      return res.status(200).json({ message: "No issue detected" });
    }

    // 4. SCENARIO (simplified v1)
    let scenario = "Overpricing / Mix Issue";

    // 5. ROOT CAUSE
    let rootCause = "Rate positioning too aggressive vs perceived value";

    // 6. ACTIONS (multi-department)
    const actions = [
      {
        department: "Revenue",
        action_text: "Adjust pricing strategy on low MPI days (weekday focus)"
      },
      {
        department: "Marketing",
        action_text: "Launch targeted campaigns to stimulate short-lead demand"
      },
      {
        department: "Sales",
        action_text: "Activate corporate accounts to support base occupancy"
      }
    ];

    // 7. IMPACT (simple v1)
    const impactValue = Math.round((100 - avgMPI) * 100); // placeholder logic

    // 8. BUILD RECOMMENDATION
    const recommendation = {
      hotel_name: hotelCode,
      title: "MPI underperformance with strong ADR positioning",
      department: "Commercial",
      finding: "Hotel is priced above market but failing to capture demand",
      impact_value: impactValue,
      impact_type: "EUR",
      status: "open",
      period: "2026"
    };

    // 9. INSERT INTO SUPABASE
    const supabaseUrl = process.env.SUPABASE_URL;
    const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

    // Insert recommendation
    const recRes = await fetch(`${supabaseUrl}/rest/v1/Recommendations`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "apikey": supabaseKey,
        "Authorization": `Bearer ${supabaseKey}`
      },
      body: JSON.stringify(recommendation)
    });

    // Insert actions
    for (let action of actions) {
      await fetch(`${supabaseUrl}/rest/v1/actions`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "apikey": supabaseKey,
          "Authorization": `Bearer ${supabaseKey}`
        },
        body: JSON.stringify({
          hotel_name: hotelCode,
          action_text: action.action_text,
          status: "open",
          period: "2026"
        })
      });
    }

    res.status(200).json({
      success: true,
      avgMPI,
      avgARI,
      avgRGI
    });

  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message });
  }
}
