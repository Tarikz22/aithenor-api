const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

module.exports = async function analyzeHandler(req, res) {
  try {
    const hotelName =
      req.body.hotel_name ||
      req.body.hotelName ||
      req.body.hotelCode ||
      "UNKNOWN-HOTEL";

    const normalizedHotelName = String(hotelName).trim().toUpperCase();

    // Optional cleanup of previous temporary imported rows for this hotel/period
    await supabase
      .from("Recommendations")
      .delete()
      .eq("hotel_name", normalizedHotelName)
      .eq("period", "2026")
      .eq("title", "Imported finding");

    // 1) Insert one main recommendation
    const { data: recommendation, error: recError } = await supabase
      .from("Recommendations")
      .insert([
        {
          hotel_name: normalizedHotelName,
          title: "Weekend RevPAR underperformance vs comp set",
          department: "Commercial",
          finding:
            "ADR premium is not converting into occupancy, which suggests weak demand capture on peak days versus competitors.",
          hotel_id: normalizedHotelName,
          impact_value: 85000,
          impact_type: "SAR",
          is_repeat: false,
          expected_impact_value: 85000,
          status: "open",
          period: "2026"
        }
      ])
      .select()
      .single();

    if (recError) {
      throw recError;
    }

    // 2) Insert linked actions
    const actionsPayload = [
      {
        hotel_name: normalizedHotelName,
        title: "Revenue action",
        action_text:
          "Adjust weekend pricing strategy and test a narrower ADR premium against the comp set.",
        hotel_id: normalizedHotelName,
        expected_impact_value: 30000,
        status: "open",
        period: "2026"
      },
      {
        hotel_name: normalizedHotelName,
        title: "Sales action",
        action_text:
          "Push short-lead transient and local demand opportunities for Friday-Saturday need periods.",
        hotel_id: normalizedHotelName,
        expected_impact_value: 25000,
        status: "open",
        period: "2026"
      },
      {
        hotel_name: normalizedHotelName,
        title: "Marketing action",
        action_text:
          "Activate targeted weekend campaign support on high-conversion channels to improve demand capture.",
        hotel_id: normalizedHotelName,
        expected_impact_value: 30000,
        status: "open",
        period: "2026"
      }
    ];

    const { data: actions, error: actError } = await supabase
      .from("actions")
      .insert(actionsPayload)
      .select();

    if (actError) {
      throw actError;
    }

    return res.status(200).json({
      success: true,
      message: "Aithenor Brain v1 completed successfully",
      recommendationInserted: recommendation,
      actionsInserted: actions?.length || 0
    });
  } catch (error) {
    console.error("Analyze error:", error);
    return res.status(500).json({
      success: false,
      error: error.message || "Unknown analyze error"
    });
  }
};
