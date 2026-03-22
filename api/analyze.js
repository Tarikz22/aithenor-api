import { createClient } from '@supabase/supabase-js';
import Anthropic from '@anthropic-ai/sdk';
import * as XLSX from 'xlsx';

// --------------------
// INIT
// --------------------
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  throw new Error("Missing Supabase environment variables");
}

const supabase = createClient(supabaseUrl, supabaseKey);

const anthropic = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY,
});

// --------------------
// LIBRARY (v3.2 CORE)
// --------------------
const library = {
  Retail: {
    pricing_positioning: {
      root_causes: [
        "Price positioning above comp set without corresponding demand support or share performance",
        "Pricing approach not fully aligned with current demand elasticity and booking pace patterns",
        "Rate strategy prioritizing ADR stability over occupancy penetration and market share capture",
        "Yield management not fully optimized across peak and need periods, limiting compression benefits",
        "Pricing decisions not consistently aligned with demand cycles, reflecting gaps in demand anticipation"
      ],
      actions: [
        "Adjust pricing corridors dynamically across need and compression periods to improve share capture",
        "Recalibrate rate positioning against key competitors across booking windows and demand levels",
        "Optimize suite and room category pricing hierarchy to strengthen overall revenue contribution"
      ]
    },

    visibility_demand_capture: {
      root_causes: [
        "Limited visibility across key distribution channels impacting overall demand capture",
        "Digital presence not fully optimized to generate qualified traffic and brand exposure",
        "Brand.com content and offer presentation not sufficiently compelling to drive direct demand"
      ],
      actions: [
        "Strengthen OTA positioning and visibility across high-impact booking windows",
        "Enhance digital marketing effectiveness through targeted campaigns and optimized budget allocation",
        "Improve Brand.com content, storytelling, and offer visibility to support direct channel performance"
      ]
    },

    conversion_channel_performance: {
      root_causes: [
        "Conversion performance below potential across key distribution channels despite available demand",
        "Booking journey friction impacting user experience and limiting conversion efficiency"
      ],
      actions: [
        "Optimize booking engine and website journey to reduce drop-off and improve conversion rates",
        "Conduct structured channel performance reviews to identify and address conversion gaps"
      ]
    },

commercial_strategy_mix: {
  root_causes: [
    "Current segment mix is not aligned with revenue optimization opportunities"
  ],
  actions: [
    "Rebalance segment mix toward higher contribution segments",
    "Refine commercial strategy to align with demand patterns and profitability drivers",
    "Conduct structured segment performance reviews to identify optimization opportunities"
  ]
}
  }
};

// --------------------
// HELPER FUNCTIONS
// --------------------
function getRandomItem(array) {
  return array[Math.floor(Math.random() * array.length)];
}

function getMultipleRandomItems(array, count = 2) {
  const shuffled = [...array].sort(() => 0.5 - Math.random());
  return shuffled.slice(0, count);
}

// --------------------
// CLAUDE FUNCTION
// --------------------
async function composeExecutiveNarrative(input) {
  try {
    const response = await anthropic.messages.create({
      model: "claude-3-sonnet-20240229",
      max_tokens: 500,
      messages: [
        {
          role: "user",
          content: `
You are a senior hotel commercial strategist.

Rewrite the following into an executive-level recommendation.

Constraints:
- Be concise
- Be specific
- Reference KPIs when relevant
- Keep a strategic tone

INPUT:
Driver: ${input.driver}
Segment: ${input.segment}

Root Cause:
${input.rootCauseText}

Actions:
${input.actions.join("\n")}

Signals:
Mix: ${input.mixSignal}
Targets: ${input.targetSignal}

OUTPUT FORMAT:
Title:
Root Cause:
Actions:
Expected Outcome:
`
        }
      ]
    });

    return response.content[0].text;

  } catch (error) {
    console.error("Claude error:", error);
    return null;
  }
}

// --------------------
// MAIN HANDLER
// --------------------
async function handler(req, res) {
  try {
    const file = req.files.file;
    const workbook = XLSX.read(file.data);

    // --------------------
    // BASIC KPI EXTRACTION (SIMPLIFIED)
    // --------------------
    const strSheet = XLSX.utils.sheet_to_json(workbook.Sheets["STR"]);

    const avgRGI = strSheet.reduce((acc, row) => acc + (row.RGI || 0), 0) / strSheet.length;
    const avgARI = strSheet.reduce((acc, row) => acc + (row.ARI || 0), 0) / strSheet.length;

    let driverCategory = "pricing_positioning";

    if (avgRGI < 100 && avgARI > 100) {
      driverCategory = "pricing_positioning";
    } else if (avgRGI < 100 && avgARI < 100) {
      driverCategory = "visibility_demand_capture";
    } else if (avgRGI >= 100 && avgARI < 100) {
      driverCategory = "conversion_channel_performance";
    }

    const segmentFocus = "Retail";

    // --------------------
    // LIBRARY SELECTION
    // --------------------
    const block = library[segmentFocus][driverCategory];

let rootCauseText;
let actions;

if (driverCategory === "commercial_strategy_mix") {
  // Action-driven logic
  rootCauseText = block.root_causes[0]; // fixed context
  actions = getMultipleRandomItems(block.actions, 2);

} else {
  // Standard logic
  rootCauseText = getRandomItem(block.root_causes);
  actions = getMultipleRandomItems(block.actions, 2);
}

    console.log("v3.2 root cause:", rootCauseText);
    console.log("v3.2 actions:", actions);

    // --------------------
    // CLAUDE ENRICHMENT
    // --------------------
    const aiResponse = await composeExecutiveNarrative({
      rootCauseText,
      actions,
      driver: driverCategory,
      segment: segmentFocus,
      mixSignal: "balanced",
      targetSignal: "below target"
    });

    let title = "Revenue Opportunity Identified";
    let finalRootCause = rootCauseText;
    let finalActions = actions;
    let expectedOutcome = "Improve performance vs comp set";

    if (aiResponse) {
      const parts = aiResponse.split("\n");

      title = parts.find(p => p.startsWith("Title:"))?.replace("Title:", "").trim() || title;
      finalRootCause = parts.find(p => p.startsWith("Root Cause:"))?.replace("Root Cause:", "").trim() || rootCauseText;
      expectedOutcome = parts.find(p => p.startsWith("Expected Outcome:"))?.replace("Expected Outcome:", "").trim() || expectedOutcome;

      finalActions = parts
        .filter(p => p.startsWith("-"))
        .map(a => a.replace("-", "").trim());

      if (finalActions.length === 0) finalActions = actions;
    }

    // --------------------
    // SAVE TO SUPABASE
    // --------------------
    await supabase.from("Recommendations").insert([
      {
        hotel_name: "Demo Hotel",
        period: "Test",
        title,
        diagnosis: driverCategory,
        root_cause: finalRootCause,
        expected_outcome: expectedOutcome,
      }
    ]);

    for (const action of finalActions) {
      await supabase.from("Actions").insert([
        {
          hotel_name: "Demo Hotel",
          period: "Test",
          title,
          action
        }
      ]);
    }

    res.status(200).json({
      message: "v3.2 completed",
      driver: driverCategory
    });

  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Processing failed" });
  }
}
module.exports = handler;
