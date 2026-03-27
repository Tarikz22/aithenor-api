const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  throw new Error('Missing Supabase environment variables');
}

const supabase = createClient(supabaseUrl, supabaseKey);

async function handler(req, res) {
  try {
    const hotelCode = (req.query?.hotelCode || '').toString().trim();
    const snapshotDate = (req.query?.snapshot_date || '').toString().trim();

    if (!hotelCode) {
      return res.status(400).json({ error: 'hotelCode is required' });
    }

    let query = supabase
      .from('engine_outputs')
      .select('*')
      .eq('hotel_code', hotelCode)
      .order('generated_at', { ascending: false })
      .limit(1);

    if (snapshotDate) {
      query = query.eq('snapshot_date', snapshotDate);
    }

    const { data, error } = await query;

    if (error) throw error;

    if (!data || data.length === 0) {
      return res.status(404).json({ error: 'No engine output found' });
    }

    return res.status(200).json(data[0].engine_json);
  } catch (error) {
    console.error('Engine output handler error:', error);
    return res.status(500).json({
      error: error.message || 'Failed to fetch engine output'
    });
  }
}

module.exports = handler;
