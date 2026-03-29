-- Slim PMS pace snapshot history for same-lead / pickup analysis across uploads.
-- Run in Supabase SQL editor or via migration tooling.
-- Service role (used by analyze.js) bypasses RLS; enable RLS + policies if exposing via anon key.

CREATE TABLE IF NOT EXISTS public.pms_pace_snapshots (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  hotel_code text NOT NULL,
  snapshot_date date NOT NULL,
  stay_date_ymd date NOT NULL,
  stay_week_key text,
  stay_week_start_ymd date,
  stay_week_end_ymd date,
  market_segment_label text NOT NULL DEFAULT '',
  source_row_index integer NOT NULL DEFAULT 0,
  row_phase text,
  future_window_class text,
  lead_days_snapshot_to_stay integer,
  rn_on_books_ty numeric,
  rn_ly_actual numeric,
  rn_stly numeric,
  booked_revenue_ty numeric,
  booked_revenue_ly_actual numeric,
  booked_revenue_stly numeric,
  forecast_room_nights_ty numeric,
  forecast_revenue_ty numeric,
  adr_ty numeric,
  adr_ly_actual numeric,
  adr_stly numeric,
  ready_ty_stly_rn boolean NOT NULL DEFAULT false,
  ready_ty_stly_rev boolean NOT NULL DEFAULT false,
  weekly_rollup_ready boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT pms_pace_snapshots_line_uniq UNIQUE (
    hotel_code,
    snapshot_date,
    stay_date_ymd,
    market_segment_label,
    source_row_index
  )
);

CREATE INDEX IF NOT EXISTS pms_pace_snapshots_hotel_stay_snap_idx
  ON public.pms_pace_snapshots (hotel_code, stay_date_ymd, snapshot_date DESC);

CREATE INDEX IF NOT EXISTS pms_pace_snapshots_hotel_week_snap_idx
  ON public.pms_pace_snapshots (hotel_code, stay_week_key, snapshot_date DESC);

COMMENT ON TABLE public.pms_pace_snapshots IS
  'One row per PMS pace line per upload snapshot; upsert key disambiguates segment + source row index.';
