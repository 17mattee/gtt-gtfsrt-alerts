-- ============================================================================
-- GTT Torino GTFS-RT Enterprise Middleware — Schema Supabase / PostgreSQL
-- ============================================================================
-- Esegui tutto questo script nel SQL Editor di Supabase.
-- È IDEMPOTENTE: puoi rilanciarlo quante volte vuoi senza rompere nulla,
-- sia su un database vuoto sia sopra le tabelle già create in dashboard.
--
-- Tabelle:
--   gtt_routes             catalogo linee GTT (215, dal GTFS-Static ufficiale)
--   gtt_vehicles           mezzi noti, con ultimo avvistamento
--   gtt_vehicle_positions  time-series delle posizioni GPS reali
--   gtt_alerts_history     storico avvisi, deviazioni, ritardi e incidenti
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. CATALOGO LINEE
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.gtt_routes (
    route_id         TEXT PRIMARY KEY,
    route_short_name TEXT,
    route_long_name  TEXT,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE public.gtt_routes
    ALTER COLUMN updated_at SET DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_gtt_routes_short_name
    ON public.gtt_routes (route_short_name);

-- ---------------------------------------------------------------------------
-- 2. MEZZI DELLA FLOTTA
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.gtt_vehicles (
    vehicle_id TEXT PRIMARY KEY,
    agency_id  TEXT NOT NULL DEFAULT 'GTT',
    last_seen  TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE public.gtt_vehicles
    ALTER COLUMN agency_id SET DEFAULT 'GTT',
    ALTER COLUMN last_seen  SET DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_gtt_vehicles_last_seen
    ON public.gtt_vehicles (last_seen DESC);

-- ---------------------------------------------------------------------------
-- 3. POSIZIONI GPS (TIME-SERIES)
--    Una riga per ogni posizione di ogni mezzo, ad ogni ciclo di refresh.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.gtt_vehicle_positions (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    vehicle_id  TEXT        NOT NULL REFERENCES public.gtt_vehicles (vehicle_id) ON DELETE CASCADE,
    trip_id     TEXT,
    route_id    TEXT        REFERENCES public.gtt_routes (route_id) ON DELETE SET NULL,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    bearing     REAL,
    speed       REAL,
    "timestamp" BIGINT,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE public.gtt_vehicle_positions
    ALTER COLUMN recorded_at SET DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_gtt_positions_recorded_at
    ON public.gtt_vehicle_positions (recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_gtt_positions_vehicle_time
    ON public.gtt_vehicle_positions (vehicle_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_gtt_positions_route_time
    ON public.gtt_vehicle_positions (route_id, recorded_at DESC);

-- ---------------------------------------------------------------------------
-- 4. STORICO AVVISI, DEVIAZIONI, RITARDI E INCIDENTI
--    Unique su alert_id: necessario per l'upsert del middleware
--    (ON CONFLICT / on_conflict="alert_id").
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.gtt_alerts_history (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    alert_id    TEXT NOT NULL,
    title       TEXT,
    description TEXT,
    route_ids   TEXT[],
    cause       TEXT,
    effect      TEXT,
    category    TEXT NOT NULL DEFAULT 'informazione',
    start_time  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE public.gtt_alerts_history
    ALTER COLUMN created_at SET DEFAULT now();

-- Vincolo UNIQUE idempotente (richiesto dagli upsert del middleware)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uq_gtt_alerts_alert_id'
    ) THEN
        ALTER TABLE public.gtt_alerts_history
            ADD CONSTRAINT uq_gtt_alerts_alert_id UNIQUE (alert_id);
    END IF;
END $$;

-- Colonna category aggiuntiva (se la tabella esisteva già senza)
ALTER TABLE public.gtt_alerts_history
    ADD COLUMN IF NOT EXISTS category TEXT NOT NULL DEFAULT 'informazione';

CREATE INDEX IF NOT EXISTS idx_gtt_alerts_created_at
    ON public.gtt_alerts_history (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_gtt_alerts_category
    ON public.gtt_alerts_history (category, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_gtt_alerts_start_time
    ON public.gtt_alerts_history (start_time DESC);

-- ---------------------------------------------------------------------------
-- 5. VISTA UTILE: ultimo stato per linea (per la Control Room e le analisi)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.v_gtt_last_positions AS
SELECT DISTINCT ON (vehicle_id)
    vehicle_id,
    trip_id,
    route_id,
    latitude,
    longitude,
    speed,
    bearing,
    recorded_at
FROM public.gtt_vehicle_positions
ORDER BY vehicle_id, recorded_at DESC;

-- --------------------------------------------------------------------------- 
-- 6. STATO DI SISTEMA PERSISTENTE (anti-crash GTT)
--    Lo snapshot dell'ultimo sync buono (flotta, corse, avvisi) vive anche su
--    Supabase: il disco del piano free di Render e' effimero tra i deploy,
--    cosi' il cold start durante un outage GTT parte comunque con dati reali.
--    Il middleware lo aggiorna con la chiave 'last_good_snapshot' ogni
--    STATE_CLOUD_WRITE_SECONDS (default 300s), mai con flotta vuota.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.gtt_system_state (
    key      TEXT PRIMARY KEY,
    payload  JSONB NOT NULL,
    saved_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE public.gtt_system_state
    ALTER COLUMN saved_at SET DEFAULT now();

-- --------------------------------------------------------------------------- 
-- 7. RETENTION (opzionale, consigliato per le time-series):
--    cron giornaliero che cancella le posizioni più vecchie di 30 giorni.
--    Versione PRONTA e ATTIVA in sql/retention.sql (richiede pg_cron).
-- ---------------------------------------------------------------------------
