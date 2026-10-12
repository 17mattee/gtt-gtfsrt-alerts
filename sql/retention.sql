-- ============================================================================
-- GTT Middleware — RETENTION AUTOMATICA (pg_cron)
-- ============================================================================
-- Incolla tutto nello SQL Editor di Supabase ed esegui. È IDEMPOTENTE:
-- può essere rilanciato quante volte vuoi.
--
-- Cancella ogni giorno alle 03:00 le posizioni GPS più vecchie di 30 giorni,
-- così la tabella gtt_vehicle_positions non cresce all'infinito.
--
-- Requisito: estensione pg_cron (attivabile in Supabase:
-- Database > Extensions > pg_cron > Enable).
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Rimuove eventuali schedule precedenti con lo stesso nome (idempotente)
SELECT cron.unschedule('gtt-positions-retention')
WHERE EXISTS (
    SELECT 1 FROM cron.job WHERE jobname = 'gtt-positions-retention'
);

SELECT cron.schedule(
    'gtt-positions-retention',
    '0 3 * * *',
    $$DELETE FROM public.gtt_vehicle_positions
      WHERE recorded_at < now() - INTERVAL '30 days'$$
);
