-- ============================================================================
-- GTT Middleware — RETENTION AUTOMATICA (pg_cron)
-- ============================================================================
-- Incolla tutto nello SQL Editor di Supabase ed esegui. È IDEMPOTENTE:
-- può essere rilanciato quante volte vuoi.
--
-- Cancella ogni giorno alle 03:00 le posizioni GPS più vecchie di 7 giorni,
-- così la tabella gtt_vehicle_positions non cresce all'infinito.
--
-- NB: il middleware salva una posizione per mezzo ogni
-- POSITIONS_PERSIST_SECONDS (default 600s = 10 minuti): ~70k righe/giorno,
-- ~15 MB/giorno. Con retention a 7 giorni la tabella si assesta su
-- ~500k righe (~75-100 MB), ampiamente dentro il free tier (500 MB).
-- Con la vecchia cadenza (30s) erano ~1,4 MILIONI di righe al giorno
-- (~300 MB/giorno): il database si saturava in circa 36 ore.
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

-- Pulizia immediata delle righe oltre il periodo di retention (una tantum,
-- utile se la tabella e' gia' cresciuta con la vecchia cadenza di scrittura)
DELETE FROM public.gtt_vehicle_positions
WHERE recorded_at < now() - INTERVAL '7 days';

SELECT cron.schedule(
    'gtt-positions-retention',
    '0 3 * * *',
    $$DELETE FROM public.gtt_vehicle_positions
      WHERE recorded_at < now() - INTERVAL '7 days'$$
);
