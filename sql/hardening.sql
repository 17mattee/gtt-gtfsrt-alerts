-- ============================================================================
-- GTT Middleware - HARDENING DI SICUREZZA Supabase (versione semplificata)
-- ============================================================================
-- ISTRUZIONI: apri l'SQL Editor, SVUOTALO COMPLETAMENTE (seleziona tutto e
-- cancella: residui di testo come una riga "x." mandano in errore l'esecuzione),
-- incolla INTERAMENTE questo script e premi Run. E' idempotente.
-- ============================================================================

-- 1) Vista: eseguita con i permessi di chi la interroga (non del creatore)
ALTER VIEW public.v_gtt_last_positions SET (security_invoker = true);

-- 2) Indice duplicato: elimina il doppione (resta idx_gtt_positions_recorded_at)
DROP INDEX IF EXISTS public.idx_vehicle_positions_time;

-- 3) Funzione pubblica: revoca l'esecuzione ad anon e authenticated.
--    Se questa singola istruzione desse "function does not exist", significa
--    che la funzione e' gia' stata rimossa: ignora l'errore e rilancia lo
--    script, oppure cancella questa istruzione.
REVOKE EXECUTE ON FUNCTION public.rls_auto_enable() FROM anon, authenticated;

-- 4) Row Level Security su tutte le tabelle: il middleware scrive con la
--    service_role key che BYPASSA RLS, quindi le sue scritture continuano;
--    anon e authenticated restano chiusi fuori.
ALTER TABLE public.gtt_routes ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.gtt_vehicles ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.gtt_vehicle_positions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.gtt_alerts_history ENABLE ROW LEVEL SECURITY;

-- 5) Policy di negazione esplicite per anon e authenticated
DROP POLICY IF EXISTS deny_anon_routes ON public.gtt_routes;
DROP POLICY IF EXISTS deny_anon_vehicles ON public.gtt_vehicles;
DROP POLICY IF EXISTS deny_anon_positions ON public.gtt_vehicle_positions;
DROP POLICY IF EXISTS deny_anon_alerts ON public.gtt_alerts_history;

CREATE POLICY deny_anon_routes ON public.gtt_routes
    FOR ALL TO anon, authenticated USING (false) WITH CHECK (false);
CREATE POLICY deny_anon_vehicles ON public.gtt_vehicles
    FOR ALL TO anon, authenticated USING (false) WITH CHECK (false);
CREATE POLICY deny_anon_positions ON public.gtt_vehicle_positions
    FOR ALL TO anon, authenticated USING (false) WITH CHECK (false);
CREATE POLICY deny_anon_alerts ON public.gtt_alerts_history
    FOR ALL TO anon, authenticated USING (false) WITH CHECK (false);
