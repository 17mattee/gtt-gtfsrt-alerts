-- ============================================================================
-- GTT Middleware — HARDENING DI SICUREZZA Supabase
-- ============================================================================
-- Incolla tutto nello SQL Editor di Supabase ed esegui. E' IDEMPOTENTE:
-- puo' essere rilanciato quante volte vuoi.
--
-- Risolve gli avvisi del Security Advisor:
--   1. Vista v_gtt_last_positions con SECURITY DEFINER  ->  security_invoker
--   2. Indice duplicato su gtt_vehicle_positions          ->  drop del doppione
--   3. Funzione rls_auto_enable() eseguibile da anon/auth ->  revoke EXECUTE
--   4. Tabelle senza Row Level Security                   ->  RLS attivo
--
-- NB su RLS: il middleware scrive nel database usando la SERVICE ROLE KEY,
-- che BYPASSA completamente RLS: attivare RLS senza policy NON blocca le
-- scritture del middleware, ma chiude l'accesso a chiunque altro (anon e
-- authenticated non hanno policy -> accesso negato a tutto).
-- Se dopo questo script la persistenza si bloccasse, significa che la tua
-- variabile SUPABASE_KEY contiene la anon key invece della service_role:
-- sostituiscila in Render (Settings > Environment) con la service_role key.
-- ============================================================================

-- 1) VISTA: eseguita con i permessi di chi la interroga (non del creatore)
ALTER VIEW public.v_gtt_last_positions SET (security_invoker = true);

-- 2) INDICE DUPLICATO: elimina il doppione (resta idx_gtt_positions_recorded_at)
DROP INDEX IF EXISTS public.idx_vehicle_positions_time;

-- 3) FUNZIONE PUBBLICA: revoca l'esecuzione a chiunque non sia il proprietario
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public' AND p.proname = 'rls_auto_enable'
    ) THEN
        REVOKE EXECUTE ON FUNCTION public.rls_auto_enable() FROM PUBLIC, anon, authenticated;
    END IF;
END $$;

-- 4) ROW LEVEL SECURITY su tutte le tabelle: accesso negato a anon/authenticated
--    (il middleware usa la service_role key che bypassa RLS)
ALTER TABLE public.gtt_routes ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.gtt_vehicles ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.gtt_vehicle_positions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.gtt_alerts_history ENABLE ROW LEVEL SECURITY;

-- Policy esplicite di negazione per chiarezza (nessuna policy = gia' negato,
-- queste lo rendono esplicito e sopravvivono anche se qualcuno aggiunge policy)
DROP POLICY IF EXISTS "deny_anon_routes" ON public.gtt_routes;
DROP POLICY IF EXISTS "deny_anon_vehicles" ON public.gtt_vehicles;
DROP POLICY IF EXISTS "deny_anon_positions" ON public.gtt_vehicle_positions;
DROP POLICY IF EXISTS "deny_anon_alerts" ON public.gtt_alerts_history;

CREATE POLICY "deny_anon_routes" ON public.gtt_routes
    FOR ALL TO anon, authenticated USING (false) WITH CHECK (false);
CREATE POLICY "deny_anon_vehicles" ON public.gtt_vehicles
    FOR ALL TO anon, authenticated USING (false) WITH CHECK (false);
CREATE POLICY "deny_anon_positions" ON public.gtt_vehicle_positions
    FOR ALL TO anon, authenticated USING (false) WITH CHECK (false);
CREATE POLICY "deny_anon_alerts" ON public.gtt_alerts_history
    FOR ALL TO anon, authenticated USING (false) WITH CHECK (false);
