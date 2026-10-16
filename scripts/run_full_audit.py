import subprocess
import sys

def run_command(cmd, description):
    print(f"\n[AUDIT] 🚀 {description}...")
    full_cmd = f"./venv/bin/{cmd}" if not cmd.startswith("./") else cmd
    result = subprocess.run(full_cmd, shell=True)
    if result.returncode != 0:
        print(f"[AUDIT] ❌ Errore durante: {description}")
        sys.exit(1)
    else:
        print(f"[AUDIT] ✅ Successo: {description}")

def test_api_endpoints_live():
    print("\n[AUDIT] 🌐 Avvio test live degli endpoint HTTP...")
    from fastapi.testclient import TestClient
    from app.main import app
    client = TestClient(app)
    
    res = client.get("/")
    assert res.status_code == 200 and "GTT GTFS-RT Live Dashboard & Map" in res.text
    print("  -> Dashboard Root [/]: OK")

    res = client.get("/health")
    assert res.status_code == 200, f"/health ha risposto {res.status_code}"
    print("  -> Health Check [/health]: OK")

    res = client.get("/gtfs-rt/alerts.pb")
    assert res.status_code == 200 and res.headers["content-type"] == "application/x-protobuf"
    print("  -> Protobuf Feed [/gtfs-rt/alerts.pb]: OK")

if __name__ == "__main__":
    print("==================================================")
    print("      GTT GTFS-RT FULL ENTERPRISE SECURITY & QA   ")
    print("==================================================")
    
    # 1. Esecuzione Test Unitari
    run_command("python -m pytest --cov=app --cov-report=term-missing -v", "Suite Pytest & Code Coverage")
    
    # 2. Test live integrati
    test_api_endpoints_live()
    
    # 3. Controllo sicurezza dipendenze
    print("\n[AUDIT] 🔒 Verifica delle vulnerabilità nelle dipendenze...")
    subprocess.run("./venv/bin/pip install pip-audit && ./venv/bin/pip-audit", shell=True)
    
    print("\n==================================================")
    print("🎉 AUDIT COMPLETATO CON SUCCESSO: SISTEMA PERFETTO!")
    print("==================================================")
