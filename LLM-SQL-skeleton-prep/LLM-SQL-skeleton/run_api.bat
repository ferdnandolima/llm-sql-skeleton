@echo off
cd /d "%~dp0"

REM Abre o navegador depois de ~3s (simples)
start "" cmd /c "timeout /t 1 /nobreak >nul & start "" "http://localhost:8080/app""

".\.venv\Scripts\python.exe" -m uvicorn api.main:app --host 0.0.0.0 --port 8080 --reload
pause