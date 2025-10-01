@echo off
set PYTHONUTF8=1
uvicorn api.main:app --host 0.0.0.0 --port 8080 --reload
