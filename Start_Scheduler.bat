@echo off
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo ERROR: .venv not found. Run your python venv setup once.
  pause
  exit /b 1
)

call .venv\Scripts\activate.bat

REM Ensure UI deps exist (scheduler is in ui extra)
python -m pip install -e ".[ui]" >nul

python -m mfp.reporting.scheduler_service
endlocal
