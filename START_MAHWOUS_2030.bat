@echo off
REM Save this file as UTF-8 without BOM or ANSI — BOM breaks cmd.exe (first line not recognized).
color 0A
echo ==========================================================
echo       Mahwous 2030 Intelligence System - Auto Launcher
echo ==========================================================
echo.
echo Installing any missing libraries...
pip install -r requirements.txt >nul 2>nul
echo Starting Mahwous 2030 Application...
streamlit run app.py
pause
