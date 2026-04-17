@echo off
title Dashboard MCMV - Inteligencia Competitiva
cd /d "%~dp0"
echo Abrindo dashboard...
python -m streamlit run dashboard/app.py
pause
