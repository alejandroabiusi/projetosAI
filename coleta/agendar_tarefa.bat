@echo off
cd /d C:\Users\aabiusi\ProjetosAI\coleta
if not exist logs mkdir logs
python run_atualizacao.py >> logs\scheduler_%date:~-4%%date:~3,2%%date:~0,2%.log 2>&1
