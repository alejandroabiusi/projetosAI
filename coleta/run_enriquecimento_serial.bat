@echo off
chcp 65001 >nul
cd /d "C:\ProjetosAI\coleta"
set PYTHONIOENCODING=utf-8
set PYTHON=C:\Users\aleja\AppData\Local\Python\pythoncore-3.14-64\python.exe

echo ============================================================
echo ENRIQUECIMENTO SERIAL - %date% %time%
echo ============================================================

echo.
echo [1/8] Geocodificacao em massa...
echo %time% - Inicio geocodificacao >> logs\enriquecimento_serial.log
%PYTHON% -c "import sqlite3,requests,time,json,os,re;DB='data/empreendimentos.db';CACHE='data/geocode_cache.json';URL='https://nominatim.openstreetmap.org/search';H={'User-Agent':'Coleta-IM/1.0'};cache={};exec(open(CACHE).read()) if os.path.exists(CACHE) else None;conn=sqlite3.connect(DB);cur=conn.cursor();cur.execute(\"SELECT id,empresa,nome,endereco,cidade,estado,bairro FROM empreendimentos WHERE (latitude IS NULL OR latitude='') AND endereco IS NOT NULL AND endereco!='' AND cidade IS NOT NULL AND cidade!='' ORDER BY empresa\");recs=cur.fetchall();print(f'Geocodificando {len(recs)} registros...');gc=0;err=0;[None for _ in range(0)];done=set();[(lambda r: (setattr(__builtins__,'_tmp',None),))(r) for r in recs[:0]];i=0" 2>>logs\enriquecimento_serial.log
if errorlevel 1 echo   AVISO: geocodificacao inline falhou, rodando script separado...

echo %time% - Rodando geocoder separado...
%PYTHON% -c ^
"^
import sqlite3, requests, time, json, os, re^
DB='data/empreendimentos.db'^
CACHE='data/geocode_cache.json'^
URL='https://nominatim.openstreetmap.org/search'^
H={'User-Agent':'Coleta-IM/1.0 (coleta@projetosai.com)'}^
cache={}^
if os.path.exists(CACHE):^
    with open(CACHE,'r',encoding='utf-8') as f: cache=json.load(f)^
conn=sqlite3.connect(DB)^
cur=conn.cursor()^
cur.execute(\"\"\"SELECT id,empresa,nome,endereco,cidade,estado,bairro FROM empreendimentos WHERE (latitude IS NULL OR latitude='') AND endereco IS NOT NULL AND endereco!='' AND cidade IS NOT NULL AND cidade!='' ORDER BY empresa\"\"\")^
recs=cur.fetchall()^
print(f'Geocodificando {len(recs)} registros...')^
gc=0^
for i,(rid,emp,nome,end,cid,est,bai) in enumerate(recs):^
    parts=[end.split(',')[0].strip()]^
    if bai: parts.append(bai)^
    parts.extend([cid,est or 'SP','Brasil'])^
    q=', '.join(parts)^
    if q in cache:^
        r=cache[q]^
        if r:^
            cur.execute('UPDATE empreendimentos SET latitude=?,longitude=? WHERE id=?',(str(r[0]),str(r[1]),rid))^
            gc+=1^
        continue^
    try:^
        resp=requests.get(URL,params={'q':q,'format':'json','limit':1,'countrycodes':'br'},headers=H,timeout=15)^
        time.sleep(1.2)^
        if resp.status_code==200 and resp.json():^
            lat=float(resp.json()[0]['lat']);lon=float(resp.json()[0]['lon'])^
            if -35<lat<6 and -75<lon<-30:^
                cache[q]=(lat,lon)^
                cur.execute('UPDATE empreendimentos SET latitude=?,longitude=? WHERE id=?',(str(lat),str(lon),rid))^
                gc+=1^
            else: cache[q]=None^
        else: cache[q]=None^
    except: cache[q]=None^
    if (i+1)%%100==0:^
        conn.commit()^
        with open(CACHE,'w',encoding='utf-8') as f: json.dump(cache,f,ensure_ascii=False)^
        print(f'  {gc} geocodificados, {i+1}/{len(recs)} processados')^
conn.commit()^
with open(CACHE,'w',encoding='utf-8') as f: json.dump(cache,f,ensure_ascii=False)^
print(f'GEOCODING DONE: {gc} geocodificados de {len(recs)}')^
conn.close()^
"
echo %time% - Geocodificacao concluida >> logs\enriquecimento_serial.log

echo.
echo [2/8] Corrigir fases VIC/Smart...
echo %time% - Inicio correcao fases >> logs\enriquecimento_serial.log
%PYTHON% scrapers\corrigir_fases_vic.py 2>>logs\enriquecimento_serial.log
echo %time% - Correcao fases concluida >> logs\enriquecimento_serial.log

echo.
echo [3/8] Corrigir nomes...
echo %time% - Inicio correcao nomes >> logs\enriquecimento_serial.log
%PYTHON% corrigir_nomes.py 2>>logs\enriquecimento_serial.log
echo %time% - Correcao nomes concluida >> logs\enriquecimento_serial.log

echo.
echo [4/8] Enriquecer unidades (flags)...
echo %time% - Inicio flags >> logs\enriquecimento_serial.log
%PYTHON% enriquecer_unidades.py flags 2>>logs\enriquecimento_serial.log
%PYTHON% enriquecer_unidades.py varanda 2>>logs\enriquecimento_serial.log
echo %time% - Flags concluidas >> logs\enriquecimento_serial.log

echo.
echo [5/8] Qualificar produto (sem imagens)...
echo %time% - Inicio qualificar >> logs\enriquecimento_serial.log
%PYTHON% qualificar_produto.py --sem-imagens --reset-progresso 2>>logs\enriquecimento_serial.log
echo %time% - Qualificar concluido >> logs\enriquecimento_serial.log

echo.
echo [6/8] Validar coordenadas...
echo %time% - Inicio validar >> logs\enriquecimento_serial.log
%PYTHON% validar_coordenadas.py relatorio 2>>logs\enriquecimento_serial.log
echo %time% - Validar concluido >> logs\enriquecimento_serial.log

echo.
echo [7/8] Gerar mapa...
echo %time% - Inicio mapa >> logs\enriquecimento_serial.log
%PYTHON% gerar_mapa.py 2>>logs\enriquecimento_serial.log
echo %time% - Mapa concluido >> logs\enriquecimento_serial.log

echo.
echo [8/8] Auditoria final...
echo %time% - Inicio auditoria >> logs\enriquecimento_serial.log
%PYTHON% -c "import sqlite3;conn=sqlite3.connect('data/empreendimentos.db');cur=conn.cursor();cur.execute('SELECT COUNT(*) FROM empreendimentos');t=cur.fetchone()[0];cur.execute(\"SELECT COUNT(*) FROM empreendimentos WHERE latitude IS NOT NULL AND latitude!='' AND CAST(latitude AS REAL)!=0\");c=cur.fetchone()[0];cur.execute('SELECT COUNT(*) FROM empreendimentos WHERE fase IS NOT NULL');f=cur.fetchone()[0];cur.execute('SELECT COUNT(*) FROM empreendimentos WHERE cidade IS NOT NULL');ci=cur.fetchone()[0];print(f'AUDITORIA FINAL:');print(f'  Total: {t}');print(f'  Com coords: {c}/{t} ({100*c/t:.1f}%%)');print(f'  Com fase: {f}/{t} ({100*f/t:.1f}%%)');print(f'  Com cidade: {ci}/{t} ({100*ci/t:.1f}%%)');conn.close()"
echo %time% - Auditoria concluida >> logs\enriquecimento_serial.log

echo.
echo ============================================================
echo ENRIQUECIMENTO COMPLETO - %date% %time%
echo ============================================================
pause
