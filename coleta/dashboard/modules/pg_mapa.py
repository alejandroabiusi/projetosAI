"""Módulo Mapa — Gera HTML no mesmo formato do gerar_mapa.py,
com dados filtrados pelo header. Sem painéis de empresa/status
(já filtrados no header), apenas MPR clusters + resumo."""
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import json
import os
import sys
import glob
import re
import xml.etree.ElementTree as ET

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dashboard.config import CORES

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")

REGIOES = {
    "SP": "São Paulo", "RMSP": "RM de São Paulo", "RJ": "Rio de Janeiro",
    "MG": "Minas Gerais", "PR": "Paraná", "RS": "Rio Grande do Sul",
    "BA": "Bahia", "PE": "Pernambuco", "CE": "Ceará", "GO": "Goiás",
    "JP": "João Pessoa", "CPS": "Campinas",
}


@st.cache_data(ttl=600)
def _load_kml_clusters():
    ns = {'kml': 'http://www.opengis.net/kml/2.2'}
    all_clusters = []
    for kml_path in sorted(glob.glob(os.path.join(DATA_DIR, "*.kml"))):
        nome = os.path.basename(kml_path)
        prefix = 'SP' if 'Clusteres MPR SP' in nome else (re.search(r'MPR_-_(\w+)\.kml', nome) or type('',(),{'group':lambda s,x:'X'})()).group(1)
        tree = ET.parse(kml_path)
        root = tree.getroot()
        sm2n = {}
        for sm in root.findall('.//kml:StyleMap', ns):
            for pair in sm.findall('kml:Pair', ns):
                if pair.find('kml:key', ns).text == 'normal':
                    sm2n[sm.get('id')] = pair.find('kml:styleUrl', ns).text.lstrip('#')
        sc = {}
        for s in root.findall('.//kml:Style', ns):
            p = s.find('kml:PolyStyle/kml:color', ns)
            if p is not None:
                c = p.text
                sc[s.get('id')] = f'#{c[6:8]}{c[4:6]}{c[2:4]}'
        for idx, pm in enumerate(root.findall('.//kml:Placemark', ns), 1):
            nm = re.search(r'\d+', pm.find('kml:name', ns).text or '')
            num = int(nm.group()) if nm else idx
            sr = pm.find('kml:styleUrl', ns).text.lstrip('#')
            color = sc.get(sm2n.get(sr, sr), '#ff0000')
            polys = []
            for poly in pm.findall('.//kml:Polygon', ns):
                ring = [[float(pt.split(',')[1]), float(pt.split(',')[0])] for pt in poly.find('.//kml:coordinates', ns).text.strip().split()]
                polys.append(ring)
            all_clusters.append({'name': f'{prefix}{num}', 'region': prefix, 'color': color, 'polygons': polys})
    return all_clusters


def _safe(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return str(val)


def _gerar_html(df):
    df_map = df.copy()
    df_map["latitude"] = pd.to_numeric(df_map["latitude"], errors="coerce")
    df_map["longitude"] = pd.to_numeric(df_map["longitude"], errors="coerce")
    df_map = df_map.dropna(subset=["latitude", "longitude"])
    df_map = df_map[(df_map["latitude"] != 0) & (df_map["longitude"] != 0)]

    points = []
    for _, r in df_map.iterrows():
        lat, lng = r["latitude"], r["longitude"]
        if not (-35 < lat < 6 and -75 < lng < -30):
            continue
        points.append({
            "n": _safe(r.get("nome")), "e": _safe(r.get("empresa")),
            "c": _safe(r.get("cidade")), "uf": _safe(r.get("estado")),
            "lat": round(lat, 6), "lng": round(lng, 6),
            "f": _safe(r.get("fase")) or "Sem info",
            "p": r["preco_a_partir"] if pd.notna(r.get("preco_a_partir")) else None,
            "end": _safe(r.get("endereco")),
            "un": int(r["total_unidades"]) if pd.notna(r.get("total_unidades")) else None,
            "url": _safe(r.get("url_fonte")),
            "cl": _safe(r.get("cluster_mpr")) or None,
        })

    kml_clusters = _load_kml_clusters()
    n_empresas = len(set(p["e"] for p in points))

    pj = json.dumps(points, ensure_ascii=False)
    cj = json.dumps(CORES, ensure_ascii=False)
    clj = json.dumps(kml_clusters, ensure_ascii=False)
    rj = json.dumps({r: REGIOES.get(r, r) for r in sorted(set(cl["region"] for cl in kml_clusters))}, ensure_ascii=False)

    # The HTML template with ONLY MPR cluster filter + summary panel (no empresa/status filters)
    html = _MAP_TEMPLATE.replace("__POINTS__", pj).replace("__CORES__", cj).replace("__CLUSTERS__", clj).replace("__REGIOES__", rj)
    return html, len(points), n_empresas


_MAP_TEMPLATE = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
html,body{width:100%;height:100%;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif}
#map{width:100%;height:100%}
.fp{position:absolute;top:10px;right:12px;z-index:1000;background:rgba(255,255,255,.95);padding:10px 14px;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.2);font-size:12px;max-height:80vh;overflow-y:auto}
.fp label{display:block;margin:3px 0;cursor:pointer}
.ct{display:flex;align-items:center;gap:6px;margin-bottom:4px;cursor:pointer}
.ct input{margin:0}
.ct-label{font-weight:600;font-size:12px;color:#2c3e50}
#cd{display:none}
.rh{display:flex;align-items:center;margin:4px 0 2px;cursor:pointer;font-weight:600;font-size:12px;color:#2c3e50}
.rh:hover{color:#2980b9}
.rn{flex:1}
.ra{display:inline-block;font-size:9px;margin-right:4px;transition:transform .15s}
.ra.open{transform:rotate(90deg)}
.rb{display:flex;gap:4px}
.rz{font-size:10px;color:#3498db;padding:1px 6px;border:1px solid #3498db;border-radius:3px;width:40px;text-align:center;cursor:pointer}
.rz:hover{background:#3498db;color:white}
.rc{margin-left:16px;display:none}
.rc.open{display:block}
.pt{font-weight:bold;font-size:14px;color:#2c3e50;margin-bottom:4px}
.pe{display:inline-block;padding:2px 8px;border-radius:10px;color:white;font-size:11px;margin-bottom:4px}
.pi{color:#555;font-size:12px}
.pl{display:inline-block;margin-top:6px;padding:4px 12px;background:#3498db;color:white;text-decoration:none;border-radius:4px;font-size:11px;font-weight:600}
.pl:hover{background:#2980b9}
.sd{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:4px;vertical-align:middle}
</style>
</head>
<body>
<div class="fp" id="filters">
  <label class="ct"><input type="checkbox" id="cmt"><span class="ct-label">Filtrar por Cluster MPR</span></label>
  <div id="cd"><div id="ccb"></div></div>
</div>
<div id="map"></div>
<script>
const P=__POINTS__,C=__CORES__,MC=__CLUSTERS__,R=__REGIOES__;
const map=L.map('map',{zoomControl:true}).setView([-15.8,-47.9],5);
L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png',{attribution:'&copy; OSM &copy; CARTO',maxZoom:19,subdomains:'abcd'}).addTo(map);
const AM=[],AC=new Set(),SC='Fora de cluster';
let CFE=false;const CCB={},CC={};
function ci(c){return L.divIcon({html:'<div style="background:'+c+';width:12px;height:12px;border-radius:50%;border:2px solid white;box-shadow:0 1px 3px rgba(0,0,0,.4)"></div>',className:'',iconSize:[12,12],iconAnchor:[6,6],popupAnchor:[0,-8]})}
P.forEach(p=>{const c=C[p.e]||'#999',m=L.marker([p.lat,p.lng],{icon:ci(c)}),cl=p.cl||SC;
let i='<div class="pt">'+p.n+'</div><span class="pe" style="background:'+c+'">'+p.e+'</span>';
if(p.cl)i+=' <span style="font-size:11px;color:#666;font-weight:600">'+p.cl+'</span>';
i+='<br><span class="pi">'+p.c+(p.uf?'/'+p.uf:'')+'</span>';
if(p.f&&p.f!=='Sem info')i+='<br><span class="pi">'+p.f+'</span>';
if(p.un)i+='<br><span class="pi">'+p.un+' unidades</span>';
if(p.end)i+='<br><span class="pi" style="font-size:11px;color:#888">'+p.end+'</span>';
if(p.url)i+='<br><a class="pl" href="'+p.url+'" target="_blank">Ver no site</a>';
m.bindPopup(i);m._e=p.e;m._c=cl;m._u=p.un||0;m._lat=p.lat;m._lng=p.lng;
AM.push(m);map.addLayer(m);CC[cl]=(CC[cl]||0)+1;AC.add(cl)});
function mv(m){return!CFE||AC.has(m._c)}
function af(){AM.forEach(m=>{if(mv(m)){if(!map.hasLayer(m))map.addLayer(m)}else map.removeLayer(m)});setTimeout(us,100)}
if(MC.length>0){const fp=document.getElementById('ccb'),ccl={},cb={},rb={};
MC.forEach(cl=>{ccl[cl.name]=cl.color;const ls=[],gs=[];cl.polygons.forEach(r=>r.forEach(p=>{ls.push(p[0]);gs.push(p[1])}));cb[cl.name]=[[Math.min(...ls),Math.min(...gs)],[Math.max(...ls),Math.max(...gs)]]});
MC.forEach(cl=>{const r=cl.region,b=cb[cl.name];if(!rb[r])rb[r]=[[b[0][0],b[0][1]],[b[1][0],b[1][1]]];else{rb[r][0][0]=Math.min(rb[r][0][0],b[0][0]);rb[r][0][1]=Math.min(rb[r][0][1],b[0][1]);rb[r][1][0]=Math.max(rb[r][1][0],b[1][0]);rb[r][1][1]=Math.max(rb[r][1][1],b[1][1])}});
const cr={};Object.keys(CC).forEach(cl=>{if(cl===SC)return;const r=MC.find(c=>c.name===cl)?.region||'?';if(!cr[r])cr[r]=[];cr[r].push(cl)});Object.values(cr).forEach(a=>a.sort());
const clg={};MC.forEach(cl=>{const lg=L.layerGroup();cl.polygons.forEach(r=>{L.polygon(r,{color:cl.color,weight:2,opacity:.7,fillColor:cl.color,fillOpacity:.2,interactive:false}).addTo(lg)});clg[cl.name]=lg});
Object.keys(cr).sort().forEach(region=>{const rl=R[region]||region,h=document.createElement('div');h.className='rh';
const ar=document.createElement('span');ar.className='ra';ar.textContent='\u25B6';h.appendChild(ar);
const nm=document.createElement('span');nm.className='rn';nm.textContent=rl;h.appendChild(nm);
const bs=document.createElement('span');bs.className='rb';
const tb=document.createElement('span');tb.className='rz';tb.textContent='todos';tb._a=true;
tb.addEventListener('click',e=>{e.stopPropagation();tb._a=!tb._a;cr[region].forEach(cl=>{const o=CCB[cl];if(o){o.cb.checked=tb._a;if(tb._a)AC.add(cl);else AC.delete(cl)}const lg=clg[cl];if(CFE&&lg){if(tb._a)lg.addTo(map);else map.removeLayer(lg)}});af()});bs.appendChild(tb);
const zb=document.createElement('span');zb.className='rz';zb.textContent='zoom';zb.addEventListener('click',e=>{e.stopPropagation();if(rb[region])map.fitBounds(rb[region],{padding:[40,40]})});bs.appendChild(zb);h.appendChild(bs);
const cn=document.createElement('div');cn.className='rc';h.addEventListener('click',()=>{cn.classList.toggle('open');ar.classList.toggle('open')});fp.appendChild(h);
cr[region].forEach(cl=>{const lb=document.createElement('label'),cb2=document.createElement('input');cb2.type='checkbox';cb2.checked=true;cb2.style.marginRight='4px';
cb2.addEventListener('change',()=>{if(cb2.checked)AC.add(cl);else AC.delete(cl);const lg=clg[cl];if(CFE&&lg){if(cb2.checked)lg.addTo(map);else map.removeLayer(lg)}af()});
const dt=document.createElement('span');dt.style.cssText='display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:4px;background:'+(ccl[cl]||'#999');
const cs=document.createElement('span');cs.textContent=' '+cl+' ('+(CC[cl]||0)+')';lb.appendChild(cb2);lb.appendChild(dt);lb.appendChild(cs);cn.appendChild(lb);CCB[cl]={cb:cb2,label:lb,countSpan:cs}});fp.appendChild(cn)});
if(CC[SC]){const lb=document.createElement('label');lb.style.marginTop='4px';const cb2=document.createElement('input');cb2.type='checkbox';cb2.checked=true;cb2.style.marginRight='4px';cb2.addEventListener('change',()=>{if(cb2.checked)AC.add(SC);else AC.delete(SC);af()});const cs=document.createElement('span');cs.textContent=' '+SC+' ('+CC[SC]+')';lb.appendChild(cb2);lb.appendChild(cs);fp.appendChild(lb);CCB[SC]={cb:cb2,label:lb,countSpan:cs}}
const mt=document.getElementById('cmt'),cd=document.getElementById('cd');mt.addEventListener('change',()=>{CFE=mt.checked;cd.style.display=CFE?'block':'none';if(CFE){Object.entries(CCB).forEach(([cl,o])=>{if(cl===SC)return;const lg=clg[cl];if(lg){if(o.cb.checked)lg.addTo(map);else map.removeLayer(lg)}})}else Object.values(clg).forEach(lg=>map.removeLayer(lg));af()})}
if(AM.length>0){const b=L.latLngBounds(AM.map(m=>[m._lat,m._lng]));map.fitBounds(b.pad(.1))}
setTimeout(()=>{map.invalidateSize()},100);
</script>
</body>
</html>"""


def render(df, filtros):
    html, n_points, n_empresas = _gerar_html(df)
    components.html(html, height=680, scrolling=False)

    # Tabela resumo abaixo do mapa (Streamlit nativo)
    st.divider()
    df_map = df.copy()
    df_map["latitude"] = pd.to_numeric(df_map["latitude"], errors="coerce")
    df_map = df_map[df_map["latitude"].notna() & (df_map["latitude"] != 0)]

    if not df_map.empty:
        # === TABELA 1: Resumo por empresa ===
        st.subheader("Resumo por empresa")
        tem_filtro_geo = bool(filtros.get("regionais_nome") or filtros.get("estados") or filtros.get("cidades"))
        has_clusters = tem_filtro_geo and df_map["cluster_mpr"].notna().any()
        rows = []
        for empresa, g in df_map.groupby("empresa"):
            total = len(g)

            lanc = (g["fase"] == "Lançamento").sum()
            breve = (g["fase"] == "Breve Lançamento").sum()

            areas = g["area_min_m2"].dropna()
            area_media = f"{areas.mean():.0f}m²" if len(areas) > 0 else ""

            tipo_cols = {"ST": "apto_studio", "1D": "apto_1_dorm", "2D": "apto_2_dorms", "3D": "apto_3_dorms"}
            tipos_presentes = []
            for label, col in tipo_cols.items():
                if col in g.columns and (g[col] == 1).sum() > total * 0.4:
                    tipos_presentes.append(label)
            tipologia = "+".join(tipos_presentes) if tipos_presentes else ""

            pct_1d = f"{100*(g['apto_1_dorm']==1).sum()/total:.0f}%" if "apto_1_dorm" in g.columns else ""
            pct_3d = f"{100*(g['apto_3_dorms']==1).sum()/total:.0f}%" if "apto_3_dorms" in g.columns else ""

            row_data = {
                "Empresa": empresa,
                "Produtos": total,
                "Lçtos": lanc,
                "Breve Lçtos": breve,
                "Área média": area_media,
                "Tip. freq.": tipologia,
                "% 1D": pct_1d,
                "% 3D": pct_3d,
            }

            if has_clusters:
                # Cluster principal (mais produtos)
                g_cl = g[g["cluster_mpr"].notna()]
                cl_principal = g_cl["cluster_mpr"].value_counts().index[0] if not g_cl.empty else ""

                # Cluster lançamentos (mais lançamentos + breves)
                g_lanc_cl = g[g["fase"].isin(["Lançamento", "Breve Lançamento"]) & g["cluster_mpr"].notna()]
                cl_lanc = g_lanc_cl["cluster_mpr"].value_counts().index[0] if not g_lanc_cl.empty else ""

                row_data["Cluster principal"] = cl_principal
                row_data["Cluster lçtos"] = cl_lanc

            rows.append(row_data)

        resumo = pd.DataFrame(rows).sort_values("Produtos", ascending=False)
        total_cols = {
            "Empresa": "TOTAL", "Produtos": resumo["Produtos"].sum(),
            "Lçtos": resumo["Lçtos"].sum(), "Breve Lçtos": resumo["Breve Lçtos"].sum(),
            "Área média": "", "Tip. freq.": "", "% 1D": "", "% 3D": "",
        }
        if has_clusters:
            total_cols["Cluster principal"] = ""
            total_cols["Cluster lçtos"] = ""
        resumo = pd.concat([resumo, pd.DataFrame([total_cols])], ignore_index=True)
        st.dataframe(resumo, use_container_width=True, hide_index=True)

        # === TABELA 2: Resumo por cluster MPR ===
        df_cluster = df_map[df_map["cluster_mpr"].notna() & (df_map["cluster_mpr"] != "")].copy()
        if not df_cluster.empty:
            import re as _re
            df_cluster["regiao_mpr"] = df_cluster["cluster_mpr"].apply(
                lambda x: _re.match(r'([A-Z]+)', x).group(1) if _re.match(r'([A-Z]+)', x) else x
            )

            # Mapear regionais selecionadas para prefixos de cluster esperados
            REGIONAL_TO_CLUSTER_PREFIX = {
                "SP + SPRM": ["SP", "RMSP"],
                "Nordeste": ["BA", "CE", "PE", "JP"],
                "Sul + MG": ["RS", "PR", "MG"],
                "RJ + CO + CPS": ["RJ", "GO", "CPS"],
                "SP Interior": ["CPS"],  # CPS pode aparecer aqui também
            }

            # Se tem regional filtrada, mostrar só clusters dessa regional
            regionais_ativas = filtros.get("regionais_nome", [])
            if regionais_ativas:
                prefixos_validos = set()
                for reg in regionais_ativas:
                    prefixos_validos.update(REGIONAL_TO_CLUSTER_PREFIX.get(reg, []))
                if prefixos_validos:
                    df_cluster = df_cluster[df_cluster["regiao_mpr"].isin(prefixos_validos)]

            if not df_cluster.empty:
                st.subheader("Resumo por Cluster MPR")

                cluster_rows = []
                for cluster, g in df_cluster.groupby("cluster_mpr"):
                    total = len(g)
                    lanc = (g["fase"] == "Lançamento").sum()
                    breve = (g["fase"] == "Breve Lançamento").sum()
                    regiao = g["regiao_mpr"].iloc[0]

                    g_lanc = g[g["fase"].isin(["Lançamento", "Breve Lançamento"])]
                    if not g_lanc.empty:
                        top = g_lanc["empresa"].value_counts().head(3)
                        players = ", ".join(f"{emp} ({cnt})" for emp, cnt in top.items())
                    else:
                        players = ""

                    cluster_rows.append({
                        "Regional": regiao,
                        "Cluster": cluster,
                        "Produtos": total,
                        "Lçtos": lanc,
                        "Breve Lçtos": breve,
                        "Principais players Lçtos": players,
                    })

                df_cl = pd.DataFrame(cluster_rows).sort_values(["Regional", "Cluster"])
                st.dataframe(df_cl, use_container_width=True, hide_index=True)
