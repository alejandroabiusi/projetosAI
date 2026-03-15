"""Gera mapa HTML interativo com pins dos empreendimentos."""

import sqlite3
import json
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "empreendimentos.db")
OUTPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mapa_empreendimentos.html")

CORES = {
    "MRV": "#e74c3c",
    "Cury": "#3498db",
    "Plano&Plano": "#2ecc71",
    "Magik JC": "#9b59b6",
    "Vivaz": "#f39c12",
    "Metrocasa": "#1abc9c",
    "Direcional": "#e67e22",
    "Vibra Residencial": "#34495e",
    "Kazzas": "#d35400",
    "Pacaembu": "#c0392b",
    "Conx": "#7f8c8d",
    "Mundo Apto": "#2980b9",
    "Viva Benx": "#e91e63",
    "Novolar": "#16a085",
    "Árbore": "#27ae60",
    "SUGOI": "#2c3e50",
    "Emccamp": "#8e44ad",
    "EPH": "#d4ac0d",
    "Ampla": "#5dade2",
    "Novvo": "#a569bd",
    "M.Lar": "#eb984e",
    "Ún1ca": "#45b39d",
}


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT nome, empresa, cidade, estado, latitude, longitude, fase, preco_a_partir, endereco,
               data_lancamento, total_unidades, url_fonte
        FROM empreendimentos
        WHERE latitude IS NOT NULL AND latitude != ''
        AND longitude IS NOT NULL AND longitude != ''
    """)

    # Normalizar fases
    fase_map = {
        "lançamento": "Lançamento",
        "lancamento": "Lançamento",
        "breve lançamento": "Breve Lançamento",
        "breve lancamento": "Breve Lançamento",
        "futuro lançamento": "Breve Lançamento",
        "futuro lancamento": "Breve Lançamento",
        "pré-lançamento": "Breve Lançamento",
        "pre-lancamento": "Breve Lançamento",
        "em construção": "Em Construção",
        "em construcao": "Em Construção",
        "em obras": "Em Construção",
        "imóvel pronto": "Pronto",
        "imovel pronto": "Pronto",
        "pronto para morar": "Pronto",
        "pronto": "Pronto",
        "prontos": "Pronto",
        "100% vendido": "100% Vendido",
        "lotes": "Lotes",
        "aluguel": "Aluguel",
    }

    def normalizar_fase(fase_raw):
        if not fase_raw:
            return "Sem info"
        f = fase_raw.strip().lower()
        if f in fase_map:
            return fase_map[f]
        # Tentar match parcial (campo sujo concatenado)
        for chave, valor in fase_map.items():
            if chave in f:
                return valor
        return fase_raw.strip()

    points = []
    for r in cur.fetchall():
        try:
            lat = float(r["latitude"])
            lng = float(r["longitude"])
            if -35 < lat < 6 and -75 < lng < -30:
                points.append({
                    "n": r["nome"],
                    "e": r["empresa"],
                    "c": r["cidade"] or "",
                    "uf": r["estado"] or "",
                    "lat": round(lat, 6),
                    "lng": round(lng, 6),
                    "f": normalizar_fase(r["fase"]),
                    "p": r["preco_a_partir"] if r["preco_a_partir"] else None,
                    "end": r["endereco"] or "",
                    "dl": r["data_lancamento"] or "",
                    "un": r["total_unidades"] if r["total_unidades"] else None,
                    "url": r["url_fonte"] or "",
                })
        except (ValueError, TypeError):
            pass
    conn.close()

    # Contagem por empresa
    empresas = {}
    for p in points:
        empresas[p["e"]] = empresas.get(p["e"], 0) + 1

    points_json = json.dumps(points, ensure_ascii=False)
    cores_json = json.dumps(CORES, ensure_ascii=False)

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Mapa de Empreendimentos</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css" />
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css" />
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }}
  #map {{ width: 100vw; height: 100vh; }}
  .header {{
    position: absolute; top: 0; left: 0; right: 0; z-index: 1000;
    background: rgba(255,255,255,0.95); padding: 10px 20px;
    display: flex; align-items: center; justify-content: space-between;
    box-shadow: 0 2px 8px rgba(0,0,0,0.15); backdrop-filter: blur(5px);
  }}
  .header h1 {{ font-size: 18px; color: #2c3e50; }}
  .header .stats {{ font-size: 13px; color: #7f8c8d; }}
  .dot {{ width: 10px; height: 10px; border-radius: 50%; margin-right: 6px; flex-shrink: 0; }}
  .filter-panel {{
    position: absolute; top: 52px; right: 12px; z-index: 1000;
    background: rgba(255,255,255,0.95); padding: 10px 14px;
    border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.2);
    font-size: 12px; max-height: 80vh; overflow-y: auto;
  }}
  .filter-panel label {{ display: block; margin: 3px 0; cursor: pointer; }}
  .filter-panel label:hover {{ color: #2980b9; }}
  .toggle-all {{ cursor: pointer; color: #2980b9; font-size: 11px; margin: 2px 0 4px; display: inline-block; }}
  .toggle-all:hover {{ text-decoration: underline; }}
  .popup-title {{ font-weight: bold; font-size: 14px; color: #2c3e50; margin-bottom: 4px; }}
  .popup-empresa {{ display: inline-block; padding: 2px 8px; border-radius: 10px; color: white; font-size: 11px; margin-bottom: 4px; }}
  .popup-info {{ color: #555; font-size: 12px; }}
  .popup-link {{ display: inline-block; margin-top: 6px; padding: 4px 12px; background: #3498db; color: white; text-decoration: none; border-radius: 4px; font-size: 11px; font-weight: 600; }}
  .popup-link:hover {{ background: #2980b9; }}
  .summary-panel {{
    position: absolute; bottom: 20px; left: 12px; z-index: 1000;
    background: rgba(255,255,255,0.95); padding: 12px 16px;
    border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.2);
    font-size: 12px; min-width: 220px; max-width: 380px; max-height: 50vh; overflow-y: auto;
  }}
  .summary-panel h3 {{ margin-bottom: 8px; color: #2c3e50; font-size: 13px; }}
  .summary-table {{ width: 100%; border-collapse: collapse; }}
  .summary-table th {{ text-align: left; font-weight: 600; color: #2c3e50; padding: 3px 4px; border-bottom: 1px solid #ddd; font-size: 11px; }}
  .summary-table td {{ padding: 3px 4px; font-size: 12px; }}
  .summary-table td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  .summary-table tr:last-child td {{ border-top: 1px solid #ccc; font-weight: 700; }}
  .summary-dot {{ display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 4px; vertical-align: middle; }}
</style>
</head>
<body>

<div class="header">
  <h1>Mapa de Empreendimentos Imobiliarios</h1>
  <div class="stats">{len(points)} empreendimentos mapeados &bull; {len(empresas)} empresas</div>
</div>

<div class="filter-panel" id="filters">
  <strong>Status:</strong><br>
  <div id="filter-status"></div>
  <hr style="margin:6px 0;border:none;border-top:1px solid #ddd">
  <strong>Empresa:</strong><br>
  <div id="filter-empresa"></div>
</div>

<div class="summary-panel" id="summary">
  <h3>Resumo da area visivel</h3>
  <div id="summary-content">Carregando...</div>
</div>

<div id="map"></div>

<script>
const pontos = {points_json};
const cores = {cores_json};

const map = L.map('map', {{ zoomControl: true }}).setView([-15.8, -47.9], 5);

L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
  attribution: '&copy; OpenStreetMap contributors',
  maxZoom: 18
}}).addTo(map);

const clusters = L.markerClusterGroup({{
  maxClusterRadius: 40,
  spiderfyOnMaxZoom: true,
  showCoverageOnHover: false,
  disableClusteringAtZoom: 14
}});

const allMarkers = [];
const activeEmpresas = new Set();
const activeStatus = new Set();

function createIcon(color) {{
  return L.divIcon({{
    html: '<div style="background:'+color+';width:12px;height:12px;border-radius:50%;border:2px solid white;box-shadow:0 1px 3px rgba(0,0,0,0.4);"></div>',
    className: '',
    iconSize: [12, 12],
    iconAnchor: [6, 6],
    popupAnchor: [0, -8]
  }});
}}

function fmt(v) {{
  if (!v) return '';
  return Number(v).toLocaleString('pt-BR');
}}

// Contagens
const empresaCount = {{}};
const statusCount = {{}};

pontos.forEach(p => {{
  const cor = cores[p.e] || '#999';
  const marker = L.marker([p.lat, p.lng], {{ icon: createIcon(cor) }});

  let info = '<div class="popup-title">' + p.n + '</div>';
  info += '<span class="popup-empresa" style="background:' + cor + '">' + p.e + '</span>';
  info += '<br><span class="popup-info">' + p.c + (p.uf ? '/' + p.uf : '') + '</span>';
  if (p.f && p.f !== 'Sem info') info += '<br><span class="popup-info">' + p.f + '</span>';
  if (p.p) info += '<br><span class="popup-info">A partir de R$ ' + fmt(p.p) + '</span>';
  if (p.un) info += '<br><span class="popup-info">' + p.un + ' unidades</span>';
  if (p.dl) info += '<br><span class="popup-info">Lanc.: ' + p.dl + '</span>';
  if (p.end) info += '<br><span class="popup-info" style="font-size:11px;color:#888">' + p.end + '</span>';
  if (p.url) info += '<br><a class="popup-link" href="' + p.url + '" target="_blank">Ver no site</a>';

  marker.bindPopup(info);
  marker._empresa = p.e;
  marker._status = p.f || 'Sem info';
  marker._unidades = p.un || 0;
  marker._lat = p.lat;
  marker._lng = p.lng;

  allMarkers.push(marker);
  clusters.addLayer(marker);

  empresaCount[p.e] = (empresaCount[p.e] || 0) + 1;
  activeEmpresas.add(p.e);
  const st = p.f || 'Sem info';
  statusCount[st] = (statusCount[st] || 0) + 1;
  activeStatus.add(st);
}});

map.addLayer(clusters);

// Cross-filter: build lookup of which empresas have which statuses
const empByStatus = {{}};
const statusByEmp = {{}};
pontos.forEach(p => {{
  const st = p.f || 'Sem info';
  if (!empByStatus[st]) empByStatus[st] = new Set();
  empByStatus[st].add(p.e);
  if (!statusByEmp[p.e]) statusByEmp[p.e] = new Set();
  statusByEmp[p.e].add(st);
}});

const empCheckboxes = {{}};
const stCheckboxes = {{}};

// Rebuild map + update cross-filter availability
function applyFilters() {{
  clusters.clearLayers();
  allMarkers.forEach(m => {{
    if (activeEmpresas.has(m._empresa) && activeStatus.has(m._status)) {{
      clusters.addLayer(m);
    }}
  }});
  updateCrossFilters();
}}

function updateCrossFilters() {{
  // Which empresas have at least one active status?
  const availEmpresas = new Set();
  activeStatus.forEach(st => {{
    if (empByStatus[st]) empByStatus[st].forEach(e => availEmpresas.add(e));
  }});
  // Which statuses have at least one active empresa?
  const availStatus = new Set();
  activeEmpresas.forEach(e => {{
    if (statusByEmp[e]) statusByEmp[e].forEach(st => availStatus.add(st));
  }});

  // Disable/enable empresa checkboxes
  Object.entries(empCheckboxes).forEach(([emp, obj]) => {{
    const avail = availEmpresas.has(emp);
    obj.label.style.opacity = avail ? '1' : '0.35';
    obj.cb.disabled = !avail;
    // Count only matching markers
    let cnt = 0;
    allMarkers.forEach(m => {{
      if (m._empresa === emp && activeStatus.has(m._status)) cnt++;
    }});
    obj.countSpan.textContent = avail ? ' ' + emp + ' (' + cnt + ')' : ' ' + emp + ' (0)';
  }});

  // Disable/enable status checkboxes
  Object.entries(stCheckboxes).forEach(([st, obj]) => {{
    const avail = availStatus.has(st);
    obj.label.style.opacity = avail ? '1' : '0.35';
    obj.cb.disabled = !avail;
    let cnt = 0;
    allMarkers.forEach(m => {{
      if (m._status === st && activeEmpresas.has(m._empresa)) cnt++;
    }});
    obj.countSpan.textContent = avail ? ' ' + st + ' (' + cnt + ')' : ' ' + st + ' (0)';
  }});
}}

// --- Status checkboxes (first) ---
const fpSt = document.getElementById('filter-status');
const stToggle = document.createElement('span');
stToggle.className = 'toggle-all';
stToggle.textContent = 'Desmarcar todos';
stToggle._allChecked = true;
stToggle.addEventListener('click', () => {{
  stToggle._allChecked = !stToggle._allChecked;
  stToggle.textContent = stToggle._allChecked ? 'Desmarcar todos' : 'Marcar todos';
  fpSt.querySelectorAll('input[type=checkbox]').forEach(cb => {{
    cb.checked = stToggle._allChecked;
    const st = cb._status;
    if (stToggle._allChecked) {{ activeStatus.add(st); }} else {{ activeStatus.delete(st); }}
  }});
  applyFilters();
}});
fpSt.appendChild(stToggle);

Object.keys(statusCount)
  .sort((a,b) => statusCount[b] - statusCount[a])
  .forEach(st => {{
    const label = document.createElement('label');
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.checked = true;
    cb.style.marginRight = '4px';
    cb._status = st;
    cb.addEventListener('change', () => {{
      if (cb.checked) {{ activeStatus.add(st); }} else {{ activeStatus.delete(st); }}
      applyFilters();
    }});
    const countSpan = document.createElement('span');
    countSpan.textContent = ' ' + st + ' (' + statusCount[st] + ')';
    label.appendChild(cb);
    label.appendChild(countSpan);
    fpSt.appendChild(label);
    stCheckboxes[st] = {{ cb, label, countSpan }};
  }});

// --- Empresa checkboxes (second) ---
const fpEmp = document.getElementById('filter-empresa');
const empToggle = document.createElement('span');
empToggle.className = 'toggle-all';
empToggle.textContent = 'Desmarcar todas';
empToggle._allChecked = true;
empToggle.addEventListener('click', () => {{
  empToggle._allChecked = !empToggle._allChecked;
  empToggle.textContent = empToggle._allChecked ? 'Desmarcar todas' : 'Marcar todas';
  fpEmp.querySelectorAll('input[type=checkbox]').forEach(cb => {{
    if (!cb.disabled) {{
      cb.checked = empToggle._allChecked;
      const emp = cb._empresa;
      if (empToggle._allChecked) {{ activeEmpresas.add(emp); }} else {{ activeEmpresas.delete(emp); }}
    }}
  }});
  applyFilters();
}});
fpEmp.appendChild(empToggle);

Object.keys(empresaCount)
  .sort((a,b) => empresaCount[b] - empresaCount[a])
  .forEach(emp => {{
    const label = document.createElement('label');
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.checked = true;
    cb.style.marginRight = '4px';
    cb._empresa = emp;
    cb.addEventListener('change', () => {{
      if (cb.checked) {{ activeEmpresas.add(emp); }} else {{ activeEmpresas.delete(emp); }}
      applyFilters();
    }});
    const dot = document.createElement('span');
    dot.className = 'dot';
    dot.style.cssText = 'display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:4px;background:' + (cores[emp]||'#999');
    const countSpan = document.createElement('span');
    countSpan.textContent = ' ' + emp + ' (' + empresaCount[emp] + ')';
    label.appendChild(cb);
    label.appendChild(dot);
    label.appendChild(countSpan);
    fpEmp.appendChild(label);
    empCheckboxes[emp] = {{ cb, label, countSpan }};
  }});

// Quadro resumo dinamico
function updateSummary() {{
  const bounds = map.getBounds();
  const resumo = {{}};
  let totalEmp = 0;
  let totalUn = 0;

  allMarkers.forEach(m => {{
    if (!activeEmpresas.has(m._empresa) || !activeStatus.has(m._status)) return;
    if (!bounds.contains([m._lat, m._lng])) return;

    if (!resumo[m._empresa]) resumo[m._empresa] = {{ count: 0, unidades: 0 }};
    resumo[m._empresa].count++;
    resumo[m._empresa].unidades += m._unidades;
    totalEmp++;
    totalUn += m._unidades;
  }});

  const panel = document.getElementById('summary-content');
  const entries = Object.entries(resumo).sort((a,b) => b[1].count - a[1].count);

  if (entries.length === 0) {{
    panel.innerHTML = '<span style="color:#999">Nenhum empreendimento nesta area</span>';
    return;
  }}

  let html = '<table class="summary-table"><tr><th>Empresa</th><th style="text-align:right">Emp.</th><th style="text-align:right">Unid.</th></tr>';
  entries.forEach(([emp, d]) => {{
    const cor = cores[emp] || '#999';
    const un = d.unidades ? Number(d.unidades).toLocaleString('pt-BR') : '-';
    html += '<tr><td><span class="summary-dot" style="background:'+cor+'"></span>' + emp + '</td>';
    html += '<td class="num">' + d.count + '</td>';
    html += '<td class="num">' + un + '</td></tr>';
  }});
  html += '<tr><td>Total</td><td class="num">' + totalEmp + '</td>';
  html += '<td class="num">' + (totalUn ? Number(totalUn).toLocaleString('pt-BR') : '-') + '</td></tr>';
  html += '</table>';
  panel.innerHTML = html;
}}

map.on('moveend', updateSummary);
map.on('zoomend', updateSummary);

// Atualizar resumo tambem quando filtros mudam
const _origApply = applyFilters;
applyFilters = function() {{ _origApply(); setTimeout(updateSummary, 100); }};

map.fitBounds(clusters.getBounds().pad(0.1));
setTimeout(() => {{ map.invalidateSize(); updateSummary(); }}, 100);
</script>
</body>
</html>"""

    with open(OUTPUT, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Mapa criado: {OUTPUT}")
    print(f"{len(points)} pins | {len(empresas)} empresas")


if __name__ == "__main__":
    main()
