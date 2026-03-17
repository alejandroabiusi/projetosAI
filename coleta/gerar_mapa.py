"""Gera mapa HTML interativo com pins dos empreendimentos."""

import sqlite3
import json
import os
import glob
import xml.etree.ElementTree as ET
import re

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "empreendimentos.db")
OUTPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mapa_empreendimentos.html")
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# Prefixo do KML -> nome amigável da região
REGIOES = {
    "SP": "São Paulo",
    "RJ": "Rio de Janeiro",
    "BH": "Belo Horizonte",
    "CWB": "Curitiba",
    "POA": "Porto Alegre",
    "BSB": "Brasília",
    "SAL": "Salvador",
    "REC": "Recife",
    "FOR": "Fortaleza",
    "CAM": "Campinas",
}

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


def _extrair_prefixo_regiao(filename):
    """Extrai prefixo da região do nome do arquivo KML.

    'Clusteres MPR SP.kml' -> 'SP'
    'Clusteres MPR RJ.kml' -> 'RJ'
    """
    base = os.path.splitext(os.path.basename(filename))[0]
    # Pega a última palavra como prefixo da região
    parts = base.split()
    return parts[-1].upper() if parts else "X"


def _parse_single_kml(kml_path, prefix):
    """Parseia um KML e retorna lista de clusters com nome prefixado."""
    tree = ET.parse(kml_path)
    root = tree.getroot()
    ns = {'kml': 'http://www.opengis.net/kml/2.2'}

    # StyleMap id -> normal Style id
    stylemap_to_normal = {}
    for sm in root.findall('.//kml:StyleMap', ns):
        sm_id = sm.get('id')
        for pair in sm.findall('kml:Pair', ns):
            if pair.find('kml:key', ns).text == 'normal':
                url = pair.find('kml:styleUrl', ns).text.lstrip('#')
                stylemap_to_normal[sm_id] = url

    # Style id -> cor hex (#rrggbb)
    style_colors = {}
    for style in root.findall('.//kml:Style', ns):
        sid = style.get('id')
        poly = style.find('kml:PolyStyle/kml:color', ns)
        if poly is not None:
            c = poly.text  # aabbggrr
            style_colors[sid] = f'#{c[6:8]}{c[4:6]}{c[2:4]}'

    clusters = []
    for idx, pm in enumerate(root.findall('.//kml:Placemark', ns), 1):
        style_ref = pm.find('kml:styleUrl', ns).text.lstrip('#')
        normal_id = stylemap_to_normal.get(style_ref, style_ref)
        color = style_colors.get(normal_id, '#ff0000')

        polygons = []
        for poly in pm.findall('.//kml:Polygon', ns):
            coords_text = poly.find('.//kml:coordinates', ns).text.strip()
            ring = []
            for pt in coords_text.split():
                parts = pt.split(',')
                lng, lat = float(parts[0]), float(parts[1])
                ring.append([lat, lng])
            polygons.append(ring)

        clusters.append({
            'name': f'{prefix}{idx}',
            'region': prefix,
            'color': color,
            'polygons': polygons,
        })

    return clusters


def load_all_kml_clusters():
    """Carrega todos os KMLs de data/ e retorna lista unificada de clusters."""
    all_clusters = []
    kml_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.kml")))
    for kml_path in kml_files:
        prefix = _extrair_prefixo_regiao(kml_path)
        clusters = _parse_single_kml(kml_path, prefix)
        all_clusters.extend(clusters)
        print(f"  KML '{os.path.basename(kml_path)}': {len(clusters)} clusters ({prefix}1..{prefix}{len(clusters)})")
    return all_clusters


def _point_in_polygon(lat, lng, ring):
    """Ray casting para testar se ponto está dentro do polígono.

    ring: lista de [lat, lng] pairs.
    """
    n = len(ring)
    inside = False
    j = n - 1
    for i in range(n):
        # ring[i] = [lat_i, lng_i]
        lat_i, lng_i = ring[i]
        lat_j, lng_j = ring[j]
        if ((lat_i > lat) != (lat_j > lat)) and \
           (lng < (lng_j - lng_i) * (lat - lat_i) / (lat_j - lat_i) + lng_i):
            inside = not inside
        j = i
    return inside


def classify_points(points, clusters):
    """Classifica cada ponto no cluster correspondente (ou None)."""
    for p in points:
        p["cl"] = None
        for cl in clusters:
            for ring in cl["polygons"]:
                if _point_in_polygon(p["lat"], p["lng"], ring):
                    p["cl"] = cl["name"]
                    break
            if p["cl"]:
                break


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

    # KML clusters
    kml_clusters = load_all_kml_clusters()

    # Classificar empreendimentos por cluster (point-in-polygon)
    if kml_clusters:
        classify_points(points, kml_clusters)
        n_classified = sum(1 for p in points if p["cl"])
        print(f"  {n_classified}/{len(points)} empreendimentos classificados em clusters")

    clusters_json = json.dumps(kml_clusters, ensure_ascii=False)

    # Regiões com nome amigável (só as que têm clusters)
    regioes_presentes = sorted(set(cl["region"] for cl in kml_clusters))
    regioes_map = {r: REGIOES.get(r, r) for r in regioes_presentes}
    regioes_json = json.dumps(regioes_map, ensure_ascii=False)

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
  .region-header {{
    display: flex; align-items: center; justify-content: space-between;
    margin: 4px 0 2px; cursor: pointer; font-weight: 600; font-size: 12px; color: #2c3e50;
  }}
  .region-header:hover {{ color: #2980b9; }}
  .region-zoom {{
    font-size: 10px; font-weight: normal; color: #3498db; margin-left: 6px;
    padding: 1px 6px; border: 1px solid #3498db; border-radius: 3px;
  }}
  .region-zoom:hover {{ background: #3498db; color: white; }}
  .region-clusters {{ margin-left: 4px; }}
</style>
</head>
<body>

<div class="header">
  <h1>Mapa de Empreendimentos Imobiliarios</h1>
  <div class="stats">{len(points)} empreendimentos mapeados &bull; {len(empresas)} empresas</div>
</div>

<div class="filter-panel" id="filters">
  <div id="filter-clusters" style="display:none">
    <strong>Cluster MPR:</strong><br>
    <div id="cluster-checkboxes"></div>
    <hr style="margin:6px 0;border:none;border-top:1px solid #ddd">
  </div>
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
const mprClusters = {clusters_json};
const regioes = {regioes_json};

const map = L.map('map', {{ zoomControl: true }}).setView([-15.8, -47.9], 5);

L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}@2x.png', {{
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/">CARTO</a>',
  maxZoom: 19,
  subdomains: 'abcd'
}}).addTo(map);

const clusters = L.layerGroup();

const allMarkers = [];
const activeEmpresas = new Set();
const activeStatus = new Set();
const activeClusters = new Set();
const SEM_CLUSTER = 'Fora de cluster';

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
const clusterCount = {{}};

pontos.forEach(p => {{
  const cor = cores[p.e] || '#999';
  const marker = L.marker([p.lat, p.lng], {{ icon: createIcon(cor) }});

  const clLabel = p.cl || SEM_CLUSTER;

  let info = '<div class="popup-title">' + p.n + '</div>';
  info += '<span class="popup-empresa" style="background:' + cor + '">' + p.e + '</span>';
  if (p.cl) info += ' <span style="font-size:11px;color:#666;font-weight:600">' + p.cl + '</span>';
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
  marker._cluster = clLabel;
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
  clusterCount[clLabel] = (clusterCount[clLabel] || 0) + 1;
  activeClusters.add(clLabel);
}});

map.addLayer(clusters);

// Cross-filter lookups (3 dimensions: status, empresa, cluster)
const empByStatus = {{}};
const statusByEmp = {{}};
const clusterByEmp = {{}};
const empByCluster = {{}};
const clusterByStatus = {{}};
const statusByCluster = {{}};

pontos.forEach(p => {{
  const st = p.f || 'Sem info';
  const cl = p.cl || SEM_CLUSTER;

  if (!empByStatus[st]) empByStatus[st] = new Set();
  empByStatus[st].add(p.e);
  if (!statusByEmp[p.e]) statusByEmp[p.e] = new Set();
  statusByEmp[p.e].add(st);

  if (!clusterByEmp[p.e]) clusterByEmp[p.e] = new Set();
  clusterByEmp[p.e].add(cl);
  if (!empByCluster[cl]) empByCluster[cl] = new Set();
  empByCluster[cl].add(p.e);

  if (!clusterByStatus[st]) clusterByStatus[st] = new Set();
  clusterByStatus[st].add(cl);
  if (!statusByCluster[cl]) statusByCluster[cl] = new Set();
  statusByCluster[cl].add(st);
}});

const empCheckboxes = {{}};
const stCheckboxes = {{}};
const clCheckboxes = {{}};

function markerVisible(m) {{
  return activeEmpresas.has(m._empresa) && activeStatus.has(m._status) && activeClusters.has(m._cluster);
}}

function applyFilters() {{
  clusters.clearLayers();
  allMarkers.forEach(m => {{
    if (markerVisible(m)) clusters.addLayer(m);
  }});
  updateCrossFilters();
}}

function updateCrossFilters() {{
  // Empresa availability: needs at least one active status AND one active cluster
  Object.entries(empCheckboxes).forEach(([emp, obj]) => {{
    const hasStatus = statusByEmp[emp] && [...statusByEmp[emp]].some(s => activeStatus.has(s));
    const hasCluster = clusterByEmp[emp] && [...clusterByEmp[emp]].some(c => activeClusters.has(c));
    const avail = hasStatus && hasCluster;
    obj.label.style.opacity = avail ? '1' : '0.35';
    obj.cb.disabled = !avail;
    let cnt = 0;
    allMarkers.forEach(m => {{
      if (m._empresa === emp && activeStatus.has(m._status) && activeClusters.has(m._cluster)) cnt++;
    }});
    obj.countSpan.textContent = ' ' + emp + ' (' + cnt + ')';
  }});

  // Status availability
  Object.entries(stCheckboxes).forEach(([st, obj]) => {{
    const hasEmp = empByStatus[st] && [...empByStatus[st]].some(e => activeEmpresas.has(e));
    const hasCluster = clusterByStatus[st] && [...clusterByStatus[st]].some(c => activeClusters.has(c));
    const avail = hasEmp && hasCluster;
    obj.label.style.opacity = avail ? '1' : '0.35';
    obj.cb.disabled = !avail;
    let cnt = 0;
    allMarkers.forEach(m => {{
      if (m._status === st && activeEmpresas.has(m._empresa) && activeClusters.has(m._cluster)) cnt++;
    }});
    obj.countSpan.textContent = ' ' + st + ' (' + cnt + ')';
  }});

  // Cluster availability
  Object.entries(clCheckboxes).forEach(([cl, obj]) => {{
    const hasEmp = empByCluster[cl] && [...empByCluster[cl]].some(e => activeEmpresas.has(e));
    const hasStatus = statusByCluster[cl] && [...statusByCluster[cl]].some(s => activeStatus.has(s));
    const avail = hasEmp && hasStatus;
    obj.label.style.opacity = avail ? '1' : '0.35';
    obj.cb.disabled = !avail;
    let cnt = 0;
    allMarkers.forEach(m => {{
      if (m._cluster === cl && activeEmpresas.has(m._empresa) && activeStatus.has(m._status)) cnt++;
    }});
    obj.countSpan.textContent = ' ' + cl + ' (' + cnt + ')';
  }});
}}

// --- Cluster filter (grouped by region, with zoom) ---
if (mprClusters.length > 0) {{
  document.getElementById('filter-clusters').style.display = 'block';
  const fpCl = document.getElementById('cluster-checkboxes');

  // Cluster color & bounds lookup
  const clusterColors = {{}};
  const clusterBounds = {{}};
  mprClusters.forEach(cl => {{
    clusterColors[cl.name] = cl.color;
    const lats = [], lngs = [];
    cl.polygons.forEach(ring => ring.forEach(pt => {{ lats.push(pt[0]); lngs.push(pt[1]); }}));
    clusterBounds[cl.name] = [[Math.min(...lats), Math.min(...lngs)], [Math.max(...lats), Math.max(...lngs)]];
  }});

  // Region bounds (union of all cluster bounds in the region)
  const regionBounds = {{}};
  mprClusters.forEach(cl => {{
    const r = cl.region;
    const b = clusterBounds[cl.name];
    if (!regionBounds[r]) {{ regionBounds[r] = [[b[0][0], b[0][1]], [b[1][0], b[1][1]]]; }}
    else {{
      regionBounds[r][0][0] = Math.min(regionBounds[r][0][0], b[0][0]);
      regionBounds[r][0][1] = Math.min(regionBounds[r][0][1], b[0][1]);
      regionBounds[r][1][0] = Math.max(regionBounds[r][1][0], b[1][0]);
      regionBounds[r][1][1] = Math.max(regionBounds[r][1][1], b[1][1]);
    }}
  }});

  // Group cluster names by region
  const clustersByRegion = {{}};
  Object.keys(clusterCount).forEach(cl => {{
    if (cl === SEM_CLUSTER) return;
    const region = mprClusters.find(c => c.name === cl)?.region || '?';
    if (!clustersByRegion[region]) clustersByRegion[region] = [];
    clustersByRegion[region].push(cl);
  }});
  Object.values(clustersByRegion).forEach(arr => arr.sort());

  // Render grouped UI
  const regionKeys = Object.keys(clustersByRegion).sort();

  regionKeys.forEach(region => {{
    const regionLabel = regioes[region] || region;
    const header = document.createElement('div');
    header.className = 'region-header';

    const headerLeft = document.createElement('span');
    headerLeft.textContent = regionLabel;
    header.appendChild(headerLeft);

    const zoomBtn = document.createElement('span');
    zoomBtn.className = 'region-zoom';
    zoomBtn.textContent = 'zoom';
    zoomBtn.addEventListener('click', (e) => {{
      e.stopPropagation();
      const rb = regionBounds[region];
      if (rb) map.fitBounds(rb, {{ padding: [40, 40] }});
    }});
    header.appendChild(zoomBtn);
    fpCl.appendChild(header);

    const container = document.createElement('div');
    container.className = 'region-clusters';

    clustersByRegion[region].forEach(cl => {{
      const label = document.createElement('label');
      const cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.checked = true;
      cb.style.marginRight = '4px';
      cb._cluster = cl;
      cb.addEventListener('change', () => {{
        if (cb.checked) {{ activeClusters.add(cl); }} else {{ activeClusters.delete(cl); }}
        applyFilters();
      }});

      const color = clusterColors[cl] || '#999';
      const dot = document.createElement('span');
      dot.style.cssText = 'display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:4px;background:' + color;
      label.appendChild(cb);
      label.appendChild(dot);
      const countSpan = document.createElement('span');
      countSpan.textContent = ' ' + cl + ' (' + (clusterCount[cl] || 0) + ')';
      label.appendChild(countSpan);
      container.appendChild(label);
      clCheckboxes[cl] = {{ cb, label, countSpan }};
    }});
    fpCl.appendChild(container);
  }});

  // 'Fora de cluster' at the end
  if (clusterCount[SEM_CLUSTER]) {{
    const label = document.createElement('label');
    label.style.marginTop = '4px';
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.checked = true;
    cb.style.marginRight = '4px';
    cb._cluster = SEM_CLUSTER;
    cb.addEventListener('change', () => {{
      if (cb.checked) {{ activeClusters.add(SEM_CLUSTER); }} else {{ activeClusters.delete(SEM_CLUSTER); }}
      applyFilters();
    }});
    label.appendChild(cb);
    const countSpan = document.createElement('span');
    countSpan.textContent = ' ' + SEM_CLUSTER + ' (' + clusterCount[SEM_CLUSTER] + ')';
    label.appendChild(countSpan);
    fpCl.appendChild(label);
    clCheckboxes[SEM_CLUSTER] = {{ cb, label, countSpan }};
  }}

  // Polygon overlays on the map
  const clusterLayerGroups = {{}};
  mprClusters.forEach(cl => {{
    const layerGroup = L.layerGroup();
    cl.polygons.forEach(ring => {{
      L.polygon(ring, {{
        color: cl.color,
        weight: 2,
        opacity: 0.7,
        fillColor: cl.color,
        fillOpacity: 0.2,
        interactive: false
      }}).addTo(layerGroup);
    }});
    layerGroup.addTo(map);
    clusterLayerGroups[cl.name] = layerGroup;
  }});

  // Sync polygon visibility with cluster filter checkboxes
  Object.entries(clCheckboxes).forEach(([cl, obj]) => {{
    if (cl === SEM_CLUSTER) return;
    obj.cb.addEventListener('change', () => {{
      const lg = clusterLayerGroups[cl];
      if (lg) {{
        if (obj.cb.checked) {{ lg.addTo(map); }} else {{ map.removeLayer(lg); }}
      }}
    }});
  }});
}}

// --- Status checkboxes ---
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

// --- Empresa checkboxes ---
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
    if (!markerVisible(m)) return;
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

const _origApply = applyFilters;
applyFilters = function() {{ _origApply(); setTimeout(updateSummary, 100); }};

if (allMarkers.length > 0) {{
  const bounds = L.latLngBounds(allMarkers.map(m => [m._lat, m._lng]));
  map.fitBounds(bounds.pad(0.1));
}}
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
