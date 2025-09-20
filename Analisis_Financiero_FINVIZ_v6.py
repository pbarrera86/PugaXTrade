# semaforo_excel_gui.py (licensed/trial + machine-locked version)
# -*- coding: utf-8 -*-
"""
Semáforo de indicadores + Excel con colores + Histórico + GUI de progreso
+ Anti-429 (rate limit, backoff, caché diaria de Finviz)
+ *** Sistema de PRUEBA de 7 días y Licencia ***
+ *** Soporte de ACTIVACIÓN por MÁQUINA ***

- Al iniciar: valida periodo de prueba de 7 días. Después solicita licencia.
  Acepta:
    1) Licencia universal: "PugaXTrade2025*"
    2) Licencia por máquina: generada a partir de la "Clave de máquina" que muestra el programa.
       (ver el generador: pugax_machine_license_generator.py)

- Hojas: 'Resumen' + 'DET_<TICKER>' + 'Ranking_Historico'
- Categoría en español; columnas: Indicator EN / Indicador ES
- Ventana Tkinter con barra de progreso y leyenda
- En 'Resumen' se agregan Price, Target Price y 'Target/Price -1 (%)' = (Target/Price - 1) * 100
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Tuple
import io, os, re, time, random, requests, pandas as pd, datetime as dt, threading, traceback, json, sys, hmac, hashlib, base64, uuid, platform
from pathlib import Path

# ====================== CONFIG ======================
DEFAULT_TICKERS = ['NVDA','MSFT', 'AAPL', 'AMZN', 'META', 'AVGO', 'GOOGL', 'GOOG', 'TSLA', 'JPM', 'ORCL', 'WMT', 'LLY', 'V', 'MA', 'NFLX', 'XOM',
                   'JNJ', 'HD', 'COST', 'ABBV', 'PLTR', 'BAC', 'PG', 'CVX', 'UNH', 'GE', 'KO', 'CSCO', 'TMUS', 'WFC', 'AMD', 'PM', 'MS', 'IBM', 'GS', 'ABT',
                   'NVO','FCN','BCPC','RIVN','BABA','TSM','EBS','ASML','DJT','MSTR','MARA','UBS','SHEL','CLSK','AVAV','MUSA','BBVA','HE','SOFI','NIO','TAL','NVAX',
                   'AXP', 'CRM', 'LIN', 'MCD', 'RTX', 'T', 'CAT', 'DIS', 'MRK', 'UBER', 'PEP', 'NOW', 'C', 'INTU', 'VZ', 'QCOM', 'ANET', 'MU', 'TMO', 'BKNG',
                   'BLK', 'GEV', 'SPGI', 'BA', 'SCHW', 'TXN', 'TJX', 'ISRG', 'LRCX', 'ADBE', 'LOW', 'ACN', 'NEE', 'AMGN', 'ETN', 'PGR', 'BX', 'APH', 'AMAT',
                   'SYK', 'COF', 'BSX', 'HON', 'DHR', 'PANW', 'GILD', 'PFE', 'KKR', 'KLAC', 'UNP', 'DE', 'INTC', 'COP', 'ADP', 'CMCSA', 'ADI', 'MDT', 'LMT',
                   'CRWD', 'WELL', 'DASH', 'NKE', 'MO', 'PLD', 'CB', 'SO', 'ICE', 'CEG', 'VRTX', 'CDNS', 'HCA', 'MCO', 'PH', 'CME', 'AMT', 'SBUX', 'MMC', 'BMY',
                   'CVS', 'DUK', 'GD', 'MCK', 'NEM', 'ORLY', 'DELL', 'SHW', 'WM', 'TT', 'RCL', 'COIN', 'NOC', 'APO', 'CTAS', 'PNC', 'MDLZ', 'MMM', 'ITW', 'EQIX',
                   'BK', 'AJG', 'ABNB', 'ECL', 'AON', 'SNPS', 'CI', 'MSI', 'USB', 'HWM', 'TDG', 'UPS', 'FI', 'EMR', 'AZO', 'JCI', 'WMB', 'VST', 'MAR', 'RSG', 'ELV',
                   'WDAY', 'NSC', 'HLT', 'MNST', 'PYPL', 'TEL', 'APD', 'CL', 'GLW', 'ZTS', 'FCX', 'EOG', 'ADSK', 'CMI', 'AFL', 'FTNT', 'KMI', 'AXON', 'SPG', 'TRV',
                   'REGN', 'TFC', 'DLR', 'URI', 'CSX', 'COR', 'AEP', 'NDAQ', 'CMG', 'FDX', 'PCAR', 'VLO', 'CARR', 'MET', 'SLB', 'ALL', 'IDXX', 'PSX', 'BDX', 'FAST',
                   'SRE', 'O', 'ROP', 'GM', 'MPC', 'PWR', 'NXPI', 'LHX', 'D', 'DHI', 'MSCI', 'AMP', 'OKE', 'STX', 'WBD', 'CPRT', 'PSA', 'ROST', 'CBRE', 'GWW', 'PAYX',
                   'DDOG', 'CTVA', 'XYZ', 'BKR', 'OXY', 'F', 'GRMN', 'TTWO', 'FANG', 'PEG', 'HSY', 'VMC', 'ETR', 'RMD', 'AME', 'EXC', 'EW', 'KR', 'LYV', 'SYY', 'CCI',
                   'KMB', 'CCL', 'AIG', 'TGT', 'EBAY', 'MPWR', 'YUM', 'EA', 'XEL', 'PRU', 'A', 'GEHC', 'OTIS', 'ACGL', 'PCG', 'RJF', 'UAL', 'CTSH', 'XYL', 'KVUE', 'LVS',
                   'CHTR', 'HIG', 'KDP', 'MLM', 'FICO', 'CSGP', 'DAL', 'ROK', 'NUE', 'LEN', 'WEC', 'TRGP', 'MCHP', 'VRSK', 'WDC', 'VICI', 'ED', 'CAH', 'FIS', 'PHM', 'VRSN',
                   'AEE', 'K', 'ROL', 'AWK', 'MTB', 'IR', 'VTR', 'TSCO', 'STT', 'NRG', 'EQT', 'IQV', 'EL', 'DD', 'WAB', 'EFX', 'HUM', 'WTW', 'HPE', 'AVB', 'WRB', 'IBKR',
                   'EXPE', 'SYF', 'DTE', 'BR', 'IRM', 'DXCM', 'KEYS', 'ADM', 'FITB', 'BRO', 'EXR', 'ODFL', 'KHC', 'ES', 'DOV', 'ULTA', 'STE', 'DRI', 'CBOE', 'STZ', 'RF',
                   'CINF', 'WSM', 'PTC', 'CNP', 'EQR', 'IP', 'NTAP', 'PPG', 'NTRS', 'HBAN', 'FE', 'MTD', 'HPQ', 'ATO', 'GIS', 'TDY', 'VLTO', 'PPL', 'SMCI', 'DVN', 'CHD',
                   'FSLR', 'CFG', 'PODD', 'JBL', 'LH', 'TPR', 'BIIB', 'CMS', 'TPL', 'EIX', 'SBAC', 'CDW', 'CPAY', 'TTD', 'NVR', 'HUBB', 'TROW', 'SW', 'DG', 'TYL', 'EXE',
                   'LDOS', 'GDDY', 'DLTR', 'L', 'ON', 'STLD', 'DGX', 'KEY', 'GPN', 'LUV', 'ERIE', 'INCY', 'TKO', 'FTV', 'IFF', 'EVRG', 'MAA', 'BG', 'LNT', 'ZBRA', 'CHRW',
                   'BBY', 'CNC', 'MAS', 'CLX', 'ALLE', 'BLDR', 'DPZ', 'KIM', 'OMC', 'TXT', 'MKC', 'WY', 'GEN', 'J', 'DECK', 'DOW', 'SNA', 'ESS', 'LYB', 'EXPD', 'ZBH', 'PSKY',
                   'GPC', 'LULU', 'TSN', 'AMCR', 'LII', 'PKG', 'TRMB', 'HAL', 'IT', 'NI', 'RL', 'FFIV', 'WST', 'CTRA', 'INVH', 'PNR', 'TER', 'WAT', 'APTV', 'PFG', 'RVTY',
                   'PNW', 'ALB', 'DVA', 'CAG', 'MTCH', 'KMX', 'AES', 'AOS', 'AIZ', 'COO', 'REG', 'DOC', 'SOLV', 'NDSN', 'WYNN', 'BEN', 'FOX', 'UDR', 'FOXA', 'BXP', 'SWK',
                   'IEX', 'MGM', 'ALGN', 'TAP', 'MOH', 'MRNA', 'HAS', 'IPG', 'IVZ', 'CPB', 'ARE', 'HOLX', 'EG', 'HRL', 'CF', 'BALL', 'JBHT', 'AVY', 'FDS', 'PAYC',
                   'UHS', 'JKHY', 'NCLH', 'CPT', 'GL', 'VTRS', 'NWSA', 'SJM', 'SWKS', 'DAY', 'MOS', 'AKAM', 'POOL', 'HST', 'BAX', 'GNRC', 'HII', 'EMN', 'CZR', 'NWS', 'CRL',
                   'MKTX', 'LW', 'EPAM', 'APA', 'LKQ', 'TECH', 'FRT', 'MHK', 'HSIC', 'ENPH']

OUTFILE = "semaforo_analisis_todas.xlsx"
HISTORY_CSV = "semaforo_historico_todas.csv"

# --- Anti-429 ---
CACHE_DIR = Path("cache_finviz")
CACHE_TTL_HOURS = 24
RATE_LIMIT_SEC = 5.5
MAX_RETRIES = 5
UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
]

# --- Nombre único para la columna nueva (evita KeyError) ---
RATIO_COL = "Target/Price -1 (%)"

# ==================== LICENCIAS =====================
# (1) Licencia universal directa (si quieres, cámbiala aquí)
LICENSE_KEY_UNIVERSAL = "PugaXTrade2025*"
# (2) Días de prueba
TRIAL_DAYS  = 7
# (3) Secreto para licencias por máquina (usa un valor propio/único)
#     *** IMPORTANTE: cámbialo antes de distribuir ***
MACHINE_SECRET = "PugaXTrade-2025-Secret-Reemplaza-ESTO"

def _license_state_path() -> Path:
    """
    Devuelve la ruta persistente del archivo de estado de licencia.
    En Windows usa %APPDATA%\\PugaX_Trade\\license_state.json
    En otros SO usa ~/.pugax_trade/license_state.json
    """
    base = os.getenv("APPDATA")
    if base:
        base_path = Path(base) / "PugaX_Trade"
    else:
        base_path = Path.home() / ".pugax_trade"
    base_path.mkdir(parents=True, exist_ok=True)
    return base_path / "license_state.json"

def _load_license_state() -> Dict[str, Any]:
    p = _license_state_path()
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _save_license_state(state: Dict[str, Any]) -> None:
    p = _license_state_path()
    try:
        p.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

# --------- Firma / verificación por máquina ----------
def get_machine_id() -> str:
    """
    Genera una 'Clave de máquina' estable y anónima (no PII).
    Usa MAC + uname. Si falla, crea un fallback persistente en license_state.json.
    """
    try:
        mac = uuid.getnode()  # puede ser aleatorio en algunas VMs, pero suele ser estable
        uname = platform.uname()
        base = f"{mac}-{uname.system}-{uname.node}-{uname.machine}"
        digest = hashlib.sha256(base.encode("utf-8")).hexdigest().upper()
        return digest[:20]  # 20 hex, p.ej. '9F3A1C...'
    except Exception:
        # Fallback: persistimos una ID aleatoria
        state = _load_license_state()
        fid = state.get("fallback_mid")
        if not fid:
            fid = hashlib.sha256(os.urandom(32)).hexdigest().upper()[:20]
            state["fallback_mid"] = fid
            _save_license_state(state)
        return fid

def _format_b32(code: str, group=5, total_len=25) -> str:
    clean = code.replace("=", "").upper()
    if total_len:
        clean = clean[:total_len]
    parts = [clean[i:i+group] for i in range(0, len(clean), group)]
    return "PX-" + "-".join(parts)

def make_machine_license(machine_id: str, secret: str = MACHINE_SECRET) -> str:
    """
    Genera una licencia legible (Base32) a partir de un machine_id y un secreto.
    Resultado ej.: 'PX-ABCD1-EFGH2-IJKL3-MNOP4-QRST5'
    """
    msg = machine_id.strip().upper().encode("utf-8")
    key = secret.encode("utf-8")
    h = hmac.new(key, msg, hashlib.sha256).digest()
    b32 = base64.b32encode(h).decode("utf-8").rstrip("=")  # sin '='
    return _format_b32(b32, group=5, total_len=25)

def is_valid_universal(key: Optional[str]) -> bool:
    return (key or "").strip() == LICENSE_KEY_UNIVERSAL

def is_valid_machine_license(key: Optional[str], machine_id: str) -> bool:
    if not key: return False
    expected = make_machine_license(machine_id)
    return hmac.compare_digest(key.strip().upper(), expected)

def is_license_valid_for_this_machine(key: Optional[str], machine_id: str) -> Tuple[bool, str]:
    """
    True/("universal"|"machine") si es válida para esta máquina, False/"" si no.
    """
    if is_valid_universal(key): return True, "universal"
    if is_valid_machine_license(key, machine_id): return True, "machine"
    return False, ""

def _trial_days_left(state: Dict[str, Any], today: Optional[dt.date] = None) -> int:
    today = today or dt.date.today()
    start_s = state.get("start_date")
    if not start_s:
        # Primera ejecución: inicia el periodo de prueba hoy
        state["start_date"] = today.isoformat()
        state["last_run"] = today.isoformat()
        _save_license_state(state)
        return TRIAL_DAYS
    try:
        start = dt.date.fromisoformat(start_s)
    except Exception:
        start = today
        state["start_date"] = today.isoformat()
        _save_license_state(state)
    days_used = (today - start).days
    left = TRIAL_DAYS - days_used
    # Guardamos last_run para tener una referencia mínima anti-manipulación de reloj
    state["last_run"] = today.isoformat()
    _save_license_state(state)
    return left

def enforce_trial_and_license(parent=None) -> Tuple[bool, str]:
    """
    Devuelve (ok, machine_id). 'ok' True si el usuario puede continuar
    (dentro del trial o con licencia válida).
    Si el trial expiró y no se ingresa licencia válida, devuelve False.
    """
    state = _load_license_state()
    machine_id = get_machine_id()
    state["machine_id"] = machine_id  # persistir para referencia
    _save_license_state(state)

    # Si ya hay licencia, verificar que sigue siendo válida para esta máquina
    key = state.get("license_key")
    valid, ltype = is_license_valid_for_this_machine(key, machine_id)
    if valid:
        return True, machine_id

    # Chequeo simple de manipulación de reloj: si "hoy" es mucho menor que el último uso, bloquea
    today = dt.date.today()
    last_run_s = state.get("last_run")
    if last_run_s:
        try:
            last_run = dt.date.fromisoformat(last_run_s)
            if (today - last_run).days < -1:  # retrocedió más de 1 día
                ok = _prompt_for_license(parent, state, machine_id, tamper=True)
                return ok, machine_id
        except Exception:
            pass

    left = _trial_days_left(state, today=today)
    if left > 0:
        # Aún tiene días de prueba
        return True, machine_id
    else:
        # Trial vencido: pedir licencia
        ok = _prompt_for_license(parent, state, machine_id, tamper=False)
        return ok, machine_id

def _prompt_for_license(parent, state: Dict[str, Any], machine_id: str, tamper: bool = False) -> bool:
    import tkinter as tk
    from tkinter import simpledialog, messagebox

    msg = "El periodo de prueba de 7 días ha expirado.\n\n" \
          "Por favor, ingresa tu licencia para continuar."
    if tamper:
        msg = "Se detectó un ajuste inusual en la fecha del sistema.\n" \
              "Para continuar, por favor ingresa tu licencia."

    msg += f"\n\nTu CLAVE DE MÁQUINA es:\n{machine_id}\n(cópiala y envíala para recibir tu licencia)"

    # Ventana de prompt modal
    tries = 3
    for n in range(tries):
        key = simpledialog.askstring(
            "Licencia requerida",
            f"{msg}\n\nIntento {n+1} de {tries}\n\nLicencia:",
            parent=parent,
            show="*"
        )
        if key is None:  # Cancelar
            break
        k = (key or "").strip()
        valid, ltype = is_license_valid_for_this_machine(k, machine_id)
        if valid:
            state["license_key"] = k
            state["license_type"] = ltype
            _save_license_state(state)
            messagebox.showinfo("Correcto", f"Licencia válida ({ltype}). ¡Gracias!")
            return True
        else:
            messagebox.showerror("Licencia inválida", "La licencia no es correcta para esta máquina. Intenta nuevamente.")
    messagebox.showwarning("Acceso denegado", "No se ingresó una licencia válida. El programa se cerrará.")
    return False

# ================= UTILIDADES FINANCIERAS ==============
def _to_float(x):
    if x is None: return None
    if isinstance(x, (int, float)): return float(x)
    s = str(x).strip().replace(",", "")
    m = re.search(r"\((-?\d+\.?\d*)%\)", s)  # "3.49 (0.69%)"
    if m and "%" in s and "(" in s:
        try: return float(m.group(1))
        except: pass
    if s.endswith("%"):
        try: return float(s[:-1])
        except: return None
    nums = re.findall(r"-?\d+\.?\d*", s)
    if len(nums) == 1:
        try: return float(nums[0])
        except: return None
    try: return float(s)
    except: return None

def _first_percent(text: str) -> Optional[float]:
    if text is None: return None
    m = re.search(r"-?\d+\.?\d*(?=%)", str(text))
    return float(m.group()) if m else None

def _last_percent(text: str) -> Optional[float]:
    if text is None: return None
    m = re.findall(r"-?\d+\.?\d*(?=%)", str(text))
    return float(m[-1]) if m else None

EMOJI = {"green": "🟢", "yellow": "🟡", "red": "🔴"}
NUMS  = {"green": 1, "yellow": 0, "red": -1}

# ================= INDICADORES Y PESOS =================
@dataclass
class IndicatorResult:
    label: str
    rating: str
    weight: float
    reason: str
    value: Optional[float] = None
    key: Optional[str] = None

def _score_lower_better(v, g, y):
    if v is None: return "yellow", "Sin dato."
    if v <= g: return "green", f"≤ {g} (mejor bajo)."
    if v <= y: return "yellow", f"entre {g}-{y}."
    return "red", f"> {y} (alto/caro)."

def _score_higher_better(v, g, y):
    if v is None: return "yellow", "Sin dato."
    if v >= g: return "green", f"≥ {g} (mejor alto)."
    if v >= y: return "yellow", f"entre {y}-{g}."
    return "red", f"< {y} (bajo)."

def _score_between(v, lo, hi, *, green_is_between=True):
    if v is None: return "yellow", "Sin dato."
    if green_is_between:
        return ("green", f"en rango saludable ({lo}-{hi}).") if lo <= v <= hi else ("yellow", f"fuera del rango óptimo ({lo}-{hi}).")
    else:
        return ("yellow", f"en zona intermedia ({lo}-{hi}).") if lo <= v <= hi else ("green", f"fuera de zona intermedia ({lo}-{hi}).")

INDICATORS = {
    # Valoración
    "pe":         {"label":"P/U", "cat":"valuation","weight":2.0, "fn":lambda m:_score_lower_better(_to_float(m.get("pe")),20,35)},
    "forward_pe": {"label":"P/U futuro", "cat":"valuation","weight":1.5,"fn":lambda m:_score_lower_better(_to_float(m.get("forward_pe")),22,30)},
    "peg":        {"label":"PEG (Precio/Utilidad vs Crec.)","cat":"valuation","weight":2.0,"fn":lambda m:_score_lower_better(_to_float(m.get("peg")),1.0,2.0)},
    "ps":         {"label":"P/Ventas","cat":"valuation","weight":1.5,"fn":lambda m:_score_lower_better(_to_float(m.get("ps")),5.0,10.0)},
    "pfcf":       {"label":"P/Flujo de Caja Libre","cat":"valuation","weight":1.5,"fn":lambda m:_score_lower_better(_to_float(m.get("pfcf")),25.0,40.0)},
    "ev_ebitda":  {"label":"EV/EBITDA","cat":"valuation","weight":1.5,"fn":lambda m:_score_lower_better(_to_float(m.get("ev_ebitda")),15.0,22.0)},
    "pb":         {"label":"P/Valor en libros","cat":"valuation","weight":0.5,"fn":lambda m:_score_lower_better(_to_float(m.get("pb")),5.0,10.0)},
    # Crecimiento
    "eps_g_yoy":   {"label":"EPS interanual (TTM) %","cat":"growth","weight":2.0,"fn":lambda m:_score_higher_better(_to_float(m.get("eps_g_yoy")),10.0,3.0)},
    "sales_g_yoy": {"label":"Ventas interanual (TTM) %","cat":"growth","weight":1.5,"fn":lambda m:_score_higher_better(_to_float(m.get("sales_g_yoy")),10.0,5.0)},
    "eps_next_y":  {"label":"EPS próximo año %","cat":"growth","weight":1.5,"fn":lambda m:_score_higher_better(_to_float(m.get("eps_next_y")),15.0,8.0)},
    "eps_next_5y": {"label":"EPS próximos 5 años %","cat":"growth","weight":1.5,"fn":lambda m:_score_higher_better(_to_float(m.get("eps_next_5y")),15.0,10.0)},
    "sales_qoq":   {"label":"Ventas Trim/Trim %","cat":"growth","weight":1.0,"fn":lambda m:_score_higher_better(_to_float(m.get("sales_qoq")),5.0,0.0)},
    "eps_surprise":{"label":"Sorpresa EPS %","cat":"growth","weight":0.5,"fn":lambda m:_score_higher_better(_to_float(m.get("eps_surprise")),0.0,-1.0)},
    # Rentabilidad
    "gross_margin":{"label":"Margen bruto %","cat":"profitability","weight":1.0,"fn":lambda m:_score_higher_better(_to_float(m.get("gross_margin")),60.0,40.0)},
    "op_margin":   {"label":"Margen operativo %","cat":"profitability","weight":1.5,"fn":lambda m:_score_higher_better(_to_float(m.get("op_margin")),20.0,12.0)},
    "net_margin":  {"label":"Margen neto %","cat":"profitability","weight":1.5,"fn":lambda m:_score_higher_better(_to_float(m.get("net_margin")),20.0,10.0)},
    "roe":         {"label":"ROE %","cat":"profitability","weight":1.5,"fn":lambda m:_score_higher_better(_to_float(m.get("roe")),20.0,10.0)},
    "roic":        {"label":"ROIC %","cat":"profitability","weight":1.5,"fn":lambda m:_score_higher_better(_to_float(m.get("roic")),15.0,8.0)},
    "roa":         {"label":"ROA %","cat":"profitability","weight":1.0,"fn":lambda m:_score_higher_better(_to_float(m.get("roa")),10.0,5.0)},
    # Salud
    "current_ratio":{"label":"Razón corriente","cat":"health","weight":1.0,"fn":lambda m:_score_higher_better(_to_float(m.get("current_ratio")),1.2,1.0)},
    "debt_equity": {"label":"Deuda/Patrimonio","cat":"health","weight":1.5,"fn":lambda m:_score_lower_better(_to_float(m.get("debt_equity")),0.5,1.0)},
    # Sentimiento/Propiedad
    "short_float": {"label":"Corto sobre float %","cat":"sentiment","weight":1.0,"fn":lambda m:_score_lower_better(_to_float(m.get("short_float")),2.0,5.0)},
    "inst_own":    {"label":"Propiedad institucional %","cat":"sentiment","weight":0.5,"fn":lambda m:_score_higher_better(_to_float(m.get("inst_own")),60.0,40.0)},
    "insider_trans":{"label":"Transacciones de insiders %","cat":"sentiment","weight":0.5,"fn":lambda m:_score_higher_better(_to_float(m.get("insider_trans")),0.0,-1.0)},
    # Técnica
    "sma20_diff":  {"label":"Distancia a SMA20 %","cat":"technical","weight":1.0,"fn":lambda m:_score_higher_better(_to_float(m.get("sma20_diff")),0.0,-2.0)},
    "sma50_diff":  {"label":"Distancia a SMA50 %","cat":"technical","weight":1.0,"fn":lambda m:_score_higher_better(_to_float(m.get("sma50_diff")),0.0,-3.0)},
    "sma200_diff": {"label":"Distancia a SMA200 %","cat":"technical","weight":1.0,"fn":lambda m:_score_higher_better(_to_float(m.get("sma200_diff")),0.0,-5.0)},
    "rsi":         {"label":"RSI(14)","cat":"technical","weight":0.8,"fn":lambda m:_score_between(_to_float(m.get("rsi")),45.0,55.0,green_is_between=True)},
    # Dividendos
    "div_yield":   {"label":"Rendimiento %","cat":"dividend","weight":0.4,"fn":lambda m:_score_higher_better(_to_float(m.get("div_yield")),1.5,0.5)},
    "payout":      {"label":"Payout %","cat":"dividend","weight":0.6,"fn":lambda m:_score_between(_to_float(m.get("payout")),20.0,60.0,green_is_between=True)},
    "div_growth":  {"label":"Crecimiento dividendo 5A %","cat":"dividend","weight":0.5,"fn":lambda m:_score_higher_better(_to_float(m.get("div_growth")),5.0,2.0)},
    # Analistas
    "recom":       {"label":"Recomendación (1=Compra fuerte)","cat":"analysts","weight":0.8,"fn":lambda m:_score_lower_better(_to_float(m.get("recom")),1.7,2.2)},
    "upside":      {"label":"Upside a precio objetivo %","cat":"analysts","weight":1.2,"fn":lambda m:_score_higher_better(_to_float(m.get("upside")),15.0,5.0)},
}

CATEGORY_WEIGHTS = {
    "valuation": 0.20, "growth": 0.20, "profitability": 0.20, "health": 0.15,
    "technical": 0.10, "sentiment": 0.05, "dividend": 0.05, "analysts": 0.05,
}
CATEGORY_ORDER = ["valuation","growth","profitability","health","technical","sentiment","dividend","analysts"]

CAT_LABEL_ES = {
    "valuation": "valoración",
    "growth": "crecimiento",
    "profitability": "rentabilidad",
    "health": "salud",
    "technical": "técnica",
    "sentiment": "sentimiento",
    "dividend": "dividendos",
    "analysts": "analistas",
}

# =================== EVALUACIÓN ====================
def evaluate_indicators(metrics: Dict[str, Any]) -> Dict[str, List[IndicatorResult]]:
    grouped: Dict[str, List[IndicatorResult]] = {}
    for key, spec in INDICATORS.items():
        label, cat, weight, fn = spec["label"], spec["cat"], spec["weight"], spec["fn"]
        rating, reason = fn(metrics)
        val = _to_float(metrics.get(key))
        grouped.setdefault(cat, []).append(IndicatorResult(label=label, rating=rating, weight=weight,
                                                           reason=reason, value=val, key=key))
    return grouped

def category_score(results: List[IndicatorResult]) -> float:
    if not results: return 0.0
    total_w = sum(abs(r.weight) for r in results) or 1.0
    num = sum({"green":1.0,"yellow":0.0,"red":-1.0}[r.rating] * r.weight for r in results)
    return max(-1.0, min(1.0, num / total_w))

def overall_score(grouped: Dict[str, List[IndicatorResult]]) -> Tuple[float, Dict[str, float]]:
    cat_details = {}
    total = 0.0
    for cat, res in grouped.items():
        cs = category_score(res)
        w = CATEGORY_WEIGHTS.get(cat, 0.0)
        total += cs * w
        cat_details[cat] = cs
    total = max(-1.0, min(1.0, total))
    return total, cat_details

def verdict_from_score(s: float) -> str:
    pct = (s + 1) * 50
    if pct >= 65: return "Comprar/Acumular (largo plazo)"
    if pct >= 55: return "Comprar parcial en retrocesos"
    if pct >= 45: return "Mantener / En observación"
    if pct >= 35: return "Esperar mejor precio"
    return "Evitar por ahora"

def mini_conclusion(grouped, total_score):
    greens = sum(1 for cat in grouped.values() for r in cat if r.rating == "green")
    yellows = sum(1 for cat in grouped.values() for r in cat if r.rating == "yellow")
    reds = sum(1 for cat in grouped.values() for r in cat if r.rating == "red")
    pct = round((total_score + 1) * 50, 1)
    cat_scores = [(cat, category_score(res)) for cat, res in grouped.items()]
    cat_scores.sort(key=lambda x: x[1], reverse=True)
    es = lambda c: CAT_LABEL_ES.get(c, c)
    mejores = ", ".join(f"{es(c)} {round(s*100)}%" for c, s in cat_scores[:3])
    peores  = ", ".join(f"{es(c)} {round(s*100)}%" for c, s in cat_scores[-2:])
    return (f"Puntaje total: {pct}/100. Puntos fuertes: {mejores}. Áreas a vigilar: {peores}.",
            greens, yellows, reds)

def analyze_stock(name: str, metrics: Dict[str, Any]) -> Dict[str, Any]:
    grouped = evaluate_indicators(metrics)
    total, cat_details = overall_score(grouped)
    verdict = verdict_from_score(total)
    conclusion, g, y, r = mini_conclusion(grouped, total)

    detalles = []
    for cat_key in CATEGORY_ORDER:
        for ritem in grouped.get(cat_key, []):
            detalles.append({
                "cat_key": cat_key,
                "cat_es": CAT_LABEL_ES.get(cat_key, cat_key),
                "key": ritem.key,
                "indicador_es": ritem.label,
                "valor": ritem.value,
                "semaforo": EMOJI[ritem.rating],
                "semaforo_score": NUMS[ritem.rating],
                "peso": ritem.weight,
                "razon": ritem.reason,
            })

    return {
        "ticker": name,
        "puntaje_0_100": round((total + 1) * 50, 1),
        "veredicto": verdict,
        "conclusion": conclusion,
        "counts": {"greens": g, "yellows": y, "reds": r},
        "categorias": {cat: round(score*100, 1) for cat, score in cat_details.items()},
        "detalles": detalles,
    }

# ============== FINVIZ: scraping + ANTI-429 ==============
FINVIZ_MAP = {
    "P/E":"pe", "Forward P/E":"forward_pe", "PEG":"peg", "P/S":"ps", "P/FCF":"pfcf",
    "EV/EBITDA":"ev_ebitda", "P/B":"pb",
    "EPS Y/Y TTM":"eps_g_yoy", "Sales Y/Y TTM":"sales_g_yoy",
    "EPS next Y":"eps_next_y", "EPS next 5Y":"eps_next_5y",
    "Sales Q/Q":"sales_qoq", "EPS/Sales Surpr.":"eps_surprise",
    "Gross Margin":"gross_margin", "Oper. Margin":"op_margin", "Profit Margin":"net_margin",
    "ROE":"roe", "ROIC":"roic", "ROA":"roa",
    "Current Ratio":"current_ratio", "Debt/Eq":"debt_equity",
    "Short Float":"short_float", "Inst Own":"inst_own", "Insider Trans":"insider_trans",
    "SMA20":"sma20_diff", "SMA50":"sma50_diff", "SMA200":"sma200_diff",
    "RSI (14)":"rsi",
    "Dividend Est.":"div_yield", "Payout":"payout", "Dividend Gr. 3/5Y":"div_growth",
    "Recom":"recom", "Target Price":"target_price", "Price":"price",
}
FINVIZ_LABEL_BY_KEY = {v: k for k, v in FINVIZ_MAP.items()}
FINVIZ_LABEL_BY_KEY["upside"] = "Upside to Target %"

def make_session() -> requests.Session:
    from requests.adapters import HTTPAdapter
    try:
        from urllib3.util.retry import Retry
        retry = Retry(
            total=MAX_RETRIES,
            backoff_factor=1.2,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
    except TypeError:
        from urllib3.util.retry import Retry  # type: ignore
        retry = Retry(
            total=MAX_RETRIES,
            backoff_factor=1.2,
            status_forcelist=(429, 500, 502, 503, 504),
            method_whitelist=frozenset(["GET"]),
            raise_on_status=False,
        )
    s = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

_last_request_at = 0.0
def _polite_get(url: str, session: requests.Session) -> str:
    global _last_request_at
    wait = max(0, RATE_LIMIT_SEC - (time.time() - _last_request_at))
    if wait > 0: time.sleep(wait)
    headers = {
        "User-Agent": random.choice(UA_LIST),
        "Referer": "https://finviz.com/",
        "Accept-Language": "es-MX,es;q=0.9,en;q=0.7",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        r = session.get(url, headers=headers, timeout=25)
        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            if ra and str(ra).isdigit():
                sleep_s = int(ra) + random.uniform(1, 3)
            else:
                sleep_s = (2 ** attempt) + random.uniform(0.5, 1.5)
            time.sleep(sleep_s); continue
        r.raise_for_status()
        _last_request_at = time.time()
        return r.text
    raise requests.HTTPError("Too many requests (429) después de reintentos.")

def _cache_path_for(ticker: str) -> Path:
    today = dt.date.today().isoformat()
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{ticker}_{today}.html"

def fetch_finviz_html(ticker: str, session: requests.Session) -> str:
    fp = _cache_path_for(ticker)
    if fp.exists():
        age_hours = (time.time() - fp.stat().st_mtime) / 3600.0
        if age_hours <= CACHE_TTL_HOURS:
            try: return fp.read_text(encoding="utf-8", errors="ignore")
            except Exception: pass
    url = f"https://finviz.com/quote.ashx?t={ticker}"
    html = _polite_get(url, session)
    try: fp.write_text(html, encoding="utf-8", errors="ignore")
    except Exception: pass
    return html

def fetch_finviz_metrics(ticker: str, session: Optional[requests.Session] = None) -> Dict[str, Any]:
    session = session or make_session()
    html = fetch_finviz_html(ticker, session)
    dfs = pd.read_html(io.StringIO(html))
    snapshot_df = None
    for df in dfs:
        if ("P/E" in df.values) or ("Index" in df.values):
            snapshot_df = df; break
    if snapshot_df is None:
        raise RuntimeError("No se encontró la tabla de snapshot en Finviz.")
    kv = {}
    for _, row in snapshot_df.iterrows():
        vals = [str(x) for x in row.tolist() if str(x) != "nan"]
        for i in range(0, len(vals)-1, 2):
            key = vals[i].strip(); val = vals[i+1].strip()
            kv[key] = val
    metrics: Dict[str, Any] = {}
    for k_f, k_i in FINVIZ_MAP.items():
        if k_f not in kv: continue
        raw = kv[k_f]
        if k_i == "div_yield":   metrics[k_i] = _first_percent(raw); continue
        if k_i == "div_growth":  metrics[k_i] = _last_percent(raw);  continue
        if k_i == "eps_surprise":metrics[k_i] = _first_percent(raw); continue
        if k_i in ("target_price", "price"):
            try: metrics[k_i] = float(re.findall(r"-?\d+\.?\d*", raw.replace(",",""))[0])
            except: metrics[k_i] = None
            continue
        metrics[k_i] = _to_float(raw)
    tp, pr = metrics.get("target_price"), metrics.get("price")
    metrics["upside"] = ((tp - pr) / pr) * 100.0 if (tp and pr) else None
    return metrics

# ================ EXCEL + HISTÓRICO ===================
def save_results_to_excel(ticker_metrics: Dict[str, Dict[str, Any]], outfile: str, gui_hooks=None) -> str:
    set_status = (gui_hooks or {}).get("set_status", lambda s: None)
    set_prog   = (gui_hooks or {}).get("set_progress", lambda x: None)

    results = {}
    steps_total = max(1, len(ticker_metrics) + 4)
    step = 0

    # Analizar por ticker
    for t, m in ticker_metrics.items():
        step += 1; set_status(f"Analizando {t}..."); set_prog(step/steps_total*100)
        results[t] = analyze_stock(t, m)

    # -------- Resumen
    set_status("Generando hoja Resumen...")
    today = dt.date.today().isoformat()
    rows = []
    for t, r in results.items():
        base = {
            "Fecha": today,
            "Ticker": r["ticker"],
            "Puntaje (0-100)": r["puntaje_0_100"],
            "Veredicto": r["veredicto"],
            "Conclusión": r["conclusion"],
            "Semáforo": f'{r["counts"]["greens"]}🟢 / {r["counts"]["yellows"]}🟡 / {r["counts"]["reds"]}🔴',
        }
        # categorías
        for cat, val in sorted(r["categorias"].items()):
            base[f"{CAT_LABEL_ES.get(cat, cat).capitalize()} %"] = val

        # --- Price / Target / (Target/Price - 1)*100 ---
        tm = ticker_metrics.get(t, {}) or {}
        price  = tm.get("price")
        target = tm.get("target_price")
        up_pct = None
        if price not in (None, 0) and target is not None:
            try:
                up_pct = ((target / price) - 1) * 100
            except Exception:
                up_pct = None
        base["Price"] = price
        base["Target Price"] = target
        base[RATIO_COL] = up_pct
        # ------------------------------------------------

        rows.append(base)

    df_summary = pd.DataFrame(rows).sort_values(by="Puntaje (0-100)", ascending=False)
    df_summary.insert(2, "Rank", range(1, len(df_summary) + 1))

    # Garantiza existencia y tipo numérico de la columna ratio
    if RATIO_COL not in df_summary.columns:
        df_summary[RATIO_COL] = None
    df_summary[RATIO_COL] = pd.to_numeric(df_summary[RATIO_COL], errors="coerce")

    # -------- Histórico CSV (solo métricas principales)
    set_status("Actualizando Ranking_Historico...")
    keep_cols = ["Fecha","Ticker","Rank","Puntaje (0-100)","Veredicto"] + [c for c in df_summary.columns if c.endswith("%")]
    hist_rows = df_summary[keep_cols].copy()
    if os.path.exists(HISTORY_CSV):
        old = pd.read_csv(HISTORY_CSV)
        allh = pd.concat([old, hist_rows], ignore_index=True)
    else:
        allh = hist_rows
    allh = allh.drop_duplicates(subset=["Fecha","Ticker"], keep="last")
    allh.to_csv(HISTORY_CSV, index=False)

    # -------- Escribir Excel
    step += 1; set_prog(step/steps_total*100)
    engine = "xlsxwriter"
    try:
        import xlsxwriter  # noqa: F401
    except Exception:
        engine = "openpyxl"

    with pd.ExcelWriter(outfile, engine=engine) as writer:
        # --- Resumen
        df_summary.to_excel(writer, sheet_name="Resumen", index=False)

        if writer.engine == "xlsxwriter":
            wb  = writer.book
            ws  = writer.sheets["Resumen"]

            ws.set_column("A:A", 12)   # Fecha
            ws.set_column("B:B", 10)   # Ticker
            ws.set_column("C:C", 6)    # Rank
            ws.set_column("D:D", 14)   # Puntaje
            ws.set_column("E:E", 28)   # Veredicto
            ws.set_column("F:F", 90)   # Conclusión
            ws.set_column("G:G", 18)   # Semáforo

            header_fmt = wb.add_format({"bold": True, "bg_color": "#F2F2F2", "border":1})
            for col in range(df_summary.shape[1]):
                ws.write(0, col, df_summary.columns[col], header_fmt)
            ws.freeze_panes(1, 2)

            last_row = len(df_summary)

            # Semáforo visual (Resumen): números + punto (●) coloreados por segmento


            green_num  = wb.add_format({"font_color":"#00B050","bold":True})  # números verde


            yellow_num = wb.add_format({"font_color":"#FFC000","bold":True})  # números amarillo


            red_num    = wb.add_format({"font_color":"#C00000","bold":True})  # números rojo


            green_dot  = wb.add_format({"font_color":"#00B050","font_size":14})              # ● verde


            yellow_dot = wb.add_format({"font_color":"#FFC000","font_size":14})              # ● amarillo


            red_dot    = wb.add_format({"font_color":"#C00000","font_size":14})              # ● rojo


            ws.set_column("G:G", 26)


            sem_col = df_summary.columns.get_loc("Semáforo")


            for i, tkr in enumerate(df_summary["Ticker"].tolist(), start=1):  # start=1 por encabezado


                cnt = results.get(tkr, {}).get("counts", {})


                g = int(cnt.get("greens", 0)); y = int(cnt.get("yellows", 0)); r = int(cnt.get("reds", 0))

                ws.set_row(i, 15)  # altura para círculos grandes


                # Escribimos: NN ● / NN ● / NN ● con cada segmento en su color


                ws.write_rich_string(i, sem_col,


                    " ",  # no usar string vacío (evita el warning de xlsxwriter)


                    green_num,  f"{g} ",


                    green_dot,  "⬤",


                    " / ",


                    yellow_num, f"{y} ",


                    yellow_dot, "⬤",


                    " / ",


                    red_num,    f"{r} ",


                    red_dot,    "⬤",


                )
            # Escala de color para Puntaje y categorías
            ws.conditional_format(1, 3, last_row, 3, {
                "type":"3_color_scale",
                "min_type":"num","min_value":0,"min_color":"#F8696B",
                "mid_type":"num","mid_value":50,"mid_color":"#FFEB84",
                "max_type":"num","max_value":100,"max_color":"#63BE7B",
            })
            for col_idx, name in enumerate(df_summary.columns):
                if name.endswith("%"):
                    ws.set_column(col_idx, col_idx, 14)
                    ws.conditional_format(1, col_idx, last_row, col_idx, {
                        "type":"3_color_scale",
                        "min_type":"num","min_value":-100,"min_color":"#F8696B",
                        "mid_type":"num","mid_value":0,"mid_color":"#FFEB84",
                        "max_type":"num","max_value":100,"max_color":"#63BE7B",
                    })

            # Formato para Price / Target / Ratio
            idx_price  = df_summary.columns.get_loc("Price")
            idx_target = df_summary.columns.get_loc("Target Price")
            idx_ratio  = df_summary.columns.get_loc(RATIO_COL)

            money_fmt = wb.add_format({"num_format":"#,##0.00"})
            ratio_fmt = wb.add_format({"num_format":"0.00"})

            ws.set_column(idx_price,  idx_price,  12, money_fmt)
            ws.set_column(idx_target, idx_target, 12, money_fmt)
            ws.set_column(idx_ratio,  idx_ratio,  20, ratio_fmt)

            # Escala de color: -100 (rojo) → 0 (amarillo) → 100 (verde)
            ws.conditional_format(1, idx_ratio, last_row, idx_ratio, {
                "type":"3_color_scale",
                "min_type":"num","min_value":-100,"min_color":"#F8696B",
                "mid_type":"num","mid_value":0,"mid_color":"#FFEB84",
                "max_type":"num","max_value":100,"max_color":"#63BE7B",
            })

        # --- Detalles por ticker
        for t, r in results.items():
            det_rows = []
            for item in r["detalles"]:
                key_int = item["key"]
                en_name = FINVIZ_LABEL_BY_KEY.get(key_int, item["indicador_es"])
                es_name = INDICATORS.get(key_int, {}).get("label", item["indicador_es"])
                det_rows.append({
                    "Categoría": item["cat_es"],
                    "Indicator EN": en_name,
                    "Indicador ES": es_name,
                    "Valor": item["valor"],
                    "Semáforo": item["semaforo"],
                    "SemaforoScore": item["semaforo_score"],
                    "Peso": item["peso"],
                    "Razón": item["razon"],
                    "_cat_key": item["cat_key"],
                })

            cat_rank = {k:i for i,k in enumerate(CATEGORY_ORDER)}
            det_rows.sort(key=lambda d: (cat_rank.get(d["_cat_key"], 999), -d["Peso"]))
            df_det = pd.DataFrame(det_rows, columns=[
                "Categoría","Indicator EN","Indicador ES","Valor",
                "Semáforo","SemaforoScore","Peso","Razón","_cat_key"
            ])
            df_det.drop(columns=["_cat_key"], inplace=True)

            sheet_name = (f"{t}")[:31]
            df_det.to_excel(writer, sheet_name=sheet_name, index=False)

            if writer.engine == "xlsxwriter":
                ws = writer.sheets[sheet_name]; wb = writer.book
                ws.set_column("A:A", 16)
                ws.set_column("B:B", 34)
                ws.set_column("C:C", 34)
                ws.set_column("D:D", 12)
                ws.set_column("E:E", 11)
                ws.set_column("F:F", 4)
                ws.set_column("G:G", 6)
                ws.set_column("H:H", 64)

                header_fmt = wb.add_format({"bold": True, "bg_color": "#F2F2F2", "border":1})
                for col in range(df_det.shape[1]):
                    ws.write(0, col, df_det.columns[col], header_fmt)
                ws.freeze_panes(1, 0)

                last_row = len(df_det)
                ws.conditional_format(1, 5, last_row, 5, {
                    "type":"icon_set",
                    "icon_style":"3_traffic_lights"
                })
                green_fmt  = wb.add_format({"font_color":"#228B22"})
                yellow_fmt = wb.add_format({"font_color":"#B8860B"})
                red_fmt    = wb.add_format({"font_color":"#B22222"})
                ws.conditional_format(1, 4, last_row, 4, {"type":"text","criteria":"containing","value":"🟢","format":green_fmt})
                ws.conditional_format(1, 4, last_row, 4, {"type":"text","criteria":"containing","value":"🟡","format":yellow_fmt})
                ws.conditional_format(1, 4, last_row, 4, {"type":"text","criteria":"containing","value":"🔴","format":red_fmt})
                ws.set_column(5, 5, None, None, {"hidden": True})

        # --- Ranking histórico
        hist_all = pd.read_csv(HISTORY_CSV)
        hist_all.sort_values(by=["Fecha","Puntaje (0-100)"], ascending=[False, False], inplace=True)
        hist_all.to_excel(writer, sheet_name="Ranking_Historico", index=False)
        if writer.engine == "xlsxwriter":
            ws = writer.sheets["Ranking_Historico"]; wb = writer.book
            ws.set_column("A:A", 12); ws.set_column("B:B", 10); ws.set_column("C:C", 6)
            ws.set_column("D:D", 14); ws.set_column("E:E", 24); ws.set_column("F:Z", 14)
            header_fmt = wb.add_format({"bold": True, "bg_color": "#F2F2F2", "border":1})
            for col in range(hist_all.shape[1]): ws.write(0, col, hist_all.columns[col], header_fmt)
            last_row = len(hist_all)
            ws.conditional_format(1, 3, last_row, 3, {
                "type":"3_color_scale",
                "min_type":"num","min_value":0,"min_color":"#F8696B",
                "mid_type":"num","mid_value":50,"mid_color":"#FFEB84",
                "max_type":"num","max_value":100,"max_color":"#63BE7B",
            })

    return outfile

# ================== GUI DE PROGRESO ==================
def read_tickers() -> List[str]:
    path = Path("tickers.txt")
    if not path.exists():
        return DEFAULT_TICKERS
    text = path.read_text(encoding="utf-8", errors="ignore")
    raw = re.split(r"[\s,;]+", text.strip().upper())
    return [t for t in raw if t]



def run_with_gui():
    import tkinter as tk
    from tkinter import ttk, messagebox, simpledialog
    from pathlib import Path

    # --- Estado inicial de licencia / trial ---
    state = _load_license_state()
    machine_id = get_machine_id()
    state["machine_id"] = machine_id
    _save_license_state(state)

    def current_status():
        # Siempre leer del disco para reflejar activaciones recientes
        s = _load_license_state()
        key = s.get("license_key")
        valid, ltype = is_license_valid_for_this_machine(key, machine_id)
        # _trial_days_left actualiza last_run/start_date si es primera vez
        days_left = max(0, _trial_days_left(s))
        return valid, ltype, days_left

    # --- Ventana principal (NO inicia hasta que se haga clic en Iniciar análisis) ---
    root = tk.Tk()
    root.title("PugaX_Trade - Análisis Financiero")
    root.geometry("840x500")
    root.resizable(False, False)

    title = tk.Label(root, text="Analizador Financiero", font=("Segoe UI", 16, "bold"))
    title.pack(pady=(12, 6))

    status_var = tk.StringVar(value="Listo para iniciar.")
    status_lbl = tk.Label(root, textvariable=status_var, font=("Segoe UI", 11))
    status_lbl.pack()

    progress = ttk.Progressbar(root, orient="horizontal", length=800, mode="determinate", maximum=100)
    progress.pack(pady=(8, 10))

    # Línea de info de licencia / trial
    lic_status_var = tk.StringVar(value="")
    info_lbl = tk.Label(root, textvariable=lic_status_var, font=("Segoe UI", 10, "italic"))
    info_lbl.pack(pady=(0, 6))

    # Bitácora
    textbox = tk.Text(root, height=12, width=104)
    textbox.pack(padx=10)
    textbox.insert("end", "Bitácora:\n")

    def set_status(msg: str):
        status_var.set(msg)
        textbox.insert("end", f"- {msg}\n")
        textbox.see("end")
        root.update_idletasks()

    def set_progress(pct: float):
        progress["value"] = max(0, min(100, pct))
        root.update_idletasks()

    # --- Botones de acción ---
    btns = tk.Frame(root)
    btns.pack(pady=8)

    btn_license = ttk.Button(btns, text="Ingresar clave de licencia")
    btn_start   = ttk.Button(btns, text="Iniciar análisis")

    btn_license.grid(row=0, column=0, padx=6)
    btn_start.grid(row=0, column=1, padx=6)

    running = {"flag": False}  # mutable para ser modificado dentro de funciones internas

    def refresh_license_ui():
        valid, ltype, days_left = current_status()
        if valid:
            lic_status_var.set(f"✅ Sistema activado ({ltype}).  Clave de máquina: {machine_id}")
            btn_start.state(["!disabled"])
        else:
            if days_left > 0:
                lic_status_var.set(f"🕒 Prueba activa: {days_left} día(s) restante(s).  Clave de máquina: {machine_id}")
                btn_start.state(["!disabled"])
            else:
                lic_status_var.set(f"⛔ Prueba vencida. Ingresa una licencia para activar.  Clave de máquina: {machine_id}")
                btn_start.state(["disabled"])

    def on_enter_license():
        key = simpledialog.askstring(
            "Ingresar licencia",
            "Pega tu licencia (acepta Licencia Universal o licencia por máquina PX-xxxxx):",
            parent=root, show="*"
        )
        if key is None:
            return
        k = (key or "").strip()
        if not k:
            messagebox.showerror("Error", "La licencia no puede estar vacía.")
            return
        valid, ltype = is_license_valid_for_this_machine(k, machine_id)
        if valid:
            s = _load_license_state()
            s["license_key"] = k
            s["license_type"] = ltype
            _save_license_state(s)
            messagebox.showinfo("Activado", f"Sistema activado correctamente ({ltype}).")
            set_status("Licencia verificada y guardada.")
            refresh_license_ui()
        else:
            messagebox.showerror("Licencia inválida", "La licencia no es correcta para esta máquina.")

    def run_worker():
        try:
            from threading import Thread
            import random, time

            def worker():
                try:
                    tickers = read_tickers()
                    set_status(f"Tickers detectados: {len(tickers)}")
                    session = make_session()

                    metrics_by_ticker = {}
                    total_steps = max(1, len(tickers) * 2 + 4)
                    done = 0

                    for tk_symbol in tickers:
                        done += 1; set_status(f"Descargando datos de {tk_symbol}…")
                        set_progress(done / total_steps * 100)
                        try:
                            metrics_by_ticker[tk_symbol] = fetch_finviz_metrics(tk_symbol, session=session)
                        except Exception as e:
                            set_status(f"[WARN] {tk_symbol}: {e}")
                            metrics_by_ticker[tk_symbol] = {}
                        time.sleep(random.uniform(RATE_LIMIT_SEC, RATE_LIMIT_SEC + 2))
                        done += 1; set_status(f"Preparando análisis de {tk_symbol}…")
                        set_progress(done / total_steps * 100)

                    set_status("Generando Excel y Ranking_Historico…")
                    save_results_to_excel(metrics_by_ticker, OUTFILE, gui_hooks={"set_status": set_status, "set_progress": set_progress})

                    set_status("Proceso concluido con éxito ✅")
                    set_progress(100)
                    messagebox.showinfo("Listo", f"Proceso concluido.\nArchivo: {str(Path(OUTFILE).resolve())}")
                except Exception as e:
                    set_status("Error durante la ejecución.")
                    import traceback as _tb
                    textbox.insert("end", _tb.format_exc())
                    messagebox.showerror("Error", str(e))
                finally:
                    running["flag"] = False
                    btn_start.state(["!disabled"])

            Thread(target=worker, daemon=True).start()
        except Exception as ex:
            messagebox.showerror("Error", str(ex))
            running["flag"] = False
            btn_start.state(["!disabled"])

    def on_start():
        valid, ltype, days_left = current_status()
        if not valid and days_left <= 0:
            messagebox.showwarning("Licencia requerida", "La prueba ha vencido. Ingresa una licencia para continuar.")
            return
        if running["flag"]:
            return
        running["flag"] = True
        btn_start.state(["disabled"])
        set_status("Iniciando análisis…")
        set_progress(0)
        run_worker()

    btn_license.configure(command=on_enter_license)
    btn_start.configure(command=on_start)

    refresh_license_ui()
    root.mainloop()

# ======================= MAIN ========================
if __name__ == "__main__":
    run_with_gui()
