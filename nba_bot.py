# -*- coding: utf-8 -*-
"""
Bot NBA v2 - Multi-usuario (listo para Google Colab)

USO EN COLAB
  Celda 1:  !pip -q install fastapi uvicorn apscheduler pyngrok
  Secrets (icono de llave, panel izquierdo; activa "Acceso desde el notebook"):
      INFER_API_KEY, THEODDS_API_KEY, ADMIN_PASS, NGROK_TOKEN (opcional pero recomendado)
  Sube este archivo al panel Files y en otra celda:  %run nba_bot.py
  (la celda se queda "ejecutando": eso mantiene vivo el bot)

También funciona fuera de Colab con variables de entorno.
"""
import os, re, sys, math, time, html, secrets, sqlite3, threading
from contextlib import contextmanager, asynccontextmanager
from datetime import datetime, timezone, timedelta

import requests
from fastapi import FastAPI, HTTPException, Header, Depends, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler
import uvicorn


# =========================
# CONFIG (secrets > env > default)
# =========================
def secret(name, default=""):
    try:
        from google.colab import userdata  # solo existe en Colab
        v = userdata.get(name)
        if v:
            return v
    except Exception:
        pass
    return os.environ.get(name, default)


INFER_API_KEY = secret("INFER_API_KEY")
THEODDS_API_KEY = secret("THEODDS_API_KEY")
NGROK_TOKEN = secret("NGROK_TOKEN")
_admin = secret("ADMIN_PASS")
ADMIN_GENERATED = not _admin
ADMIN_PASS = _admin or secrets.token_urlsafe(12)

PORT = int(secret("PORT", "8000"))
DB = secret("DB_PATH", "nba_v2.db")      # en Colab puedes apuntar a /content/drive/MyDrive/nba_v2.db
MAX_USERS = int(secret("MAX_USERS", "50"))

POLL_INTERVAL = 120          # seg entre revisiones
MIN_DIFF = 7.0               # diferencia mínima (pts) proyección vs línea
NO_ANCHOR_EXTRA = 2.0        # diff extra exigida si no hay línea pregame
MIN_EV = 0.03                # EV mínimo (3%) para enviar señal
SIGMA_K = 2.6                # sd del total restante ≈ SIGMA_K * sqrt(min restantes)
SHRINK = 0.5                 # encoge la prob. del modelo hacia 50% (modelo sin calibrar)
MAX_ODDS_AGE_MIN = 5.0       # descarta líneas más viejas que esto
GT_MARGIN, GT_MINUTES = 18, 6.0   # garbage time: margen > 18 y < 6 min en Q4
PREGAME_INTERVAL_MIN = 30    # cada cuánto intentar capturar líneas pregame
PREGAME_WINDOW_H = 3         # solo captura para juegos que empiezan en <= 3 h
VOID_AFTER_H = 36            # señales sin resultado tras 36 h -> VOID

ESPN_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
ESPN_SUMMARY = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary"

UID_RE = re.compile(r"^[0-9a-f]{32}$")
TOKEN_RE = re.compile(r"^\d{6,12}:[A-Za-z0-9_-]{30,50}$")
CHAT_RE = re.compile(r"^(-?\d{1,20}|@[A-Za-z0-9_]{4,64})$")
EVENT_RE = re.compile(r"^\d{1,20}$")


# =========================
# UTILIDADES
# =========================
def now_iso():
    return datetime.now(timezone.utc).isoformat()


def parse_iso(s):
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def ttl_cache(seconds):
    """Cachea resultados no vacíos durante `seconds`."""
    def deco(fn):
        lock, store = threading.Lock(), {}

        def wrapper(*a):
            now = time.time()
            with lock:
                hit = store.get(a)
                if hit and now - hit[0] < seconds:
                    return hit[1]
            val = fn(*a)
            if val:
                with lock:
                    store[a] = (now, val)
            return val
        return wrapper
    return deco


def team_key(name):
    """Último token del nombre: 'LA Clippers' == 'Los Angeles Clippers' -> 'clippers'."""
    s = re.sub(r"[^a-z0-9 ]", "", (name or "").lower()).split()
    return s[-1] if s else ""


def norm_cdf(z):
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def parse_clock(clock):
    """Minutos restantes del cuarto. '5:32' -> 5.53 ; '9.8' (segundos) -> 0.16. None si no se puede."""
    if not clock:
        return None
    try:
        if ":" in clock:
            mm, ss = clock.split(":")
            return int(mm) + float(ss) / 60.0
        return float(clock) / 60.0   # sin ':' ESPN muestra segundos
    except Exception:
        return None


# =========================
# BASE DE DATOS
# =========================
@contextmanager
def db():
    con = sqlite3.connect(DB, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=30000")
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def init_db():
    with db() as con:
        con.execute("PRAGMA journal_mode=WAL")
        con.executescript("""
        CREATE TABLE IF NOT EXISTS users(
            id TEXT PRIMARY KEY,
            tg_token TEXT DEFAULT '',
            tg_chat_id TEXT DEFAULT '',
            active INTEGER DEFAULT 0,
            auto_mode INTEGER DEFAULT 0,
            is_blocked INTEGER DEFAULT 0,
            created_at TEXT);
        CREATE TABLE IF NOT EXISTS selection(
            user_id TEXT, event_id TEXT, matchup TEXT,
            active INTEGER DEFAULT 1, sent INTEGER DEFAULT 0,
            PRIMARY KEY(user_id, event_id));
        CREATE TABLE IF NOT EXISTS signals(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT, event_id TEXT, matchup TEXT, decision TEXT,
            line REAL, cuota REAL, proyeccion REAL, diff REAL,
            confianza INTEGER, ev REAL, total INTEGER, period INTEGER,
            created_at TEXT, status TEXT DEFAULT 'PENDING', final_total INTEGER,
            message TEXT, notified INTEGER DEFAULT 0,
            UNIQUE(user_id, event_id));
        CREATE TABLE IF NOT EXISTS anchors(
            event_id TEXT PRIMARY KEY, matchup TEXT, initial_line REAL, captured_at TEXT);
        CREATE TABLE IF NOT EXISTS snapshots(
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, event_id TEXT, matchup TEXT,
            period INTEGER, clock TEXT, minutes REAL, total INTEGER,
            line REAL, line_age REAL, anchor REAL, proj REAL, diff REAL, p REAL, ev REAL);
        CREATE INDEX IF NOT EXISTS ix_sig_status ON signals(status);
        """)


# =========================
# ESPN
# =========================
def _score(comp, linescores):
    try:
        return int(comp.get("score"))
    except Exception:
        pass
    t = 0
    for q in linescores:
        try:
            t += int(float(q.get("value", 0)))
        except Exception:
            pass
    return t


@ttl_cache(20)
def espn_games():
    try:
        r = requests.get(ESPN_URL, timeout=15)
        r.raise_for_status()
        out = []
        for ev in r.json().get("events", []):
            comp = ev.get("competitions", [{}])[0]
            cs = comp.get("competitors", [])
            if len(cs) < 2:
                continue
            home = next((c for c in cs if c.get("homeAway") == "home"), cs[0])
            away = next((c for c in cs if c.get("homeAway") == "away"), cs[1])
            ht = home.get("team", {}).get("displayName", "Home")
            at = away.get("team", {}).get("displayName", "Away")
            st = ev.get("status", {})
            tp = st.get("type", {})
            q_home, q_away = home.get("linescores", []), away.get("linescores", [])
            hs, as_ = _score(home, q_home), _score(away, q_away)
            out.append({
                "id": str(ev.get("id", "")),
                "matchup": f"{at} vs {ht}",
                "key": frozenset({team_key(at), team_key(ht)}),
                "start": ev.get("date", ""),
                "status": f"{tp.get('description', '')} | {tp.get('detail', '')}".strip(" |"),
                "state": tp.get("state", ""),
                "period": int(st.get("period", 0) or 0),
                "clock": st.get("displayClock", "") or "",
                "total": hs + as_,
                "home_score": hs,
                "away_score": as_,
                "q_home": q_home,
                "q_away": q_away,
            })
        return out
    except Exception as e:
        print(f"[espn] {e}")
        return []


def espn_event_result(event_id):
    """(state, total) de un partido concreto, aunque ya no esté en el scoreboard de hoy."""
    try:
        r = requests.get(ESPN_SUMMARY, params={"event": event_id}, timeout=15)
        if r.status_code != 200:
            return None
        comp = r.json().get("header", {}).get("competitions", [{}])[0]
        state = comp.get("status", {}).get("type", {}).get("state", "")
        total = sum(int(c.get("score", 0) or 0) for c in comp.get("competitors", []))
        return state, total
    except Exception as e:
        print(f"[espn summary] {e}")
        return None


# =========================
# CUOTAS
# =========================
def odds_totals_infer():
    out = {}
    if not INFER_API_KEY:
        return out
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Authorization": f"Bearer {INFER_API_KEY}",
        }
        r = requests.get("https://api.infersports.dev/v1/events/live", headers=headers, timeout=15)
        if r.status_code != 200:
            print(f"[infer] HTTP {r.status_code}")
            return out
        for ev in r.json().get("data", []):
            try:  # un fallo en un evento ya no tumba a los demás
                if ev.get("sport") != "basketball" or ev.get("league", {}).get("name") != "NBA":
                    continue
                home = ev["home_team"]["name"].split(".")[-1].strip()
                away = ev["away_team"]["name"].split(".")[-1].strip()
                r2 = requests.get(f"https://api.infersports.dev/v1/events/{ev['id']}/odds?format=decimal",
                                  headers=headers, timeout=15)
                if r2.status_code != 200:
                    continue
                best = None
                for odd in r2.json().get("odds", []):
                    if not (odd.get("market_type") == "totals" and odd.get("period") == "full_time"
                            and odd.get("status") == "open"):
                        continue
                    t = parse_iso(odd.get("as_of") or "")
                    pr = odd.get("prices") or {}
                    if not t or odd.get("line") is None or not pr.get("over") or not pr.get("under"):
                        continue
                    if best is None or t > best[0]:
                        best = (t, float(odd["line"]), float(pr["over"]), float(pr["under"]))
                if not best:
                    continue
                age = (datetime.now(timezone.utc) - best[0]).total_seconds() / 60.0
                if age > MAX_ODDS_AGE_MIN:
                    print(f"[infer] línea vieja ({age:.1f} min) {away} vs {home}, se ignora")
                    continue
                out[frozenset({team_key(away), team_key(home)})] = {
                    "line": best[1], "over": best[2], "under": best[3],
                    "bookmaker": "InferSports", "age_min": age}
            except Exception as e:
                print(f"[infer evento] {e}")
    except Exception as e:
        print(f"[odds infer error] {e}")
    return out


@ttl_cache(90)
def odds_totals_theoddsapi():
    out = {}
    if not THEODDS_API_KEY:
        return out
    try:
        r = requests.get("https://api.the-odds-api.com/v4/sports/basketball_nba/odds/",
                         params={"apiKey": THEODDS_API_KEY, "regions": "us", "markets": "totals",
                                 "oddsFormat": "decimal"}, timeout=15)
        if r.status_code != 200:
            print(f"[TheOddsAPI] HTTP {r.status_code}")
            return out
        print(f"[TheOddsAPI] requests restantes: {r.headers.get('x-requests-remaining', '?')}")
        for m in r.json():
            key = frozenset({team_key(m["away_team"]), team_key(m["home_team"])})
            bms = m.get("bookmakers") or []
            bm = next((b for b in bms if b.get("key") == "pinnacle"), None) or (bms[0] if bms else None)
            if not bm:
                continue
            mk = next((x for x in bm.get("markets", []) if x.get("key") == "totals"), None)
            if not mk:
                continue
            line = over = under = None
            for o in mk.get("outcomes", []):
                if "point" in o and "price" in o:
                    line = float(o["point"])
                    nm = o.get("name", "").lower()
                    if nm == "over":
                        over = float(o["price"])
                    elif nm == "under":
                        under = float(o["price"])
            if line is None:
                continue
            t = parse_iso(mk.get("last_update") or bm.get("last_update") or "")
            age = (datetime.now(timezone.utc) - t).total_seconds() / 60.0 if t else 0.0
            out[key] = {"line": line, "over": over, "under": under,
                        "bookmaker": bm.get("title", "TheOddsAPI"), "age_min": age}
    except Exception as e:
        print(f"[TheOddsAPI error] {e}")
    return out


def collect_odds(valid_games):
    """event_id -> línea. InferSports primero, TheOddsAPI de respaldo. Solo líneas frescas."""
    odds = {}
    infer = odds_totals_infer()
    for g in valid_games:
        if g["key"] in infer:
            odds[g["id"]] = infer[g["key"]]
    missing = [g for g in valid_games if g["id"] not in odds]
    if missing:
        print(f"[PLAN B] InferSports sin cuotas frescas para {len(missing)} partido(s). Usando TheOddsAPI...")
        th = odds_totals_theoddsapi()
        for g in missing:
            ln = th.get(g["key"])
            if ln and ln["age_min"] <= MAX_ODDS_AGE_MIN:
                odds[g["id"]] = ln
                print(f"[PLAN B OK] {g['matchup']}")
    return odds


# =========================
# ANCLA PREGAME
# =========================
def get_anchor(event_id):
    with db() as con:
        r = con.execute("SELECT initial_line FROM anchors WHERE event_id=?", (event_id,)).fetchone()
    return r["initial_line"] if r else None


def capture_pregame_anchors():
    """Guarda la línea ANTES del inicio (si no, el 'ancla' sería ya una línea en vivo)."""
    try:
        with db() as con:
            n = con.execute("SELECT COUNT(*) c FROM users WHERE (active=1 OR auto_mode=1) AND is_blocked=0").fetchone()["c"]
            have = {r["event_id"] for r in con.execute("SELECT event_id FROM anchors")}
        if not n:
            return
        now = datetime.now(timezone.utc)
        need = []
        for g in espn_games():
            if g["state"] != "pre" or g["id"] in have:
                continue
            t = parse_iso(g["start"])
            if t and 0 <= (t - now).total_seconds() <= PREGAME_WINDOW_H * 3600:
                need.append(g)
        if not need:
            return
        th = odds_totals_theoddsapi()
        with db() as con:
            for g in need:
                ln = th.get(g["key"])
                if ln:
                    con.execute("INSERT OR IGNORE INTO anchors(event_id,matchup,initial_line,captured_at) VALUES(?,?,?,?)",
                                (g["id"], g["matchup"], ln["line"], now_iso()))
                    print(f"[ANCHOR] {g['matchup']} pregame = {ln['line']:.1f}")
    except Exception as e:
        print(f"[pregame error] {e}")


# =========================
# MODELO
# =========================
def minutes_played(period, clock, status=""):
    if period <= 0:
        return 0.0
    s = (status or "").lower()
    if "half" in s and period == 2:
        return 24.0
    plen = 12.0 if period <= 4 else 5.0
    completed = period - 1
    mins = min(4, completed) * 12 + max(0, completed - 4) * 5
    if "end of" in s:
        return mins + plen
    rem = parse_clock(clock)
    if rem is None:
        return float(mins)
    return mins + max(0.0, plen - rem)


def completed_quarters(period, status):
    s = (status or "").lower()
    if "half" in s:
        return 2
    if "end of" in s:
        return min(4, period)
    return min(4, max(0, period - 1))


def project_total(total, minutes, period, status, q_home, q_away, anchor):
    """Proyección final = total + ritmo (ponderado por recencia) * minutos restantes,
    mezclada con la línea pregame según avanza el juego. Sin ancla -> solo ritmo."""
    if minutes <= 0.1:
        return None
    n = completed_quarters(period, status)
    q_pts = []
    for i in range(min(n, len(q_home), len(q_away))):
        try:
            q_pts.append(int(float(q_home[i].get("value", 0))) + int(float(q_away[i].get("value", 0))))
        except Exception:
            pass
    if q_pts:
        w = list(range(1, len(q_pts) + 1))
        rate = (sum(p * x for p, x in zip(q_pts, w)) / sum(w)) / 12.0   # pts por minuto
    else:
        rate = total / minutes
    live_proj = total + rate * max(0.0, 48.0 - minutes)
    if anchor is None:
        return live_proj
    wl = min(1.0, max(0.0, minutes / 48.0))
    return live_proj * wl + anchor * (1.0 - wl)


def analyze_game(g, ln):
    mins = minutes_played(g["period"], g["clock"], g["status"])
    anchor = get_anchor(g["id"])
    proj = project_total(g["total"], mins, g["period"], g["status"], g["q_home"], g["q_away"], anchor)
    if proj is None:
        return None
    diff = proj - ln["line"]
    decision = "OVER" if diff > 0 else "UNDER"
    cuota = ln["over"] if decision == "OVER" else ln["under"]
    sigma = SIGMA_K * math.sqrt(max(1.0, 48.0 - mins))
    p_raw = norm_cdf(abs(diff) / sigma)
    p = 0.5 + (p_raw - 0.5) * SHRINK
    ev = (p * cuota - 1.0) if cuota and cuota > 1.0 else None
    need = MIN_DIFF + (0.0 if anchor is not None else NO_ANCHOR_EXTRA)
    signal = (abs(diff) >= need) and ev is not None and ev >= MIN_EV
    return {"mins": mins, "anchor": anchor, "proj": proj, "diff": diff, "decision": decision,
            "cuota": cuota, "p": p, "ev": ev, "signal": signal}


def save_snapshot(g, ln, a):
    try:
        with db() as con:
            con.execute("INSERT INTO snapshots(ts,event_id,matchup,period,clock,minutes,total,line,line_age,anchor,proj,diff,p,ev) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (now_iso(), g["id"], g["matchup"], g["period"], g["clock"], a["mins"], g["total"],
                         ln["line"], ln.get("age_min", 0.0), a["anchor"], a["proj"], a["diff"], a["p"], a["ev"]))
    except Exception as e:
        print(f"[snapshot] {e}")


# =========================
# TELEGRAM / MENSAJES
# =========================
def send_tg(token, chat_id, msg):
    try:
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=15)
        return r.status_code == 200
    except Exception:
        return False


def format_signal(g, ln, a, auto=False):
    arrow = "⬆️ OVER" if a["decision"] == "OVER" else "⬇️ UNDER"
    color = "🟢" if a["decision"] == "OVER" else "🔴"
    modo = "AUTO" if auto else "MANUAL"
    anchor_txt = f"{a['anchor']:.1f}" if a["anchor"] is not None else "n/d (solo ritmo)"
    return (
        f"📡 <b>SEÑAL EN VIVO — NBA ({modo})</b>\n\n"
        f"🏀 <b>{html.escape(g['matchup'])}</b>\n"
        f"📌 <b>Línea ({html.escape(ln['bookmaker'])}):</b> {ln['line']:.1f}\n"
        f"🔄 <b>Actualizada:</b> hace {ln.get('age_min', 0):.1f} min\n"
        f"⏱️ <b>Estado:</b> P{g['period']} ({html.escape(g['clock'])})\n"
        f"📍 <b>Línea pregame:</b> {anchor_txt}\n\n"
        f"📈 <b>Proyección final:</b> {a['proj']:.1f}\n"
        f"🔻 <b>Diferencia:</b> {a['diff']:+.1f} pts\n"
        f"💡 <b>Recomendación:</b> <b>{arrow} {ln['line']:.1f}</b>\n"
        f"🪙 <b>Cuota @{a['cuota']:.2f}</b>\n"
        f"{color} <b>Prob. estimada:</b> {a['p'] * 100:.0f}%  |  <b>EV:</b> {a['ev'] * 100:+.1f}%\n\n"
        f"🕑 <i>Estimación del modelo, no una garantía. Juega con responsabilidad.</i>"
    )


# =========================
# RESULTADOS
# =========================
def check_results():
    try:
        with db() as con:
            pending = con.execute("SELECT * FROM signals WHERE status='PENDING'").fetchall()
        if not pending:
            return
        board = {g["id"]: g for g in espn_games()}
        finals = {}
        for eid in {s["event_id"] for s in pending if s["event_id"]}:
            g = board.get(eid)
            res = (g["state"], g["total"]) if g else espn_event_result(eid)
            if res and res[0] == "post":
                finals[eid] = res[1]
        now = datetime.now(timezone.utc)
        for sig in pending:
            ft = finals.get(sig["event_id"])
            if ft is None:
                t = parse_iso(sig["created_at"])
                if t and (now - t) > timedelta(hours=VOID_AFTER_H):
                    with db() as con:
                        con.execute("UPDATE signals SET status='VOID' WHERE id=?", (sig["id"],))
                continue
            line, dec = sig["line"], sig["decision"]
            status = "PUSH"
            if dec == "OVER":
                status = "WON" if ft > line else ("LOST" if ft < line else "PUSH")
            else:
                status = "WON" if ft < line else ("LOST" if ft > line else "PUSH")
            with db() as con:
                con.execute("UPDATE signals SET status=?, final_total=? WHERE id=?", (status, ft, sig["id"]))
            print(f"[RESULT] {sig['matchup']} {dec} {line} -> Final {ft} ({status})")
    except Exception as e:
        print(f"[check_results error] {e}")


def retry_notifications():
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()
        with db() as con:
            rows = con.execute(
                "SELECT s.id, s.message, u.tg_token, u.tg_chat_id FROM signals s JOIN users u ON u.id=s.user_id "
                "WHERE s.notified=0 AND s.created_at>=? AND u.tg_token!='' AND u.tg_chat_id!='' AND u.is_blocked=0",
                (cutoff,)).fetchall()
        for r in rows:
            if send_tg(r["tg_token"], r["tg_chat_id"], r["message"]):
                with db() as con:
                    con.execute("UPDATE signals SET notified=1 WHERE id=?", (r["id"],))
                print(f"[TG retry OK] señal {r['id']}")
    except Exception as e:
        print(f"[retry error] {e}")


# =========================
# POLL
# =========================
def is_eval_window(g):
    s = g["status"].lower()
    if g["state"] != "in":
        return False
    return ("half" in s) or g["period"] in (3, 4)


def is_garbage_time(g):
    if g["period"] != 4 or g["state"] != "in":
        return False
    margin = abs(g["home_score"] - g["away_score"])
    left = parse_clock(g["clock"])
    left = 12.0 if left is None else left
    if margin > GT_MARGIN and left < GT_MINUTES:
        print(f"[GARBAGE TIME] {g['matchup']} | margen {margin} | faltan {left:.1f} min. Se ignora.")
        return True
    return False


def dispatch(u, g, ln, a):
    msg = format_signal(g, ln, a, auto=bool(u["auto_mode"]))
    has_tg = bool(u["tg_token"] and u["tg_chat_id"])
    with db() as con:
        cur = con.execute(
            "INSERT OR IGNORE INTO signals(user_id,event_id,matchup,decision,line,cuota,proyeccion,diff,confianza,ev,"
            "total,period,created_at,message,notified) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (u["id"], g["id"], g["matchup"], a["decision"], ln["line"], a["cuota"], a["proj"], a["diff"],
             int(round(a["p"] * 100)), a["ev"], g["total"], g["period"], now_iso(), msg, 0 if has_tg else 1))
        new, sid = cur.rowcount > 0, cur.lastrowid
        con.execute("UPDATE selection SET sent=1 WHERE user_id=? AND event_id=?", (u["id"], g["id"]))
    if not new:
        return
    tag = u["id"][:8]
    if has_tg:
        if send_tg(u["tg_token"], u["tg_chat_id"], msg):
            with db() as con:
                con.execute("UPDATE signals SET notified=1 WHERE id=?", (sid,))
            print(f"[SIGNAL + TG] {tag} {g['matchup']} {a['decision']} {ln['line']:.1f}")
        else:
            print(f"[SIGNAL] {tag} {g['matchup']} guardada; Telegram falló (se reintentará)")
    else:
        print(f"[SIGNAL] {tag} {g['matchup']} {a['decision']} {ln['line']:.1f} (solo Web)")


def poll():
    try:
        with db() as con:
            users = con.execute("SELECT * FROM users WHERE (active=1 OR auto_mode=1) AND is_blocked=0").fetchall()
        if not users:
            return
        retry_notifications()
        games = espn_games()
        if not games:
            return
        valid = [g for g in games if is_eval_window(g) and not is_garbage_time(g)]
        if not valid:
            return
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] --- REVISIÓN ({len(valid)} partidos válidos) ---")
        odds = collect_odds(valid)
        if not odds:
            print("[POLL] Sin cuotas frescas de ninguna fuente.")
            return

        # Cada partido se analiza UNA vez (antes se repetía por usuario)
        analyses = {}
        for g in valid:
            ln = odds.get(g["id"])
            if not ln:
                continue
            a = analyze_game(g, ln)
            if not a:
                continue
            save_snapshot(g, ln, a)
            ev_txt = f"{a['ev'] * 100:+.1f}%" if a["ev"] is not None else "n/d"
            anc = f"{a['anchor']:.1f}" if a["anchor"] is not None else "—"
            print(f"[EVAL] {g['matchup']} | Ancla {anc} | Proy {a['proj']:.1f} vs Línea {ln['line']:.1f} "
                  f"| Diff {a['diff']:+.1f} | p {a['p'] * 100:.0f}% | EV {ev_txt} | {'SEÑAL' if a['signal'] else '-'}")
            analyses[g["id"]] = (g, ln, a)

        for u in users:
            try:
                if u["auto_mode"]:
                    ids = list(analyses)
                else:
                    with db() as con:
                        ids = [r["event_id"] for r in con.execute(
                            "SELECT event_id FROM selection WHERE user_id=? AND active=1 AND sent=0", (u["id"],))]
                for eid in ids:
                    item = analyses.get(eid)
                    if item and item[2]["signal"]:
                        dispatch(u, *item)
            except Exception as e:
                print(f"[poll user {u['id'][:8]}] {e}")
    except Exception as e:
        print(f"[poll error] {e}")


# =========================
# FASTAPI
# =========================
class UserCfg(BaseModel):
    tg_token: str = ""
    tg_chat_id: str = ""


class Sel(BaseModel):
    events: list[str] = []


scheduler = None


@asynccontextmanager
async def lifespan(app):
    global scheduler
    init_db()
    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(poll, "interval", seconds=POLL_INTERVAL, max_instances=1, coalesce=True)
    scheduler.add_job(check_results, "interval", seconds=300, max_instances=1, coalesce=True)
    scheduler.add_job(capture_pregame_anchors, "interval", minutes=PREGAME_INTERVAL_MIN,
                      max_instances=1, coalesce=True)
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)


def get_uid(x_user_id: str = Header(None)):
    if not x_user_id or not UID_RE.match(x_user_id):
        raise HTTPException(401, "ID de usuario inválido")
    return x_user_id


def get_user(uid: str = Depends(get_uid)):
    with db() as con:
        u = con.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    if not u:
        raise HTTPException(404, "not configured")
    if u["is_blocked"]:
        raise HTTPException(403, "Bloqueado")
    return u


def admin_auth(x_admin_pass: str = Header(None)):
    if not x_admin_pass or not secrets.compare_digest(x_admin_pass.encode(), ADMIN_PASS.encode()):
        raise HTTPException(401, "Contraseña incorrecta")


_new_user_hits = {}


def _rate_ok(ip, limit=10, window=3600):
    now = time.time()
    hits = [t for t in _new_user_hits.get(ip, []) if now - t < window]
    ok = len(hits) < limit
    if ok:
        hits.append(now)
    _new_user_hits[ip] = hits
    return ok


def mask(s):
    return (s[:4] + "..." + s[-4:]) if s and len(s) >= 10 else ("***" if s else "")


@app.post("/api/user/test")
def test_user(body: UserCfg, uid: str = Depends(get_uid)):
    tk, chat = body.tg_token.strip(), body.tg_chat_id.strip()
    with db() as con:
        u = con.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    if u and u["is_blocked"]:
        raise HTTPException(403, "Bloqueado")
    if u:
        tk, chat = tk or u["tg_token"], chat or u["tg_chat_id"]
    errors, tg_user = [], None
    if tk:
        if not TOKEN_RE.match(tk):
            errors.append("Formato de token inválido")
        else:
            try:
                r = requests.get(f"https://api.telegram.org/bot{tk}/getMe", timeout=10)
                if r.status_code != 200:
                    errors.append("Token de Telegram inválido")
                else:
                    tg_user = (r.json().get("result") or {}).get("username")
                    if not chat:
                        errors.append("Falta el chat ID")
                    elif not send_tg(tk, chat, "✅ Conexión de prueba correcta"):
                        errors.append("No pude escribir al chat ID (¿le diste /start al bot?)")
            except Exception:
                errors.append("Error de red con Telegram")
    return {"ok": not errors, "errors": errors, "tg_user": tg_user,
            "odds_remaining": "InferSports + respaldo TheOddsAPI"}


@app.post("/api/user")
def save_user(body: UserCfg, request: Request, uid: str = Depends(get_uid)):
    tk, chat = body.tg_token.strip(), body.tg_chat_id.strip()
    if tk and not TOKEN_RE.match(tk):
        raise HTTPException(400, "Formato de token de Telegram inválido")
    if chat and not CHAT_RE.match(chat):
        raise HTTPException(400, "Formato de chat ID inválido")
    with db() as con:
        ex = con.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
        if ex:
            if ex["is_blocked"]:
                raise HTTPException(403, "Bloqueado")
            if chat and not tk:
                tk = ex["tg_token"]          # conserva el token ya guardado
            if bool(tk) != bool(chat):
                raise HTTPException(400, "Si usas Telegram, completa Token y Chat ID")
            con.execute("UPDATE users SET tg_token=?, tg_chat_id=? WHERE id=?", (tk, chat, uid))
        else:
            if bool(tk) != bool(chat):
                raise HTTPException(400, "Si usas Telegram, completa Token y Chat ID")
            ip = (request.headers.get("x-forwarded-for", "").split(",")[0].strip()
                  or (request.client.host if request.client else "?"))
            if con.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] >= MAX_USERS:
                raise HTTPException(403, "Cupo de usuarios lleno")
            if not _rate_ok(ip):
                raise HTTPException(429, "Demasiados registros desde tu red, intenta más tarde")
            con.execute("INSERT INTO users(id, tg_token, tg_chat_id, created_at) VALUES(?,?,?,?)",
                        (uid, tk, chat, now_iso()))
    return {"ok": True}


@app.get("/api/user")
def api_get_user(u=Depends(get_user)):
    return {"configured": True, "tg_token": mask(u["tg_token"]), "tg_chat_id": u["tg_chat_id"],
            "active": bool(u["active"]), "auto_mode": bool(u["auto_mode"])}


def _toggle(uid, col):
    with db() as con:
        cur = con.execute(f"SELECT {col} FROM users WHERE id=?", (uid,)).fetchone()
        new = 0 if cur[col] else 1
        con.execute(f"UPDATE users SET {col}=? WHERE id=?", (new, uid))
    return bool(new)


@app.post("/api/user/toggle")
def toggle_user(u=Depends(get_user)):
    return {"active": _toggle(u["id"], "active")}


@app.post("/api/user/toggle_auto")
def toggle_auto(u=Depends(get_user)):
    return {"auto_mode": _toggle(u["id"], "auto_mode")}


@app.get("/api/games")
def api_games(u=Depends(get_user)):
    with db() as con:
        sel = {r["event_id"]: dict(r) for r in con.execute("SELECT * FROM selection WHERE user_id=?", (u["id"],))}
    out = []
    for g in espn_games():
        s = sel.get(g["id"], {})
        out.append({"id": g["id"], "matchup": g["matchup"], "status": g["status"], "state": g["state"],
                    "period": g["period"], "clock": g["clock"], "total": g["total"],
                    "selected": bool(s.get("active", 0)), "sent": bool(s.get("sent", 0))})
    return out


@app.post("/api/selection")
def set_selection(body: Sel, u=Depends(get_user)):
    events = [e for e in body.events[:30] if EVENT_RE.match(e)]
    names = {g["id"]: g["matchup"] for g in espn_games()}
    with db() as con:
        con.execute("UPDATE selection SET active=0 WHERE user_id=?", (u["id"],))
        for e in events:
            con.execute("INSERT INTO selection(user_id,event_id,matchup,active,sent) VALUES(?,?,?,1,0) "
                        "ON CONFLICT(user_id,event_id) DO UPDATE SET active=1", (u["id"], e, names.get(e, "")))
    return {"ok": True, "selected": events}


@app.get("/api/signals")
def api_signals(u=Depends(get_user)):
    with db() as con:
        rows = [dict(r) for r in con.execute(
            "SELECT id,matchup,decision,line,cuota,proyeccion,diff,confianza,ev,total,period,created_at,status,final_total "
            "FROM signals WHERE user_id=? ORDER BY id DESC LIMIT 50", (u["id"],))]
    return rows


@app.get("/api/stats")
def api_stats(u=Depends(get_user)):
    with db() as con:
        r = con.execute(
            "SELECT COUNT(*) total, "
            "SUM(CASE WHEN status='WON' THEN 1 ELSE 0 END) won, "
            "SUM(CASE WHEN status='LOST' THEN 1 ELSE 0 END) lost, "
            "SUM(CASE WHEN status='PUSH' THEN 1 ELSE 0 END) push, "
            "SUM(CASE WHEN status='WON' THEN COALESCE(cuota,1)-1 WHEN status='LOST' THEN -1 ELSE 0 END) units "
            "FROM signals WHERE user_id=?", (u["id"],)).fetchone()
    won, lost, push = r["won"] or 0, r["lost"] or 0, r["push"] or 0
    settled = won + lost
    units = round(r["units"] or 0.0, 2)
    return {"total": r["total"] or 0, "won": won, "lost": lost, "push": push, "settled": settled,
            "win_rate": round(won / settled * 100, 1) if settled else 0,
            "units": units, "roi": round(units / settled * 100, 1) if settled else 0}


# ---------- Admin ----------
@app.get("/api/admin/users")
def admin_users(_=Depends(admin_auth)):
    with db() as con:
        rows = [dict(r) for r in con.execute(
            "SELECT u.id, u.created_at, u.active, u.auto_mode, u.is_blocked, COUNT(s.id) AS signals "
            "FROM users u LEFT JOIN signals s ON s.user_id=u.id GROUP BY u.id ORDER BY u.created_at DESC")]
    return rows


@app.post("/api/admin/block/{user_id}")
def admin_block(user_id: str, _=Depends(admin_auth)):
    if not UID_RE.match(user_id):
        raise HTTPException(400, "ID inválido")
    with db() as con:
        u = con.execute("SELECT is_blocked FROM users WHERE id=?", (user_id,)).fetchone()
        if not u:
            raise HTTPException(404, "No encontrado")
        new = 0 if u["is_blocked"] else 1
        con.execute("UPDATE users SET is_blocked=? WHERE id=?", (new, user_id))
    return {"blocked": bool(new)}


@app.get("/admin", response_class=HTMLResponse)
def admin_page():
    return ADMIN_HTML   # la página no contiene secretos; la API exige X-Admin-Pass


@app.get("/", response_class=HTMLResponse)
def index():
    return HTML_PAGE


# =========================
# FRONTEND
# =========================
HTML_PAGE = r"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-title" content="NBA Signals">
<title>NBA Signals</title>
<style>
  :root{--bg:#0f172a;--card:#1e293b;--txt:#e2e8f0;--muted:#94a3b8;--blue:#3b82f6;--green:#22c55e;--red:#ef4444;--purple:#8b5cf6;}
  *{box-sizing:border-box;}
  body{font-family:-apple-system,system-ui;margin:0;padding:16px;background:var(--bg);color:var(--txt);}
  header{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;}
  h1{font-size:20px;margin:0;}
  h2{font-size:13px;margin:24px 0 8px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;}
  .icon{background:none;border:none;font-size:22px;cursor:pointer;padding:4px 8px;color:var(--txt);}
  .card{background:var(--card);padding:12px;border-radius:10px;margin-bottom:8px;}
  .game{display:flex;align-items:center;gap:12px;cursor:pointer;}
  .game input{width:20px;height:20px;flex-shrink:0;margin:0;}
  .meta{font-size:12px;color:var(--muted);margin-top:2px;}
  .signal{border-left:4px solid var(--green);}
  .signal.under{border-left-color:var(--red);}
  button.primary{background:var(--blue);color:#fff;border:none;padding:14px;border-radius:8px;font-size:16px;width:100%;margin-top:12px;font-weight:600;cursor:pointer;}
  button.primary:disabled{background:#475569;}
  button.secondary{background:#334155;color:#fff;border:none;padding:10px;border-radius:8px;font-size:14px;width:100%;margin-top:8px;cursor:pointer;}
  input.field{width:100%;padding:12px;border-radius:8px;border:1px solid #334155;background:#0f172a;color:var(--txt);font-size:14px;margin-bottom:8px;}
  .hidden{display:none !important;}
  .empty{color:#64748b;font-size:13px;padding:12px;text-align:center;}
  .msg{font-size:13px;margin-top:8px;padding:8px;border-radius:6px;}
  .msg.ok{background:#14532d;color:#86efac;}
  .msg.err{background:#7f1d1d;color:#fca5a5;}
  .hint{font-size:12px;color:var(--muted);margin-bottom:8px;}
  a.token-link{color:var(--blue);font-size:12px;word-break:break-all;}
</style>
</head><body>

<header>
  <h1>NBA Signals</h1>
  <button class="icon" onclick="toggleSettings()">&#9881;</button>
</header>

<div id="settings" class="hidden">
  <div class="card">
    <h2>Tus credenciales</h2>
    <p class="hint">Telegram es opcional: sin él verás las señales aquí en la web.</p>
    <label class="hint">Telegram bot token (Opcional)</label>
    <input class="field" id="tgToken" placeholder="ej: 7726757580:AAH...">
    <label class="hint">Telegram chat ID (Opcional)</label>
    <input class="field" id="tgChat" placeholder="ej: 1058217995">
    <button class="primary" onclick="saveUser()">Guardar credenciales</button>
    <button class="secondary" onclick="testUser()">Probar credenciales</button>
    <div id="settingsMsg"></div>
    <hr style="border-color:#334155;margin:16px 0;">
    <p class="hint">Para usar tu cuenta en otro dispositivo, abre este enlace (no lo compartas):</p>
    <a class="token-link" id="tokenLink" href="#">-</a>
  </div>
</div>

<div id="main" class="hidden">
  <button id="vigilanciaBtn" class="primary" style="background:#ef4444; margin-bottom: 8px;" onclick="toggleVigilancia()">
    🔴 VIGILANCIA APAGADA (Haz clic para encender)
  </button>
  <button id="autoBtn" class="primary" style="background:#475569; margin-bottom: 20px;" onclick="toggleAuto()">
    🎯 PILOTO AUTOMÁTICO APAGADO (HT, Q3 y Q4)
  </button>

  <h2>Rendimiento</h2>
  <div id="stats" class="empty">Cargando...</div>

  <h2>Partidos (Modo Manual)</h2>
  <div id="games" class="empty">Cargando...</div>
  <button class="primary" id="saveBtn" onclick="saveSel()">Guardar seleccion</button>

  <h2>Senales</h2>
  <div id="signals" class="empty">Sin senales aun</div>
</div>

<script>
let userId = null;
const ID_RE = /^[0-9a-f]{32}$/;

function esc(s){
  return String(s == null ? '' : s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}

function newId(){
  if (window.crypto && crypto.randomUUID) return crypto.randomUUID().replace(/-/g,'');
  const a = new Uint8Array(16); crypto.getRandomValues(a);
  return Array.from(a, b => b.toString(16).padStart(2,'0')).join('');
}

function getOrCreateUserId() {
  const fromUrl = new URLSearchParams(window.location.search).get('u');
  if (fromUrl && ID_RE.test(fromUrl)) {
    localStorage.setItem('nba_user_id', fromUrl);
    window.history.replaceState({}, '', '/');
    return fromUrl;
  }
  let id = localStorage.getItem('nba_user_id');
  if (!id || !ID_RE.test(id)) { id = newId(); localStorage.setItem('nba_user_id', id); }
  return id;
}

async function api(path, opts) {
  opts = opts || {};
  const headers = Object.assign({}, opts.headers || {}, {'X-User-Id': userId});
  return fetch(path, Object.assign({}, opts, {headers: headers}));
}

function toggleSettings() { document.getElementById('settings').classList.toggle('hidden'); }

function setVig(on){
  const btn = document.getElementById('vigilanciaBtn');
  btn.style.background = on ? '#22c55e' : '#ef4444';
  btn.innerHTML = on ? '🟢 VIGILANCIA ENCENDIDA (Haz clic para apagar)' : '🔴 VIGILANCIA APAGADA (Haz clic para encender)';
}
function setAuto(on){
  const btn = document.getElementById('autoBtn');
  btn.style.background = on ? '#8b5cf6' : '#475569';
  btn.innerHTML = on ? '🎯 PILOTO AUTOMÁTICO ENCENDIDO (HT, Q3 y Q4)' : '🎯 PILOTO AUTOMÁTICO APAGADO (HT, Q3 y Q4)';
}

async function errText(r){
  try { const d = await r.json(); return d.detail || ('Error ' + r.status); } catch(e){ return 'Error ' + r.status; }
}

async function loadUser() {
  try {
    const r = await api('/api/user');
    if (r.status === 404 || r.status === 403) {
      document.getElementById('settings').classList.remove('hidden');
      document.getElementById('main').classList.add('hidden');
      if (r.status === 403) {
        const msg = document.getElementById('settingsMsg');
        msg.className = 'msg err';
        msg.textContent = 'Tu cuenta ha sido bloqueada por el administrador.';
      }
      return false;
    }
    if (r.status === 200) {
      const data = await r.json();
      document.getElementById('tgToken').value = '';
      document.getElementById('tgChat').value = data.tg_chat_id || '';
      document.getElementById('tgToken').placeholder = data.tg_token ? ('Guardado: ' + data.tg_token) : 'ej: 7726757580:AAH...';
      setVig(data.active); setAuto(data.auto_mode);
      document.getElementById('main').classList.remove('hidden');
      return true;
    }
  } catch (e) { console.error(e); }
  return false;
}

async function saveUser() {
  const tk = document.getElementById('tgToken').value.trim();
  const chat = document.getElementById('tgChat').value.trim();
  const msg = document.getElementById('settingsMsg');
  const r = await api('/api/user', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({tg_token: tk, tg_chat_id: chat})
  });
  if (r.ok) {
    msg.className = 'msg ok';
    msg.textContent = 'Guardado. Probando...';
    await testUser();
    await loadUser();
    loadGames(); loadStats(); loadSignals();
    if (!window.__timers) startTimers();
  } else {
    msg.className = 'msg err';
    msg.textContent = await errText(r);
  }
}

async function testUser() {
  const tk = document.getElementById('tgToken').value.trim();
  const chat = document.getElementById('tgChat').value.trim();
  const msg = document.getElementById('settingsMsg');
  msg.className = 'msg';
  msg.textContent = 'Probando...';
  const r = await api('/api/user/test', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({tg_token: tk, tg_chat_id: chat})
  });
  if (!r.ok) { msg.className = 'msg err'; msg.textContent = await errText(r); return; }
  const d = await r.json();
  if (d.ok) {
    msg.className = 'msg ok';
    const extra = d.tg_user ? '<br>Bot: @' + esc(d.tg_user) : '<br>Telegram: No configurado';
    msg.innerHTML = 'OK' + extra + '<br>API Cuotas: ' + esc(d.odds_remaining);
  } else {
    msg.className = 'msg err';
    msg.textContent = 'Error: ' + d.errors.join(' | ');
  }
}

let selected = new Set();
let dirty = false;

async function toggleVigilancia() {
  const btn = document.getElementById('vigilanciaBtn');
  btn.disabled = true; btn.innerHTML = 'Cambiando...';
  try { const d = await (await api('/api/user/toggle', {method: 'POST'})).json(); setVig(d.active); }
  catch (e) { console.error(e); }
  btn.disabled = false;
}

async function toggleAuto() {
  const btn = document.getElementById('autoBtn');
  btn.disabled = true; btn.innerHTML = 'Cambiando...';
  try { const d = await (await api('/api/user/toggle_auto', {method: 'POST'})).json(); setAuto(d.auto_mode); }
  catch (e) { console.error(e); }
  btn.disabled = false;
}

async function loadStats() {
  try {
    const r = await api('/api/stats');
    if (!r.ok) return;
    const d = await r.json();
    const div = document.getElementById('stats');
    const color = d.settled === 0 ? '#94a3b8' : (d.win_rate >= 50 ? '#22c55e' : '#ef4444');
    const roi = d.settled > 0
      ? ((d.roi >= 0 ? '+' : '') + d.roi + '% ROI (' + (d.units >= 0 ? '+' : '') + d.units + ' u)')
      : 'Sin resultados aún';
    div.className = '';
    div.innerHTML = `
      <div class="card" style="text-align:center;">
        <div style="font-size:40px; font-weight:bold; color:${color};">${d.win_rate}%</div>
        <div style="color:#94a3b8; font-size:12px; text-transform:uppercase; letter-spacing:1px; margin-bottom:6px;">Tasa de Acierto</div>
        <div style="color:#94a3b8; font-size:13px; margin-bottom:16px;">${esc(roi)}</div>
        <div style="display:flex; justify-content:space-around; font-size:14px;">
          <div><span style="color:#22c55e;">✅</span> ${d.won} Ganadas</div>
          <div><span style="color:#ef4444;">❌</span> ${d.lost} Perdidas</div>
          <div><span style="color:#64748b;">⚖️</span> ${d.push} Empates</div>
        </div>
      </div>`;
  } catch (e) { console.error(e); }
}

async function loadGames() {
  try {
    const r = await api('/api/games');
    if (!r.ok) return;
    const games = await r.json();
    const div = document.getElementById('games');
    if (!games.length) { div.className = 'empty'; div.innerHTML = 'No hay partidos ahora'; return; }
    div.className = '';
    if (!dirty) selected = new Set(games.filter(g => g.selected).map(g => g.id));  // no pisa cambios sin guardar
    div.innerHTML = games.map(function(g){
      const checked = selected.has(g.id) ? 'checked' : '';
      const badge = g.sent ? ' - YA ENVIADA' : '';
      return `<div class="card">
        <label class="game">
          <input type="checkbox" data-id="${esc(g.id)}" ${checked}>
          <div>
            <div>${esc(g.matchup)}</div>
            <div class="meta">${esc(g.status)} - P${g.period} - ${g.total} pts${badge}</div>
          </div>
        </label>
      </div>`;
    }).join('');
    div.querySelectorAll('input[type=checkbox]').forEach(function(cb){
      cb.addEventListener('change', function(){ toggle(cb.dataset.id, cb.checked); });
    });
  } catch (e) { console.error(e); }
}

function toggle(id, on) {
  dirty = true;
  if (on) selected.add(id); else selected.delete(id);
}

async function saveSel() {
  const btn = document.getElementById('saveBtn');
  btn.disabled = true; btn.textContent = 'Guardando...';
  await api('/api/selection', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({events: Array.from(selected)})
  });
  dirty = false;
  btn.disabled = false; btn.textContent = 'Guardar seleccion';
  loadGames();
}

async function loadSignals() {
  try {
    const r = await api('/api/signals');
    if (!r.ok) return;
    const sigs = await r.json();
    const div = document.getElementById('signals');
    if (!sigs.length) { div.className = 'empty'; div.innerHTML = 'Sin senales aun'; return; }
    div.className = '';
    div.innerHTML = sigs.map(function(s){
      let badge;
      if (s.status === 'WON') badge = '<div style="color:#22c55e; font-weight:bold; margin-top:4px;">✅ GANADA (Total final: ' + s.final_total + ')</div>';
      else if (s.status === 'LOST') badge = '<div style="color:#ef4444; font-weight:bold; margin-top:4px;">❌ PERDIDA (Total final: ' + s.final_total + ')</div>';
      else if (s.status === 'PUSH') badge = '<div style="color:#64748b; font-weight:bold; margin-top:4px;">⚖️ EMPATE (Total final: ' + s.final_total + ')</div>';
      else if (s.status === 'VOID') badge = '<div style="color:#64748b; font-weight:bold; margin-top:4px;">🚫 ANULADA (sin resultado)</div>';
      else badge = '<div style="color:#eab308; font-weight:bold; margin-top:4px;">⏳ PENDIENTE</div>';
      const ev = (s.ev == null) ? '' : (' - EV ' + (s.ev >= 0 ? '+' : '') + (s.ev * 100).toFixed(1) + '%');
      return `<div class="card signal ${s.decision.toLowerCase() === 'under' ? 'under' : ''}">
        <div><b>${esc(s.matchup)}</b></div>
        <div>${esc(s.decision)} ${s.line.toFixed(1)} @ ${s.cuota ? s.cuota.toFixed(2) : 'N/A'}</div>
        <div class="meta">Proyeccion ${s.proyeccion.toFixed(1)} - Diff ${s.diff > 0 ? '+' : ''}${s.diff.toFixed(1)} - Prob ${s.confianza}%${ev}</div>
        ${badge}
        <div class="meta">${esc(new Date(s.created_at).toLocaleString())}</div>
      </div>`;
    }).join('');
  } catch (e) { console.error(e); }
}

function startTimers(){
  window.__timers = true;
  setInterval(loadGames, 30000);
  setInterval(loadSignals, 15000);
  setInterval(loadStats, 60000);
}

async function init() {
  userId = getOrCreateUserId();
  const link = window.location.origin + '/?u=' + userId;
  document.getElementById('tokenLink').href = link;
  document.getElementById('tokenLink').textContent = link;
  const configured = await loadUser();
  if (configured) { loadGames(); loadSignals(); loadStats(); startTimers(); }
}

init();
</script>
</body></html>
"""

ADMIN_HTML = r"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Admin</title>
<style>
:root{--bg:#0f172a;--card:#1e293b;--txt:#e2e8f0;--muted:#94a3b8;--red:#ef4444;--green:#22c55e;}
body{font-family:-apple-system,system-ui;margin:0;padding:16px;background:var(--bg);color:var(--txt);}
h1{font-size:22px;margin-bottom:16px;}
table{width:100%;border-collapse:collapse;margin-top:16px;background:var(--card);border-radius:8px;overflow:hidden;}
th,td{padding:12px;text-align:left;border-bottom:1px solid #334155;font-size:14px;}
th{background:#1e293b;color:var(--muted);font-weight:600;text-transform:uppercase;font-size:12px;}
tr:last-child td{border-bottom:none;}
.badge{padding:4px 8px;border-radius:4px;font-size:12px;font-weight:bold;}
.bg-green{background:#14532d;color:#86efac;}.bg-red{background:#7f1d1d;color:#fca5a5;}
button{padding:6px 12px;border:none;border-radius:4px;cursor:pointer;font-weight:bold;color:#fff;}
.btn-block{background:var(--red);}.btn-unblock{background:var(--green);}
</style></head><body>
<h1>🕵️‍♂️ Panel de Administrador</h1>
<p style="color:#94a3b8;font-size:13px;">Usuarios registrados en el bot.</p>
<table><thead><tr><th>ID Usuario</th><th>Registro</th><th>Estado</th><th>Señales</th><th>Acciones</th></tr></thead>
<tbody id="usersBody"><tr><td colspan="5" style="text-align:center;">Cargando...</td></tr></tbody></table>
<script>
function esc(s){return String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');}
let pass = sessionStorage.getItem('adm') || prompt('Contraseña de administrador');
if (pass) sessionStorage.setItem('adm', pass);
const tbody = document.getElementById('usersBody');

async function loadUsers() {
  const res = await fetch('/api/admin/users', {headers: {'X-Admin-Pass': pass || ''}});
  if (!res.ok) {
    sessionStorage.removeItem('adm');
    tbody.innerHTML = '<tr><td colspan="5" style="color:#fca5a5;">Contraseña incorrecta. Recarga la página.</td></tr>';
    return;
  }
  const users = await res.json();
  if (!users.length) { tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;">No hay usuarios.</td></tr>'; return; }
  tbody.innerHTML = users.map(function(u){
    const status = u.is_blocked ? '<span class="badge bg-red">BLOQUEADO</span>' : '<span class="badge bg-green">ACTIVO</span>';
    const bot = u.active ? '🟢' : '⚪';
    const auto = u.auto_mode ? '🎯' : '';
    const date = new Date(u.created_at).toLocaleString();
    return `<tr><td><code>${esc(u.id)}</code></td><td>${esc(date)}</td><td>${status} ${bot} ${auto}</td><td>${u.signals}</td>
      <td><button class="${u.is_blocked ? 'btn-unblock' : 'btn-block'}" data-id="${esc(u.id)}" data-b="${u.is_blocked ? 1 : 0}">${u.is_blocked ? 'Desbloquear' : 'Bloquear'}</button></td></tr>`;
  }).join('');
  tbody.querySelectorAll('button').forEach(function(b){
    b.addEventListener('click', function(){ toggleBlock(b.dataset.id, b.dataset.b === '1'); });
  });
}

async function toggleBlock(id, blocked) {
  if (!confirm('¿Seguro que quieres ' + (blocked ? 'desbloquear' : 'bloquear') + ' a este usuario?')) return;
  const res = await fetch('/api/admin/block/' + encodeURIComponent(id), {method: 'POST', headers: {'X-Admin-Pass': pass}});
  if (res.ok) loadUsers(); else alert('Error al cambiar el estado del usuario.');
}
loadUsers();
</script></body></html>
"""


# =========================
# LANZADOR (Colab / local)
# =========================
def open_tunnel():
    if NGROK_TOKEN:
        try:
            from pyngrok import ngrok
            ngrok.set_auth_token(NGROK_TOKEN)
            return ngrok.connect(PORT, "http").public_url.replace("http://", "https://")
        except Exception as e:
            print(f"[ngrok] {e}")
    if "google.colab" in sys.modules:
        try:
            from google.colab import output
            return output.eval_js(f"google.colab.kernel.proxyPort({PORT})")
        except Exception as e:
            print(f"[colab proxy] {e}")
    return f"http://localhost:{PORT}"


def main():
    if not INFER_API_KEY and not THEODDS_API_KEY:
        print("⚠️  No hay INFER_API_KEY ni THEODDS_API_KEY: el bot no tendrá cuotas.")
    if ADMIN_GENERATED:
        print(f"🔑 ADMIN_PASS no definido; contraseña temporal de admin: {ADMIN_PASS}")
    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=PORT, log_level="warning"))
    threading.Thread(target=server.run, daemon=True).start()
    time.sleep(2)
    url = open_tunnel()
    print(f"\n✅ Bot en marcha\n   App:   {url}\n   Admin: {url}/admin\n"
          f"   (deja esta celda ejecutándose; si la detienes o Colab se desconecta, el bot se apaga)\n")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        server.should_exit = True


# Al final de nba_bot.py
if __name__ == "__main__":
    import uvicorn
    # Render asigna el puerto a través de la variable de entorno PORT
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("nba_bot:app", host="0.0.0.0", port=port)
