import requests, telebot, time, random, os, threading, re
from datetime import datetime
import pytz
from flask import Flask
from groq import Groq
from google_play_scraper import search, app as gplay
from telebot.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton

# ─── FLASK ───────────────────────────────────────────────────
web_app = Flask(__name__)

@web_app.route('/')
def home(): return "Bot is Alive!"

@web_app.route('/health')
def health(): return "OK", 200

def run_web():
    port = int(os.environ.get("PORT", 10000))
    web_app.run(host="0.0.0.0", port=port, use_reloader=False)

# ─── CONFIG ──────────────────────────────────────────────────
SHEET_URL = os.environ.get("SHEET_URL", "https://script.google.com/macros/s/AKfycbzI5eCCU_Gci6M0jFr5I_Ph48CqUvvP4_nkpngWtjFafVSr_i75yqKX37ZMG4qwG0_V/exec")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8709829378:AAEJJQ8jm_oTyAcGenBrIfLi4KYHRVcSJbo")
GROQ_KEY  = os.environ.get("GROQ_KEY",  "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")

bot = telebot.TeleBot(BOT_TOKEN, threaded=False)
ai  = Groq(api_key=GROQ_KEY)

# ─── STATE ───────────────────────────────────────────────────
state = {
    "status":          "IDLE",
    "generated_kws":   [],
    "kw_index":        0,
    "scraped_ids":     set(),
    "total_scraped":   0,
    "total_emailed":   0,
    "chat_id":         None,
    "tmp_url":         None,
    "tmp_email":       None,
    "tmp_test_email":  None,
    "current_set_id":  None,
    "qualified_count": 0,
    "seen_emails":     set(),
    "settings":        {},
    "kw_stats":        {},
    "ai_working":      True,
    "ai_fail_count":   0,
}

GOV = ['gov','government','ministry','department','council',
       'national','authority','federal','municipal']

AI_MODEL = "llama-3.3-70b-versatile"

# ════════════════════════════════════════════════════════════
#  CORE AI CALL — retry on rate-limit, auto-disable on hard fail
# ════════════════════════════════════════════════════════════
def call_ai(prompt, max_tokens=2000, retries=2, silent_fallback=False):
    for attempt in range(retries + 1):
        try:
            r = ai.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model=AI_MODEL,
                max_tokens=max_tokens,
                temperature=0.7,
            )
            state["ai_fail_count"] = 0
            state["ai_working"]    = True
            return r.choices[0].message.content.strip()
        except Exception as e:
            err = str(e)
            print(f"[AI] attempt {attempt+1}: {err[:150]}")
            if "rate_limit" in err or "429" in err:
                if silent_fallback:
                    print("[AI] Rate limit during email — using fallback instantly")
                    return None
                wait = 20 * (attempt + 1)
                send(f"⏳ AI rate limit — waiting {wait}s then retrying...")
                time.sleep(wait)
                continue
            elif "organization_restricted" in err or "401" in err or "403" in err:
                send("❌ Groq API key issue. Switching to fallback mode.")
                state["ai_working"]    = False
                state["ai_fail_count"] += 1
                return None
            else:
                state["ai_fail_count"] += 1
                if attempt < retries:
                    time.sleep(3 if silent_fallback else 5)
                    continue
                return None
    return None

# ─── KEYBOARDS ───────────────────────────────────────────────
def kb():
    m = ReplyKeyboardMarkup(resize_keyboard=True)
    s = state["status"]
    if s == "IDLE":
        m.add(KeyboardButton("🚀 Start Automation"))
        m.add(KeyboardButton("📤 Send Emails"),  KeyboardButton("🔑 Keywords"))
        m.add(KeyboardButton("📅 Schedules"),    KeyboardButton("📧 Senders"))
        m.add(KeyboardButton("🧪 Spam Test"),    KeyboardButton("🔄 Refresh"))
    elif s in ["SCRAPING", "FILTERING", "EMAILING"]:
        m.add(KeyboardButton("🛑 Pause"), KeyboardButton("⏹️ Stop"))
    elif s == "PAUSED":
        m.add(KeyboardButton("▶️ Resume"), KeyboardButton("⏹️ Stop"), KeyboardButton("⏹️ Reset"))
    return m

def back_kb():
    m = ReplyKeyboardMarkup(resize_keyboard=True)
    m.add(KeyboardButton("🔙 Back"))
    return m

def send(text, md="Markdown"):
    if not state["chat_id"]:
        print(f"[NO CHAT_ID] {text[:120]}")
        return
    try:
        bot.send_message(state["chat_id"], text, parse_mode=md)
    except Exception as e:
        err = str(e).lower()
        if "can't parse" in err or "parse" in err or "entity" in err:
            try:
                plain = re.sub(r'[*_`\[\]]', '', text)
                bot.send_message(state["chat_id"], plain)
            except Exception as e2:
                print(f"[SEND ERR] {e2} | text={text[:80]}")
        else:
            try: bot.send_message(state["chat_id"], text)
            except Exception as e2:
                print(f"[SEND ERR] {e2}")

def parse_time(s):
    s = s.strip().upper()
    for f in ("%I:%M %p", "%H:%M"):
        try: return datetime.strptime(s, f).strftime("%H:%M")
        except: pass
    return None

def get_email(d):
    for field, src in [("developerEmail","dev"), ("supportEmail","support")]:
        v = str(d.get(field,'') or '').strip().lower()
        if v and '@' in v and '.' in v:
            return v, src
    for field in ["developerWebsite","privacyPolicy","developerAddress"]:
        v = str(d.get(field,'') or '')
        found = re.findall(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', v)
        if found: return found[0].lower(), "extracted"
    return "", "none"

# ════════════════════════════════════════════════════════════
#  SETTINGS
# ════════════════════════════════════════════════════════════
def get_settings():
    if state["settings"]:
        return state["settings"]
    try:
        r = requests.post(SHEET_URL, json={"action":"get_settings"}, timeout=20)
        if r.status_code == 200:
            state["settings"] = r.json()
            return state["settings"]
    except Exception as e:
        print(f"get_settings error: {e}")
    return {}

# ════════════════════════════════════════════════════════════
#  KEYWORD SET MANAGEMENT
# ════════════════════════════════════════════════════════════
def get_keyword_sets():
    try:
        r = requests.post(SHEET_URL, json={"action":"get_keyword_sets"}, timeout=15)
        if r.status_code == 200:
            return r.json() if isinstance(r.json(), list) else []
    except Exception as e:
        print(f"get_keyword_sets error: {e}")
    return []

def add_keyword_set(set_text):
    try:
        requests.post(SHEET_URL, json={"action":"add_keyword_set","set":set_text}, timeout=15)
    except Exception as e:
        print(f"add_keyword_set error: {e}")

def delete_keyword_set(set_id):
    try:
        requests.post(SHEET_URL, json={"action":"delete_keyword_set","id":set_id}, timeout=15)
    except Exception as e:
        print(f"delete_keyword_set error: {e}")

def mark_keyword_set_used(set_id):
    try:
        requests.post(SHEET_URL, json={"action":"mark_keyword_set_used","id":set_id}, timeout=15)
    except Exception as e:
        print(f"mark_keyword_set_used error: {e}")

def get_next_keyword_set():
    sets = get_keyword_sets()
    for s in sets:
        if s.get('status') == 'pending':
            return s.get('id'), s.get('set_text')
    return None, None

# ════════════════════════════════════════════════════════════
#  SCHEDULE MANAGEMENT
# ════════════════════════════════════════════════════════════
def get_schedule_times():
    try:
        r = requests.post(SHEET_URL, json={"action":"get_schedule_times"}, timeout=15)
        if r.status_code != 200:
            return []
        raw = r.json()
        if not isinstance(raw, list):
            return []
        cleaned = []
        for item in raw:
            t = str(item).strip()
            m = re.match(r'^(\d{1,2}):(\d{2})$', t)
            if m:
                cleaned.append(f"{int(m.group(1)):02d}:{m.group(2)}"); continue
            m = re.match(r'^(\d{1,2}):(\d{2}):\d{2}', t)
            if m:
                cleaned.append(f"{int(m.group(1)):02d}:{m.group(2)}"); continue
            m = re.search(r'(\d{1,2}):(\d{2}):\d{2}', t)
            if m:
                cleaned.append(f"{int(m.group(1)):02d}:{m.group(2)}"); continue
            try:
                fval = float(t)
                if 0.0 <= fval < 1.0:
                    total_min = round(fval * 24 * 60)
                    cleaned.append(f"{(total_min//60)%24:02d}:{total_min%60:02d}"); continue
            except: pass
            print(f"[Scheduler] Cannot parse: {repr(item)}")
        return cleaned
    except Exception as e:
        print(f"get_schedule_times error: {e}")
        return []

def add_schedule_time(time_str):
    try:
        requests.post(SHEET_URL, json={"action":"add_schedule_time","time":time_str}, timeout=15)
    except Exception as e:
        print(f"add_schedule_time error: {e}")

def delete_schedule_time(time_str):
    try:
        requests.post(SHEET_URL, json={"action":"delete_schedule_time","time":time_str}, timeout=15)
    except Exception as e:
        print(f"delete_schedule_time error: {e}")

# ════════════════════════════════════════════════════════════
#  KEYWORD GENERATION
# ════════════════════════════════════════════════════════════
def parse_ai_keywords(raw_text):
    terms = []
    for t in raw_text.replace('\n', ',').replace('\r', ',').split(','):
        t = re.sub(r'^\d+[\.)\-\s]+', '', t)
        t = t.replace('**','').replace('*','').replace('#','').strip('" \' ')
        if 3 < len(t) < 60 and t not in terms:
            terms.append(t)
    return terms

def generate_keywords_from_base(base):
    send(f"🧠 Generating keywords for '{base}'...")
    settings  = get_settings()
    kw_prompt = settings.get('keyword_prompt', '')

    base_words = base.lower().strip()
    all_kws = []
    seen = set()

    def add(kw):
        kw = re.sub(r'\s+', ' ', kw).strip()
        if 2 < len(kw) < 55 and kw not in seen:
            seen.add(kw); all_kws.append(kw)

    add(base_words)
    for w in ['app', 'free', 'best', 'top', 'new', 'simple', 'easy',
              'pro', 'safe', 'secure', 'fast', 'offline', 'lite',
              'for android', 'no ads', '2025', 'tracker', 'manager']:
        add(f"{base_words} {w}")
    for w in ['best', 'top', 'free', 'new', 'simple', 'easy', 'secure',
              'popular', 'trusted', 'good', 'reliable', 'fast']:
        add(f"{w} {base_words}")
    for w in ['best', 'top', 'free', 'most popular', 'highly rated',
              'simple', 'easy to use', 'beginner', 'advanced']:
        add(f"{w} {base_words} app")

    prompt = (
        f'You are a Google Play Store search expert.\n'
        f'Base niche: "{base}"\n'
        + (f'Context: {kw_prompt}\n' if kw_prompt else '') +
        f'\nGenerate 150 search terms that return MANY real Android apps on Play Store.\n'
        f'Rules:\n'
        f'- Broad category terms only (no specific obscure brand/coin names)\n'
        f'- Think about PROBLEMS users solve, FEATURES they want, RELATED categories\n'
        f'- Every term should return 20+ apps when searched on Play Store\n'
        f'- Do NOT use: specific altcoin names, obscure brands, technical jargon\n'
        f'- DO use: common user language, popular category names, general feature terms\n'
        f'\nOutput: comma-separated terms only. No numbers or explanations.'
    )
    result = call_ai(prompt, max_tokens=2500)
    ai_terms = parse_ai_keywords(result) if result else []

    for t in ai_terms:
        t_low = t.lower()
        if re.search(r'\bfor\s+\w+coin\b|\bfor\s+\w+chain\b', t_low):
            continue
        add(t)

    send(f"✅ {len(all_kws)} keywords ready (base:{min(50,len(all_kws))} + AI:{len(ai_terms)})")
    return all_kws[:200] if len(all_kws) >= 200 else all_kws


_consecutive_empty = 0

def play_search_safe(query, country, n_hits=250):
    global _consecutive_empty
    for attempt in range(3):
        try:
            results = search(query, lang='en', country=country, n_hits=n_hits)
            if results:
                _consecutive_empty = 0
            return results or []
        except Exception as e:
            err = str(e).lower()
            if '429' in err or 'too many' in err or 'rate' in err or 'quota' in err:
                wait = 30 * (attempt + 1)
                send(f"⏸️ Rate limit — cooling {wait}s...")
                time.sleep(wait)
            else:
                print(f"[Search] Error '{query}' [{country}]: {e}")
                return []
    return []

def get_search_ids_for_keyword(kw):
    global _consecutive_empty

    if _consecutive_empty >= 4:
        send("⏸️ Rate limit suspected — cooling 90s...")
        time.sleep(90)
        _consecutive_empty = 0

    words = kw.split()
    if len(words) == 1:
        queries = [kw, f"{kw} app", f"best {kw}", f"{kw} free"]
    elif len(words) == 2:
        queries = [kw, f"best {kw}", f"{kw} app", f"{kw} free"]
    elif len(words) == 3:
        queries = [kw, f"best {kw}", f"{' '.join(words[:2])} app"]
    else:
        queries = [kw, ' '.join(words[:3]), ' '.join(words[:2])]

    queries = list(dict.fromkeys(q.strip() for q in queries if q.strip()))

    raw_ids = []
    for q in queries:
        for country in ['us', 'in', 'gb']:
            results = play_search_safe(q, country, n_hits=250)
            raw_ids.extend(r['appId'] for r in results)
            time.sleep(random.uniform(0.7, 1.2))

    _consecutive_empty = (_consecutive_empty + 1) if not raw_ids else 0

    seen_kw, new_ids = set(), []
    for i in raw_ids:
        if i not in seen_kw and i not in state["scraped_ids"]:
            seen_kw.add(i); new_ids.append(i)
    return new_ids

# ════════════════════════════════════════════════════════════
#  FILTER
# ════════════════════════════════════════════════════════════
def is_qualified(app_dict, max_rating, max_installs, seen_emails, stats):
    dev      = str(app_dict.get('dev_name','') or '').lower()
    rating   = float(app_dict.get('rating') or 0.0)
    installs = int(app_dict.get('installs') or 0)
    email    = str(app_dict.get('email','') or '').strip().lower()

    if any(g in dev for g in GOV):
        stats["gov"] += 1; return False, "gov"
    if not email or '@' not in email or '.' not in email.split('@')[-1]:
        stats["no_email"] += 1; return False, "no_email"
    if email in seen_emails:
        stats["dup"] += 1; return False, "dup"
    if rating == 0.0:
        stats["zero_rating"] += 1; return False, "zero_rating"
    if installs > max_installs:
        stats["installs"] += 1; return False, "installs"
    if rating >= max_rating:
        stats["rating"] += 1; return False, "rating"
    stats["passed"] += 1
    return True, "passed"

def save_qualified_lead(row):
    try:
        requests.post(SHEET_URL,
            json={"action":"save_qualified_batch","rows":[row]}, timeout=15)
        return True
    except Exception as e:
        print(f"save_qualified_lead error: {e}")
        return False

# ════════════════════════════════════════════════════════════
#  PHASE 1 — SCRAPE
# ════════════════════════════════════════════════════════════
def phase1_scrape():
    cid = state["chat_id"]
    if not cid:
        print("Cannot start phase1: no chat_id"); return

    state["status"]        = "SCRAPING"
    state["ai_working"]    = True
    state["ai_fail_count"] = 0
    bot.send_message(cid, "🔄 Automation started.", reply_markup=kb())

    try:
        state["settings"] = {}
        settings         = get_settings()
        base_max_installs = int(str(settings.get('max_installs','500000')).replace(',','').strip())
        base_max_rating   = float(str(settings.get('max_rating','4.8')).strip())
        MIN_LEADS = 100

        state["scraped_ids"]     = set()
        state["qualified_count"] = 0
        state["total_scraped"]   = 0
        state["kw_stats"]        = {}

        try:
            eq = requests.post(SHEET_URL, json={"action":"get_qualified_emails"}, timeout=20).json()
            state["seen_emails"] = set(eq) if isinstance(eq, list) else set()
        except:
            state["seen_emails"] = set()

        set_id, base_kw = get_next_keyword_set()
        if not set_id:
            send("❌ No pending keyword sets. Add via 🔑 Keywords.")
            state["status"] = "IDLE"
            bot.send_message(cid, ".", reply_markup=kb()); return

        state["current_set_id"] = set_id

        send(f"🎯 Target: *{MIN_LEADS} qualified leads* | Keyword: *{base_kw}*\n"
             f"Existing emails blocked: *{len(state['seen_emails'])}*\n"
             f"Filter auto-relaxes each round until target is reached.")

        round_num  = 0
        MAX_ROUNDS = 5

        while True:
            while state["status"] == "PAUSED": time.sleep(1)
            if state["status"] == "IDLE": return

            round_num += 1
            if round_num > MAX_ROUNDS:
                send(f"⚠️ Reached max {MAX_ROUNDS} rounds. "
                     f"Collected *{state['qualified_count']}* leads total. Proceeding to email phase.")
                break

            cur_max_rating = base_max_rating
            if round_num == 1:
                cur_max_installs = base_max_installs
            elif round_num == 2:
                cur_max_installs = base_max_installs * 10
            elif round_num == 3:
                cur_max_installs = base_max_installs * 50
            else:
                cur_max_installs = 999_999_999

            send(f"🔄 *Round {round_num}* | Filter: rating < {cur_max_rating} (strict) | "
                 f"installs ≤ {cur_max_installs:,}")

            keywords = generate_keywords_from_base(base_kw)
            if not keywords:
                send("❌ Keyword generation failed.")
                break

            state["generated_kws"] = keywords
            state["kw_index"]      = 0

            while state["kw_index"] < len(state["generated_kws"]):
                while state["status"] == "PAUSED": time.sleep(1)
                if state["status"] == "IDLE": return

                kw    = state["generated_kws"][state["kw_index"]]
                kw_no = state["kw_index"] + 1
                total = len(state["generated_kws"])
                send(f"🔍 *R{round_num} KW {kw_no}/{total}:* `{kw}`")

                ids = get_search_ids_for_keyword(kw)

                if not ids:
                    send(f"⬛ 0 new apps for `{kw}` — skip")
                    state["kw_index"] += 1
                    continue

                send(f"📦 *{len(ids)}* apps — filtering...")

                kw_count, qualified_from_kw = 0, 0
                batch_raw = []
                fs = {"gov":0,"zero_rating":0,"rating":0,"installs":0,
                      "no_email":0,"dup":0,"passed":0}

                for app_id in ids:
                    while state["status"] == "PAUSED": time.sleep(1)
                    if state["status"] == "IDLE": break

                    state["scraped_ids"].add(app_id)
                    d = None
                    for _att in range(2):
                        try:
                            d = gplay(app_id, lang='en', country='us')
                            break
                        except Exception as _ge:
                            if _att == 0: time.sleep(1)
                    if not d:
                        continue

                    email, esrc = get_email(d)
                    rating      = float(d.get('score') or 0.0)
                    raw_inst    = d.get('minInstalls') or d.get('realInstalls') or 0
                    installs    = int(raw_inst) if raw_inst else 0

                    app_dict = {
                        "app_id":      app_id,
                        "app_name":    str(d.get('title','Unknown')),
                        "dev_name":    str(d.get('developer','') or ''),
                        "email":       email,
                        "email_source":esrc,
                        "rating":      rating,
                        "installs":    installs,
                        "genre":       str(d.get('genre','') or ''),
                        "summary":     str(d.get('summary','') or ''),
                        "description": str(d.get('description','') or '')[:1000],
                        "website":     str(d.get('developerWebsite','') or ''),
                        "privacy":     str(d.get('privacyPolicy','') or ''),
                        "link":        str(d.get('url','') or ''),
                        "updated":     str(d.get('updated','') or ''),
                        "keyword":     kw,
                    }
                    batch_raw.append(app_dict)

                    qual, _ = is_qualified(app_dict, cur_max_rating, cur_max_installs,
                                           state["seen_emails"], fs)
                    if qual:
                        if save_qualified_lead(app_dict):
                            state["seen_emails"].add(email)
                            state["qualified_count"] += 1
                            qualified_from_kw += 1
                            if state["qualified_count"] == MIN_LEADS:
                                send(f"🎯 *{MIN_LEADS} lead target reached!* "
                                     f"Continuing to scrape remaining keywords...")

                    kw_count               += 1
                    state["total_scraped"] += 1

                    if len(batch_raw) >= 50:
                        try:
                            requests.post(SHEET_URL,
                                json={"action":"save_raw_batch","rows":batch_raw}, timeout=30)
                            batch_raw = []
                        except: pass

                    time.sleep(random.uniform(0.05, 0.12))

                if batch_raw:
                    try:
                        requests.post(SHEET_URL,
                            json={"action":"save_raw_batch","rows":batch_raw}, timeout=30)
                    except: pass

                total_dropped = sum(fs[k] for k in fs if k != 'passed')
                drop_info = (f"NoEmail:{fs['no_email']} HighRating:{fs['rating']} "
                             f"HighInstalls:{fs['installs']} Dup:{fs['dup']}")
                send(f"✅ `{kw}` — {kw_count} apps | {qualified_from_kw}✅\n"
                     f"📊 {drop_info} | Passed:{fs['passed']}\n"
                     f"🏆 Progress: *{state['qualified_count']}/{MIN_LEADS}* leads")

                state["kw_index"] += 1

            if state["status"] == "IDLE": return

            if state["qualified_count"] >= MIN_LEADS:
                send(f"🎉 *TARGET REACHED!* {state['qualified_count']} qualified leads collected.\n"
                     f"Total apps scraped: *{state['total_scraped']}*")
                break

            still_need = MIN_LEADS - state["qualified_count"]
            if round_num == 1:   next_installs = base_max_installs * 10
            elif round_num == 2: next_installs = base_max_installs * 50
            else:                next_installs = 999_999_999
            send(f"⚠️ Round {round_num} done — *{state['qualified_count']}* leads | need {still_need} more.\n"
                 f"🔓 Next round: installs ≤ {next_installs:,} | rating still strictly < {base_max_rating}\n"
                 f"Starting Round {round_num+1}...")

        if state["status"] == "IDLE": return

        if state["current_set_id"]:
            mark_keyword_set_used(state["current_set_id"])
            state["current_set_id"] = None

        if state["qualified_count"] > 0:
            send(f"⏩ Starting email phase for *{state['qualified_count']}* leads...")
            state["status"] = "EMAILING"
            bot.send_message(cid, ".", reply_markup=kb())
            threading.Thread(target=phase2_email_only, daemon=True).start()
        else:
            send("⚠️ No qualified leads found at all.\nCheck: do apps in this niche have public emails?")
            state["status"] = "IDLE"
            bot.send_message(cid, ".", reply_markup=kb())

    except Exception as e:
        state["status"] = "IDLE"
        send(f"❌ Phase 1 Error: {e}")
        bot.send_message(cid, ".", reply_markup=kb())


# ════════════════════════════════════════════════════════════
#  EMAIL SEND HELPER
# ════════════════════════════════════════════════════════════
def _is_gmail_limit(resp):
    r = resp.lower()
    return ("too many times" in r or "service invoked" in r or
            "daily limit" in r or "quota exceeded" in r or
            "limit exceeded" in r)

def _is_html_response(resp):
    return resp.strip().startswith("<!") or "<html" in resp.lower()[:50]

def _try_send_with_sender(sender, email, subject, body_html):
    try:
        r = requests.post(
            sender['url'],
            json={"action":"send_email","to":email,"subject":subject,"body":body_html},
            timeout=30
        )
        resp = r.text.strip()
    except Exception as e:
        return f"error:Connection error: {e}"

    if resp == "Success":
        return "success"
    elif _is_gmail_limit(resp):
        return "gmail_limit"
    elif _is_html_response(resp):
        return "html_error"
    else:
        return f"error:{resp[:80]}"

def _exhaust_sender(sender_email):
    try:
        requests.post(SHEET_URL,
            json={"action":"increment_sender","email":sender_email,"force_exhaust":True},
            timeout=15)
    except: pass

def _get_next_sender(skip_email=None):
    try:
        senders = requests.post(SHEET_URL, json={"action":"get_senders"}, timeout=15).json()
        for s in senders:
            try:
                if skip_email and s.get('email') == skip_email: continue
                if int(s.get('sent',0)) < int(s.get('limit',0)):
                    return s, senders
            except: continue
        return None, senders
    except:
        return None, []

def _send_email_with_fallback(row, email_prompt, cid):
    email = str(row.get('email',''))
    esrc  = str(row.get('email_source','dev'))

    sender, senders = _get_next_sender()
    if not sender:
        info = "⚠️ All senders hit daily limit!\n"
        for s in senders:
            info += f"📧 {s.get('email','')} — {s.get('sent',0)}/{s.get('limit',0)}\n"
        send(info + "\nBot paused. Resume tomorrow.")
        state["status"] = "PAUSED"
        bot.send_message(cid, ".", reply_markup=kb())
        return None

    subject, body_html = build_clean_email(row, sender['email'], email_prompt)
    tried = set()

    while sender and state["status"] == "EMAILING":
        tried.add(sender['email'])
        result = _try_send_with_sender(sender, email, subject, body_html)

        if result == "success":
            try:
                requests.post(SHEET_URL, json={"action":"increment_sender","email":sender['email']}, timeout=15)
                requests.post(SHEET_URL, json={"action":"mark_emailed","email":email}, timeout=15)
            except: pass

            state["total_emailed"] += 1
            etag = {"dev":"📧","support":"📩","extracted":"📬"}.get(esrc,"📬")
            sent_now  = int(sender.get('sent',0)) + 1
            remaining = max(0, int(sender.get('limit',1)) - sent_now)
            quota_str = (f"{sent_now}/{sender.get('limit','?')} — {remaining} left"
                         if remaining > 0 else "LIMIT REACHED, switching next")
            send(f"✅ Email #{state['total_emailed']} sent\n"
                 f"App: {str(row.get('app_name','?'))[:35]}\n"
                 f"{etag} To: {email}\n"
                 f"Via: {sender['email']} ({quota_str})")
            return True

        elif result in ("gmail_limit", "html_error"):
            reason = "Gmail daily limit" if result == "gmail_limit" else "Script deployment error"
            send(f"🔄 {sender['email']} — {reason}. Switching sender...")
            _exhaust_sender(sender['email'])
            sender, senders = _get_next_sender(skip_email=None)
            while sender and sender['email'] in tried:
                tried.add(sender['email'])
                _exhaust_sender(sender['email'])
                sender, senders = _get_next_sender()

            if not sender:
                info = "⚠️ All senders exhausted!\n"
                for s in senders:
                    info += f"📧 {s.get('email','')} — {s.get('sent',0)}/{s.get('limit',0)}\n"
                send(info + "\nBot paused. Resume tomorrow.")
                state["status"] = "PAUSED"
                bot.send_message(cid, ".", reply_markup=kb())
                return None

        else:
            send(f"❌ Skip {email}: {result[6:] if result.startswith('error:') else result}")
            return False

    return False

# ════════════════════════════════════════════════════════════
#  STANDALONE EMAIL SENDER
# ════════════════════════════════════════════════════════════
def phase2_send_pending():
    cid = state["chat_id"]
    if not cid: return

    state["status"] = "EMAILING"
    bot.send_message(cid, ".", reply_markup=kb())

    try:
        state["settings"] = {}
        settings     = get_settings()
        email_prompt = settings.get('email_prompt','Write a professional cold outreach email.')

        send("📤 Loading pending leads from Sheet...")
        pending = get_pending_qualified_leads()
        if not pending:
            send("⚠️ No pending leads found in Sheet.")
            state["status"] = "IDLE"
            bot.send_message(cid, ".", reply_markup=kb()); return

        send(f"📤 Found *{len(pending)}* pending leads — starting.\nPress Stop to cancel.")
        state["total_emailed"] = 0

        for row in pending:
            while state["status"] == "PAUSED": time.sleep(1)
            if state["status"] != "EMAILING": break

            result = _send_email_with_fallback(row, email_prompt, cid)
            if result is None: break

            if result:
                wait = random.randint(60, 120)
                send(f"⏳ Waiting {wait}s...")
                for _ in range(wait):
                    if state["status"] != "EMAILING": break
                    time.sleep(1)

        if state["status"] == "EMAILING":
            send(f"🎉 Done! Total sent: *{state['total_emailed']}*")
            state["status"] = "IDLE"
            bot.send_message(cid, ".", reply_markup=kb())
        elif state["status"] == "IDLE":
            send(f"⏹️ Stopped. Sent *{state['total_emailed']}* emails.")
            bot.send_message(cid, ".", reply_markup=kb())

    except Exception as e:
        state["status"] = "IDLE"
        send(f"❌ Send Emails Error: {e}")
        bot.send_message(cid, ".", reply_markup=kb())

# ════════════════════════════════════════════════════════════
#  PHASE 2 — EMAIL
# ════════════════════════════════════════════════════════════
def phase2_email_only():
    cid = state["chat_id"]
    if not cid:
        print("Cannot start phase2: no chat_id"); return

    try:
        settings     = get_settings()
        email_prompt = settings.get('email_prompt','Write a professional cold outreach email.')

        send("📧 *Starting email phase...* Loading pending leads.")
        pending = get_pending_qualified_leads()
        if not pending:
            send("⚠️ No pending qualified leads found.")
            state["status"] = "IDLE"
            bot.send_message(cid, ".", reply_markup=kb()); return

        send(f"📧 *Sending to {len(pending)} qualified leads*\n⏳ 1-2 min gap between each.")
        state["total_emailed"] = 0

        for row in pending:
            while state["status"] == "PAUSED": time.sleep(1)
            if state["status"] != "EMAILING": break

            result = _send_email_with_fallback(row, email_prompt, cid)
            if result is None: break

            if result:
                wait = random.randint(60, 120)
                send(f"⏳ Waiting {wait}s before next...")
                for _ in range(wait):
                    if state["status"] != "EMAILING": break
                    time.sleep(1)

        if state["status"] == "EMAILING":
            send(f"🎉 *Email Phase Complete!* Total sent: *{state['total_emailed']}*")
            state["status"] = "IDLE"
            bot.send_message(cid, ".", reply_markup=kb())
        elif state["status"] == "PAUSED":
            bot.send_message(cid, "⏸️ Paused during email phase.", reply_markup=kb())

    except Exception as e:
        state["status"] = "IDLE"
        send(f"❌ Email Phase Error: {e}")
        bot.send_message(cid, ".", reply_markup=kb())

def get_pending_qualified_leads():
    try:
        r = requests.post(SHEET_URL, json={"action":"get_pending_qualified_leads"}, timeout=30)
        if r.status_code == 200:
            return r.json() if isinstance(r.json(), list) else []
    except: pass
    return []

# ════════════════════════════════════════════════════════════
#  AI EMAIL BUILDER
#  Sheet এর email_prompt হলো পুরো email template/instructions।
#  AI শুধু app-specific placeholders fill করবে।
#  Template এ {app_name}, {dev_name}, {rating}, {genre},
#  {personalization}, {urgency_note} placeholders থাকলে replace হবে।
#  না থাকলে AI কে context দিয়ে সেই prompt অনুযায়ী লিখতে বলবে।
# ════════════════════════════════════════════════════════════
def build_clean_email(row, sender_email, email_prompt):
    app_name    = str(row.get('app_name','Unknown App'))
    dev_name    = str(row.get('dev_name','') or '').strip()
    if not dev_name or len(dev_name) < 2 or len(dev_name) > 40:
        dev_name = "Developer"
    rating      = float(row.get('rating') or 0.0)
    genre       = str(row.get('genre','') or '')
    website_url = str(row.get('website','') or '')
    description = str(row.get('description','') or '')[:400]
    summary     = str(row.get('summary','') or '')

    # Rating context
    if rating < 3.5:
        urgency_note = f"currently rated {rating:.1f} stars — critically low"
    elif rating < 4.0:
        urgency_note = f"currently rated {rating:.1f} stars — below average"
    else:
        urgency_note = f"currently rated {rating:.1f} stars"

    # Genre-specific angle
    genre_lower = genre.lower()
    if any(w in genre_lower for w in ['finance','bank','payment','fintech','money']):
        service_angle = "Finance apps lose user trust rapidly when ratings drop."
    elif any(w in genre_lower for w in ['shopping','delivery','ecommerce','store']):
        service_angle = "Low ratings in shopping apps push customers straight to competitors."
    elif any(w in genre_lower for w in ['game','gaming','puzzle','casual']):
        service_angle = "Game ratings directly impact Play Store ranking and daily installs."
    elif any(w in genre_lower for w in ['health','fitness','medical','workout']):
        service_angle = "Health apps need strong ratings to earn user trust and retention."
    elif any(w in genre_lower for w in ['education','learning','kids','school']):
        service_angle = "Educational apps rely on ratings to gain parent and school adoption."
    else:
        service_angle = "Play Store ratings directly affect your app's visibility and downloads."

    # Personalization from website
    personalization = ""
    if website_url and "http" in website_url:
        try:
            resp = requests.get(website_url, timeout=5, headers={"User-Agent":"Mozilla/5.0"})
            text = re.sub(r'<[^>]+',' ', resp.text)
            text = re.sub(r'\s+',' ', text).strip()[:400]
            match = re.search(r'(\d[\d,]+\+?\s*(users?|downloads?|customers?|installs?))', text, re.I)
            if match:
                personalization = f"Impressive — I saw you have {match.group(0)}!"
        except: pass

    if not personalization:
        if summary and len(summary) > 15:
            personalization = f"Your app's focus on '{summary[:60]}' caught my attention."
        elif description:
            first = description.split('.')[0].strip()
            if len(first) > 15:
                personalization = f"I liked your approach: '{first[:70]}'."
        else:
            personalization = f"I came across {app_name} on the Play Store."

    # ── Check if template has placeholders — if yes, fill directly ──
    has_placeholders = any(p in email_prompt for p in [
        '{app_name}', '{dev_name}', '{rating}', '{genre}',
        '{personalization}', '{urgency_note}', '{service_angle}'
    ])

    if has_placeholders:
        # Direct fill — no AI needed
        filled = (email_prompt
                  .replace('{app_name}',       app_name)
                  .replace('{dev_name}',        dev_name)
                  .replace('{rating}',          f"{rating:.1f}")
                  .replace('{urgency_note}',    urgency_note)
                  .replace('{genre}',           genre)
                  .replace('{personalization}', personalization)
                  .replace('{service_angle}',   service_angle))

        # Extract subject if present (format: SUBJECT: ...\nBODY: ...)
        if "SUBJECT:" in filled and "BODY:" in filled:
            try:
                subject  = filled.split("SUBJECT:")[1].split("BODY:")[0].strip()
                raw_body = filled.split("BODY:")[1].strip()
            except:
                subject  = f"Quick idea to boost {app_name}'s Play Store rating"
                raw_body = filled
        elif "SUBJECT:" in filled:
            lines    = filled.split('\n', 1)
            subject  = lines[0].replace("SUBJECT:","").strip()
            raw_body = lines[1].strip() if len(lines) > 1 else filled
        else:
            subject  = f"Quick idea to boost {app_name}'s Play Store rating"
            raw_body = filled

        body_html = _wrap_body_html(raw_body, sender_email)
        return subject, body_html

    # ── No placeholders — AI writes using sheet prompt as instructions ──
    prompt = f"""{email_prompt}

App details to personalize with:
- App Name: {app_name}
- Developer: {dev_name}
- Category: {genre}
- Rating: {urgency_note}
- Personalization: {personalization}
- Why it matters: {service_angle}

Output ONLY in this exact format (no extra text):
SUBJECT: [subject line]
BODY: [email body, use <br> for line breaks, no markdown]"""

    result = call_ai(prompt, max_tokens=600, silent_fallback=True)

    if result and "SUBJECT:" in result and "BODY:" in result:
        try:
            subject  = result.split("SUBJECT:")[1].split("BODY:")[0].strip()
            raw_body = result.split("BODY:")[1].strip()
            raw_body = raw_body.replace('**','').replace('*','').replace('##','').replace('#','').strip()
            body_html = _wrap_body_html(raw_body, sender_email)
            return subject, body_html
        except Exception as e:
            print(f"Email parse error: {e}")

    # Fallback
    return _fallback_email(app_name, dev_name, urgency_note, service_angle,
                           personalization, sender_email)

def _wrap_body_html(raw_body, sender_email):
    """Wrap body text in styled HTML container with unsubscribe footer."""
    body_html   = raw_body.replace('\n\n','<br><br>').replace('\n','<br>')
    unsubscribe = (f'<br><br><hr style="border:0;border-top:1px solid #eee;margin:16px 0;">'
                   f'<p style="text-align:center;font-size:11px;color:#bbb;">'
                   f'<a href="mailto:{sender_email}?subject=Unsubscribe&body=Remove me." '
                   f'style="color:#bbb;">Unsubscribe</a></p>')
    return (f'<div style="font-family:Arial,sans-serif;font-size:14px;'
            f'line-height:1.7;color:#333;max-width:600px;margin:0 auto;">'
            f'{body_html}{unsubscribe}</div>')

def _fallback_email(app_name, dev_name, urgency_note, service_angle,
                    personalization, sender_email):
    """Static fallback when AI unavailable."""
    body = (
        f"Dear {dev_name},<br><br>"
        f"{personalization}<br><br>"
        f"I noticed {app_name} is {urgency_note}. {service_angle}<br><br>"
        f"I help Android app developers improve their Play Store ratings through genuine "
        f"review management. I'd love to share a quick strategy that's worked for similar apps.<br><br>"
        f"Would you be open to a short chat?<br><br>"
        f"Best regards,<br>"
        f"Abu Raihan<br>"
        f"Play Store Review Specialist<br>"
        f"WhatsApp: +8801902911261<br>"
        f"Telegram: t.me/abu_raihan69"
    )
    subject   = f"Quick idea to boost {app_name}'s Play Store rating"
    body_html = _wrap_body_html(body, sender_email)
    return subject, body_html

# ─── SPAM TEST ────────────────────────────────────────────────
def run_spam_test_with_sender(test_email, sender):
    sender_email = sender.get('email', 'your@email.com')
    subject = "Test: Can You See This? [ASO Audit]"
    body = f"""<div style="font-family:Arial,sans-serif;font-size:14px;line-height:1.7;color:#333;max-width:600px;margin:0 auto;">
<p>Hi Developer,</p>
<p>This is a <strong>test email</strong> from your Leadgen automation bot.</p>
<p>If you received this, your sender <code>{sender_email}</code> is working correctly ✅</p>
<p>Your bot is ready to send personalized cold outreach emails to app developers on Google Play Store.</p>
<p>Each real email will be personalized based on the app's name, rating, genre, and developer info.</p>
<hr style="border:none;border-top:1px solid #eee;margin:20px 0;">
<p style="font-size:12px;color:#888;">
Sent via Leadgen Bot | Sender: {sender_email}<br>
<a href="mailto:{sender_email}?subject=Unsubscribe&body=Remove me." style="color:#bbb;">Unsubscribe</a>
</p>
</div>"""
    send(f"📤 Sending test to `{test_email}` via `{sender_email}`...")
    try:
        r2   = requests.post(
            sender['url'],
            json={"action":"send_email","to":test_email,"subject":subject,"body":body},
            timeout=30
        )
        resp = r2.text.strip()
        if resp == "Success":
            send(f"✅ *Test email sent!*\nTo: `{test_email}`\nVia: `{sender_email}`\n📌 Subject: {subject}")
        else:
            send(f"❌ *Sender returned error:* `{resp}`\n\n"
                 f"Check:\n• Is the Apps Script URL correct?\n• Is the script deployed as 'Anyone'?\n• Does the script have Gmail permission?")
    except requests.exceptions.Timeout:
        send(f"❌ *Timeout* — Apps Script URL took too long. Check the URL is correct.")
    except Exception as e:
        send(f"❌ *Error sending test:* `{e}`")

def show_sender_selection(test_email):
    try:
        senders = requests.post(SHEET_URL, json={"action":"get_senders"}, timeout=15).json()
        if not senders:
            send("❌ No senders available. Add one first.")
            bot.send_message(state["chat_id"], ".", reply_markup=kb()); return
        mk = InlineKeyboardMarkup()
        for s in senders:
            mk.add(InlineKeyboardButton(s['email'], callback_data=f"testsend_{s['email']}"))
        mk.add(InlineKeyboardButton("🔙 Cancel", callback_data="cancel_test"))
        bot.send_message(state["chat_id"], "📧 Choose a sender for the test:", reply_markup=mk)
        state["tmp_test_email"] = test_email
        state["status"]         = "WAITING_TEST_SENDER"
    except Exception as e:
        send(f"❌ Error: {e}")
        state["status"] = "IDLE"
        bot.send_message(state["chat_id"], ".", reply_markup=kb())

# ════════════════════════════════════════════════════════════
#  SCHEDULER — runs in its own thread, never blocks polling
# ════════════════════════════════════════════════════════════
def run_scheduler():
    tz = pytz.timezone('Asia/Dhaka')
    print("⏰ Scheduler started.")
    triggered_today = {}

    while True:
        try:
            now_dt = datetime.now(tz)
            now_hm = now_dt.strftime("%H:%M")
            today  = now_dt.strftime("%Y-%m-%d")

            if state["status"] == "IDLE" and state["chat_id"]:
                times = get_schedule_times()
                print(f"[Scheduler] now={now_hm} | schedules={times}")
                for t in times:
                    if t == now_hm and triggered_today.get(t) != today:
                        triggered_today[t] = today
                        send(f"⏰ Scheduled time *{t}* — starting automation...")
                        bot.send_message(state["chat_id"], ".", reply_markup=kb())
                        threading.Thread(target=phase1_scrape, daemon=True).start()
                        break
        except Exception as e:
            print(f"[Scheduler] Error: {e}")
        time.sleep(10)

# ─── REFRESH ─────────────────────────────────────────────────
def refresh_status():
    sets    = get_keyword_sets()
    pending = [s for s in sets if s.get('status') == 'pending']
    ai_st   = "✅ Working" if state["ai_working"] else "❌ Offline (fallback mode)"
    send(f"🔄 *Status Report*\n\n"
         f"Bot: `{state['status']}`\n"
         f"AI: {ai_st}\n"
         f"Pending keyword sets: *{len(pending)}*\n"
         f"Scraped this run: *{state['total_scraped']}*\n"
         f"Qualified: *{state['qualified_count']}*\n"
         f"Emailed: *{state['total_emailed']}*")

# ─── BOT HANDLERS ────────────────────────────────────────────
@bot.message_handler(commands=['start'])
def welcome(message):
    state["chat_id"]  = message.chat.id
    state["status"]   = "IDLE"
    state["settings"] = {}
    bot.reply_to(message,
        "👋 *Welcome Boss!*\n\n"
        "*🚀 Start Automation:* AI scrape + filter + personalized email — fully automatic.\n"
        "*📅 Schedules:* Set daily auto-start times (Dhaka timezone).\n"
        "*🔑 Keywords:* Add sets like `[crypto wallet] [travel app]` — used one by one.\n"
        "*📧 Senders:* Manage Gmail sender accounts.\n"
        "*🧪 Spam Test:* Send a test email to verify sender.\n"
        "*🔄 Refresh:* Show bot status & AI health.\n\n"
        "Use the buttons below 👇",
        parse_mode="Markdown", reply_markup=kb())

@bot.callback_query_handler(func=lambda c: True)
def callbacks(call):
    cid = call.message.chat.id
    d   = call.data

    if d == "back":
        state["status"] = "IDLE"
        bot.send_message(cid, "🔙 Main Menu.", reply_markup=kb())

    elif d == "add_sender":
        code = """function doPost(e) {
  var data = JSON.parse(e.postData.contents);
  if (data.action == "send_email") {
    try {
      GmailApp.sendEmail(data.to, data.subject, "", {htmlBody: data.body});
      return ContentService.createTextOutput("Success");
    } catch(err) { return ContentService.createTextOutput("Error: " + err); }
  }
}"""
        bot.send_message(cid, f"📝 Deploy this in Apps Script, then send the URL:\n\n`{code}`",
            parse_mode="Markdown", reply_markup=back_kb())
        state["status"] = "WAITING_URL"

    elif d.startswith("del_sender_"):
        e2 = d.split("del_sender_")[1]
        mk = InlineKeyboardMarkup()
        mk.add(InlineKeyboardButton("✅ Delete", callback_data=f"cfm_sender_{e2}"),
               InlineKeyboardButton("❌ Cancel", callback_data="cancel"))
        bot.send_message(cid, f"Delete sender *{e2}*?", parse_mode="Markdown", reply_markup=mk)

    elif d.startswith("cfm_sender_"):
        e2 = d.split("cfm_sender_")[1]
        try:
            requests.post(SHEET_URL, json={"action":"delete_sender","email":e2}, timeout=15)
            bot.send_message(cid, f"🗑️ Deleted *{e2}*", parse_mode="Markdown")
        except:
            bot.send_message(cid, "❌ Failed to delete.")

    elif d == "add_schedule":
        state["status"] = "WAITING_SCHEDULE"
        bot.send_message(cid, "⏰ Send time (*02:30 PM* or *14:30*)", reply_markup=back_kb())

    elif d.startswith("del_schedule_"):
        tm = d.split("del_schedule_")[1]
        mk = InlineKeyboardMarkup()
        mk.add(InlineKeyboardButton("✅ Delete", callback_data=f"cfm_schedule_{tm}"),
               InlineKeyboardButton("❌ Cancel", callback_data="cancel"))
        bot.send_message(cid, f"Delete schedule *{tm}*?", parse_mode="Markdown", reply_markup=mk)

    elif d.startswith("cfm_schedule_"):
        tm = d.split("cfm_schedule_")[1]
        delete_schedule_time(tm)
        bot.send_message(cid, f"🗑️ Deleted schedule *{tm}*", parse_mode="Markdown")

    elif d == "add_keyword":
        state["status"] = "WAITING_KEYWORD"
        bot.send_message(cid,
            "🔑 Send keyword sets like:\n`[crypto wallet] [travel app] [fitness tracker]`\nEach bracket = one set.",
            reply_markup=back_kb())

    elif d.startswith("del_keyword_"):
        kid = d.split("del_keyword_")[1]
        mk  = InlineKeyboardMarkup()
        mk.add(InlineKeyboardButton("✅ Delete", callback_data=f"cfm_keyword_{kid}"),
               InlineKeyboardButton("❌ Cancel", callback_data="cancel"))
        bot.send_message(cid, "Delete this keyword set?", reply_markup=mk)

    elif d.startswith("cfm_keyword_"):
        kid = d.split("cfm_keyword_")[1]
        delete_keyword_set(kid)
        bot.send_message(cid, "🗑️ Keyword set deleted.")

    elif d == "cancel":
        bot.send_message(cid, "Cancelled.")

    elif d == "cancel_test":
        state["status"]         = "IDLE"
        state["tmp_test_email"] = None
        bot.send_message(cid, "Test cancelled.", reply_markup=kb())

    elif d.startswith("testsend_"):
        sender_email = d.split("testsend_")[1]
        test_email   = state.get("tmp_test_email")
        if not test_email:
            bot.send_message(cid, "❌ No test email in memory. Start over.")
            state["status"] = "IDLE"; return
        try:
            senders = requests.post(SHEET_URL, json={"action":"get_senders"}, timeout=15).json()
            sender  = next((s for s in senders if s['email'] == sender_email), None)
            if not sender:
                bot.send_message(cid, "❌ Sender not found.")
                state["status"] = "IDLE"; return
        except:
            bot.send_message(cid, "❌ Failed to fetch sender.")
            state["status"] = "IDLE"; return
        bot.send_message(cid, f"Sending test to *{test_email}* via {sender_email}...", parse_mode="Markdown")
        threading.Thread(target=run_spam_test_with_sender, args=(test_email, sender), daemon=True).start()
        state["status"]         = "IDLE"
        state["tmp_test_email"] = None

@bot.message_handler(func=lambda m: True)
def handle(message):
    text = message.text.strip()
    state["chat_id"] = message.chat.id

    if text == "🔙 Back":
        state["status"]         = "IDLE"
        state["tmp_url"]        = None
        state["tmp_email"]      = None
        state["tmp_test_email"] = None
        bot.reply_to(message, "🔙 Main Menu.", reply_markup=kb()); return

    if state["status"] == "WAITING_URL":
        if "script.google.com" in text:
            state["tmp_url"] = text
            state["status"]  = "WAITING_EMAIL"
            bot.reply_to(message, "✅ URL saved! Send the *email address* of this sender.",
                parse_mode="Markdown", reply_markup=back_kb())
        else:
            bot.reply_to(message, "❌ Invalid. Must be a Google Apps Script URL.", reply_markup=back_kb())
        return

    elif state["status"] == "WAITING_EMAIL":
        if "@" in text:
            state["tmp_email"] = text
            state["status"]    = "WAITING_LIMIT"
            bot.reply_to(message, "✅ Email saved! Send *daily send limit* (e.g. 20).",
                parse_mode="Markdown", reply_markup=back_kb())
        else:
            bot.reply_to(message, "❌ Invalid email.", reply_markup=back_kb())
        return

    elif state["status"] == "WAITING_LIMIT":
        if text.isdigit():
            try:
                requests.post(SHEET_URL, json={
                    "action":"add_sender","email":state["tmp_email"],
                    "url":state["tmp_url"],"limit":int(text)
                }, timeout=15)
                bot.reply_to(message, f"🎉 Sender *{state['tmp_email']}* added! Limit: {text}/day",
                    parse_mode="Markdown", reply_markup=kb())
            except:
                bot.reply_to(message, "❌ Failed. Check sheet connection.", reply_markup=kb())
            state["status"]    = "IDLE"
            state["tmp_url"]   = None
            state["tmp_email"] = None
        else:
            bot.reply_to(message, "❌ Send a number.", reply_markup=back_kb())
        return

    elif state["status"] == "WAITING_SCHEDULE":
        p = parse_time(text)
        if p:
            add_schedule_time(p)
            bot.reply_to(message, f"✅ Schedule set for *{p}* daily (Dhaka time)!",
                parse_mode="Markdown", reply_markup=kb())
            state["status"] = "IDLE"
        else:
            bot.reply_to(message, "❌ Format: 02:30 PM or 14:30", reply_markup=back_kb())
        return

    elif state["status"] == "WAITING_KEYWORD":
        sets = re.findall(r'\[(.*?)\]', text)
        if sets:
            for s in sets:
                s = s.strip()
                if s: add_keyword_set(s)
            bot.reply_to(message, f"✅ Added {len(sets)} keyword set(s).",
                parse_mode="Markdown", reply_markup=kb())
        else:
            bot.reply_to(message, "❌ No brackets found. Example: `[crypto wallet]`",
                reply_markup=back_kb())
        state["status"] = "IDLE"; return

    elif state["status"] == "WAITING_TEST_EMAIL":
        if "@" in text:
            show_sender_selection(text)
        else:
            bot.reply_to(message, "❌ Invalid email. Try again.", reply_markup=back_kb())
        return

    # ── Main buttons ──
    if text == "🚀 Start Automation":
        if state["status"] == "IDLE":
            threading.Thread(target=phase1_scrape, daemon=True).start()

    elif text == "🛑 Pause":
        if state["status"] in ["SCRAPING","FILTERING","EMAILING"]:
            state["status"] = "PAUSED"
            bot.reply_to(message, "🛑 *Paused.* Progress saved.", reply_markup=kb())

    elif text == "▶️ Resume":
        if state["status"] == "PAUSED":
            if state["generated_kws"] and state["kw_index"] < len(state["generated_kws"]):
                state["status"] = "SCRAPING"
            else:
                state["status"] = "EMAILING"
            bot.reply_to(message, "▶️ *Resuming...*", reply_markup=kb())

    elif text == "⏹️ Stop":
        if state["status"] in ["SCRAPING","FILTERING","EMAILING","PAUSED"]:
            state["status"]          = "IDLE"
            state["generated_kws"]   = []
            state["kw_index"]        = 0
            state["current_set_id"]  = None
            state["qualified_count"] = 0
            bot.reply_to(message, "⏹️ *Stopped.* Keyword set remains pending.", reply_markup=kb())

    elif text == "⏹️ Reset":
        state.update({
            "status":"IDLE","generated_kws":[],"kw_index":0,
            "scraped_ids":set(),"total_scraped":0,"total_emailed":0,
            "current_set_id":None,"qualified_count":0,
            "seen_emails":set(),"settings":{},"ai_working":True,"ai_fail_count":0
        })
        bot.reply_to(message, "⏹️ *Fully reset.*", reply_markup=kb())

    elif text == "🔄 Refresh":
        refresh_status()

    elif text == "📅 Schedules":
        times = get_schedule_times()
        mk    = InlineKeyboardMarkup()
        txt   = "📋 *Scheduled times (Dhaka):*\n\n"
        if not times:
            txt += "_None set._\n"
        else:
            for t in times:
                txt += f"• {t}\n"
                mk.add(InlineKeyboardButton(f"🗑️ Delete {t}", callback_data=f"del_schedule_{t}"))
        mk.add(InlineKeyboardButton("➕ Add Time", callback_data="add_schedule"))
        mk.add(InlineKeyboardButton("🔙 Back",     callback_data="back"))
        bot.reply_to(message, txt, parse_mode="Markdown", reply_markup=mk)

    elif text == "🔑 Keywords":
        sets = get_keyword_sets()
        mk   = InlineKeyboardMarkup()
        txt  = "🔑 *Keyword sets:*\n\n"
        if not sets:
            txt += "_None added._\n"
        else:
            for s in sets:
                icon = "✅" if s.get('status') == 'used' else "⏳"
                txt += f"{icon} `{s.get('set_text','')}`\n"
                if s.get('status') == 'pending':
                    mk.add(InlineKeyboardButton(
                        f"🗑️ {s.get('set_text','')[:22]}",
                        callback_data=f"del_keyword_{s.get('id')}"))
        mk.add(InlineKeyboardButton("➕ Add Set", callback_data="add_keyword"))
        mk.add(InlineKeyboardButton("🔙 Back",    callback_data="back"))
        bot.reply_to(message, txt, parse_mode="Markdown", reply_markup=mk)

    elif text == "📤 Send Emails":
        if state["status"] == "IDLE":
            threading.Thread(target=phase2_send_pending, daemon=True).start()
        else:
            bot.reply_to(message, "⚠️ Bot is busy. Stop current task first.", reply_markup=kb())

    elif text == "🧪 Spam Test":
        if state["status"] == "IDLE":
            state["status"] = "WAITING_TEST_EMAIL"
            bot.reply_to(message, "📧 Send the email address you want to test with.",
                reply_markup=back_kb())

    elif text == "📧 Senders":
        try:
            senders = requests.post(SHEET_URL, json={"action":"get_senders"}, timeout=15).json()
        except:
            bot.reply_to(message, "❌ Cannot reach Sheet.", reply_markup=kb()); return
        mk  = InlineKeyboardMarkup()
        txt = "📋 *Senders:*\n\n"
        if not senders:
            txt += "_None yet._\n"
        else:
            for i, s in enumerate(senders):
                txt += f"{i+1}. `{s.get('email')}` — {s.get('sent',0)}/{s.get('limit',0)}\n"
                mk.add(InlineKeyboardButton(f"🗑️ {s.get('email')}", callback_data=f"del_sender_{s.get('email')}"))
        mk.add(InlineKeyboardButton("➕ Add Sender", callback_data="add_sender"))
        mk.add(InlineKeyboardButton("🔙 Back",       callback_data="back"))
        bot.reply_to(message, txt, parse_mode="Markdown", reply_markup=mk)

# ════════════════════════════════════════════════════════════
#  MAIN — Render-safe: polling in own thread, Flask in main thread
# ════════════════════════════════════════════════════════════
def run_polling():
    while True:
        try:
            print("🤖 Polling...")
            bot.polling(none_stop=True, interval=0, timeout=30)
        except Exception as e:
            print(f"Poll error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    print("🚀 Starting Lead Gen Bot...")
    threading.Thread(target=run_polling,    daemon=True).start()
    threading.Thread(target=run_scheduler,  daemon=True).start()
    # Flask runs in main thread — keeps Render's HTTP port alive, prevents sleep/block
    run_web()
