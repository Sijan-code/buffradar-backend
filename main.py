from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse, JSONResponse
from starlette.concurrency import run_in_threadpool
from starlette.background import BackgroundTask
from pydantic import BaseModel
from typing import List, Optional
import functools
import glob
import hashlib
import inspect
import json
import mimetypes
import os
import re
import secrets
import time
import requests
import shutil
import subprocess
import tempfile
import threading
import urllib.parse
import urllib.request
import imageio_ffmpeg
import yt_dlp
from media_extras import router as extras_router, build_thumbnails, build_subtitle_tracks

app = FastAPI()

# Next.js ফ্রন্টএন্ডের কানেকশন পারমিশন
# ⚠️ মার্কেটে ছাড়ার আগে ["*"] বদলে শুধু নিজের ডোমেইন দাও, যেমন ["https://buffradar.com"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
    "https://buffradar.com",
    "https://www.buffradar.com",
    "https://buffradar-frontend.vercel.app",
    "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(extras_router)


class VideoRequest(BaseModel):
    url: str


# ---------------------------------------------------------------------------
# YouTube-এর "Sign in to confirm you're not a bot" সমস্যার জন্য কুকি (cookies) সাপোর্ট
# backend ফোল্ডারে cookies.txt (Netscape ফরম্যাট) থাকলে প্রতিটি yt-dlp কলে নিজে থেকে ব্যবহার হয়।
# অন্য জায়গায় রাখতে চাইলে YT_COOKIES_FILE এনভায়রনমেন্ট ভেরিয়েবলে পাথ দিন (Render-এ Secret File)।
# cookies.txt-এ আপনার লগইন সেশন থাকে: এটা কখনো GitHub-এ তুলবেন না।
# ---------------------------------------------------------------------------
YT_COOKIES_SRC = os.environ.get("YT_COOKIES_FILE") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies.txt")
_YT_COOKIES_COPY = {"path": None, "mtime": 0.0}


def ytdlp_cookie_path():
    """cookies.txt-এর একটা কপি ফেরত দেয় (yt-dlp ফাইলে লিখতে পারে বলে read-only জায়গা থেকেও যেন চলে)।"""
    if not os.path.isfile(YT_COOKIES_SRC):
        return None
    mtime = os.path.getmtime(YT_COOKIES_SRC)
    cp = _YT_COOKIES_COPY
    if not cp["path"] or not os.path.exists(cp["path"]) or mtime > cp["mtime"]:
        if not cp["path"] or not os.path.exists(cp["path"]):
            fd, cp["path"] = tempfile.mkstemp(prefix="yt_cookies_", suffix=".txt")
            os.close(fd)
        shutil.copyfile(YT_COOKIES_SRC, cp["path"])
        cp["mtime"] = mtime
    return cp["path"]


def ytdlp_cookie_args():
    p = ytdlp_cookie_path()
    return ["--cookies", p] if p else []


def with_cookies(opts: dict) -> dict:
    p = ytdlp_cookie_path()
    return {**opts, "cookiefile": p} if p else opts


print("[yt] cookies.txt ব্যবহার হচ্ছে" if os.path.isfile(YT_COOKIES_SRC) else "[yt] cookies.txt পাওয়া যায়নি (YouTube বট-চেকে আটকাতে পারে)")


def fetch_youtube_api_tags(video_id):
    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key or not video_id:
        return []
    try:
        api = (
            "https://www.googleapis.com/youtube/v3/videos?part=snippet&id="
            + urllib.parse.quote(str(video_id))
            + "&key="
            + urllib.parse.quote(api_key)
        )
        with urllib.request.urlopen(api, timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        items = data.get("items") or []
        return (items[0].get("snippet", {}).get("tags") or []) if items else []
    except Exception:
        return []


def parse_height(quality: str) -> int:
    """'2160p' / '1080' / '4k' / '2k' -> সর্বোচ্চ ভিডিও হাইট (পিক্সেল)।"""
    q = (quality or "").lower().strip()
    if q == "4k":
        return 2160
    if q == "2k":
        return 1440
    digits = q.replace("p", "")
    if digits.isdigit() and 100 <= int(digits) <= 4320:
        return int(digits)
    return 1080


def parse_bitrate(bitrate: str) -> int:
    """'320kbps' / '128' -> kbps সংখ্যা (৩২–৩২০ এর মধ্যে), ভুল হলে 192।"""
    digits = (bitrate or "").lower().replace("kbps", "").strip()
    if digits.isdigit() and 32 <= int(digits) <= 320:
        return int(digits)
    return 192


def is_valid_http_url(u: str) -> bool:
    """শুধু http/https লিংক গ্রহণ করবে। '-' দিয়ে শুরু হওয়া কিছু yt-dlp-কে অপশন হিসেবে ধরিয়ে দিতে পারে,
    সেটা আটকানোর জন্য এই চেক আর নিচের কমান্ডে '--' দুটোই দরকার।"""
    try:
        p = urllib.parse.urlparse(u or "")
    except Exception:
        return False
    return p.scheme in ("http", "https") and bool(p.netloc)


# ---------------------------------------------------------------------------
# ব্যবহারের সীমা (প্রথম ব্যবহার থেকে rolling ২৪ ঘণ্টা) — Supabase-এর usage_events টেবিল (usage_setup.sql) দিয়ে
# Render-এ লাগবে: SUPABASE_URL আর SUPABASE_SERVICE_ROLE_KEY (গোপন চাবি! শুধু Render-এ, কখনো GitHub/ছবিতে নয়)
# দুটোর একটা না থাকলে সীমা বন্ধ থাকে (লোকাল টেস্টে সুবিধা)। জরুরিতে বন্ধ করতে USAGE_LIMITS=off।
# অতিথি = বেনামি (anonymous) Supabase অ্যাকাউন্ট; সদস্য = Google ইত্যাদি দিয়ে সাইন-আপ করা অ্যাকাউন্ট (সীমা ×৩)।
# Supabase বা নেটে সমস্যা হলে সাইট বন্ধ না করে সীমা ছাড়াই কাজ চলতে দেওয়া হয় (fail-open)।
# ---------------------------------------------------------------------------
def _env_int(name, default):
    try:
        return max(1, int(os.environ.get(name, default)))
    except (TypeError, ValueError):
        return default


SUPABASE_URL = (os.environ.get("SUPABASE_URL") or "").strip().rstrip("/")
SUPABASE_SERVICE_KEY = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
USAGE_ENABLED = bool(SUPABASE_URL and SUPABASE_SERVICE_KEY) and os.environ.get("USAGE_LIMITS", "on").lower() != "off"
GUEST_LIMITS = {"download": 4, "convert": 1, "edit_manual": 1, "edit_ai": 1}
MEMBER_MULT = _env_int("USAGE_MEMBER_MULT", 3)   # সাইন-আপ করলে সীমা এত গুণ
IP_MULT = _env_int("USAGE_IP_MULT", 5)           # নতুন বেনামি অ্যাকাউন্ট বানিয়ে সীমা এড়ানো ঠেকাতে IP-ভিত্তিক দ্বিতীয় বাধা
USAGE_HOURS = 24
_TOKEN_CACHE = {}                                # sha256(token) -> (মেয়াদ, user_id, tier)

print("[usage] সীমা চালু আছে" if USAGE_ENABLED else "[usage] সীমা বন্ধ (SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY সেট নেই)")


def _sha(text: str, n: int = 20) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()[:n]


def _sb_headers():
    # নতুন ধরনের secret key (sb_secret_...) JWT নয়, তাই শুধু apikey হেডারে পাঠাতে হয়;
    # Authorization: Bearer-এ দিলে Supabase "Invalid JWT" বলে প্রত্যাখ্যান করে। পুরোনো service_role key (JWT) দুই হেডারেই চলে।
    h = {"apikey": SUPABASE_SERVICE_KEY, "Content-Type": "application/json"}
    if SUPABASE_SERVICE_KEY.startswith("eyJ"):
        h["Authorization"] = f"Bearer {SUPABASE_SERVICE_KEY}"
    return h


def _sb_rpc(name: str, payload: dict):
    r = requests.post(f"{SUPABASE_URL}/rest/v1/rpc/{name}", headers=_sb_headers(), json=payload, timeout=8)
    if r.status_code >= 300:
        raise RuntimeError(f"Supabase rpc {name} HTTP {r.status_code}: {r.text[:200]}")
    return r.json() if r.text else None


def client_ip(http_request: Request) -> str:
    fwd = http_request.headers.get("x-forwarded-for", "")
    if fwd:
        return fwd.split(",")[0].strip()
    return http_request.client.host if http_request.client else "unknown"


def resolve_identity(http_request: Request):
    """(subject, tier, ip_subject) ফেরত দেয়। টোকেন ঠিক হলে ব্যবহারকারীর uuid, না হলে IP দিয়ে অতিথি।"""
    ip_subject = "ip:" + _sha(client_ip(http_request))
    auth = http_request.headers.get("authorization", "")
    token = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
    if token:
        key = _sha(token, 40)
        now = time.time()
        hit = _TOKEN_CACHE.get(key)
        if hit and hit[0] > now:
            return hit[1], hit[2], ip_subject
        # নেট/Supabase সমস্যা হলে exception উঠে যায়, ডাকা জায়গা fail-open করে
        r = requests.get(
            f"{SUPABASE_URL}/auth/v1/user",
            headers={"apikey": SUPABASE_SERVICE_KEY, "Authorization": f"Bearer {token}"},
            timeout=8,
        )
        if r.status_code == 200:
            u = r.json()
            uid = u.get("id")
            if uid:
                anon = u.get("is_anonymous")
                if anon is None:
                    anon = not bool(u.get("email"))
                tier = "guest" if anon else "member"
                if len(_TOKEN_CACHE) > 5000:
                    _TOKEN_CACHE.clear()
                _TOKEN_CACHE[key] = (now + 120, uid, tier)
                return uid, tier, ip_subject
    return ip_subject, "guest", ip_subject


def usage_limit_for(kind: str, tier: str) -> int:
    return GUEST_LIMITS[kind] * (MEMBER_MULT if tier == "member" else 1)


def usage_consume(http_request: Request, kind: str, ref=None, ticket: str = ""):
    """সীমা ঠিক থাকলে {'ok': True, ...state}, পেরোলে {'ok': False, 'info': {...}}। অবকাঠামো ব্যর্থ হলে exception।"""
    subject, tier, ip_subject = resolve_identity(http_request)

    # Text to edit-এর প্ল্যান ধাপের টিকিট থাকলে এডিট ধাপ আলাদা করে গোনা হয় না
    if ticket:
        if _sb_rpc("redeem_ticket", {"p_subject": subject, "p_ref": ticket}):
            return {"ok": True, "subject": subject, "tier": tier, "consumed": False, "ip_subject": None, "ip_consumed": False}

    limit = usage_limit_for(kind, tier)
    res = _sb_rpc("consume_usage", {"p_subject": subject, "p_kind": kind, "p_limit": limit, "p_ref": ref, "p_hours": USAGE_HOURS}) or {}
    if not res.get("ok"):
        return {"ok": False, "info": {"kind": kind, "tier": tier, "used": res.get("used", limit), "limit": limit,
                                      "reset_at": res.get("reset_at"), "hours": USAGE_HOURS}}
    state = {"ok": True, "subject": subject, "tier": tier, "consumed": not res.get("duplicate"), "ip_subject": None, "ip_consumed": False}

    # অতিথির জন্য IP-ভিত্তিক বাধা (নতুন বেনামি অ্যাকাউন্ট বানিয়ে সীমা এড়ানো কঠিন করে; মোবাইল নেটে IP ভাগ হয় বলে সীমা উদার)
    if tier == "guest" and subject != ip_subject:
        ip_limit = GUEST_LIMITS[kind] * IP_MULT
        ipres = _sb_rpc("consume_usage", {"p_subject": ip_subject, "p_kind": kind, "p_limit": ip_limit, "p_ref": ref, "p_hours": USAGE_HOURS}) or {}
        if not ipres.get("ok"):
            if state["consumed"]:
                _sb_rpc("refund_usage", {"p_subject": subject, "p_kind": kind, "p_ref": ref})
            return {"ok": False, "info": {"kind": kind, "tier": tier, "used": GUEST_LIMITS[kind], "limit": GUEST_LIMITS[kind],
                                          "reset_at": ipres.get("reset_at"), "hours": USAGE_HOURS, "network": True}}
        state["ip_subject"] = ip_subject
        state["ip_consumed"] = not ipres.get("duplicate")
    return state


def usage_refund(state, kind: str, ref=None):
    """কাজ ব্যর্থ হলে গোনা ফেরত (কখনো exception ছুড়ে না)।"""
    if not state:
        return
    try:
        if state.get("consumed"):
            _sb_rpc("refund_usage", {"p_subject": state["subject"], "p_kind": kind, "p_ref": ref})
        if state.get("ip_consumed") and state.get("ip_subject"):
            _sb_rpc("refund_usage", {"p_subject": state["ip_subject"], "p_kind": kind, "p_ref": ref})
    except Exception as e:
        print(f"[usage] refund ব্যর্থ: {e}")


def limit_response(info: dict):
    return JSONResponse(status_code=429, content={"error": "limit", "detail": "আপনার ফ্রি ব্যবহারের সীমা শেষ।", **info})


def limited(kind: str, ref_from=None, ticket_param: str = ""):
    """এন্ডপয়েন্টে সীমা বসানোর ডেকোরেটর। ফাংশনে `http_request: Request` প্যারামিটার থাকতে হবে।"""

    def deco(fn):
        def _ref(kwargs):
            try:
                return ref_from(kwargs) if ref_from else None
            except Exception:
                return None

        def _ticket(kwargs):
            return (kwargs.get(ticket_param) or "") if ticket_param else ""

        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def wrapper(*args, **kwargs):
                http_request = kwargs.get("http_request")
                state, ref = None, _ref(kwargs)
                if USAGE_ENABLED and http_request is not None:
                    try:
                        out = await run_in_threadpool(usage_consume, http_request, kind, ref, _ticket(kwargs))
                    except Exception as e:
                        print(f"[usage] সীমা যাচাই ব্যর্থ, কাজ চলতে দেওয়া হলো: {e}")
                        out = None
                    if out and not out["ok"]:
                        return limit_response(out["info"])
                    state = out
                try:
                    return await fn(*args, **kwargs)
                except BaseException:
                    if state:
                        await run_in_threadpool(usage_refund, state, kind, ref)
                    raise

        else:

            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                http_request = kwargs.get("http_request")
                state, ref = None, _ref(kwargs)
                if USAGE_ENABLED and http_request is not None:
                    try:
                        out = usage_consume(http_request, kind, ref, _ticket(kwargs))
                    except Exception as e:
                        print(f"[usage] সীমা যাচাই ব্যর্থ, কাজ চলতে দেওয়া হলো: {e}")
                        out = None
                    if out and not out["ok"]:
                        return limit_response(out["info"])
                    state = out
                try:
                    return fn(*args, **kwargs)
                except BaseException:
                    if state:
                        usage_refund(state, kind, ref)
                    raise

        return wrapper

    return deco


@app.get("/api/usage")
def usage_status(http_request: Request):
    """ফ্রন্টএন্ডে 'আজ কতটা ব্যবহার হয়েছে' দেখানোর জন্য।"""
    if not USAGE_ENABLED:
        missing = [n for n, v in (("SUPABASE_URL", SUPABASE_URL), ("SUPABASE_SERVICE_ROLE_KEY", SUPABASE_SERVICE_KEY)) if not v]
        reason = "USAGE_LIMITS=off" if not missing else "missing_env"
        return {"enabled": False, "reason": reason, "missing": missing}
    try:
        subject, tier, _ = resolve_identity(http_request)
        peek = _sb_rpc("peek_usage", {"p_subject": subject, "p_hours": USAGE_HOURS}) or {}
        limits = {k: usage_limit_for(k, tier) for k in GUEST_LIMITS}
        return {"enabled": True, "tier": tier, "hours": USAGE_HOURS, "limits": limits,
                "usage": {k: (peek.get(k) or {}).get("used", 0) for k in GUEST_LIMITS}}
    except Exception as e:
        print(f"[usage] status ব্যর্থ: {e}")
        m = re.search(r"HTTP (\d{3})", str(e))
        return {"enabled": False, "reason": "supabase_error", "http_status": int(m.group(1)) if m else None}


@app.api_route("/", methods=["GET", "HEAD"])
def health_root():
    """Render-এর স্বাস্থ্য-পরীক্ষা (HEAD /) আর ঘুম ভাঙানোর জন্য ছোট উত্তর।"""
    return {"status": "ok"}


@app.post("/api/extract")
@limited("download", ref_from=lambda kw: _sha(kw["request"].url.strip()))
def extract_video_info(request: VideoRequest, http_request: Request):
    video_url = request.url

    if not video_url:
        raise HTTPException(status_code=400, detail="অনুগ্রহ করে একটি বৈধ লিঙ্ক দিন।")

    ydl_opts = {
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        'no_warnings': True,
        'quiet': True,
        'http_headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        }
    }

    try:
        with yt_dlp.YoutubeDL(with_cookies(ydl_opts)) as ydl:
            info = ydl.extract_info(video_url, download=False)

        # ---- ভিডিও (combined video+audio) ডাইরেক্ট লিংক বের করা ----
        direct_url = info.get('url')
        if not direct_url and info.get('formats'):
            valid_formats = [
                f for f in info['formats']
                if f.get('url') and f.get('acodec') != 'none' and f.get('vcodec') != 'none'
            ]
            if valid_formats:
                direct_url = valid_formats[-1].get('url')
            else:
                direct_url = info['formats'][-1].get('url')

        if not direct_url:
            raise HTTPException(status_code=404, detail="ভিডিওর সরাসরি সোর্স লিঙ্ক পাওয়া যায়নি।")

        # ---- audio-only স্ট্রিম বের করা (Audio tab-এর জন্য আলাদা লিংক) ----
        audio_url = None
        if info.get('formats'):
            audio_formats = [
                f for f in info['formats']
                if f.get('url') and f.get('acodec') != 'none' and f.get('vcodec') == 'none'
            ]
            if audio_formats:
                # সবচেয়ে বেশি bitrate-এরটা বাছাই করা
                audio_formats.sort(key=lambda f: f.get('abr') or 0)
                audio_url = audio_formats[-1].get('url')
        if not audio_url:
            # আলাদা audio-only ট্র্যাক না থাকলে, video url থেকেই proxy পরে audio বের করে নেবে
            audio_url = direct_url

        tags = list(dict.fromkeys(t for t in (info.get('tags') or []) if t))
        if not tags and info.get('extractor_key') == 'youtube':
            tags = fetch_youtube_api_tags(info.get('id'))

        return {
            "success": True,
            "title": info.get('title', 'Buffradar Universal Video'),
            "thumbnail": info.get('thumbnail'),
            "download_url": direct_url,
            "audio_url": audio_url,
            "duration": info.get('duration'),
            "tags": tags,
            "platform": info.get('extractor_key'),
            "thumbnails": build_thumbnails(info),
            "subtitle_tracks": build_subtitle_tracks(info),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"লিঙ্কটি প্রসেস করা যায়নি: {str(e)}")


# ---------------------------------------------------------------------------
# নতুন: আসল সাইজসহ ডাউনলোড (এটা দিয়েই ফ্রন্টএন্ডে আসল % দেখাবে)
# আগে ফাইলটা সার্ভারে বানিয়ে নেয়, তারপর পাঠায়। তাই ফাইলের ঠিক সাইজ (Content-Length) আগে থেকেই জানা থাকে।
# ---------------------------------------------------------------------------
@app.get("/api/download")
def download_file(
    url: str,
    quality: str = "1080p",
    is_audio: bool = False,
    bitrate: str = "192kbps",
):
    if not is_valid_http_url(url):
        raise HTTPException(status_code=400, detail="অনুগ্রহ করে একটি বৈধ লিঙ্ক দিন।")

    tmpdir = tempfile.mkdtemp(prefix="buffradar_")
    try:
        ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()
        outtmpl = os.path.join(tmpdir, "out.%(ext)s")

        if is_audio:
            abr = parse_bitrate(bitrate)
            cmd = [
                "yt-dlp", *ytdlp_cookie_args(),
                "-f", "bestaudio/best",
                "-x", "--audio-format", "mp3", "--audio-quality", f"{abr}K",
                "--ffmpeg-location", ffmpeg_path,
                "-o", outtmpl,
                "--no-playlist", "--no-warnings", "-q",
                "--", url,
            ]
            base_name = "Buffradar_audio"
        else:
            height_limit = parse_height(quality)
            fmt = f"bestvideo[height<={height_limit}]+bestaudio/best[height<={height_limit}]/best"
            cmd = [
                "yt-dlp", *ytdlp_cookie_args(),
                "-f", fmt,
                "--merge-output-format", "mp4",
                "--ffmpeg-location", ffmpeg_path,
                "-o", outtmpl,
                "--no-playlist", "--no-warnings", "-q",
                "--", url,
            ]
            base_name = "Buffradar_download"

        result = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            timeout=1200,  # সর্বোচ্চ ২০ মিনিট
        )

        files = [
            f for f in glob.glob(os.path.join(tmpdir, "out.*"))
            if not f.endswith((".part", ".ytdl"))
        ]
        if result.returncode != 0 or not files:
            detail = (result.stderr or "").strip()[-300:] or "ফাইল তৈরি করা যায়নি।"
            raise RuntimeError(detail)

        filepath = max(files, key=os.path.getsize)
        ext = os.path.splitext(filepath)[1] or (".mp3" if is_audio else ".mp4")
        media_type = "audio/mpeg" if is_audio else (mimetypes.guess_type(filepath)[0] or "application/octet-stream")

        # FileResponse নিজে থেকেই সঠিক Content-Length পাঠায়; পাঠানো শেষে টেম্প ফোল্ডার মুছে যায়
        return FileResponse(
            filepath,
            media_type=media_type,
            filename=f"{base_name}{ext}",
            background=BackgroundTask(shutil.rmtree, tmpdir, ignore_errors=True),
        )

    except subprocess.TimeoutExpired:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise HTTPException(status_code=504, detail="ফাইল তৈরি করতে বেশি সময় লাগছে।")
    except Exception as e:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# নতুন: ভিডিও/অডিও কনভার্টার (/api/convert)
# yt-dlp দিয়ে সোর্স নামায় → ffmpeg দিয়ে পছন্দের ফরম্যাটে বদলায় → ফাইল পাঠায়।
# কনভার্ট ভারী কাজ (CPU/RAM), তাই নিচে ৩টা সেফটি লিমিট আছে — দরকার হলে Render-এর Environment থেকে বদলানো যাবে:
#   MAX_CONCURRENT_CONVERTS = একসাথে কয়টা কনভার্শন (ডিফল্ট ১)
#   MAX_CONVERT_SECONDS     = সর্বোচ্চ ভিডিও দৈর্ঘ্য সেকেন্ডে (ডিফল্ট ৯০০ = ১৫ মিনিট)
#   MAX_CONVERT_HEIGHT      = সর্বোচ্চ রেজোলিউশন (ডিফল্ট ১০৮০)
# ---------------------------------------------------------------------------
CONVERT_SLOTS = threading.BoundedSemaphore(int(os.environ.get("MAX_CONCURRENT_CONVERTS", "1")))
MAX_CONVERT_SECONDS = int(os.environ.get("MAX_CONVERT_SECONDS", "900"))
MAX_CONVERT_HEIGHT = int(os.environ.get("MAX_CONVERT_HEIGHT", "1080"))

_H264_ARGS = [
    "-map", "0:v:0", "-map", "0:a:0?",
    "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2",  # বেজোড় সাইজ হলে libx264 ফেইল করে, তাই জোড় করে নেওয়া
    "-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-pix_fmt", "yuv420p",
    "-c:a", "aac", "-b:a", "160k",
]

# kind: video | gif | audio   (lossless=True হলে bitrate লাগে না)
CONVERT_FORMATS = {
    "mp4":  {"kind": "video", "media": "video/mp4",        "args": _H264_ARGS + ["-movflags", "+faststart"]},
    "mov":  {"kind": "video", "media": "video/quicktime",  "args": _H264_ARGS + ["-movflags", "+faststart"]},
    "mkv":  {"kind": "video", "media": "video/x-matroska", "args": _H264_ARGS},
    "webm": {"kind": "video", "media": "video/webm", "args": [
        "-map", "0:v:0", "-map", "0:a:0?",
        "-c:v", "libvpx-vp9", "-b:v", "0", "-crf", "33",
        "-deadline", "realtime", "-cpu-used", "6", "-row-mt", "1",
        "-c:a", "libopus", "-b:a", "128k",
    ]},
    "avi":  {"kind": "video", "media": "video/x-msvideo", "args": [
        "-map", "0:v:0", "-map", "0:a:0?",
        "-c:v", "mpeg4", "-q:v", "4",
        "-c:a", "libmp3lame", "-b:a", "160k",
    ]},
    # GIF: সর্বোচ্চ ৩০ সেকেন্ড, ৪৮০px চওড়া, ১২ fps (নাহলে ফাইল অনেক বড় হয়ে যায়)
    "gif":  {"kind": "gif", "media": "image/gif", "args": [
        "-t", "30", "-an",
        "-vf", "fps=12,scale='min(480,iw)':-1:flags=lanczos,split[s0][s1];[s0]palettegen=max_colors=128[p];[s1][p]paletteuse=dither=bayer:bayer_scale=4",
        "-loop", "0",
    ]},
    "mp3":  {"kind": "audio", "media": "audio/mpeg", "codec": ["-c:a", "libmp3lame"], "lossy": True},
    "m4a":  {"kind": "audio", "media": "audio/mp4",  "codec": ["-c:a", "aac"],        "lossy": True},
    "ogg":  {"kind": "audio", "media": "audio/ogg",  "codec": ["-c:a", "libvorbis"],  "lossy": True},
    "wav":  {"kind": "audio", "media": "audio/wav",  "codec": ["-c:a", "pcm_s16le"],  "lossy": False},
    "flac": {"kind": "audio", "media": "audio/flac", "codec": ["-c:a", "flac"],       "lossy": False},
}


def build_ffmpeg_args(fmt: str, bitrate: str):
    spec = CONVERT_FORMATS[fmt]
    if spec["kind"] != "audio":
        return spec["args"]
    args = ["-vn", "-map", "0:a:0"] + spec["codec"]
    if spec["lossy"]:
        args += ["-b:a", f"{parse_bitrate(bitrate)}k"]
    return args


def run_or_raise(cmd, timeout, fail_msg):
    result = subprocess.run(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        detail = (result.stderr or "").strip()[-300:] or fail_msg
        raise RuntimeError(detail)


@app.get("/api/convert")
@limited("convert", ref_from=lambda kw: _sha("|".join(str(kw.get(k)) for k in ("url", "fmt", "quality", "bitrate"))))
def convert_media(
    http_request: Request,
    url: str,
    fmt: str = "mp4",
    quality: str = "1080p",
    bitrate: str = "192kbps",
):
    fmt = (fmt or "").lower().strip()
    if not is_valid_http_url(url):
        raise HTTPException(status_code=400, detail="অনুগ্রহ করে একটি বৈধ লিঙ্ক দিন।")
    if fmt not in CONVERT_FORMATS:
        raise HTTPException(status_code=400, detail="এই ফরম্যাটটি সাপোর্টেড নয়।")

    # সার্ভার ব্যস্ত থাকলে লাইনে না রেখে সাথে সাথে জানিয়ে দেওয়া (নাহলে সার্ভার ক্র্যাশ করতে পারে)
    if not CONVERT_SLOTS.acquire(blocking=False):
        raise HTTPException(status_code=429, detail="সার্ভার এখন অন্য একটি কনভার্শন করছে। ১-২ মিনিট পরে আবার চেষ্টা করুন।")

    tmpdir = None
    handed_off = False
    try:
        # ---- আগে দৈর্ঘ্য যাচাই: বেশি লম্বা ভিডিও শুরুতেই আটকানো ----
        with yt_dlp.YoutubeDL(with_cookies({"quiet": True, "no_warnings": True, "noplaylist": True})) as ydl:
            info = ydl.extract_info(url, download=False)
        if info.get("is_live"):
            raise HTTPException(status_code=400, detail="লাইভ ভিডিও কনভার্ট করা যাবে না।")
        duration = info.get("duration") or 0
        if duration and duration > MAX_CONVERT_SECONDS:
            raise HTTPException(
                status_code=400,
                detail=f"কনভার্টারে সর্বোচ্চ {MAX_CONVERT_SECONDS // 60} মিনিটের ভিডিও করা যায়। এই ভিডিওটি {int(duration) // 60} মিনিটের।",
            )

        tmpdir = tempfile.mkdtemp(prefix="buffradar_conv_")
        ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()
        kind = CONVERT_FORMATS[fmt]["kind"]

        # ---- ধাপ ১: সোর্স নামানো ----
        if kind == "audio":
            fmt_selector = "bestaudio/best"
        else:
            h = MAX_CONVERT_HEIGHT if kind == "video" else 480
            h = min(parse_height(quality), h) if kind == "video" else h
            if kind == "gif":
                fmt_selector = f"bestvideo[height<={h}]/best[height<={h}]/best"
            else:
                fmt_selector = f"bestvideo[height<={h}]+bestaudio/best[height<={h}]/best"

        run_or_raise(
            [
                "yt-dlp", *ytdlp_cookie_args(), "-f", fmt_selector,
                "--merge-output-format", "mkv",
                "--ffmpeg-location", ffmpeg_path,
                "-o", os.path.join(tmpdir, "src.%(ext)s"),
                "--no-playlist", "--no-warnings", "-q",
                "--", url,
            ],
            timeout=1200,
            fail_msg="সোর্স ফাইল নামানো যায়নি।",
        )
        sources = [
            f for f in glob.glob(os.path.join(tmpdir, "src.*"))
            if not f.endswith((".part", ".ytdl"))
        ]
        if not sources:
            raise RuntimeError("সোর্স ফাইল নামানো যায়নি।")
        src_path = max(sources, key=os.path.getsize)

        # ---- ধাপ ২: ffmpeg দিয়ে কনভার্ট ----
        out_path = os.path.join(tmpdir, f"out.{fmt}")
        run_or_raise(
            [ffmpeg_path, "-y", "-nostdin", "-hide_banner", "-loglevel", "error", "-i", src_path]
            + build_ffmpeg_args(fmt, bitrate)
            + [out_path],
            timeout=1800,
            fail_msg="কনভার্ট করা যায়নি।",
        )
        if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
            raise RuntimeError("কনভার্ট করা ফাইল তৈরি হয়নি।")

        try:
            os.remove(src_path)  # জায়গা বাঁচাতে সোর্স ফাইল আগেই মুছে ফেলা
        except OSError:
            pass

        handed_off = True
        return FileResponse(
            out_path,
            media_type=CONVERT_FORMATS[fmt]["media"],
            filename=f"Buffradar_convert.{fmt}",
            background=BackgroundTask(shutil.rmtree, tmpdir, ignore_errors=True),
        )

    except HTTPException:
        raise
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="কনভার্ট করতে বেশি সময় লাগছে। ছোট ভিডিও বা কম কোয়ালিটি দিয়ে চেষ্টা করুন।")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # ফাইল পাঠানো শুরু হয়ে গেলে ফোল্ডার BackgroundTask মুছবে; আর কোনো এরর হলে এখানেই মুছে যাবে
        if tmpdir and not handed_off:
            shutil.rmtree(tmpdir, ignore_errors=True)
        CONVERT_SLOTS.release()


# ---------------------------------------------------------------------------
# নতুন: সরাসরি ফাইল আপলোড করে কনভার্ট (/api/convert-upload)
# ইউজার নিজের কম্পিউটার/ফোন থেকে ফাইল আপলোড করলে এখানে আসে — কোনো yt-dlp ডাউনলোড ধাপ লাগে না,
# সরাসরি ffmpeg দিয়ে কনভার্ট করে পাঠিয়ে দেয়।
# ---------------------------------------------------------------------------
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", str(500 * 1024 * 1024)))  # ডিফল্ট ৫০০MB


@app.post("/api/convert-upload")
@limited("convert")
async def convert_uploaded_file(
    http_request: Request,
    file: UploadFile = File(...),
    fmt: str = Form("mp4"),
    quality: str = Form("1080p"),
    bitrate: str = Form("192kbps"),
):
    fmt = (fmt or "").lower().strip()
    if fmt not in CONVERT_FORMATS:
        raise HTTPException(status_code=400, detail="এই ফরম্যাটটি সাপোর্টেড নয়।")

    # সার্ভার ব্যস্ত থাকলে লাইনে না রেখে সাথে সাথে জানিয়ে দেওয়া
    if not CONVERT_SLOTS.acquire(blocking=False):
        raise HTTPException(status_code=429, detail="সার্ভার এখন অন্য একটি কনভার্শন করছে। ১-২ মিনিট পরে আবার চেষ্টা করুন।")

    tmpdir = None
    handed_off = False
    try:
        tmpdir = tempfile.mkdtemp(prefix="buffradar_upload_")

        # ---- ধাপ ১: আপলোড করা ফাইল ডিস্কে সেভ করা (সাইজ লিমিট চেক করতে করতে) ----
        orig_ext = os.path.splitext(file.filename or "")[1] or ".bin"
        src_path = os.path.join(tmpdir, f"src{orig_ext}")
        written = 0
        with open(src_path, "wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                written += len(chunk)
                if written > MAX_UPLOAD_BYTES:
                    raise HTTPException(
                        status_code=413,
                        detail=f"ফাইলটি অনেক বড়। সর্বোচ্চ {MAX_UPLOAD_BYTES // (1024 * 1024)}MB পর্যন্ত সাপোর্টেড।",
                    )
                out.write(chunk)
        if written == 0:
            raise HTTPException(status_code=400, detail="খালি ফাইল আপলোড করা যাবে না।")

        # ---- ধাপ ২: ffmpeg দিয়ে কনভার্ট ----
        ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()
        out_path = os.path.join(tmpdir, f"out.{fmt}")
        run_or_raise(
            [ffmpeg_path, "-y", "-nostdin", "-hide_banner", "-loglevel", "error", "-i", src_path]
            + build_ffmpeg_args(fmt, bitrate)
            + [out_path],
            timeout=1800,
            fail_msg="কনভার্ট করা যায়নি।",
        )
        if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
            raise RuntimeError("কনভার্ট করা ফাইল তৈরি হয়নি।")

        try:
            os.remove(src_path)  # জায়গা বাঁচাতে সোর্স ফাইল আগেই মুছে ফেলা
        except OSError:
            pass

        handed_off = True
        return FileResponse(
            out_path,
            media_type=CONVERT_FORMATS[fmt]["media"],
            filename=f"Buffradar_convert.{fmt}",
            background=BackgroundTask(shutil.rmtree, tmpdir, ignore_errors=True),
        )

    except HTTPException:
        raise
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="কনভার্ট করতে বেশি সময় লাগছে। ছোট ফাইল বা কম কোয়ালিটি দিয়ে চেষ্টা করুন।")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if tmpdir and not handed_off:
            shutil.rmtree(tmpdir, ignore_errors=True)
        CONVERT_SLOTS.release()


# ---------------------------------------------------------------------------
# নতুন: Editor ট্যাব (/api/edit) — একই সোর্স-ডাউনলোড প্যাটার্ন, শুধু ffmpeg ফিল্টার আলাদা।
#   mode=trim     -> নির্দিষ্ট অংশ কেটে + crop/aspect বদলে (9:16 / 1:1 / 16:9)
#   mode=remove   -> নির্দিষ্ট জায়গার ওয়াটারমার্ক/লোগো/হার্ডকোডেড সাবটাইটেল ঝাপসা করে মুছে দেয় (ffmpeg delogo,
#                    সত্যিকারের object-detection AI না — নির্দিষ্ট বক্স এলাকায় ইন্টারপোলেশন করে ঢেকে দেয়)
#   mode=compress -> HD মান রেখে ফাইল সাইজ ছোট করে (light/medium/high)
# ---------------------------------------------------------------------------
EDIT_SLOTS = threading.BoundedSemaphore(int(os.environ.get("MAX_CONCURRENT_EDITS", "1")))
MAX_EDIT_SECONDS = int(os.environ.get("MAX_EDIT_SECONDS", "900"))

# 🪄 Object remover-এর পজিশন প্রিসেট: (x_fraction, y_fraction, w_fraction, h_fraction) — ভিডিওর প্রস্থ/উচ্চতার শতাংশ হিসেবে
REMOVE_POSITION_PRESETS = {
    "top-left":     (0.02, 0.02, 0.24, 0.13),
    "top-right":    (0.74, 0.02, 0.24, 0.13),
    "top-band":     (0.05, 0.02, 0.90, 0.15),
    "bottom-left":  (0.02, 0.85, 0.24, 0.13),
    "bottom-right": (0.74, 0.85, 0.24, 0.13),
    "bottom-band":  (0.05, 0.82, 0.90, 0.16),
}

# 📉 কমপ্রেশন লেভেল অনুযায়ী CRF আর সর্বোচ্চ উচ্চতা
COMPRESS_LEVELS = {
    "light":  {"crf": 24, "max_height": 1080},
    "medium": {"crf": 28, "max_height": 1080},
    "high":   {"crf": 32, "max_height": 720},
}


def get_video_dimensions(ffmpeg_path: str, path: str):
    """ffprobe নেই, তাই ffmpeg -i চালিয়ে stderr থেকে রেজোলিউশন বের করা হয়।"""
    result = subprocess.run(
        [ffmpeg_path, "-hide_banner", "-i", path],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    m = re.search(r"Video:.*?(\d{2,5})x(\d{2,5})", result.stderr or "")
    if m:
        return int(m.group(1)), int(m.group(2))
    return 1280, 720  # খুঁজে না পেলে নিরাপদ ডিফল্ট


def build_crop_filter(preset: str) -> str:
    if preset == "9:16":
        return "crop='if(gt(iw/ih,9/16),trunc(ih*9/16/2)*2,iw)':'if(gt(iw/ih,9/16),ih,trunc(iw*16/9/2)*2)'"
    if preset == "1:1":
        return "crop='trunc(min(iw,ih)/2)*2':'trunc(min(iw,ih)/2)*2'"
    if preset == "16:9":
        return "crop='if(gt(iw/ih,16/9),trunc(ih*16/9/2)*2,iw)':'if(gt(iw/ih,16/9),ih,trunc(iw*9/16/2)*2)'"
    return ""  # original — কোনো ক্রপ না


@app.get("/api/edit")
@limited("edit_manual", ref_from=lambda kw: _sha("|".join(str(kw.get(k)) for k in ("url", "mode", "start", "end", "crop", "position", "level"))))
def edit_media(
    http_request: Request,
    url: str,
    mode: str,
    start: float = 0,
    end: float = 0,
    crop: str = "original",
    position: str = "bottom-band",
    level: str = "medium",
):
    mode = (mode or "").lower().strip()
    if not is_valid_http_url(url):
        raise HTTPException(status_code=400, detail="অনুগ্রহ করে একটি বৈধ লিঙ্ক দিন।")
    if mode not in ("trim", "remove", "compress"):
        raise HTTPException(status_code=400, detail="এই এডিট টুলটি সাপোর্টেড নয়।")
    if mode == "remove" and position not in REMOVE_POSITION_PRESETS:
        raise HTTPException(status_code=400, detail="ভুল পজিশন দেওয়া হয়েছে।")
    if mode == "compress" and level not in COMPRESS_LEVELS:
        raise HTTPException(status_code=400, detail="ভুল কমপ্রেশন লেভেল দেওয়া হয়েছে।")

    if not EDIT_SLOTS.acquire(blocking=False):
        raise HTTPException(status_code=429, detail="সার্ভার এখন অন্য একটি এডিট প্রসেস করছে। ১-২ মিনিট পরে আবার চেষ্টা করুন।")

    tmpdir = None
    handed_off = False
    try:
        with yt_dlp.YoutubeDL(with_cookies({"quiet": True, "no_warnings": True, "noplaylist": True})) as ydl:
            info = ydl.extract_info(url, download=False)
        if info.get("is_live"):
            raise HTTPException(status_code=400, detail="লাইভ ভিডিও এডিট করা যাবে না।")
        duration = info.get("duration") or 0
        if duration and duration > MAX_EDIT_SECONDS:
            raise HTTPException(
                status_code=400,
                detail=f"এডিটরে সর্বোচ্চ {MAX_EDIT_SECONDS // 60} মিনিটের ভিডিও করা যায়। এই ভিডিওটি {int(duration) // 60} মিনিটের।",
            )

        tmpdir = tempfile.mkdtemp(prefix="buffradar_edit_")
        ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()

        # ---- ধাপ ১: সোর্স নামানো ----
        max_h = COMPRESS_LEVELS[level]["max_height"] if mode == "compress" else MAX_CONVERT_HEIGHT
        fmt_selector = f"bestvideo[height<={max_h}]+bestaudio/best[height<={max_h}]/best"
        run_or_raise(
            [
                "yt-dlp", *ytdlp_cookie_args(), "-f", fmt_selector,
                "--merge-output-format", "mkv",
                "--ffmpeg-location", ffmpeg_path,
                "-o", os.path.join(tmpdir, "src.%(ext)s"),
                "--no-playlist", "--no-warnings", "-q",
                "--", url,
            ],
            timeout=1200,
            fail_msg="সোর্স ফাইল নামানো যায়নি।",
        )
        sources = [
            f for f in glob.glob(os.path.join(tmpdir, "src.*"))
            if not f.endswith((".part", ".ytdl"))
        ]
        if not sources:
            raise RuntimeError("সোর্স ফাইল নামানো যায়নি।")
        src_path = max(sources, key=os.path.getsize)

        # ---- ধাপ ২: মোড অনুযায়ী ffmpeg কমান্ড বানানো ----
        out_path = os.path.join(tmpdir, "out.mp4")
        pre_args, vf_parts = [], []

        if mode == "trim":
            s = max(0.0, float(start or 0))
            e = float(end or 0)
            dur = e - s
            if dur <= 0:
                raise HTTPException(status_code=400, detail="End time অবশ্যই Start time-এর চেয়ে বেশি হতে হবে।")
            pre_args = ["-ss", str(s), "-t", str(dur)]
            crop_vf = build_crop_filter(crop)
            if crop_vf:
                vf_parts.append(crop_vf)

        elif mode == "remove":
            frac = REMOVE_POSITION_PRESETS[position]
            w, h = get_video_dimensions(ffmpeg_path, src_path)
            fx, fy, fw, fh = frac
            px, py = int(w * fx), int(h * fy)
            pw, ph = max(2, int(w * fw)), max(2, int(h * fh))
            vf_parts.append(f"delogo=x={px}:y={py}:w={pw}:h={ph}:show=0")

        elif mode == "compress":
            cfg = COMPRESS_LEVELS[level]
            vf_parts.append(f"scale='min({cfg['max_height']}*iw/ih,iw)':'min({cfg['max_height']},ih)':force_original_aspect_ratio=decrease")

        vf_parts.append("scale=trunc(iw/2)*2:trunc(ih/2)*2")  # বেজোড় সাইজ হলে libx264 ফেইল করে
        vf_chain = ",".join(vf_parts)

        crf = str(COMPRESS_LEVELS[level]["crf"]) if mode == "compress" else "22"
        cmd = (
            [ffmpeg_path, "-y", "-nostdin", "-hide_banner", "-loglevel", "error"]
            + pre_args
            + ["-i", src_path, "-vf", vf_chain]
            + ["-c:v", "libx264", "-preset", "veryfast", "-crf", crf, "-pix_fmt", "yuv420p"]
            + ["-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", out_path]
        )
        run_or_raise(cmd, timeout=1800, fail_msg="এডিট করা যায়নি।")

        if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
            raise RuntimeError("এডিট করা ফাইল তৈরি হয়নি।")

        try:
            os.remove(src_path)
        except OSError:
            pass

        handed_off = True
        return FileResponse(
            out_path,
            media_type="video/mp4",
            filename="Buffradar_edited.mp4",
            background=BackgroundTask(shutil.rmtree, tmpdir, ignore_errors=True),
        )

    except HTTPException:
        raise
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="এডিট করতে বেশি সময় লাগছে। ছোট অংশ বা কম কোয়ালিটি দিয়ে চেষ্টা করুন।")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if tmpdir and not handed_off:
            shutil.rmtree(tmpdir, ignore_errors=True)
        EDIT_SLOTS.release()


# ---------------------------------------------------------------------------
# নতুন: Manual Editor (/api/edit-upload) — আপলোড করা ভিডিওতে Trim / Rotate / Crop / Resize / Speed / Audio / Text
# টেক্সট: ffmpeg-এর drawtext ফিল্টার সব বিল্ডে থাকে না, তাই Pillow দিয়ে টেক্সটের PNG বানিয়ে overlay করা হয়
#       (requirements.txt-এ Pillow লাগবে)। ফন্ট: backend/fonts/editor.ttf রাখুন বা EDIT_FONT_PATH দিন।
#       না থাকলে Pillow-র ডিফল্ট ফন্ট চলবে (ইংরেজি লেখার জন্য ঠিক আছে; বাংলার জন্য বাংলা ফন্ট দিতে হবে)।
# ---------------------------------------------------------------------------
EDIT_RESOLUTIONS = {"original": None, "1080p": 1080, "720p": 720, "480p": 480, "360p": 360}
EDIT_TEXT_COLORS = {"white", "black", "yellow", "red"}
# ফ্রি সার্ভারে ধীর বা মেমোরি কম হলে Render-এর Environment-এ বদলান (কোড বদলাতে হবে না):
# ডিফল্ট এখন ফ্রি সার্ভারের জন্য (ultrafast + ১ থ্রেড: দ্রুত ও মেমোরি কম)। ভালো সার্ভারে বদলান:
#   EDIT_X264_PRESET = veryfast (ছোট ফাইল)    EDIT_FFMPEG_THREADS = 4
EDIT_X264_PRESET = os.environ.get("EDIT_X264_PRESET", "ultrafast").strip() or "ultrafast"
if EDIT_X264_PRESET not in {"ultrafast", "superfast", "veryfast", "faster", "fast", "medium"}:
    EDIT_X264_PRESET = "veryfast"
_threads = (os.environ.get("EDIT_FFMPEG_THREADS", "1") or "1").strip()
EDIT_THREAD_ARGS = ["-threads", _threads] if _threads.isdigit() and int(_threads) > 0 else []
MAX_MUSIC_BYTES = 40 * 1024 * 1024
MAX_STICKER_BYTES = 6 * 1024 * 1024
MAX_STICKERS = 8
EDIT_FILTER_PRESETS = {
    "none": None,
    "bw": "hue=s=0",
    "warm": "colorbalance=rs=0.12:gs=0.02:bs=-0.12:rm=0.10:bm=-0.10",
    "cool": "colorbalance=rs=-0.10:bs=0.15:rm=-0.08:bm=0.12",
    "vintage": "curves=preset=vintage",
    "vivid": "eq=saturation=1.4:contrast=1.08",
}


def find_edit_font():
    candidates = [
        os.environ.get("EDIT_FONT_PATH"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "fonts", "editor.ttf"),
        "C:/Windows/Fonts/arialbd.ttf",
        "C:/Windows/Fonts/arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
    ]
    for p in candidates:
        if p and os.path.exists(p):
            return p
    return None


def crop_dims(w: int, h: int, preset: str):
    """build_crop_filter-এর সাথে মিলিয়ে ক্রপের পর ফ্রেমের মাপ (টেক্সটের পজিশন ঠিক করতে লাগে)।"""
    ev = lambda n: int(n // 2) * 2
    if preset == "9:16":
        return (ev(h * 9 / 16), h) if w / h > 9 / 16 else (w, ev(w * 16 / 9))
    if preset == "1:1":
        m = ev(min(w, h))
        return m, m
    if preset == "16:9":
        return (ev(h * 16 / 9), h) if w / h > 16 / 9 else (w, ev(w * 9 / 16))
    return w, h


def render_text_png(text: str, out_path: str, font_path, size: int, color: str, max_width: int, box: bool = False):
    """টেক্সটকে স্বচ্ছ PNG-তে আঁকে (শ্যাডো বা ব্যাকগ্রাউন্ড বক্সসহ), প্রয়োজনে লাইন ভেঙে।"""
    from PIL import Image, ImageDraw, ImageFont

    try:
        font = ImageFont.truetype(font_path, size) if font_path else ImageFont.load_default(size=size)
    except Exception:
        font = ImageFont.load_default(size=size)

    probe = ImageDraw.Draw(Image.new("RGBA", (1, 1)))
    lines = []
    for para in text.splitlines() or [text]:
        cur = ""
        for word in para.split(" "):
            trial = (cur + " " + word).strip()
            if cur and probe.textlength(trial, font=font) > max_width:
                lines.append(cur)
                cur = word
            else:
                cur = trial
        lines.append(cur)

    pad = max(4, int(size * 0.4)) if box else 4
    gap = int(size * 0.2)
    widths = [int(probe.textlength(ln, font=font)) for ln in lines]
    line_h = int(size * 1.2)
    w = max(widths + [1]) + pad * 2 + 2
    h = line_h * len(lines) + gap * (len(lines) - 1) + pad * 2 + 2
    img = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    if box:
        d.rounded_rectangle((0, 0, w - 1, h - 1), radius=max(6, int(size * 0.3)), fill=(0, 0, 0, 150))
    for i, ln in enumerate(lines):
        x = pad + (max(widths) - widths[i]) // 2
        y = pad + i * (line_h + gap)
        if not box:
            d.text((x + 1, y + 1), ln, font=font, fill=(0, 0, 0, 180))
        d.text((x, y), ln, font=font, fill=color)
    img.save(out_path)


EDIT_EFFECTS = {"none", "blur", "sharpen", "vignette", "grain"}
EDIT_FLIPS = {"none", "h", "v", "both"}
EDIT_FITS = {"crop", "blur"}
EDIT_FORMATS = {"mp4", "gif", "mp3"}
MAX_CUTS = 30
MAX_REVERSE_SECONDS = 60
MAX_GIF_SECONDS = 30


def effect_filter(effect: str, amount: int):
    a = min(max(int(amount), 0), 100)
    if effect == "blur":
        return f"gblur=sigma={1 + a * 0.2:.1f}"
    if effect == "sharpen":
        return f"unsharp=5:5:{a / 100 * 2.5:.2f}:5:5:0"
    if effect == "vignette":
        return f"vignette=angle={0.3 + a / 100 * 0.9:.2f}"
    if effect == "grain":
        return f"noise=alls={max(1, int(a * 0.4))}:allf=t"
    return None


def pad_canvas(w: int, h: int, preset: str):
    """ক্রপ না করে পুরো ফ্রেম রেখে (ঝাপসা ব্যাকগ্রাউন্ডসহ) টার্গেট aspect-এর ক্যানভাস মাপ।"""
    ev = lambda n: max(2, int(n // 2) * 2)
    r = {"9:16": 9 / 16, "1:1": 1.0, "16:9": 16 / 9}[preset]
    if w / h > r:
        return ev(w), ev(w / r)
    return ev(h * r), ev(h)


def parse_cuts(raw: str, lo: float, hi: float):
    """[[from,to],...] -> সাজানো, মিশানো, সীমার ভেতরের রেঞ্জ।"""
    try:
        data = json.loads(raw or "[]")
    except Exception:
        raise HTTPException(status_code=400, detail="কাটার তথ্য ঠিক নেই।")
    if not isinstance(data, list) or len(data) > MAX_CUTS:
        raise HTTPException(status_code=400, detail=f"কাটার তথ্য ঠিক নেই (সর্বোচ্চ {MAX_CUTS}টি)।")
    rngs = []
    for item in data:
        try:
            x, y = float(item[0]), float(item[1])
        except Exception:
            raise HTTPException(status_code=400, detail="কাটার তথ্য ঠিক নেই।")
        x, y = max(lo, x), min(hi, y)
        if y - x >= 0.05:
            rngs.append((x, y))
    rngs.sort()
    merged = []
    for x, y in rngs:
        if merged and x <= merged[-1][1] + 1e-6:
            merged[-1] = (merged[-1][0], max(merged[-1][1], y))
        else:
            merged.append((x, y))
    return merged


def kept_from_cuts(lo: float, hi: float, cuts):
    kept, pos = [], lo
    for x, y in cuts:
        if x - pos >= 0.1:
            kept.append((pos, x))
        pos = max(pos, y)
    if hi - pos >= 0.1:
        kept.append((pos, hi))
    return kept


async def save_upload_limited(up, path: str, limit: int, label: str):
    written = 0
    with open(path, "wb") as out:
        while True:
            chunk = await up.read(1024 * 1024)
            if not chunk:
                break
            written += len(chunk)
            if written > limit:
                raise HTTPException(status_code=413, detail=f"{label} ফাইলটি অনেক বড় (সর্বোচ্চ {limit // (1024 * 1024)}MB)।")
            out.write(chunk)
    if written == 0:
        raise HTTPException(status_code=400, detail=f"খালি {label} ফাইল।")


def has_audio_stream(ffmpeg_path: str, path: str) -> bool:
    result = subprocess.run(
        [ffmpeg_path, "-hide_banner", "-i", path],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    return "Audio:" in (result.stderr or "")


def get_video_duration(ffmpeg_path: str, path: str) -> float:
    result = subprocess.run(
        [ffmpeg_path, "-hide_banner", "-i", path],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    m = re.search(r"Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)", result.stderr or "")
    if not m:
        return 0.0
    return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + float(m.group(3))


@app.post("/api/edit-upload")
@limited("edit_manual", ticket_param="ai_ticket")
async def edit_uploaded_file(
    http_request: Request,
    ai_ticket: str = Form(""),
    file: UploadFile = File(...),
    start: float = Form(0),
    end: float = Form(0),
    rotate: int = Form(0),
    aspect: str = Form("original"),
    resolution: str = Form("original"),
    speed: float = Form(1),
    mute: str = Form("false"),
    volume: int = Form(100),
    text: str = Form(""),
    text_position: str = Form("bottom"),
    text_size: int = Form(36),
    text_color: str = Form("white"),
    filter_preset: str = Form("none"),
    brightness: int = Form(0),
    contrast: int = Form(0),
    saturation: int = Form(0),
    fade_in: float = Form(0),
    fade_out: float = Form(0),
    music: Optional[UploadFile] = File(None),
    music_volume: int = Form(100),
    music_loop: str = Form("false"),
    stickers: List[UploadFile] = File([]),
    sticker_meta: str = Form("[]"),
    flip: str = Form("none"),
    reverse: str = Form("false"),
    effect: str = Form("none"),
    effect_amount: int = Form(50),
    text_bg: str = Form("false"),
    text_from: float = Form(0),
    text_to: float = Form(0),
    cuts: str = Form("[]"),
    fit: str = Form("crop"),
    output_format: str = Form("mp4"),
):
    # ---- ইনপুট যাচাই ----
    if rotate not in (0, 90, 180, 270):
        raise HTTPException(status_code=400, detail="ভুল রোটেশন দেওয়া হয়েছে।")
    if aspect not in ("original", "9:16", "1:1", "16:9"):
        raise HTTPException(status_code=400, detail="ভুল aspect ratio দেওয়া হয়েছে।")
    if resolution not in EDIT_RESOLUTIONS:
        raise HTTPException(status_code=400, detail="ভুল রেজোলিউশন দেওয়া হয়েছে।")
    if not (0.5 <= speed <= 2):
        raise HTTPException(status_code=400, detail="স্পিড ০.৫x থেকে ২x-এর মধ্যে হতে হবে।")
    if not (0 <= volume <= 200):
        raise HTTPException(status_code=400, detail="ভলিউম ০ থেকে ২০০%-এর মধ্যে হতে হবে।")
    if text_position not in ("top", "center", "bottom"):
        raise HTTPException(status_code=400, detail="ভুল টেক্সট পজিশন দেওয়া হয়েছে।")
    muted = (mute or "").lower() == "true"
    start = max(0.0, float(start or 0))
    end = float(end or 0)
    if end > 0 and end <= start:
        raise HTTPException(status_code=400, detail="End time অবশ্যই Start time-এর চেয়ে বেশি হতে হবে।")
    text = (text or "").strip()[:200]
    text_size = min(max(int(text_size or 36), 8), 200)
    if filter_preset not in EDIT_FILTER_PRESETS:
        raise HTTPException(status_code=400, detail="ভুল ফিল্টার দেওয়া হয়েছে।")
    brightness = min(max(int(brightness or 0), -100), 100)
    contrast = min(max(int(contrast or 0), -100), 100)
    saturation = min(max(int(saturation or 0), -100), 100)
    fade_in = min(max(float(fade_in or 0), 0.0), 3.0)
    fade_out = min(max(float(fade_out or 0), 0.0), 3.0)
    music_volume = min(max(int(music_volume or 0), 0), 200)
    music_loop_on = (music_loop or "").lower() == "true"
    try:
        meta = json.loads(sticker_meta or "[]")
    except Exception:
        raise HTTPException(status_code=400, detail="স্টিকারের তথ্য ঠিক নেই।")
    stickers = [u for u in (stickers or []) if getattr(u, "filename", None)]
    if not isinstance(meta, list) or len(meta) != len(stickers) or len(stickers) > MAX_STICKERS:
        raise HTTPException(status_code=400, detail=f"স্টিকার সর্বোচ্চ {MAX_STICKERS}টি দেওয়া যায়।")
    clean_meta = []
    for m in meta:
        try:
            clean_meta.append({
                "x": min(max(float(m["x"]), 0.0), 1.0),
                "y": min(max(float(m["y"]), 0.0), 1.0),
                "w": min(max(float(m["w"]), 0.03), 1.0),
                "from": max(0.0, float(m.get("from") or 0)),
                "to": max(0.0, float(m.get("to") or 0)),
            })
        except Exception:
            raise HTTPException(status_code=400, detail="স্টিকারের তথ্য ঠিক নেই।")


    if flip not in EDIT_FLIPS or effect not in EDIT_EFFECTS or fit not in EDIT_FITS or output_format not in EDIT_FORMATS:
        raise HTTPException(status_code=400, detail="ভুল অপশন দেওয়া হয়েছে।")
    effect_amount = min(max(int(effect_amount or 0), 0), 100)
    reverse_on = (reverse or "").lower() == "true"
    text_box = (text_bg or "").lower() == "true"
    text_from = max(0.0, float(text_from or 0))
    text_to = max(0.0, float(text_to or 0))

    if not EDIT_SLOTS.acquire(blocking=False):
        raise HTTPException(status_code=429, detail="সার্ভার এখন অন্য একটি এডিট প্রসেস করছে। ১-২ মিনিট পরে আবার চেষ্টা করুন।")

    tmpdir = None
    handed_off = False
    try:
        tmpdir = tempfile.mkdtemp(prefix="buffradar_editup_")

        # ---- ধাপ ১: আপলোড করা ফাইল সেভ (সাইজ লিমিটসহ) ----
        orig_ext = os.path.splitext(file.filename or "")[1] or ".bin"
        src_path = os.path.join(tmpdir, f"src{orig_ext}")
        written = 0
        with open(src_path, "wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                written += len(chunk)
                if written > MAX_UPLOAD_BYTES:
                    raise HTTPException(
                        status_code=413,
                        detail=f"ফাইলটি অনেক বড়। সর্বোচ্চ {MAX_UPLOAD_BYTES // (1024 * 1024)}MB পর্যন্ত সাপোর্টেড।",
                    )
                out.write(chunk)
        if written == 0:
            raise HTTPException(status_code=400, detail="খালি ফাইল আপলোড করা যাবে না।")

        ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()
        duration = get_video_duration(ffmpeg_path, src_path)
        if duration and duration > MAX_EDIT_SECONDS:
            raise HTTPException(
                status_code=400,
                detail=f"এডিটরে সর্বোচ্চ {MAX_EDIT_SECONDS // 60} মিনিটের ভিডিও করা যায়। এই ভিডিওটি {int(duration) // 60} মিনিটের।",
            )

        main_has_audio = has_audio_stream(ffmpeg_path, src_path)

        # ---- অতিরিক্ত ফাইল: মিউজিক ও স্টিকার ----
        music_path = None
        if music is not None and getattr(music, "filename", None):
            music_path = os.path.join(tmpdir, "music" + (os.path.splitext(music.filename)[1] or ".mp3"))
            await save_upload_limited(music, music_path, MAX_MUSIC_BYTES, "অডিও")
            if not has_audio_stream(ffmpeg_path, music_path):
                raise HTTPException(status_code=400, detail="অডিও ফাইলটি পড়া যায়নি। MP3, WAV বা M4A দিন।")
        sticker_items = []
        for k, up in enumerate(stickers):
            sp = os.path.join(tmpdir, f"sticker{k}" + (os.path.splitext(up.filename)[1] or ".png"))
            await save_upload_limited(up, sp, MAX_STICKER_BYTES, "স্টিকার")
            sticker_items.append((sp, clean_meta[k]))

        # ---- ধাপ ২: কোন কোন অংশ রাখা হবে (Trim + Cut) ----
        hi = end if end > 0 else (duration if duration > 0 else 0.0)
        cut_ranges = parse_cuts(cuts, start, hi) if hi > 0 else []
        if hi > 0:
            kept = kept_from_cuts(start, hi, cut_ranges)
            if not kept:
                raise HTTPException(status_code=400, detail="পুরো ভিডিও কেটে ফেলা যায় না।")
        else:
            kept = [(start, None)]
        clip_dur = sum(y - x for x, y in kept) if hi > 0 else 0.0

        def to_out(t):
            acc = 0.0
            for x, y in kept:
                yy = float("inf") if y is None else y
                if t >= yy:
                    acc += yy - x
                elif t > x:
                    acc += t - x
                    break
                else:
                    break
            return acc

        def window(t_from, t_to):
            """সোর্স-সময়ের (from,to) থেকে ওভারলের enable অংশ; দুটো ০ হলে সবসময় দেখাবে।"""
            if not t_from and not t_to:
                return ""
            s0 = to_out(t_from) if t_from else 0.0
            e0 = to_out(t_to) if t_to else 1e6
            if reverse_on and clip_dur > 0:
                s0, e0 = max(0.0, clip_dur - min(e0, clip_dur)), max(0.0, clip_dur - s0)
            return f":enable='between(t,{s0:.3f},{e0:.3f})'"

        if reverse_on and clip_dur > MAX_REVERSE_SECONDS:
            raise HTTPException(status_code=400, detail=f"Reverse শুধু {MAX_REVERSE_SECONDS} সেকেন্ডের ছোট ক্লিপে করা যায়।")

        fi, fo = fade_in, fade_out
        if clip_dur <= 0:
            fo = 0.0  # দৈর্ঘ্য জানা না গেলে fade-out বসানো যায় না
        elif fi + fo > clip_dur:
            k = clip_dur / (fi + fo)
            fi, fo = fi * k, fo * k
        out_dur = clip_dur / speed if clip_dur > 0 else 0.0

        if output_format == "gif" and out_dur > MAX_GIF_SECONDS:
            raise HTTPException(status_code=400, detail=f"GIF সর্বোচ্চ {MAX_GIF_SECONDS} সেকেন্ডের হতে পারে। Trim বা Cut করে ছোট করুন।")

        want_v = output_format != "mp3"
        ext = {"mp4": "mp4", "gif": "gif", "mp3": "mp3"}[output_format]
        out_path = os.path.join(tmpdir, f"out.{ext}")
        inputs = ["-i", src_path]
        n_in = 1
        graph = []
        pre_args = list(EDIT_THREAD_ARGS)  # ডিকোডারের থ্রেড/মেমোরি সীমা

        if len(kept) == 1:
            x0, y0 = kept[0]
            if x0 > 0:
                pre_args += ["-ss", str(x0)]
            if y0 is not None:
                pre_args += ["-t", str(y0 - x0)]
            vcur = "[0:v]" if want_v else None
            acur = "[0:a]" if main_has_audio else None
        else:
            for k, (x, y) in enumerate(kept):
                if want_v:
                    graph.append(f"[0:v]trim=start={x:.3f}:end={y:.3f},setpts=PTS-STARTPTS[sv{k}]")
                if main_has_audio:
                    graph.append(f"[0:a]atrim=start={x:.3f}:end={y:.3f},asetpts=PTS-STARTPTS[sa{k}]")
            ins = "".join((f"[sv{k}]" if want_v else "") + (f"[sa{k}]" if main_has_audio else "") for k in range(len(kept)))
            outs = ("[vc]" if want_v else "") + ("[ac]" if main_has_audio else "")
            graph.append(f"{ins}concat=n={len(kept)}:v={1 if want_v else 0}:a={1 if main_has_audio else 0}{outs}")
            vcur = "[vc]" if want_v else None
            acur = "[ac]" if main_has_audio else None

        # ---- ধাপ ৩: ভিডিও ফিল্টার ----
        # ক্রম: ঘোরানো/ফ্লিপ/রিভার্স -> স্টিকার -> ক্রপ বা ঝাপসা-ফিট -> রঙ ও ইফেক্ট -> টেক্সট -> রেজোলিউশন/ফেড -> স্পিড
        video_map = []
        if want_v:
            st = {"cur": vcur, "n": 0}

            def vstep(chain):
                st["n"] += 1
                nxt = f"[g{st['n']}]"
                graph.append(f"{st['cur']}{chain}{nxt}")
                st["cur"] = nxt

            vw, vh = get_video_dimensions(ffmpeg_path, src_path)
            if rotate in (90, 270):
                vw, vh = vh, vw

            first = []
            if rotate == 90:
                first.append("transpose=1")
            elif rotate == 180:
                first.append("transpose=1,transpose=1")
            elif rotate == 270:
                first.append("transpose=2")
            if flip in ("h", "both"):
                first.append("hflip")
            if flip in ("v", "both"):
                first.append("vflip")
            if reverse_on:
                first.append("reverse")
            vstep(",".join(first) if first else "null")

            for k, (sp, m) in enumerate(sticker_items):
                inputs += ["-i", sp]
                idx = n_in
                n_in += 1
                sw = max(2, int(round(m["w"] * vw)))
                graph.append(f"[{idx}:v]scale={sw}:-1,format=rgba[s{k}]")
                st["n"] += 1
                nxt = f"[g{st['n']}]"
                graph.append(
                    f"{st['cur']}[s{k}]overlay=x={m['x']:.4f}*W-w/2:y={m['y']:.4f}*H-h/2:eof_action=repeat{window(m['from'], m['to'])}{nxt}"
                )
                st["cur"] = nxt

            fw, fh = vw, vh
            if aspect != "original":
                if fit == "blur":
                    cw, ch = pad_canvas(vw, vh, aspect)
                    n = st["n"] + 1
                    st["n"] = n
                    graph.append(f"{st['cur']}split[fa{n}][fb{n}]")
                    graph.append(f"[fa{n}]scale={cw}:{ch}:force_original_aspect_ratio=increase,crop={cw}:{ch},gblur=sigma=25[fbg{n}]")
                    graph.append(f"[fb{n}]scale={cw}:{ch}:force_original_aspect_ratio=decrease[ffg{n}]")
                    graph.append(f"[fbg{n}][ffg{n}]overlay=(W-w)/2:(H-h)/2[g{n}]")
                    st["cur"] = f"[g{n}]"
                    fw, fh = cw, ch
                else:
                    vstep(build_crop_filter(aspect))
                    fw, fh = crop_dims(vw, vh, aspect)

            color = []
            preset_vf = EDIT_FILTER_PRESETS[filter_preset]
            if preset_vf:
                color.append(preset_vf)
            if brightness or contrast or saturation:
                color.append(
                    f"eq=brightness={brightness / 200:.3f}:contrast={1 + contrast / 100:.2f}:saturation={1 + saturation / 100:.2f}"
                )
            fx = effect_filter(effect, effect_amount)
            if fx:
                color.append(fx)
            if color:
                vstep(",".join(color))

            if text:
                if re.fullmatch(r"#[0-9a-fA-F]{6}", text_color or "") or text_color in EDIT_TEXT_COLORS:
                    tcolor = text_color
                else:
                    tcolor = "white"
                png_path = os.path.join(tmpdir, "text.png")
                try:
                    render_text_png(text, png_path, find_edit_font(), text_size, tcolor, int(fw * 0.9), box=text_box)
                except ImportError:
                    raise HTTPException(status_code=500, detail="টেক্সট যোগ করতে সার্ভারে Pillow ইনস্টল করা দরকার (pip install Pillow)।")
                y_expr = {"top": "H*0.05", "center": "(H-h)/2", "bottom": "H-h-H*0.05"}[text_position]
                inputs += ["-i", png_path]
                idx = n_in
                n_in += 1
                st["n"] += 1
                nxt = f"[g{st['n']}]"
                graph.append(
                    f"{st['cur']}[{idx}:v]overlay=x=(W-w)/2:y={y_expr}:eof_action=repeat{window(text_from, text_to)}{nxt}"
                )
                st["cur"] = nxt

            post = []
            target_h = EDIT_RESOLUTIONS[resolution]
            if target_h:
                post.append(f"scale=-2:'min({target_h},ih)'")
            if fi > 0.01:
                post.append(f"fade=t=in:st=0:d={fi:.2f}")
            if fo > 0.01:
                post.append(f"fade=t=out:st={max(0.0, clip_dur - fo):.2f}:d={fo:.2f}")
            if speed != 1:
                post.append(f"setpts=PTS/{speed}")
            post.append("scale=trunc(iw/2)*2:trunc(ih/2)*2")  # বেজোড় সাইজ হলে libx264 ফেইল করে
            if output_format == "gif":
                post.append("fps=12")
                post.append("scale='min(480,iw)':-2:flags=lanczos")
            vstep(",".join(post))

            if output_format == "gif":
                graph.append(f"{st['cur']}split[gifa][gifb]")
                graph.append("[gifa]palettegen[gifp]")
                graph.append("[gifb][gifp]paletteuse[vout]")
                video_map = ["-map", "[vout]"]
            else:
                video_map = ["-map", st["cur"]]

        # ---- ধাপ ৪: অডিও (GIF-এ কোনো অডিও থাকে না) ----
        audio_map = []
        if output_format != "gif":
            use_orig = acur is not None and not muted
            final_a = None
            if use_orig:
                ops = []
                if reverse_on:
                    ops.append("areverse")
                if volume != 100:
                    ops.append(f"volume={volume / 100:.2f}")
                if speed != 1:
                    ops.append(f"atempo={speed}")
                graph.append(f"{acur}{','.join(ops) if ops else 'anull'}[oa]")
                final_a = "[oa]"
            if music_path:
                if music_loop_on:
                    inputs += ["-stream_loop", "-1"]
                inputs += ["-i", music_path]
                midx = n_in
                n_in += 1
                graph.append(f"[{midx}:a]volume={music_volume / 100:.2f},apad[ma]")
                if use_orig:
                    graph.append("[oa][ma]amix=inputs=2:duration=first:normalize=0[mix]")
                else:
                    graph.append("[ma]anull[mix]")
                final_a = "[mix]"
            if final_a:
                fl = []
                if fi > 0.01:
                    fl.append(f"afade=t=in:st=0:d={fi / speed:.2f}")
                if fo > 0.01 and out_dur > 0:
                    fl.append(f"afade=t=out:st={max(0.0, out_dur - fo / speed):.2f}:d={fo / speed:.2f}")
                graph.append(f"{final_a}{','.join(fl) if fl else 'anull'}[aout]")
                audio_map = ["-map", "[aout]"]
        if output_format == "mp3" and not audio_map:
            raise HTTPException(status_code=400, detail="MP3 বানাতে অডিও দরকার, কিন্তু এই ভিডিওতে (বা মিউট করা থাকায়) কোনো অডিও নেই।")

        if output_format == "mp4":
            codec_args = ["-c:v", "libx264", "-preset", EDIT_X264_PRESET, "-crf", "22", "-pix_fmt", "yuv420p"] + EDIT_THREAD_ARGS
            codec_args += ["-c:a", "aac", "-b:a", "128k"] if audio_map else ["-an"]
            codec_args += ["-movflags", "+faststart", "-shortest"]
        elif output_format == "gif":
            codec_args = ["-an", "-loop", "0"]
        else:
            codec_args = ["-vn", "-c:a", "libmp3lame", "-q:a", "2"]
            if out_dur > 0:
                codec_args += ["-t", f"{out_dur:.3f}"]

        cmd = (
            [ffmpeg_path, "-y", "-nostdin", "-hide_banner", "-loglevel", "error"]
            + (["-filter_complex_threads", "1"] if EDIT_THREAD_ARGS else [])
            + pre_args
            + inputs
            + (["-filter_complex", ";".join(graph)] if graph else [])
            + video_map
            + audio_map
            + codec_args
            + [out_path]
        )
        run_or_raise(cmd, timeout=1800, fail_msg="এডিট করা যায়নি।")

        if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
            raise RuntimeError("এডিট করা ফাইল তৈরি হয়নি।")

        try:
            os.remove(src_path)
        except OSError:
            pass

        handed_off = True
        return FileResponse(
            out_path,
            media_type={"mp4": "video/mp4", "gif": "image/gif", "mp3": "audio/mpeg"}[output_format],
            filename=f"Buffradar_edited.{ext}",
            background=BackgroundTask(shutil.rmtree, tmpdir, ignore_errors=True),
        )

    except HTTPException:
        raise
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="এডিট করতে বেশি সময় লাগছে। ছোট অংশ বা কম কোয়ালিটি দিয়ে চেষ্টা করুন।")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if tmpdir and not handed_off:
            shutil.rmtree(tmpdir, ignore_errors=True)
        EDIT_SLOTS.release()


# ---------------------------------------------------------------------------
# Text to edit — ব্যবহারকারীর লেখা (প্রম্পট) থেকে এডিটের সেটিংস বানায় (Groq, মডেল openai/gpt-oss-20b)
# সেটআপ: সার্ভারে GROQ_API_KEY এনভায়রনমেন্ট ভেরিয়েবল দিন। মডেল বদলাতে GROQ_MODEL (ডিফল্ট openai/gpt-oss-20b)।
# এই রুট শুধু সেটিংস ফেরত দেয়; আসল এডিট করে /api/edit-upload।
# ---------------------------------------------------------------------------
AI_PLAN_DEFAULTS = {
    "start": 0.0, "end": 0.0, "rotate": 0, "aspect": "original", "resolution": "original",
    "speed": 1.0, "mute": False, "volume": 100, "text": "", "text_position": "bottom",
    "text_color": "white", "text_size": 36, "text_bg": False, "text_from": 0.0, "text_to": 0.0,
    "filter_preset": "none", "brightness": 0, "contrast": 0, "saturation": 0,
    "fade_in": 0.0, "fade_out": 0.0, "flip": "none", "reverse": False,
    "effect": "none", "effect_amount": 50, "cuts": [], "fit": "crop", "output_format": "mp4",
}
AI_PLAN_COLORS = {"white", "black", "yellow", "red", "#22d3ee", "#a78bfa"}
AI_RATE = {}  # ip -> [timestamps]
AI_RATE_LIMIT = 15       # প্রতি ১০ মিনিটে একই IP থেকে সর্বোচ্চ অনুরোধ
AI_RATE_WINDOW = 600

AI_SYSTEM_PROMPT = """You convert a user's video-editing request into JSON settings for a simple video editor. Respond with a single JSON object and nothing else.

All times are in seconds measured from the beginning of the CURRENT clip (0 = start of the clip).

Keys you may include (omit a key if the user did not ask for it):
- start: where the clip should begin. "cut/remove the first 5 seconds" -> start=5
- end: where the clip should end. "keep only the first 10 seconds" -> end=10. "remove the last 3 seconds" -> end = clip_length - 3
- cuts: list of [from, to] ranges to REMOVE from the middle of the clip. "remove 5 to 8 seconds" -> [[5, 8]]. (Use start/end instead for the very beginning or very end.)
- rotate: 90, 180 or 270 (clockwise degrees)
- flip: "h" (mirror left-right), "v" (upside down) or "both"
- reverse: true to play the video backwards
- aspect: "9:16" | "1:1" | "16:9". Vertical / reels / shorts / tiktok -> "9:16". Square -> "1:1". Landscape / youtube -> "16:9"
- fit: "blur" to keep the whole video with a blurred background (only with aspect), or "crop" to fill by cropping
- resolution: "1080p" | "720p" | "480p" | "360p" (can only make smaller)
- speed: number from 0.5 to 2 ("twice as fast" -> 2, "slow motion" -> 0.5)
- mute: true or false
- volume: integer 0..200 (percent, 100 = unchanged)
- text: text to show on the video
- text_position: "top" | "center" | "bottom"
- text_color: "white" | "black" | "yellow" | "red" | "#22d3ee" | "#a78bfa"
- text_size: integer 12..120 (normal is 36; "big" about 70, "small" about 24)
- text_bg: true if the user wants a background box behind the text
- text_from, text_to: seconds between which the text is visible (omit for the whole clip)
- filter_preset: "bw" | "warm" | "cool" | "vintage" | "vivid"
- brightness, contrast, saturation: integers -100..100
- effect: "blur" | "sharpen" | "vignette" | "grain", with effect_amount integer 0..100 (default 50)
- fade_in, fade_out: seconds from 0 to 3
- output_format: "gif" (animated GIF, max 30 seconds, no sound) or "mp3" (audio only). Omit for a normal video.

Also always include:
- summary: one short sentence saying what you will do, written in the language named at the end of the user message.
- unsupported: one short sentence (same language as summary) naming anything requested that this editor cannot do here (for example adding music or stickers, subtitles, removing background, joining several clips), or an empty string.

Never add edits the user did not ask for. If the request is unclear or is not about editing the video, include no settings and explain briefly in unsupported."""


class AIPlanRequest(BaseModel):
    prompt: str
    duration: float = 0
    width: int = 0
    height: int = 0


def _num(v, default=None):
    try:
        f = float(v)
        return f if f == f and abs(f) != float("inf") else default
    except (TypeError, ValueError):
        return default


def sanitize_ai_plan(raw: dict, duration: float) -> dict:
    """AI-এর উত্তর কখনো সরাসরি বিশ্বাস করা হয় না — সব মান যাচাই করে সীমার ভেতরে আনা হয়।"""
    plan = {k: (list(v) if isinstance(v, list) else v) for k, v in AI_PLAN_DEFAULTS.items()}
    if not isinstance(raw, dict):
        return plan
    cap = duration if duration > 0 else 1e9

    start = _num(raw.get("start"))
    end = _num(raw.get("end"))
    if start is not None:
        plan["start"] = min(max(0.0, start), cap)
    if end is not None and end > 0:
        if duration > 0 and end >= duration - 0.05:
            end = 0.0
        plan["end"] = end
    if plan["end"] > 0 and plan["end"] <= plan["start"] + 0.1:
        plan["end"] = 0.0
    if duration > 0 and plan["start"] >= duration - 0.1:
        plan["start"] = 0.0

    # মাঝখান থেকে কাটা অংশ
    cuts = []
    if isinstance(raw.get("cuts"), list):
        for item in raw["cuts"][:10]:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                a, b = _num(item[0]), _num(item[1])
                if a is None or b is None:
                    continue
                a, b = min(max(0.0, min(a, b)), cap), min(max(0.0, max(a, b)), cap)
                if b - a >= 0.1:
                    cuts.append([round(a, 3), round(b, 3)])
    plan["cuts"] = sorted(cuts)

    rot = _num(raw.get("rotate"))
    if rot is not None:
        rot = int(rot) % 360
        plan["rotate"] = rot if rot in (0, 90, 180, 270) else 0
    if raw.get("flip") in ("h", "v", "both"):
        plan["flip"] = raw["flip"]
    if isinstance(raw.get("reverse"), bool):
        plan["reverse"] = raw["reverse"]
    if raw.get("aspect") in ("9:16", "1:1", "16:9"):
        plan["aspect"] = raw["aspect"]
    if raw.get("fit") in ("blur", "crop"):
        plan["fit"] = raw["fit"]
    if raw.get("resolution") in ("1080p", "720p", "480p", "360p"):
        plan["resolution"] = raw["resolution"]
    sp = _num(raw.get("speed"))
    if sp is not None:
        plan["speed"] = min(max(sp, 0.5), 2.0)
    if isinstance(raw.get("mute"), bool):
        plan["mute"] = raw["mute"]
    vol = _num(raw.get("volume"))
    if vol is not None:
        plan["volume"] = int(min(max(vol, 0), 200))

    if isinstance(raw.get("text"), str):
        plan["text"] = raw["text"].strip()[:200]
    if raw.get("text_position") in ("top", "center", "bottom"):
        plan["text_position"] = raw["text_position"]
    if raw.get("text_color") in AI_PLAN_COLORS:
        plan["text_color"] = raw["text_color"]
    ts = _num(raw.get("text_size"))
    if ts is not None:
        plan["text_size"] = int(min(max(ts, 12), 120))
    if isinstance(raw.get("text_bg"), bool):
        plan["text_bg"] = raw["text_bg"]
    tf, tt = _num(raw.get("text_from")), _num(raw.get("text_to"))
    if tf is not None:
        plan["text_from"] = min(max(0.0, tf), cap)
    if tt is not None and tt > 0:
        plan["text_to"] = min(tt, cap)
    if plan["text_to"] > 0 and plan["text_to"] <= plan["text_from"] + 0.1:
        plan["text_to"] = 0.0

    if raw.get("filter_preset") in EDIT_FILTER_PRESETS:
        plan["filter_preset"] = raw["filter_preset"]
    for key in ("brightness", "contrast", "saturation"):
        v = _num(raw.get(key))
        if v is not None:
            plan[key] = int(min(max(v, -100), 100))
    if raw.get("effect") in EDIT_EFFECTS:
        plan["effect"] = raw["effect"]
    ea = _num(raw.get("effect_amount"))
    if ea is not None:
        plan["effect_amount"] = int(min(max(ea, 0), 100))
    for key in ("fade_in", "fade_out"):
        v = _num(raw.get(key))
        if v is not None:
            plan[key] = min(max(v, 0.0), 3.0)
    if raw.get("output_format") in EDIT_FORMATS:
        plan["output_format"] = raw["output_format"]
    return plan


def plan_has_changes(plan: dict) -> bool:
    """শুধু সহায়ক মান (যেমন টেক্সটের সাইজ) বদলালে সেটাকে এডিট ধরা হয় না।"""
    ignore = set()
    if plan["effect"] == "none":
        ignore.add("effect_amount")
    if not plan["text"]:
        ignore |= {"text_size", "text_position", "text_color", "text_bg", "text_from", "text_to"}
    if plan["aspect"] == "original":
        ignore.add("fit")
    return any(plan[k] != AI_PLAN_DEFAULTS[k] for k in AI_PLAN_DEFAULTS if k not in ignore)


def parse_ai_json(text: str) -> dict:
    t = (text or "").strip()
    if t.startswith("```"):
        t = re.sub(r"^```(?:json)?\s*|\s*```$", "", t, flags=re.IGNORECASE).strip()
    if not t.startswith("{"):
        m = re.search(r"\{.*\}", t, flags=re.DOTALL)
        if m:
            t = m.group(0)
    return json.loads(t)


def call_groq_plan(key: str, model: str, user_msg: str):
    """Groq (OpenAI-সামঞ্জস্যপূর্ণ API) ডাকে। কোনো প্যারামিটার প্রত্যাখ্যাত হলে সহজ রূপে আবার চেষ্টা করে।"""
    base = {
        "model": model,
        "messages": [
            {"role": "system", "content": AI_SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
        "temperature": 0.1,
    }
    variants = [
        {**base, "response_format": {"type": "json_object"}, "reasoning_effort": "low", "include_reasoning": False, "max_completion_tokens": 1500},
        {**base, "response_format": {"type": "json_object"}, "max_completion_tokens": 1500},
        {**base, "max_completion_tokens": 1500},
    ]
    resp = None
    for body in variants:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json=body,
            timeout=40,
        )
        if resp.status_code != 400:
            break
        print(f"[ai-edit-plan] Groq HTTP 400, সহজ রূপে আবার চেষ্টা: {resp.text[:300]}")
    return resp


def _ai_plan_core(body: AIPlanRequest, request: Request):
    prompt = (body.prompt or "").strip()
    if not prompt:
        raise HTTPException(status_code=400, detail="কী এডিট করতে চান তা লিখুন।")
    if len(prompt) > 500:
        raise HTTPException(status_code=400, detail="লেখাটি অনেক বড়। ৫০০ অক্ষরের মধ্যে লিখুন।")

    key = os.environ.get("GROQ_API_KEY")
    if not key:
        raise HTTPException(status_code=503, detail="AI এখনো চালু করা হয়নি (সার্ভারে GROQ_API_KEY সেট করা নেই)।")

    # একই IP থেকে অতিরিক্ত অনুরোধ আটকানো (API খরচ বাঁচাতে)
    fwd = request.headers.get("x-forwarded-for", "")
    ip = (fwd.split(",")[0].strip() if fwd else (request.client.host if request.client else "unknown"))
    now = time.time()
    hits = [t for t in AI_RATE.get(ip, []) if now - t < AI_RATE_WINDOW]
    if len(hits) >= AI_RATE_LIMIT:
        raise HTTPException(status_code=429, detail="অনেকগুলো অনুরোধ হয়ে গেছে। কিছুক্ষণ পরে আবার চেষ্টা করুন।")
    hits.append(now)
    AI_RATE[ip] = hits

    duration = max(0.0, float(body.duration or 0))
    user_msg = (
        f"Clip length: {duration:.1f} seconds. Resolution: {int(body.width or 0)}x{int(body.height or 0)}.\n"
        f"User request: {prompt}\n"
        f"Write summary and unsupported in {'Bengali' if re.search(r'[\u0980-\u09FF]', prompt) else 'English'}."
    )
    model = os.environ.get("GROQ_MODEL", "").strip() or "openai/gpt-oss-20b"
    try:
        resp = call_groq_plan(key, model, user_msg)
    except requests.RequestException:
        raise HTTPException(status_code=502, detail="AI সার্ভারের সাথে যোগাযোগ করা যায়নি। আবার চেষ্টা করুন।")

    if resp.status_code != 200:
        # সমস্যা খুঁজতে সুবিধা হয়: Groq কী বলেছে সার্ভারের টার্মিনালে দেখা যাবে (key কখনো প্রিন্ট হয় না)
        print(f"[ai-edit-plan] Groq HTTP {resp.status_code} (model={model}): {resp.text[:400]}")
    if resp.status_code == 429:
        raise HTTPException(status_code=429, detail="AI এখন ব্যস্ত। কিছুক্ষণ পরে আবার চেষ্টা করুন।")
    if resp.status_code in (401, 403):
        raise HTTPException(status_code=502, detail="AI-এর key ঠিক নেই। সার্ভারের GROQ_API_KEY যাচাই করুন।")
    if resp.status_code == 404:
        raise HTTPException(status_code=502, detail="AI-এর মডেল পাওয়া যায়নি। সার্ভারের টার্মিনালে বিস্তারিত দেখুন।")
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail="AI উত্তর দিতে পারেনি। আবার চেষ্টা করুন।")
    try:
        content = resp.json()["choices"][0]["message"]["content"]
        raw = parse_ai_json(content)
        if not isinstance(raw, dict):
            raise ValueError("not an object")
    except Exception:
        raise HTTPException(status_code=502, detail="AI-এর উত্তর বোঝা যায়নি। অন্যভাবে লিখে আবার চেষ্টা করুন।")

    plan = sanitize_ai_plan(raw, duration)
    summary = raw.get("summary") if isinstance(raw.get("summary"), str) else ""
    unsupported = raw.get("unsupported") if isinstance(raw.get("unsupported"), str) else ""
    return {
        "plan": plan,
        "changed": plan_has_changes(plan),
        "summary": summary.strip()[:300],
        "unsupported": unsupported.strip()[:300],
    }


@app.post("/api/ai-edit-plan")
def ai_edit_plan(body: AIPlanRequest, request: Request):
    """AI ধাপ = ১টা edit_ai গোনা। সফল হলে এডিট ধাপের জন্য এক-ব্যবহারের টিকিট দেয় (এডিট ধাপ আলাদা করে গোনা হয় না)।"""
    state = None
    if USAGE_ENABLED:
        try:
            out = usage_consume(request, "edit_ai", None)
        except Exception as e:
            print(f"[usage] সীমা যাচাই ব্যর্থ, কাজ চলতে দেওয়া হলো: {e}")
            out = None
        if out and not out["ok"]:
            return limit_response(out["info"])
        state = out
    try:
        result = _ai_plan_core(body, request)
    except BaseException:
        usage_refund(state, "edit_ai", None)
        raise
    if state and state.get("consumed") is not None:
        if not result.get("changed"):
            usage_refund(state, "edit_ai", None)   # কোনো এডিট বোঝা না গেলে সীমা কাটে না
        else:
            nonce = secrets.token_urlsafe(16)
            try:
                _sb_rpc("issue_ticket", {"p_subject": state["subject"], "p_ref": nonce})
                result["ticket"] = nonce
            except Exception as e:
                print(f"[usage] টিকিট বানানো ব্যর্থ: {e}")
    return result


# থাম্বনেইল আর জরুরি ফলব্যাকের জন্য পুরোনো স্ট্রিমিং এন্ডপয়েন্ট আগের মতোই আছে
# (শুধু লিংক যাচাই আর '--' যোগ হয়েছে)
@app.get("/api/proxy-download")
async def proxy_download(
    url: str,
    quality: str = "1080p",
    is_audio: bool = False,
    bitrate: str = "192kbps",
):
    try:
        decoded_url = urllib.parse.unquote(url)
        if not is_valid_http_url(decoded_url):
            raise HTTPException(status_code=400, detail="অনুগ্রহ করে একটি বৈধ লিঙ্ক দিন।")
        ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()

        if is_audio:
            abr = parse_bitrate(bitrate)

            ytdlp_cmd = [
                "yt-dlp", *ytdlp_cookie_args(),
                "-f", "bestaudio/best",
                "--ffmpeg-location", ffmpeg_path,
                "-o", "-",
                "--no-playlist",
                "--", decoded_url,
            ]
            ffmpeg_cmd = [
                ffmpeg_path,
                "-i", "pipe:0",
                "-vn",
                "-f", "mp3",
                "-ab", f"{abr}k",
                "pipe:1",
            ]

            p1 = subprocess.Popen(ytdlp_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            p2 = subprocess.Popen(ffmpeg_cmd, stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            p1.stdout.close()
            process = p2
            media_type = "audio/mpeg"
            filename = "Buffradar_audio.mp3"
        else:
            height_limit = parse_height(quality)

            format_selector = f"bestvideo[height<={height_limit}]+bestaudio/best[height<={height_limit}]/best"

            cmd = [
                "yt-dlp", *ytdlp_cookie_args(),
                "-f", format_selector,
                "--ffmpeg-location", ffmpeg_path,
                "-o", "-",
                "--no-playlist",
                "--", decoded_url,
            ]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            media_type = "video/mp4"
            filename = "Buffradar_download.mp4"

        def stream_process():
            while True:
                chunk = process.stdout.read(65536)
                if not chunk:
                    break
                yield chunk
            process.stdout.close()
            process.wait()

        return StreamingResponse(
            stream_process(),
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
