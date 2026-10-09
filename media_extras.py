"""
BuffRadar extras: থাম্বনেইল ডাউনলোডার + সাবটাইটেল ডাউনলোডার।

main.py-তে শুধু এই ২টা বদল লাগবে (নিচে README-র মতো করে দেওয়া আছে):
  1) ইমপোর্ট + রাউটার যোগ:
        from media_extras import router as extras_router, build_thumbnails, build_subtitle_tracks
        app.include_router(extras_router)
  2) /api/extract-এর return dict-এ ২টা লাইন:
        "thumbnails": build_thumbnails(info),
        "subtitle_tracks": build_subtitle_tracks(info),
"""
import glob
import html
import ipaddress
import os
import re
import shutil
import socket
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, Response
from starlette.background import BackgroundTask

router = APIRouter()


def is_http_url(u: str) -> bool:
    try:
        p = urllib.parse.urlparse(u or "")
    except Exception:
        return False
    return p.scheme in ("http", "https") and bool(p.netloc)


# ---------------------------------------------------------------------------
# /api/extract-এর জন্য হেল্পার (yt-dlp-র info থেকে, অতিরিক্ত নেটওয়ার্ক কল ছাড়াই)
# ---------------------------------------------------------------------------
def build_thumbnails(info, limit: int = 5):
    """বড় সাইজ আগে; একই সাইজ একবারই; কিছু না পেলে একক thumbnail ফলব্যাক।"""
    seen, out = set(), []
    for t in (info.get("thumbnails") or []):
        u = t.get("url")
        if not u or not is_http_url(u):
            continue
        w, h = t.get("width"), t.get("height")
        key = (w, h) if (w and h) else u
        if key in seen:
            continue
        seen.add(key)
        out.append({"url": u, "width": w, "height": h})
    out.sort(key=lambda t: (t["width"] or 0) * (t["height"] or 0), reverse=True)
    out = out[:limit]
    if not out and info.get("thumbnail"):
        out = [{"url": info["thumbnail"], "width": None, "height": None}]
    return out


def build_subtitle_tracks(info, max_manual: int = 60):
    """
    manual সাবটাইটেল সবগুলো। অটো-জেনারেটেড শুধু ভিডিওর নিজের ভাষার (YouTube অটো-অনুবাদের শত শত ভাষা দেখাই না)।
    """
    tracks = []
    for lang, fmts in (info.get("subtitles") or {}).items():
        if lang == "live_chat":
            continue
        name = (fmts[0].get("name") if fmts else None) or lang
        tracks.append({"lang": lang, "name": name, "auto": False})
    tracks = tracks[:max_manual]

    auto = info.get("automatic_captions") or {}
    orig = [l for l in auto if l.endswith("-orig")]
    video_lang = info.get("language")
    candidates = orig or ([video_lang] if video_lang else ["en"])
    for lang in candidates:
        if lang in auto:
            fmts = auto[lang]
            name = (fmts[0].get("name") if fmts else None) or lang
            tracks.append({"lang": lang, "name": name, "auto": True})
    return tracks


# ---------------------------------------------------------------------------
# সাবটাইটেল ডাউনলোড: GET /api/subtitles?url=...&lang=en&fmt=srt|vtt|txt&auto=false
# ---------------------------------------------------------------------------
LANG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.\-]{0,31}$")
_TAG_RE = re.compile(r"<[^>]+>")
_META_PREFIXES = ("WEBVTT", "NOTE", "Kind:", "Language:", "STYLE", "X-TIMESTAMP")


def vtt_to_text(raw: str) -> str:
    """টাইমিং/নম্বর/ট্যাগ বাদ দিয়ে শুধু সংলাপ। YouTube অটো-ক্যাপশনের পরপর দুবার আসা লাইন একবার রাখে।"""
    lines = [l.strip() for l in raw.replace("\r", "").split("\n")]
    out, last = [], None
    for i, s in enumerate(lines):
        if not s or s.startswith(_META_PREFIXES) or "-->" in s:
            continue
        if s.isdigit():  # শুধু cue নম্বর হলে বাদ (পরের লাইন টাইমিং হলে)
            nxt = next((x for x in lines[i + 1:] if x), "")
            if "-->" in nxt:
                continue
        s = html.unescape(_TAG_RE.sub("", s)).strip()
        if not s or s == last:
            continue
        out.append(s)
        last = s
    return "\n".join(out)


@router.get("/api/subtitles")
def download_subtitles(url: str, lang: str, fmt: str = "srt", auto: bool = False):
    fmt = (fmt or "").lower().strip()
    if not is_http_url(url):
        raise HTTPException(status_code=400, detail="অনুগ্রহ করে একটি বৈধ লিঙ্ক দিন।")
    if fmt not in ("srt", "vtt", "txt"):
        raise HTTPException(status_code=400, detail="এই ফরম্যাটটি সাপোর্টেড নয়।")
    if not LANG_RE.match(lang or ""):
        raise HTTPException(status_code=400, detail="ভুল ভাষা কোড।")

    import imageio_ffmpeg  # এখানে ইমপোর্ট, যাতে মডিউল লোড হালকা থাকে

    tmpdir = tempfile.mkdtemp(prefix="buffradar_sub_")
    try:
        target = "srt" if fmt == "srt" else "vtt"  # txt-এর জন্য vtt থেকে টেক্সট বানাই
        cmd = [
            "yt-dlp", "--skip-download",
            "--write-auto-subs" if auto else "--write-subs",
            "--sub-langs", re.escape(lang),
            "--sub-format", "vtt/srt/best",
            "--convert-subs", target,
            "--ffmpeg-location", imageio_ffmpeg.get_ffmpeg_exe(),
            "-o", os.path.join(tmpdir, "sub"),
            "--no-playlist", "--no-warnings", "-q",
            "--", url,
        ]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, timeout=120)

        files = glob.glob(os.path.join(tmpdir, f"sub*.{target}"))
        if not files:
            raise HTTPException(status_code=404, detail="এই ভাষার সাবটাইটেল পাওয়া যায়নি বা এখন আনা যাচ্ছে না।")
        path = max(files, key=os.path.getsize)

        media = {"srt": "application/x-subrip", "vtt": "text/vtt"}.get(fmt, "text/plain")
        if fmt == "txt":
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                text = vtt_to_text(f.read())
            if not text:
                raise HTTPException(status_code=404, detail="সাবটাইটেলে কোনো লেখা পাওয়া যায়নি।")
            path = os.path.join(tmpdir, "out.txt")
            with open(path, "w", encoding="utf-8") as f:
                f.write(text)

        safe_lang = re.sub(r"[^A-Za-z0-9_.-]+", "_", lang)
        return FileResponse(
            path,
            media_type=f"{media}; charset=utf-8",
            filename=f"Buffradar_subtitle_{safe_lang}.{fmt}",
            background=BackgroundTask(shutil.rmtree, tmpdir, ignore_errors=True),
        )
    except HTTPException:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise
    except subprocess.TimeoutExpired:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise HTTPException(status_code=504, detail="সাবটাইটেল আনতে বেশি সময় লাগছে। আবার চেষ্টা করুন।")
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise HTTPException(status_code=500, detail="সাবটাইটেল ডাউনলোড করা যায়নি।")


# ---------------------------------------------------------------------------
# থাম্বনেইল ডাউনলোড: GET /api/thumbnail?url=<ছবির লিংক>
# ব্রাউজার সরাসরি নামাতে গেলে CORS আটকায়, তাই সার্ভার এনে দেয়। ছবি ছোট, খরচ নগণ্য।
# সার্ভার যেন ভেতরের নেটওয়ার্কে (localhost/private IP) হাত না দিতে পারে, তাই SSRF গার্ড আছে।
# ---------------------------------------------------------------------------
MAX_THUMB_BYTES = 15 * 1024 * 1024
_IMAGE_EXT = {"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp", "image/gif": "gif", "image/avif": "avif"}
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def host_is_public(host: str) -> bool:
    try:
        infos = socket.getaddrinfo(host, None)
    except OSError:
        return False
    if not infos:
        return False
    for info in infos:
        ip = ipaddress.ip_address(info[4][0].split("%")[0])
        if not ip.is_global:
            return False
    return True


class _SafeRedirect(urllib.request.HTTPRedirectHandler):
    """রিডাইরেক্টের মাধ্যমে ভেতরের নেটওয়ার্কে ঢোকা আটকায়।"""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        p = urllib.parse.urlparse(newurl)
        if p.scheme not in ("http", "https") or not p.hostname or not host_is_public(p.hostname):
            raise urllib.error.URLError("blocked redirect")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


@router.get("/api/thumbnail")
def download_thumbnail(url: str, name: str = "Buffradar_thumbnail"):
    if not is_http_url(url):
        raise HTTPException(status_code=400, detail="অনুগ্রহ করে একটি বৈধ লিঙ্ক দিন।")
    host = urllib.parse.urlparse(url).hostname
    if not host or not host_is_public(host):
        raise HTTPException(status_code=400, detail="এই লিংকটি গ্রহণযোগ্য নয়।")

    opener = urllib.request.build_opener(_SafeRedirect)
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    try:
        with opener.open(req, timeout=10) as resp:
            ctype = (resp.headers.get_content_type() or "").lower()
            if ctype not in _IMAGE_EXT:
                raise HTTPException(status_code=400, detail="এটা সাপোর্টেড ছবি নয়।")
            data = resp.read(MAX_THUMB_BYTES + 1)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=502, detail="ছবিটি আনা যায়নি। অন্য সাইজ চেষ্টা করুন।")
    if len(data) > MAX_THUMB_BYTES:
        raise HTTPException(status_code=413, detail="ছবিটি অনেক বড়।")

    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)[:60] or "Buffradar_thumbnail"
    return Response(
        content=data,
        media_type=ctype,
        headers={"Content-Disposition": f'attachment; filename="{safe_name}.{_IMAGE_EXT[ctype]}"'},
    )
