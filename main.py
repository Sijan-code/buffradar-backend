from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
from starlette.background import BackgroundTask
from pydantic import BaseModel
import glob
import json
import mimetypes
import os
import shutil
import subprocess
import tempfile
import threading
import urllib.parse
import urllib.request
import imageio_ffmpeg
import yt_dlp

app = FastAPI()

# Next.js ফ্রন্টএন্ডের কানেকশন পারমিশন
# ⚠️ মার্কেটে ছাড়ার আগে ["*"] বদলে শুধু নিজের ডোমেইন দাও, যেমন ["https://buffradar.com"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class VideoRequest(BaseModel):
    url: str


# ---------------------------------------------------------------------------
# YouTube cookies সাপোর্ট (Render/cloud সার্ভার থেকে গেলে YouTube বট-চেক করে
# ব্লক করে দেয়: "Sign in to confirm you're not a bot" এরর আসে)।
# Render dashboard -> Environment -> Secret Files এ "cookies.txt" নামে একটা
# ফাইল আপলোড করলে এটা /etc/secrets/cookies.txt পাথে পাওয়া যাবে।
# লোকাল/ডেভ মেশিনে ফাইলটা না থাকলে চুপচাপ স্কিপ হয়ে যাবে, এরর দেবে না।
# ---------------------------------------------------------------------------
COOKIES_PATH = os.environ.get("YTDLP_COOKIES_PATH", "/etc/secrets/cookies.txt")
print(
    "YT COOKIES:",

os.path.exists(COOKIES_PATH),
    COOKIES_PATH,

os.path.getsize(COOKIES_PATH) if
os.path.exists(COOKIES_PATH)
else 0
)


def cookie_ydl_opts():
    """yt_dlp.YoutubeDL(...) এ পাস করার জন্য cookies অপশন (dict)।"""
    return {"cookiefile": COOKIES_PATH} if os.path.exists(COOKIES_PATH) else {}


def cookie_cli_args():
    """subprocess দিয়ে চালানো yt-dlp কমান্ডের জন্য cookies আর্গুমেন্ট (list)।"""
    return ["--cookies", COOKIES_PATH] if os.path.exists(COOKIES_PATH) else []


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


@app.post("/api/extract")
def extract_video_info(request: VideoRequest):
    video_url = request.url

    if not video_url:
        raise HTTPException(status_code=400, detail="অনুগ্রহ করে একটি বৈধ লিঙ্ক দিন।")

    ydl_opts = {
        # ইউটিউবের সব ধরনের নতুন ফরম্যাট (WebM/Opus) সাপোর্ট করার জন্য নমনীয় ফরম্যাট
        'format': 'bestvideo*+bestaudio/best',
        'no_warnings': True,
        'quiet': True,
        # ইউটিউব যাতে রেন্ডার সার্ভারকে সহজে ব্লক না করতে পারে তার জন্য কিছু বাড়তি অপশন
        'extractor_args': {
            'youtube': {
                'player_client': ['web', 'android', 'ios'],
            }
        },
        'http_headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        },
        **cookie_ydl_opts(),
    }

    print("YT-DLP VERSION:",
    yt_dlp.version.__version__)

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
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
                "yt-dlp",
                *cookie_cli_args(),
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
                "yt-dlp",
                *cookie_cli_args(),
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
def convert_media(
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
        with yt_dlp.YoutubeDL({"quiet": True, "no_warnings": True, "noplaylist": True, **cookie_ydl_opts()}) as ydl:
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
                "yt-dlp",
                *cookie_cli_args(),
                "-f", fmt_selector,
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
                "yt-dlp",
                *cookie_cli_args(),
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
                "yt-dlp",
                *cookie_cli_args(),
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
