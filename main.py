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
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        'no_warnings': True,
        'quiet': True,
        'http_headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        }
    }

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
