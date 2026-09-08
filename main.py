from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from fastapi.responses import StreamingResponse
import requests
import yt_dlp

app = FastAPI()

# Next.js ফ্রন্টএন্ডের কানেকশন পারমিশন
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class VideoRequest(BaseModel):
    url: str

@app.post("/api/extract")
def extract_video_info(request: VideoRequest):
    video_url = request.url
    
    if not video_url:
        raise HTTPException(status_code=400, detail="অনুগ্রহ করে একটি বৈধ লিঙ্ক দিন।")

    ydl_opts = {
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',  # সবচেয়ে ভালো কম্বাইন্ড ভিডিও+অডিও ফাইল নেবে
        'no_warnings': True,
        'quiet': True,
        'http_headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        }
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # ভিডিও ফাইল ডাউনলোড না করে শুধু মেটাডেটা এবং সরাসরি সোর্স ইউআরএল বের করা
            info = ydl.extract_info(video_url, download=False)
            
            # সবচেয়ে সেরা কম্বাইন্ড ভিডিও ও অডিও লিঙ্কটি খুঁজে বের করার লজিক
        direct_url = info.get('url')
        if not direct_url and info.get('formats'):
            # ফরম্যাটগুলোর মধ্য থেকে mp4 এবং সরাসরি url আছে এমন সেরা লিঙ্ক খোঁজা
            valid_formats = [f for f in info['formats'] if f.get('url') and f.get('acodec') != 'none' and f.get('vcodec') != 'none']
            if valid_formats:
                direct_url = valid_formats[-1].get('url')
            else:
                direct_url = info['formats'][-1].get('url')
            
            
            if not direct_url:
                raise HTTPException(status_code=404, detail="ভি디오র সরাসরি সোর্স লিঙ্ক পাওয়া যায়নি।")

            return {
                "success": True,
                "title": info.get('title', 'Buffradar Universal Video'),
                "thumbnail": info.get('thumbnail'),
                "download_url": direct_url,
                "platform": info.get('extractor_key')
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"লিঙ্কটি প্রসেস করা যায়নি: {str(e)}")

@app.get("/api/proxy-download")
async def proxy_download(url: str, quality: str = "1080p"):
            try:
                import urllib.parse
                import subprocess
                from fastapi.responses import StreamingResponse

                decoded_url = urllib.parse.unquote(url)

                height_limit = "1080"
                if quality == "4k":
                    height_limit = "2160"
                elif quality == "720p":
                    height_limit = "720"

                format_selector = f"bestvideo[height<={height_limit}]+bestaudio/best[height<={height_limit}]/best"

                import imageio_ffmpeg
                ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()

                cmd = [

                    "yt-dlp",
                    "-f", format_selector,
                    "--ffmpeg-location", ffmpeg_path,
                    "-o", "-",
                    "--no-playlist",
                    decoded_url
                ]

                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)

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
                    media_type="video/mp4",
                    headers={"Content-Disposition": "attachment; filename=Buffradar_download.mp4"}
                )

            except Exception as e:
                return {"error: str(e)"}                


