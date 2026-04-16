# import speech_recognition as sr

# recognizer = sr.Recognizer()

# with sr.Microphone() as source:
#     print("Speak now...")
#     audio = recognizer.listen(source)

# try:
#     text = recognizer.recognize_google(audio)
#     print("You said:", text)
# except Exception as e:
#     print("Error:", e)


# import vosk
# import sounddevice as sd
# import queue
# import json

# q = queue.Queue()

# def callback(indata, frames, time, status):
#     q.put(bytes(indata))

# model = vosk.Model("C:/Users/prabhakaranp/Documents/prabhakaran/ktr_generator/transformer/vosk-model-small-en-us-0.15")
# rec = vosk.KaldiRecognizer(model, 16000)

# with sd.RawInputStream(samplerate=16000, blocksize=8000, dtype='int16',
#                        channels=1, callback=callback):
#     print("Speak now...")
#     while True:
#         data = q.get()
#         if rec.AcceptWaveform(data):
#             print(json.loads(rec.Result())["text"])



# def is_palindrome(text):
#     return text == text[::-1]


# print(is_palindrome("madam"))
# print(is_palindrome("nurses tu"))


"""
YouTube Video Uploader — Python Script
=======================================
REQUIREMENTS:
    pip install google-auth google-auth-oauthlib google-api-python-client

SETUP STEPS (one time only):
    1. Go to https://console.cloud.google.com/
    2. Create a new project
    3. Enable "YouTube Data API v3"
    4. Go to Credentials → Create → OAuth 2.0 Client ID → Desktop App
    5. Download the JSON file → rename it to "client_secrets.json"
    6. Place client_secrets.json in the SAME folder as this script
    7. Run the script — it opens a browser for Google login (first time only)
    8. After login, a "token.json" is saved — reuse it forever (no login again)
"""

# import os
# import sys
# import json
# import time
# import pickle

# # ── Google API imports ──────────────────────────────────────────────────────
# try:
#     from google.auth.transport.requests import Request
#     from google.oauth2.credentials import Credentials
#     from google_auth_oauthlib.flow import InstalledAppFlow
#     from googleapiclient.discovery import build
#     from googleapiclient.http import MediaFileUpload
#     from googleapiclient.errors import HttpError
# except ImportError:
#     print("\n❌ Missing libraries. Run this first:")
#     print("   pip install google-auth google-auth-oauthlib google-api-python-client\n")
#     sys.exit(1)


# # ════════════════════════════════════════════════════════════════════════════
# #  EDIT THESE SETTINGS BEFORE RUNNING
# # ════════════════════════════════════════════════════════════════════════════

# VIDEO_FILE        = "my_video.mp4"          # ← path to your video file
# THUMBNAIL_FILE    = ""                       # ← path to thumbnail image (optional, leave "" to skip)
# CLIENT_SECRETS    = "client_secrets.json"   # ← downloaded from Google Cloud Console
# TOKEN_FILE        = "token.json"            # ← auto-created after first login

# TITLE             = "My Awesome Video"
# DESCRIPTION       = """This is my video description.

# You can write multiple lines here.
# Add links, timestamps, social handles — anything.

# #MyChannel #Python #YouTube"""

# TAGS              = ["python", "youtube", "automation", "tutorial"]

# # Category IDs (pick one):
# #   1=Film  2=Autos  10=Music  15=Pets  17=Sports  20=Gaming
# #   22=People  23=Comedy  24=Entertainment  25=News  26=How-to  27=Education
# #   28=Science  29=NonProfits
# CATEGORY_ID       = "22"

# # Privacy: "public" | "private" | "unlisted"
# PRIVACY           = "private"

# # Language of the video
# LANGUAGE          = "en"

# # ════════════════════════════════════════════════════════════════════════════


# SCOPES = ["https://www.googleapis.com/auth/youtube.upload",
#           "https://www.googleapis.com/auth/youtube"]


# def authenticate():
#     """Handle OAuth2 login. Opens browser on first run, reuses token after."""
#     creds = None

#     # Load saved token if it exists
#     if os.path.exists(TOKEN_FILE):
#         with open(TOKEN_FILE, "rb") as f:
#             creds = pickle.load(f)

#     # Refresh or re-authenticate if needed
#     if not creds or not creds.valid:
#         if creds and creds.expired and creds.refresh_token:
#             print("🔄 Refreshing access token...")
#             creds.refresh(Request())
#         else:
#             if not os.path.exists(CLIENT_SECRETS):
#                 print(f"\n❌ '{CLIENT_SECRETS}' not found.")
#                 print("   Download it from Google Cloud Console → Credentials → OAuth 2.0 Client IDs")
#                 sys.exit(1)
#             print("🌐 Opening browser for Google login...")
#             flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS, SCOPES)
#             creds = flow.run_local_server(port=0)

#         # Save token for future use
#         with open(TOKEN_FILE, "wb") as f:
#             pickle.dump(creds, f)
#         print(f"✅ Token saved to '{TOKEN_FILE}' — no login needed next time.\n")

#     return build("youtube", "v3", credentials=creds)


# def upload_video(youtube):
#     """Upload the video file with metadata and resumable upload support."""

#     if not os.path.exists(VIDEO_FILE):
#         print(f"\n❌ Video file not found: '{VIDEO_FILE}'")
#         print("   Update VIDEO_FILE at the top of this script.\n")
#         sys.exit(1)

#     file_size_mb = os.path.getsize(VIDEO_FILE) / (1024 * 1024)
#     print(f"📁 Video file : {VIDEO_FILE}")
#     print(f"   Size       : {file_size_mb:.1f} MB")
#     print(f"   Title      : {TITLE}")
#     print(f"   Privacy    : {PRIVACY.upper()}")
#     print(f"   Category   : {CATEGORY_ID}")
#     print()

#     body = {
#         "snippet": {
#             "title":                TITLE,
#             "description":          DESCRIPTION,
#             "tags":                 TAGS,
#             "categoryId":           CATEGORY_ID,
#             "defaultLanguage":      LANGUAGE,
#             "defaultAudioLanguage": LANGUAGE,
#         },
#         "status": {
#             "privacyStatus":           PRIVACY,
#             "selfDeclaredMadeForKids": False,
#         }
#     }

#     media = MediaFileUpload(
#         VIDEO_FILE,
#         chunksize=1024 * 1024 * 5,   # upload in 5 MB chunks
#         resumable=True
#     )

#     request = youtube.videos().insert(
#         part="snippet,status",
#         body=body,
#         media_body=media
#     )

#     print("⬆️  Uploading... (this may take a while for large files)")
#     print("-" * 50)

#     response     = None
#     last_percent = -1

#     while response is None:
#         try:
#             status, response = request.next_chunk()
#             if status:
#                 percent = int(status.progress() * 100)
#                 if percent != last_percent:
#                     bar    = "█" * (percent // 5) + "░" * (20 - percent // 5)
#                     print(f"\r   [{bar}] {percent}%", end="", flush=True)
#                     last_percent = percent

#         except HttpError as e:
#             if e.resp.status in [500, 502, 503, 504]:
#                 # Transient server error — retry after delay
#                 print(f"\n⚠️  Server error ({e.resp.status}), retrying in 5s...")
#                 time.sleep(5)
#             else:
#                 print(f"\n❌ Upload failed: {e}")
#                 sys.exit(1)

#     print("\n" + "-" * 50)
#     video_id = response["id"]
#     print(f"\n✅ Upload complete!")
#     print(f"   Video ID  : {video_id}")
#     print(f"   Watch URL : https://www.youtube.com/watch?v={video_id}")
#     print(f"   Studio    : https://studio.youtube.com/video/{video_id}/edit\n")
#     return video_id


# def set_thumbnail(youtube, video_id):
#     """Upload a custom thumbnail image (JPG/PNG, max 2MB, 1280x720 recommended)."""
#     if not THUMBNAIL_FILE:
#         return
#     if not os.path.exists(THUMBNAIL_FILE):
#         print(f"⚠️  Thumbnail file not found: '{THUMBNAIL_FILE}' — skipping.")
#         return

#     print(f"🖼️  Uploading thumbnail: {THUMBNAIL_FILE}")
#     youtube.thumbnails().set(
#         videoId=video_id,
#         media_body=MediaFileUpload(THUMBNAIL_FILE)
#     ).execute()
#     print("✅ Thumbnail set successfully.\n")


# def print_summary(video_id):
#     """Print final summary with useful links."""
#     print("═" * 50)
#     print("  UPLOAD SUMMARY")
#     print("═" * 50)
#     print(f"  Title   : {TITLE}")
#     print(f"  Privacy : {PRIVACY.upper()}")
#     print(f"  Tags    : {', '.join(TAGS[:5])}")
#     print(f"  Watch   : https://www.youtube.com/watch?v={video_id}")
#     print(f"  Edit    : https://studio.youtube.com/video/{video_id}/edit")
#     print("═" * 50)
#     print()
#     if PRIVACY == "private":
#         print("💡 Tip: Your video is PRIVATE. Change it to Public in YouTube Studio when ready.")


# # ── Main ────────────────────────────────────────────────────────────────────
# if __name__ == "__main__":
#     print("\n" + "═" * 50)
#     print("  YouTube Video Uploader")
#     print("═" * 50 + "\n")

#     youtube  = authenticate()
#     video_id = upload_video(youtube)
#     set_thumbnail(youtube, video_id)
#     print_summary(video_id)

# import subprocess

# result = subprocess.run(
#     [
#         r"C:\Users\prabhakaranp\Downloads\pdi-ce-11.0.0.0-237\data-integration\pan.bat",
#         r"-file=C:\Users\prabhakaranp\Documents\prabhakaran\Q27\ST table name variable set.ktr"
#     ],
#     capture_output=True,
#     text=True
# )

# print("STDOUT:", result.stdout)
# print("STDERR:", result.stderr)
# print("Return code:", result.returncode)


# import subprocess

# result = subprocess.run(
#     [
#         r"C:\Users\prabhakaranp\Downloads\pdi-ce-11.0.0.0-237\data-integration\kitchen.bat",
#         r"-file=C:\Users\prabhakaranp\Documents\prabhakaran\Q27\Data type check main job.kjb"
#     ],
#     capture_output=True,
#     text=True
# )

# stdout = result.stdout
# stderr = result.stderr

# print("STDOUT:", stdout)
# print("STDERR:", stderr)

# if "ERROR" in stderr:
#     print("❌ Job Failed")
# else:
#     print("✅ Job Success")


# import subprocess

# subprocess.run([
#     r"C:\Users\prabhakaranp\Downloads\pdi-ce-11.0.0.0-237\data-integration\kitchen.bat",
#     r"-file=C:\Users\prabhakaranp\Documents\prabhakaran\Q26\master_data_pipeline.kjb"
# ])


# import subprocess

# spoon_path = r"C:\Users\prabhakaranp\Downloads\pdi-ce-11.0.0.0-237\data-integration\spoon.bat"

# file_path = r"C:\Users\prabhakaranp\Documents\prabhakaran\Q27\data type check.ktr"

# cmd = f'"{spoon_path}" -file="{file_path}"'

# subprocess.Popen(cmd, shell=True)

import subprocess

proc = subprocess.Popen(
    r"C:\Users\prabhakaranp\Downloads\pdi-ce-11.0.0.0-237\data-integration\spoon.bat",
    shell=True
)

# BUT this only tracks bat, not java