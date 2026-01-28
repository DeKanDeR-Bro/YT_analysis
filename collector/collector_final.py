import os
import sqlite3
import logging
from datetime import datetime
import dateutil.parser
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
from urllib.parse import urlparse

# --- НАСТРОЙКИ ---
load_dotenv()
API_KEY = os.getenv('YOUTUBE_API_KEY')

# ОПРЕДЕЛЯЕМ ПУТЬ К ПАПКЕ СО СКРИПТОМ
# База создастся там же, где лежит этот файл
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_NAME = os.path.join(BASE_DIR, 'minecraft_war.db')

# Период
START_DATE = datetime(2024, 1, 1).replace(tzinfo=None)
END_DATE = datetime(2026, 2, 1).replace(tzinfo=None)

# УМНЫЕ ЛИМИТЫ (Комментариев на 1 видео)
LIMIT_FULL = 10000  # Для участников войны (качаем всё)

# ПОЛНЫЙ СПИСОК ЦЕЛЕЙ
TARGETS = [
    # --- ГРУППА FT ---
    {'url': 'https://youtube.com/@iiuoner', 'tag': 'FT'},
    {'url': 'https://youtube.com/@tuke', 'tag': 'FT'},
    {'url': 'https://youtube.com/@urmomgg', 'tag': 'FT'},
    {'url': 'https://youtube.com/@fonix5890', 'tag': 'FT'},
    {'url': 'https://youtube.com/channel/UCUXQN6yNISAKRoHdH5LXQwg', 'tag': 'FT'}, # NSAI
    {'url': 'https://youtube.com/@akvi4', 'tag': 'FT'},
    {'url': 'https://www.youtube.com/@fokus1311', 'tag': 'FT'}, # Fokus1
    {'url': 'https://www.youtube.com/@Filinok', 'tag': 'FT_OLD'},

    # --- SKY ---
    {'url': 'https://youtube.com/@skypl1ne?si=bcq3zdwobq_39UTN', 'tag': 'SKY'}, # Skypline
    {'url': 'https://youtube.com/channel/UC7d5-in3MUp3tVrOKCoUdBw?si=9GFUF9cBR1XyRXs0', 'tag': 'SKY_CHEAT'}, 

    # --- ГРУППА LITE ---
    {'url': 'https://youtube.com/channel/UCEiFJNTeKO6vD3p4e13lRvg', 'tag': 'LITE'}, # Нефор
    {'url': 'https://youtube.com/channel/UCgleGgwRTEpefhPLbZA0HGQ', 'tag': 'LITE'}, # Bain
    {'url': 'https://youtube.com/@wasabyc', 'tag': 'LITE'},

    # --- ГРУППА LITE_CONTROL (Внутренний контроль) ---
    {'url': 'https://youtube.com/@zakoo', 'tag': 'LITE_CONTROL'}, # Zako

    # --- ГРУППА CLASSIC (Контроль) ---
    {'url': 'https://youtube.com/@jake50', 'tag': 'CLASSIC'},

]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def init_clean_db():
    """Удаляет старую базу и создает новую"""
    if os.path.exists(DB_NAME):
        try:
            os.remove(DB_NAME)
            print(f"♻️ Старая база удалена: {DB_NAME}")
        except PermissionError:
            print(f"❌ ОШИБКА: Закрой программу (DB Browser), которая держит файл {DB_NAME}!")
            return None

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS channels
                 (channel_id TEXT PRIMARY KEY, title TEXT, category TEXT, 
                  subscriber_count INTEGER, view_count INTEGER, video_count INTEGER)''')
    c.execute('''CREATE TABLE IF NOT EXISTS videos
                 (video_id TEXT PRIMARY KEY, channel_id TEXT, title TEXT, 
                  published_at TEXT, view_count INTEGER, like_count INTEGER, 
                  comment_count INTEGER, duration TEXT,
                  FOREIGN KEY(channel_id) REFERENCES channels(channel_id))''')
    c.execute('''CREATE TABLE IF NOT EXISTS comments
                 (comment_id TEXT PRIMARY KEY, video_id TEXT, author_name TEXT, 
                  text TEXT, published_at TEXT, like_count INTEGER,
                  FOREIGN KEY(video_id) REFERENCES videos(video_id))''')
    conn.commit()
    return conn

def resolve_channel_id(youtube, url_or_id):
    parsed = urlparse(url_or_id)
    path_parts = parsed.path.strip('/').split('/')
    request = None
    if 'channel' in path_parts:
        cid = path_parts[path_parts.index('channel')+1]
        request = youtube.channels().list(part='snippet,statistics,contentDetails', id=cid)
    elif url_or_id.startswith('@') or (len(path_parts)>0 and path_parts[-1].startswith('@')):
        handle = path_parts[-1] if not url_or_id.startswith('@') else url_or_id
        request = youtube.channels().list(part='snippet,statistics,contentDetails', forHandle=handle)
    
    if request:
        response = request.execute()
        if response['items']: return response['items'][0]
    return None

def get_channel_videos(youtube, upload_playlist_id):
    video_ids = []
    next_page = None
    while True:
        res = youtube.playlistItems().list(
            playlistId=upload_playlist_id, part='snippet', maxResults=50, pageToken=next_page
        ).execute()
        for item in res['items']:
            pub_date = dateutil.parser.isoparse(item['snippet']['publishedAt']).replace(tzinfo=None)
            if pub_date > END_DATE: continue
            if pub_date < START_DATE: return video_ids
            video_ids.append(item['snippet']['resourceId']['videoId'])
        next_page = res.get('nextPageToken')
        if not next_page: break
    return video_ids

def get_comments(youtube, video_id, limit):
    comments_data = []
    try:
        req = youtube.commentThreads().list(
            part="snippet", videoId=video_id, maxResults=100, textFormat="plainText", order="time"
        )
        while req and len(comments_data) < limit:
            res = req.execute()
            for item in res['items']:
                top = item['snippet']['topLevelComment']['snippet']
                comments_data.append((
                    item['id'], video_id, top['authorDisplayName'],
                    top['textDisplay'], top['publishedAt'], top['likeCount']
                ))
            if 'nextPageToken' in res:
                req = youtube.commentThreads().list(
                    part="snippet", videoId=video_id, maxResults=100, 
                    pageToken=res['nextPageToken'], textFormat="plainText", order="time"
                )
            else: break
    except: pass
    return comments_data

def main():
    if not API_KEY:
        print("❌ ОШИБКА: Не найден API ключ в .env файле!")
        return

    print(f"📂 База данных будет сохранена здесь: {DB_NAME}")
    conn = init_clean_db()
    if not conn: return
    c = conn.cursor()
    
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    
    print(f"--- ЗАПУСК ФИНАЛЬНОГО СБОРА (2024-2026) ---")
    
    for target in TARGETS:
        try:       
            # 1. Резолвим канал
            ch_data = resolve_channel_id(youtube, target['url'])
            if not ch_data:
                logging.warning(f"Канал не найден: {target['url']}")
                continue
            
            cid = ch_data['id']
            title = ch_data['snippet']['title']
            stats = ch_data['statistics']
            
            print(f"\n📺 Обработка канала: {title} [{target['tag']}]")
            
            c.execute("INSERT OR REPLACE INTO channels VALUES (?, ?, ?, ?, ?, ?)",
                      (cid, title, target['tag'], 
                       int(stats.get('subscriberCount', 0)),
                       int(stats.get('viewCount', 0)),
                       int(stats.get('videoCount', 0))))
            conn.commit()
            
            # 2. Качаем видео
            uploads_id = ch_data['contentDetails']['relatedPlaylists']['uploads']
            video_ids = get_channel_videos(youtube, uploads_id)
            print(f"   Видео в обработке: {len(video_ids)}")
            
            # 3. Детали и комментарии
            total_comments = 0
            for i in range(0, len(video_ids), 50):
                chunk = video_ids[i:i+50]
                v_res = youtube.videos().list(part='snippet,statistics,contentDetails', id=','.join(chunk)).execute()
                
                for item in v_res['items']:
                    vid = item['id']
                    
                    c.execute("INSERT OR REPLACE INTO videos VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                              (vid, cid, item['snippet']['title'], item['snippet']['publishedAt'],
                               int(item['statistics'].get('viewCount', 0)),
                               int(item['statistics'].get('likeCount', 0)),
                               int(item['statistics'].get('commentCount', 0)),
                               item['contentDetails']['duration']))
                    
                    # Качаем комменты
                    comms = get_comments(youtube, vid, limit=LIMIT_FULL)
                    if comms:
                        c.executemany("INSERT OR REPLACE INTO comments VALUES (?, ?, ?, ?, ?, ?)", comms)
                        total_comments += len(comms)
                    
                    conn.commit()
                print(f"   ...обработано {min(i+50, len(video_ids))} видео")
            
            print(f"   ✅ Комментариев собрано: {total_comments}")
                
        except Exception as e:
            logging.error(f"Ошибка с {target['url']}: {e}")

    conn.close()
    print(f"\n🎉 ГОТОВО! Полная база собрана: {DB_NAME}")

if __name__ == "__main__":
    main()