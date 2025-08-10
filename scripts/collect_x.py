import os, time, requests, logging, pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

BEARER_TOKEN = os.getenv("Bearer_Token")
if not BEARER_TOKEN:
    raise ValueError("Bearer Token is not set in the environment variables.")
URL = "https://api.twitter.com/2/tweets/search/recent"
QUERY = '(("bitcoin" OR "btc" OR "비트코인") (lang:en OR lang:ko) -is:retweet)'
FIELDS = "id, text, created_at, lang, author_id"
OUT_PATH = "data/btc_tweets.csv"
os.makedirs("logs", exist_ok=True)

# logging 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/x_collector.log", encoding="utf-8"),
        logging.StreamHandler()  # 콘솔 출력
    ]
)
def headers(): return {"Authorization" : f"Bearer {BEARER_TOKEN}"}
# Authorization 헤더에 Bearer Token 넣어서 인증

# 트윗 데이터 수집 함수
def fetch_recent(next_token=None): 
    params = {
        "query": QUERY,
        "max_results": 100,
        "tweet.fields": FIELDS,
    }
    if next_token:
        params["next_token"] = next_token # 다음 페이지 토큰 있으면 추가
    try:
        r = requests.get(URL, headers=headers(), params=params, timeout=20) # GET 요청
        r.raise_for_status()
        j = r.json() # JSON 응답 파싱
        data = j.get("data", []) # 'data' 키에서 트윗 데이터 추출
        next_token = j.get("meta", {}).get("next_token") # 다음 페이지 토큰 추출
        return data, next_token
    except Exception as e:
        logging.error(f"fetch_recent failed: {repr(e)}")
        return [], None

# CSV 저장 함수
def save_to_csv(rows):
    df_new = pd.DataFrame(rows) # 새 데이터프레임 생성
    if df_new.empty:
        return 0
    if os.path.exists(OUT_PATH):
        df = pd.read_csv(OUT_PATH)
        df = pd.concat([df, df_new], ignore_index=True)
        df.drop_duplicates(subset="id", inplace=True) # 중복 제거
    # 기존 CSV 파일 있으면 불러와서 새 데이터 합치기
    else:
        df = df_new.drop_duplicates(subset="id")
    
    df.to_csv(OUT_PATH, index=False, encoding="utf-8-sig") 
    return len(df_new) # 새로 저장된 트윗 개수 반환

# 메인 함수
if __name__ == "__main__":
    os.makedirs("data", exist_ok=True) # data 폴더 없으면 생성
    logging.info("X recent search collector started.")
    while True:
        collected = [] # 이번에 수집한 트윗 저장
        data, next_token = fetch_recent() # 첫 페이지 수집
        collected.extend(data)

        if next_token:
            more, _ = fetch_recent(next_token)
            collected.extend(more) # 다음 페이지 수집
        
        n = save_to_csv(collected) # CSV에 저장
        logging.info(f"pulled {n}, total_csv={len(pd.read_csv(OUT_PATH)) if os.path.exists(OUT_PATH) else 0}")
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        time.sleep(90) # 90초 대기 후 다음 호출 



