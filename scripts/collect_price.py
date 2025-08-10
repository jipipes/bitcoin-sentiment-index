import requests, time, logging, pandas as pd, os
from datetime import datetime, timezone, timedelta

OUT_PATH = "data/upbit_price.csv" # 결과 CSV 
INTERVAL_SEC = 60 # 수집 간격 60초
URL = "https://api.upbit.com/v1/ticker?markets=KRW-BTC"
os.makedirs("logs", exist_ok=True)

# logging 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/price_collector.log", encoding="utf-8"),
        logging.StreamHandler() # 콘솔 출력
    ]
)

# 현재 시간을 어느 서버/로컬이든 KST ISO 포맷으로 라벨링
def now_kst_isodate():
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst).isoformat(timespec='seconds')

# 현재가 수집 함수
def fetch_btc_price():
    try:
        r = requests.get(URL, timeout=10) # GET 요청
        r.raise_for_status()
        j = r.json()[0] # 필요한 필드만 추출해서 row 구성
        row = {
            "ts": now_kst_isodate(), # 수집 시각(KST)
            "trade_price": j["trade_price"], # 현재가
            "acc_trade_price_24h": j["acc_trade_price_24h"],
            "acc_trade_volume_24h": j["acc_trade_volume_24h"]
        }
        return row
    except Exception as e:
        logging.error(f"fetch_btc_price failed: {repr(e)}")
        return None

# CSV 저장 함수
def save_to_csv(row: dict):
    if row is None:
        return 0
    
    df_new = pd.DataFrame([row]) # 새 데이터프레임 생성
    if os.path.exists(OUT_PATH):
        df = pd.read_csv(OUT_PATH)
        df = pd.concat([df, df_new], ignore_index=True)
        df.drop_duplicates(subset="ts", inplace=True) # 중복 제거
    else:
        df = df_new
    
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True) # 폴더 없으면 생성
    df.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")
    return 1 # 새로 시도한 insert 개수 반환 (성공시 1, 실패시 0)

# 메인 함수
if __name__ == "__main__":
    logging.info(f"Upbit btc price collector strarted. interval = {INTERVAL_SEC} seconds")
    while True:
        row = fetch_btc_price()
        n = save_to_csv(row)
        if n:
            logging.info(f"{row['ts']} price={row['trade_price']}")
        time.sleep(INTERVAL_SEC)


