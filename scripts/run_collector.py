import threading, subprocess, time, sys, logging, os

# logging 설정
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/collector.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

collectors = [
    "scripts/collect_x.py",
    "scripts/collect_price.py"
]

# 스크립트 실행 함수
def run_script(path):
    try:
        logging.info(f"Starting collector: {path}")
        subprocess.run([sys.executable, path])
    except Exception as e:
        logging.error(f"Collector crashed {path} - {repr(e)}")
    finally:
        logging.warning(f"Collector stopped {path}")

# 메인 함수
if __name__ == "__main__":
    logging.info("=== Bitcoin Sentiment Index Launcher started ===")
    threads = []
    for script in collectors:
        t = threading.Thread(target=run_script, args=(script,))
        t.start()
        threads.append(t)
        time.sleep(1) # 각 스크립트 사이에 1초 간격

    # 모든 스크립트가 종료될 때까지 대기
    for t in threads:
        t.join()
    
    logging.info("=== All collectors finished ===")