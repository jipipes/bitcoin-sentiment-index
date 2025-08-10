bitcoin-sentiment-index/

├── scripts/

│   ├── collect_x.py          # API를 통해 X의 게시글 데이터 수집 후 CSV 저장

│   ├── collect_price.py      # Upbit 비트코인 가격 데이터 수집 후 CSV 저장

│   ├── run_collector.py

│

│   ├── clean_text.py         # 불필요한 글 제거, 감정 분석 불가한 글 필터링

│   ├── analyze_sentiment.py  # VADER 또는 TextBlob 통해 감정 점수 붙이기

│

│   └── run_streamlit.py      # Streamlit 대시보드 실행

├── data/

├── output/

├── docs/

│   ├── api_spec.md

│   ├── script_plan.md

│   ├── pipeline_architecture.png

│   └── usage_example.md

├── README.md

├── requirements.txt

├── config.py

├── .env