import sqlite3
import datetime
import os
import sys

# Windows 환경 호환성을 위해 sys.stdout의 인코딩을 강제로 cp949로 변경하거나 에러를 무시하도록 처리
sys.stdout.reconfigure(encoding='utf-8')

db_path = r'c:\BTC_Arb\01\01A\01Aa\arb_trading.db'

if not os.path.exists(db_path):
    print(f"Error: {db_path} does not exist.")
    exit(1)

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 데이터 건수 확인
    cursor.execute("SELECT COUNT(*) FROM snapshot_58min")
    count = cursor.fetchone()[0]
    print(f"총 {count}개의 스냅샷 데이터가 저장되어 있습니다.\n")

    # 최신 3개 데이터 조회
    cursor.execute("""
        SELECT ts_utc, category, ask_p1, bid_p1, sprd_ask1, sprd_bid1
        FROM snapshot_58min 
        ORDER BY id DESC LIMIT 3
    """)
    rows = cursor.fetchall()

    if not rows:
        print("DB에 저장된 스냅샷 데이터가 없습니다.")
    else:
        print("--- 최근 저장된 스냅샷 데이터 3건 ---")
        # 헤더 출력
        print(f"{'시간(KST)':<10} | {'상품':<12} | {'매도1(Ask)':>10} | {'매수1(Bid)':>10} | {'스프레드(Ask)':>14} | {'스프레드(Bid)':>14}")
        print("-" * 75)
        
        for r in rows:
            dt = datetime.datetime.fromtimestamp(r[0], tz=datetime.timezone.utc)
            # 한국 시간으로 변환해서 보기 편하게 출력
            dt_kst = dt.astimezone(datetime.timezone(datetime.timedelta(hours=9)))
            time_str = dt_kst.strftime('%H:%M:%S')
            
            # 카테고리 문자열 길이에 따라 정렬 조정 (한글이 없으므로 단순 처리)
            cat = r[1]
            ask_p = f"{r[2]:.1f}"
            bid_p = f"{r[3]:.1f}"
            sprd_a = f"{r[4]:.4f}"
            sprd_b = f"{r[5]:.4f}"
            
            print(f"[{time_str}] | {cat:<12} | {ask_p:>10} | {bid_p:>10} | {sprd_a:>14}% | {sprd_b:>14}%")
            
except sqlite3.Error as e:
    print(f"SQLite DB 에러: {e}")
except Exception as e:
    print(f"오류 발생: {e}")
finally:
    if 'conn' in locals():
        conn.close()
