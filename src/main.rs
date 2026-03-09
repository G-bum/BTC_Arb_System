use futures_util::StreamExt;
use serde::Deserialize;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use rusqlite::{params, Connection}; // params 경고 해결 및 사용

#[derive(Debug, Deserialize, Clone)] // Clone 추가
struct DepthStream {
    stream: String,
    data: DepthData,
}

#[derive(Debug, Deserialize, Clone)]
struct DepthData {
    #[serde(rename = "b")]
    bids: Vec<Vec<String>>,
    #[serde(rename = "a")]
    asks: Vec<Vec<String>>,
    #[serde(rename = "E")]
    _event_time: u64,
}

fn init_db() -> Connection {
    let conn = Connection::open("arb_trading.db").expect("DB 파일 생성 실패");
    conn.execute(
        "CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY,
            time TEXT NOT NULL,
            category TEXT NOT NULL,
            bid1_price REAL,
            ask1_price REAL,
            spread_rate REAL
        )",
        [],
    ).expect("테이블 생성 실패");
    conn
}

fn get_active_symbols() -> (String, String) {
    let file = File::open("symbols.txt").expect("symbols.txt 파일이 없습니다.");
    let reader = BufReader::new(file);
    let mut valid_codes = Vec::new();
    for line in reader.lines() {
        let code = line.unwrap().trim().to_string();
        if code.starts_with("btcusd_") && !code.contains("perp") {
            valid_codes.push(code);
        }
    }
    (valid_codes[0].clone(), valid_codes[1].clone()) 
}

#[tokio::main]
async fn main() {
    let conn = init_db(); // DB 연결 유지
    let (current, next) = get_active_symbols();
    
    println!("🚀 [황금 수식 & DB 저장 모드] 가동");
    
    let url = format!("wss://dstream.binance.com/stream?streams=btcusd_perp@depth5/{}@depth5/{}@depth5", current, next);
    let (ws_stream, _) = connect_async(&url).await.expect("연결 실패");
    let (_, mut read) = ws_stream.split();

    // 실시간 계산을 위해 마지막 가격을 기억할 변수
    let mut last_current_bid = 0.0;
    let mut last_next_bid = 0.0;

    while let Some(Ok(msg)) = read.next().await {
        if let Message::Text(text) = msg {
            if let Ok(depth) = serde_json::from_str::<DepthStream>(&text) {
                let bid1: f64 = depth.data.bids[0][0].parse().unwrap_or(0.0);
                let ask1: f64 = depth.data.asks[0][0].parse().unwrap_or(0.0);
                
                // 상품별 가격 업데이트
                if depth.stream.contains(&current) { last_current_bid = bid1; }
                if depth.stream.contains(&next) { last_next_bid = bid1; }

                // --- 💎 기범 선생님의 황금 수식 적용 ---
                let mut spread_rate = 0.0;
                if last_current_bid > 0.0 && last_next_bid > 0.0 {
                    let diff = last_next_bid - last_current_bid;
                    let avg = (last_next_bid + last_current_bid) / 2.0;
                    spread_rate = (diff / avg) * 100.0; // 선생님의 수식 그대로!
                }

                // DB에 저장
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs().to_string();
                conn.execute(
                    "INSERT INTO trades (time, category, bid1_price, ask1_price, spread_rate) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![now, depth.stream, bid1, ask1, spread_rate],
                ).expect("데이터 저장 실패");

                // 5호가 출력 및 실시간 스프레드 표시
                print_depth_info(depth, &current, &next, spread_rate);
            }
        }
    }
}

fn print_depth_info(depth: DepthStream, current: &str, next: &str, spread: f64) {
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let t = now_ms / 1000;
    let role = if depth.stream.contains("perp") { "[스왑]" }
               else if depth.stream.contains(current) { "[당분기]" }
               else if depth.stream.contains(next) { "[차분기]" }
               else { "[기타]" };

    println!("\n[UTC {:02}:{:02}:{:02}] {} | 실시간 스프레드: {:.4}%", 
             (t/3600)%24, (t/60)%60, t%60, role, spread);
    println!("--------------------------------------------------");

    // 1. 매도 호가 출력 (Asks) + 누적 계산
    let mut ask_accum = 0.0;
    for (i, ask) in depth.data.asks.iter().take(5).enumerate() {
        let price: f64 = ask[0].parse().unwrap_or(0.0);
        let qty: f64 = ask[1].parse().unwrap_or(0.0);
        ask_accum += qty;
        println!("  매도 {}호가: {} | 수량: {:>8.0} | 누적: {:>8.0}", i + 1, price, qty, ask_accum);
    }

    println!("  -- (현재가) --");

    // 2. 매수 호가 출력 (Bids) + 누적 계산
    let mut bid_accum = 0.0;
    for (i, bid) in depth.data.bids.iter().take(5).enumerate() {
        let price: f64 = bid[0].parse().unwrap_or(0.0);
        let qty: f64 = bid[1].parse().unwrap_or(0.0);
        bid_accum += qty;
        println!("  매수 {}호가: {} | 수량: {:>8.0} | 누적: {:>8.0}", i + 1, price, qty, bid_accum);
    }
}