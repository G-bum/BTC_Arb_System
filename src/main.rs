use futures_util::StreamExt;
use serde::Deserialize;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[derive(Debug, Deserialize)]
struct DepthStream {
    stream: String,
    data: DepthData,
}

#[derive(Debug, Deserialize)]
struct DepthData {
    #[serde(rename = "b")]
    bids: Vec<Vec<String>>,
    #[serde(rename = "a")]
    asks: Vec<Vec<String>>,
    #[serde(rename = "E")]
    _event_time: u64, // 경고 해결을 위해 _ 추가
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
    // 날짜순 정렬 후 당분기, 차분기 선정
    (valid_codes[0].clone(), valid_codes[1].clone()) 
}

#[tokio::main]
async fn main() {
    let (current, next) = get_active_symbols();
    let url = format!("wss://dstream.binance.com/stream?streams=btcusd_perp@depth5/{}@depth5/{}@depth5", current, next);

    println!("🚀 [하이브리드 5호가 모드] 수집 시작");
    println!("📍 당분기: {}, 차분기: {}", current, next);

    let (ws_stream, _) = connect_async(&url).await.expect("연결 실패");
    let (_, mut read) = ws_stream.split();

    while let Some(Ok(msg)) = read.next().await {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let hour = (now / 3600) % 24;
        let min = (now / 60) % 60;

        // UTC 08:05 자동 로드 감시
        if hour == 8 && min == 5 {
            println!("⚠️ UTC 08:05 감지! 시스템이 자동으로 코드를 갱신합니다.");
            break; 
        }

        if let Message::Text(text) = msg {
            if let Ok(depth) = serde_json::from_str::<DepthStream>(&text) {
                print_depth_info(depth, &current, &next);
            }
        }
    }
}

fn print_depth_info(depth: DepthStream, current: &str, next: &str) {
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let t = now_ms / 1000;
    
    let role = if depth.stream.contains("perp") { "[스왑]" }
               else if depth.stream.contains(current) { "[당분기]" }
               else if depth.stream.contains(next) { "[차분기]" }
               else { "[기타]" };

    println!("\n[UTC {:02}:{:02}:{:02}] {}", (t/3600)%24, (t/60)%60, t%60, role);
    println!("--------------------------------------------------");

    // 매수 5호가 출력
    let mut bid_accum = 0.0;
    for (i, bid) in depth.data.bids.iter().enumerate() {
        let price: f64 = bid[0].parse().unwrap_or(0.0);
        let qty: f64 = bid[1].parse().unwrap_or(0.0);
        bid_accum += qty;
        println!("  매수 {}호가: {} | 수량: {:.3} | 누적: {:.3}", i+1, price, qty, bid_accum);
    }

    // 매도 5호가 출력
    let mut ask_accum = 0.0;
    for (i, ask) in depth.data.asks.iter().enumerate() {
        let price: f64 = ask[0].parse().unwrap_or(0.0);
        let qty: f64 = ask[1].parse().unwrap_or(0.0);
        ask_accum += qty;
        println!("  매도 {}호가: {} | 수량: {:.3} | 누적: {:.3}", i+1, price, qty, ask_accum);
    }
}