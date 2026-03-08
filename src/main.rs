use futures_util::StreamExt;
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

// --- 데이터 구조 정의 ---
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
    event_time: u64,
}

#[tokio::main]
async fn main() {
    // 수집 대상 설정
    let url = "wss://dstream.binance.com/stream?streams=btcusd_perp@depth5/btcusd_260327@depth5/btcusd_260626@depth5";

    let (ws_stream, _) = connect_async(url).await.expect("연결 실패");
    let (_, mut read) = ws_stream.split();

    println!("🚀 [UTC 기준] 선물 3종 5호가 실시간 수집 시작...");

    while let Some(Ok(msg)) = read.next().await {
        if let Message::Text(text) = msg {
            if let Ok(depth) = serde_json::from_str::<DepthStream>(&text) {
                print_depth_info(depth);
            }
        }
    }
}

fn print_depth_info(depth: DepthStream) {
    let now_system = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let total_ms = now_system.as_millis() as u64;
    
    // --- 🌐 세계 표준시(UTC) 계산 ---
    let total_secs = total_ms / 1000;
    let hour = (total_secs / 3600) % 24; 
    let min = (total_secs / 60) % 60;
    let sec = total_secs % 60;
    let ms = total_ms % 1000;

    // --- 🏷️ 별명(Alias) 부여 ---
    let role = match depth.stream.as_str() {
        s if s.contains("perp") => "[스왑]",
        s if s.contains("260327") => "[당분기]",
        s if s.contains("260626") => "[차분기]",
        _ => "[기타]",
    };

    let delay = if total_ms > depth.data.event_time {
        total_ms - depth.data.event_time
    } else {
        0
    };

    println!("\n[UTC {:02}:{:02}:{:02}.{:03}] {} | 지연: {}ms", hour, min, sec, ms, role, delay);
    println!("--------------------------------------------------");

    // 매수 5호가 및 누적 수량 출력 (슬리피지 확인용)
    let mut bid_accum = 0.0;
    println!("  <매수 5호가>");
    for (i, bid) in depth.data.bids.iter().enumerate() {
        let price: f64 = bid[0].parse().unwrap_or(0.0);
        let qty: f64 = bid[1].parse().unwrap_or(0.0);
        bid_accum += qty;
        println!("    {}호가: {} | 수량: {:.3} | 누적: {:.3}", i+1, price, qty, bid_accum);
    }

    // 매도 5호가 및 누적 수량 출력
    let mut ask_accum = 0.0;
    println!("  <매도 5호가>");
    for (i, ask) in depth.data.asks.iter().enumerate() {
        let price: f64 = ask[0].parse().unwrap_or(0.0);
        let qty: f64 = ask[1].parse().unwrap_or(0.0);
        ask_accum += qty;
        println!("    {}호가: {} | 수량: {:.3} | 누적: {:.3}", i+1, price, qty, ask_accum);
    }
}