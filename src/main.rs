use futures_util::StreamExt;
use rusqlite::Connection;
use serde::Deserialize;
use tokio_tungstenite::connect_async;

#[derive(Deserialize, Debug)]
struct OrderBook {
    #[serde(rename = "b")]
    bids: Vec<Vec<String>>,
    #[serde(rename = "a")]
    asks: Vec<Vec<String>>,
}

#[tokio::main]
async fn main() {
    // 1. 데이터 저장 경로 설정 (선생님이 정하신 01Ab 폴더)
    let db_path = "C:/BTC_Arb/01/01A/01Ab/01Ab01price.db"; 
    let conn = Connection::open(db_path).expect("DB 연결 실패");

    // 2. 테이블 생성 (데이터를 담을 그릇)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS price (
            id INTEGER PRIMARY KEY,
            time TEXT,
            bid_price REAL,
            ask_price REAL
        )",
        [],
    ).unwrap();

    println!("🚀 바이낸스 실시간 호가 수집 시작... (저장소: 01Ab01price.db)");

    // 3. 바이낸스 웹소켓 연결 (BTC 선물 5단계 호가)
    let url = "wss://fstream.binance.com/ws/btcusdt@depth5@100ms";
    let (ws_stream, _) = connect_async(url).await.expect("바이낸스 연결 실패");
    let (_, mut read) = ws_stream.split();

    while let Some(message) = read.next().await {
        if let Ok(msg) = message {
            if let Ok(text) = msg.to_text() {
                if let Ok(data) = serde_json::from_str::<OrderBook>(text) {
                    let bid = data.bids[0][0].parse::<f64>().unwrap_or(0.0);
                    let ask = data.asks[0][0].parse::<f64>().unwrap_or(0.0);
                    let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();

                    // 4. DB에 저장
                    let _ = conn.execute(
                        "INSERT INTO price (time, bid_price, ask_price) VALUES (?1, ?2, ?3)",
                        (&now, &bid, &ask),
                    );

                    println!("[{}] 매수: {} | 매도: {}", now, bid, ask);
                }
            }
        }
    }
}