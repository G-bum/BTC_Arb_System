use anyhow::Result;
use chrono::{Duration, TimeZone, Utc, Timelike};
use rusqlite::{params, Connection};
use serde::Deserialize;
use std::time::Duration as StdDuration;
use tokio::time::sleep;

// --- 데이터 수신용 구조체 ---
#[derive(Debug, Deserialize)]
struct BinanceRate {
    symbol: String,
    #[serde(rename = "fundingRate")]
    funding_rate: String,
    #[serde(rename = "fundingTime")]
    funding_time: i64,
}

#[derive(Debug, Deserialize)]
struct BybitResponse {
    result: BybitResult,
}
#[derive(Debug, Deserialize)]
struct BybitResult {
    list: Vec<BybitItem>,
}
#[derive(Debug, Deserialize)]
struct BybitItem {
    #[serde(rename = "fundingRate")]
    funding_rate: String,
    #[serde(rename = "fundingRateTimestamp")]
    funding_rate_timestamp: String,
}

#[derive(Debug, Deserialize)]
struct DeribitResponse {
    result: Vec<DeribitItem>,
}
#[derive(Debug, Deserialize)]
struct DeribitItem {
    timestamp: i64,
    interest_8h: f64,
}

// --- 데이터베이스 초기화 ---
fn init_db() -> Result<Connection> {
    let conn = Connection::open("FUNDING_DATA.db")?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS funding_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            rate REAL NOT NULL,
            funding_time TEXT NOT NULL,
            UNIQUE(exchange, symbol, funding_time)
        )",
        [],
    )?;
    Ok(conn)
}

// --- 거래소별 데이터 수집 및 저장 로직 ---
async fn fetch_and_store(conn: &Connection) -> Result<()> {
    let client = reqwest::Client::new();
    let now = Utc::now();

    // 1. Binance (BTCUSD_PERP - 코인마진)
    if let Ok(resp) = client.get("https://dapi.binance.com/dapi/v1/fundingRate?symbol=BTCUSD_PERP&limit=5").send().await {
        if let Ok(rates) = resp.json::<Vec<BinanceRate>>().await {
            for r in rates {
                let dt = Utc.timestamp_millis_opt(r.funding_time).unwrap().to_rfc3339();
                let _ = conn.execute(
                    "INSERT OR IGNORE INTO funding_rates (exchange, symbol, rate, funding_time) VALUES (?1, ?2, ?3, ?4)",
                    params!["Binance", "BTCUSD_PERP", r.funding_rate.parse::<f64>()?, dt],
                );
            }
        }
    }

    // 2. Bybit (BTCUSD - 인버스 코인마진)
    let bybit_url = "https://api.bybit.com/v5/market/funding/history?category=inverse&symbol=BTCUSD&limit=5";
    if let Ok(resp) = client.get(bybit_url).send().await {
        if let Ok(res) = resp.json::<BybitResponse>().await {
            for item in res.result.list {
                if let Ok(ts) = item.funding_rate_timestamp.parse::<i64>() {
                    let dt = Utc.timestamp_millis_opt(ts).unwrap().to_rfc3339();
                    let _ = conn.execute(
                        "INSERT OR IGNORE INTO funding_rates (exchange, symbol, rate, funding_time) VALUES (?1, ?2, ?3, ?4)",
                        params!["Bybit", "BTCUSD", item.funding_rate.parse::<f64>()?, dt],
                    );
                }
            }
        }
    }

    // 3. Deribit (BTC-PERPETUAL - 코인마진)
    let start_ts = (now - Duration::hours(24)).timestamp_millis();
    let deribit_url = format!("https://www.deribit.com/api/v2/public/get_funding_rate_history?instrument_name=BTC-PERPETUAL&start_timestamp={}&end_timestamp={}", start_ts, now.timestamp_millis());
    if let Ok(resp) = client.get(deribit_url).send().await {
        if let Ok(res) = resp.json::<DeribitResponse>().await {
            for item in res.result {
                // 8시간 간격(0, 8, 16시) 데이터만 필터링
                if item.timestamp % (8 * 3600 * 1000) == 0 {
                    let dt = Utc.timestamp_millis_opt(item.timestamp).unwrap().to_rfc3339();
                    let _ = conn.execute(
                        "INSERT OR IGNORE INTO funding_rates (exchange, symbol, rate, funding_time) VALUES (?1, ?2, ?3, ?4)",
                        params!["Deribit", "BTC-PERPETUAL", item.interest_8h, dt],
                    );
                }
            }
        }
    }

    println!("[{}] 수집 완료 및 누락 데이터 보충됨", now.format("%Y-%m-%d %H:%M:%S"));
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let conn = init_db()?;
    println!("🚀 펀딩 레이트 수집기 가동 (Binance, Bybit, Deribit)");

    // 초기 실행 시 최근 24시간 누락분 즉시 수집
    let _ = fetch_and_store(&conn).await;

    loop {
        let now = Utc::now();
        
        // 매일 UTC 0, 8, 16시 2분 0초에 수집 실행
        if (now.hour() % 8 == 0) && now.minute() == 2 && now.second() == 0 {
            if let Err(e) = fetch_and_store(&conn).await {
                eprintln!("수집 에러: {}", e);
            }
            // 중복 실행 방지를 위해 1분간 대기
            sleep(StdDuration::from_secs(60)).await;
        }

        // 1초 단위로 시간 체크
        sleep(StdDuration::from_secs(1)).await;
    }
}