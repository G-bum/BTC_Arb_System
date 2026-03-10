use futures_util::StreamExt;
use serde::Deserialize;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use rusqlite::{params, Connection, ToSql};
use chrono::{Timelike, Utc};

#[derive(Debug, Deserialize, Clone)]
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

#[derive(Debug, Clone)]
struct Snapshot58 {
    ts_utc: u64,
    category: String,
    ask_p: [f64; 5], ask_q: [f64; 5],
    bid_p: [f64; 5], bid_q: [f64; 5],
    cur_basis_ask: [f64; 5], cur_basis_bid: [f64; 5],
    nxt_basis_ask: [f64; 5], nxt_basis_bid: [f64; 5],
    sprd_ask: [f64; 5], sprd_bid: [f64; 5],
}

// 58분 스냅샷 및 5호가 연산용 함수
fn calculate_metrics(
    swap: &Option<DepthData>,
    curr: &Option<DepthData>,
    next: &Option<DepthData>,
) -> ([f64; 5], [f64; 5], [f64; 5], [f64; 5], [f64; 5], [f64; 5]) {
    let mut cur_basis_ask = [0.0; 5];
    let mut cur_basis_bid = [0.0; 5];
    let mut nxt_basis_ask = [0.0; 5];
    let mut nxt_basis_bid = [0.0; 5];
    let mut sprd_ask = [0.0; 5];
    let mut sprd_bid = [0.0; 5];

    let get_p = |depth_opt: &Option<DepthData>, is_ask: bool, idx: usize| -> f64 {
        if let Some(d) = depth_opt {
            let list = if is_ask { &d.asks } else { &d.bids };
            if idx < list.len() { return list[idx][0].parse().unwrap_or(0.0); }
        }
        0.0
    };

    let calc = |far: f64, near: f64| -> f64 {
        if far > 0.0 && near > 0.0 { (far - near) / ((far + near) / 2.0) * 100.0 } else { 0.0 }
    };

    for i in 0..5 {
        let swap_a = get_p(swap, true, i);  let swap_b = get_p(swap, false, i);
        let curr_a = get_p(curr, true, i);  let curr_b = get_p(curr, false, i);
        let next_a = get_p(next, true, i);  let next_b = get_p(next, false, i);

        cur_basis_ask[i] = calc(curr_a, swap_b);
        cur_basis_bid[i] = calc(curr_b, swap_a);
        nxt_basis_ask[i] = calc(next_a, swap_b);
        nxt_basis_bid[i] = calc(next_b, swap_a);
        sprd_ask[i] = calc(next_a, curr_b);
        sprd_bid[i] = calc(next_b, curr_a);
    }
    (cur_basis_ask, cur_basis_bid, nxt_basis_ask, nxt_basis_bid, sprd_ask, sprd_bid)
}

fn get_depth_arrays(depth: &Option<DepthData>) -> ([f64; 5], [f64; 5], [f64; 5], [f64; 5]) {
    let mut ask_p = [0.0; 5]; let mut ask_q = [0.0; 5];
    let mut bid_p = [0.0; 5]; let mut bid_q = [0.0; 5];
    if let Some(d) = depth {
        let mut ask_accum = 0.0;
        for (i, v) in d.asks.iter().take(5).enumerate() {
            ask_p[i] = v[0].parse().unwrap_or(0.0);
            ask_accum += v[1].parse::<f64>().unwrap_or(0.0);
            ask_q[i] = ask_accum;
        }
        let mut bid_accum = 0.0;
        for (i, v) in d.bids.iter().take(5).enumerate() {
            bid_p[i] = v[0].parse().unwrap_or(0.0);
            bid_accum += v[1].parse::<f64>().unwrap_or(0.0);
            bid_q[i] = bid_accum;
        }
    }
    (ask_p, ask_q, bid_p, bid_q)
}

fn insert_snapshot(conn: &Connection, snap: &Snapshot58) {
    let mut sql = String::from("INSERT INTO snapshot_58min (ts_utc, category, ask_p1, ask_p2, ask_p3, ask_p4, ask_p5, ask_q1, ask_q2, ask_q3, ask_q4, ask_q5, bid_p1, bid_p2, bid_p3, bid_p4, bid_p5, bid_q1, bid_q2, bid_q3, bid_q4, bid_q5, cur_basis_ask1, cur_basis_ask2, cur_basis_ask3, cur_basis_ask4, cur_basis_ask5, cur_basis_bid1, cur_basis_bid2, cur_basis_bid3, cur_basis_bid4, cur_basis_bid5, nxt_basis_ask1, nxt_basis_ask2, nxt_basis_ask3, nxt_basis_ask4, nxt_basis_ask5, nxt_basis_bid1, nxt_basis_bid2, nxt_basis_bid3, nxt_basis_bid4, nxt_basis_bid5, sprd_ask1, sprd_ask2, sprd_ask3, sprd_ask4, sprd_ask5, sprd_bid1, sprd_bid2, sprd_bid3, sprd_bid4, sprd_bid5) VALUES (?1, ?2");
    for i in 3..=52 { sql.push_str(&format!(", ?{}", i)); }
    sql.push_str(")");
    let mut params: Vec<&dyn ToSql> = Vec::new();
    params.push(&snap.ts_utc); params.push(&snap.category);
    for v in &snap.ask_p { params.push(v); } for v in &snap.ask_q { params.push(v); }
    for v in &snap.bid_p { params.push(v); } for v in &snap.bid_q { params.push(v); }
    for v in &snap.cur_basis_ask { params.push(v); } for v in &snap.cur_basis_bid { params.push(v); }
    for v in &snap.nxt_basis_ask { params.push(v); } for v in &snap.nxt_basis_bid { params.push(v); }
    for v in &snap.sprd_ask { params.push(v); } for v in &snap.sprd_bid { params.push(v); }
    conn.execute(&sql, rusqlite::params_from_iter(params)).expect("스냅샷 저장 실패");
}

fn init_db() -> Connection {
    let conn = Connection::open("TRADING_DATA.db").expect("DB 연결 실패");
    conn.execute(
        "CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY,
            time TEXT NOT NULL,
            category TEXT NOT NULL,
            bid5_price REAL,
            ask5_price REAL,
            ask_q5 REAL,
            bid_q5 REAL,
            cur_basis_ask REAL,
            cur_basis_bid REAL,
            nxt_basis_ask REAL,
            nxt_basis_bid REAL,
            sprd_ask REAL,
            sprd_bid REAL
        )",
        [],
    ).expect("trades 테이블 생성 실패");

    conn.execute(
        "CREATE TABLE IF NOT EXISTS snapshot_58min (
            id INTEGER PRIMARY KEY, ts_utc INTEGER NOT NULL, category TEXT NOT NULL,
            ask_p1 REAL, ask_p2 REAL, ask_p3 REAL, ask_p4 REAL, ask_p5 REAL,
            ask_q1 REAL, ask_q2 REAL, ask_q3 REAL, ask_q4 REAL, ask_q5 REAL,
            bid_p1 REAL, bid_p2 REAL, bid_p3 REAL, bid_p4 REAL, bid_p5 REAL,
            bid_q1 REAL, bid_q2 REAL, bid_q3 REAL, bid_q4 REAL, bid_q5 REAL,
            cur_basis_ask1 REAL, cur_basis_ask2 REAL, cur_basis_ask3 REAL, cur_basis_ask4 REAL, cur_basis_ask5 REAL,
            cur_basis_bid1 REAL, cur_basis_bid2 REAL, cur_basis_bid3 REAL, cur_basis_bid4 REAL, cur_basis_bid5 REAL,
            nxt_basis_ask1 REAL, nxt_basis_ask2 REAL, nxt_basis_ask3 REAL, nxt_basis_ask4 REAL, nxt_basis_ask5 REAL,
            nxt_basis_bid1 REAL, nxt_basis_bid2 REAL, nxt_basis_bid3 REAL, nxt_basis_bid4 REAL, nxt_basis_bid5 REAL,
            sprd_ask1 REAL, sprd_ask2 REAL, sprd_ask3 REAL, sprd_ask4 REAL, sprd_ask5 REAL,
            sprd_bid1 REAL, sprd_bid2 REAL, sprd_bid3 REAL, sprd_bid4 REAL, sprd_bid5 REAL
        )",
        [],
    ).expect("snapshot_58min 테이블 생성 실패");
    conn
}

// (추가) 선별 삭제 및 다운샘플링 함수
fn cleanup_database(conn: &Connection) -> rusqlite::Result<()> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    
    // 시간 기준 (초 단위)
    let one_hour = 3600;
    let one_day = 86400;
    let seven_days = 604800;

    println!("🧹 [데이터 정리 시작] 현재 시각 기준 선별 삭제를 수행합니다...");

    // 1. 7일 넘은 것 완전 삭제
    let del1 = conn.execute(
        "DELETE FROM trades WHERE CAST(time AS INTEGER) < ?1",
        params![(now - seven_days).to_string()],
    )?;

    // 2. 1일 ~ 7일 사이: 10분(600초) 단위로 1개만 남기고 삭제
    let del2 = conn.execute(
        "DELETE FROM trades 
         WHERE CAST(time AS INTEGER) < ?1 
         AND CAST(time AS INTEGER) >= ?2
         AND id NOT IN (
             SELECT MIN(id) FROM trades 
             WHERE CAST(time AS INTEGER) < ?1 AND CAST(time AS INTEGER) >= ?2
             GROUP BY (CAST(time AS INTEGER) / 600)
         )",
        params![(now - one_day).to_string(), (now - seven_days).to_string()],
    )?;

    // 3. 1시간 ~ 1일 사이: 1분(60초) 단위로 1개만 남기고 삭제
    let del3 = conn.execute(
        "DELETE FROM trades 
         WHERE CAST(time AS INTEGER) < ?1 
         AND CAST(time AS INTEGER) >= ?2
         AND id NOT IN (
             SELECT MIN(id) FROM trades 
             WHERE CAST(time AS INTEGER) < ?1 AND CAST(time AS INTEGER) >= ?2
             GROUP BY (CAST(time AS INTEGER) / 60)
         )",
        params![(now - one_hour).to_string(), (now - one_day).to_string()],
    )?;

    println!("✅ 정리 완료: (7일경과: {}건, 1일~7일샘플링: {}건, 1시간~1일샘플링: {}건)", del1, del2, del3);
    
    // 물리적 공간 반환
    conn.execute("VACUUM", [])?;
    Ok(())
}

fn get_active_symbols() -> (String, String) {
    let file = File::open("SYMBOLS.txt").expect("SYMBOLS.txt 파일 없음");
    let reader = BufReader::new(file);
    let mut valid_codes = Vec::new();
    for line in reader.lines() {
        let code = line.unwrap().trim().to_string();
        if code.starts_with("btcusd_") && !code.contains("perp") { valid_codes.push(code); }
    }
    (valid_codes[0].clone(), valid_codes[1].clone())
}

#[tokio::main]
async fn main() {
    let conn = init_db();
    let (current, next) = get_active_symbols();
    println!("🚀 [5호가 연산 & 매시 15분 선별삭제 모드] 가동");

    // 시작 시 1회 정리 수행
    let _ = cleanup_database(&conn);
    
    let url = format!("wss://dstream.binance.com/stream?streams=btcusd_perp@depth5/{}@depth5/{}@depth5", current, next);
    let (ws_stream, _) = connect_async(&url).await.expect("연결 실패");
    let (_, mut read) = ws_stream.split();

    let mut last_swap_ask5 = 0.0; let mut last_swap_bid5 = 0.0;
    let mut last_curr_ask5 = 0.0; let mut last_curr_bid5 = 0.0;
    let mut last_next_ask5 = 0.0; let mut last_next_bid5 = 0.0;
    
    let mut last_swap_depth: Option<DepthData> = None;
    let mut last_curr_depth: Option<DepthData> = None;
    let mut last_next_depth: Option<DepthData> = None;
    let mut last_snapshot_min = Utc::now().minute();
    let mut last_cleanup_hour = -1i32; // 삭제 실행 시간 기록용
    let mut is_first_run = true;

    while let Some(Ok(msg)) = read.next().await {
        if let Message::Text(text) = msg {
            if let Ok(depth) = serde_json::from_str::<DepthStream>(&text) {
                let (ap, aq, bp, bq) = get_depth_arrays(&Some(depth.data.clone()));
                let cur_ask5 = ap[4]; let cur_bid5 = bp[4];
                let cur_aq5 = aq[4];  let cur_bq5 = bq[4];

                if depth.stream.contains("perp") { 
                    last_swap_bid5 = cur_bid5; last_swap_ask5 = cur_ask5;
                    last_swap_depth = Some(depth.data.clone()); 
                } else if depth.stream.contains(&current) { 
                    last_curr_bid5 = cur_bid5; last_curr_ask5 = cur_ask5;
                    last_curr_depth = Some(depth.data.clone()); 
                } else if depth.stream.contains(&next) { 
                    last_next_bid5 = cur_bid5; last_next_ask5 = cur_ask5;
                    last_next_depth = Some(depth.data.clone()); 
                }

                let now_utc = Utc::now();
                
                // (1) 매시 15분 선별 삭제 트리거
                if now_utc.minute() == 15 && last_cleanup_hour != now_utc.hour() as i32 {
                    let _ = cleanup_database(&conn);
                    last_cleanup_hour = now_utc.hour() as i32;
                }

                // (2) 58분 정기 스냅샷 처리
                let has_all_depths = last_swap_depth.is_some() && last_curr_depth.is_some() && last_next_depth.is_some();
                let is_58min = now_utc.minute() == 58 && now_utc.second() == 0 && last_snapshot_min != 58;
                
                if has_all_depths && (is_first_run || is_58min) {
                    if is_58min { last_snapshot_min = 58; }
                    let metrics = calculate_metrics(&last_swap_depth, &last_curr_depth, &last_next_depth);
                    let ts_utc = now_utc.timestamp() as u64;
                    let categories = ["SWAP", &current, &next];
                    let depths = [&last_swap_depth, &last_curr_depth, &last_next_depth];
                    
                    for (i, cat) in categories.iter().enumerate() {
                        let (a_p, a_q, b_p, b_q) = get_depth_arrays(depths[i]);
                        insert_snapshot(&conn, &Snapshot58 {
                            ts_utc, category: cat.to_string(),
                            ask_p: a_p, ask_q: a_q, bid_p: b_p, bid_q: b_q,
                            cur_basis_ask: metrics.0, cur_basis_bid: metrics.1,
                            nxt_basis_ask: metrics.2, nxt_basis_bid: metrics.3,
                            sprd_ask: metrics.4, sprd_bid: metrics.5
                        });
                    }
                    is_first_run = false;
                } else if now_utc.minute() != 58 { last_snapshot_min = now_utc.minute(); }

                let calc = |far: f64, near: f64| -> f64 {
                    if far > 0.0 && near > 0.0 { (far - near) / ((far + near) / 2.0) * 100.0 } else { 0.0 }
                };

                let cur_basis_ask = calc(last_curr_ask5, last_swap_bid5);
                let cur_basis_bid = calc(last_curr_bid5, last_swap_ask5);
                let nxt_basis_ask = calc(last_next_ask5, last_swap_bid5);
                let nxt_basis_bid = calc(last_next_bid5, last_swap_ask5);
                let sprd_ask = calc(last_next_ask5, last_curr_bid5);
                let sprd_bid = calc(last_next_bid5, last_curr_ask5);

                let now_ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs().to_string();
                conn.execute(
                    "INSERT INTO trades (time, category, bid5_price, ask5_price, ask_q5, bid_q5, cur_basis_ask, cur_basis_bid, nxt_basis_ask, nxt_basis_bid, sprd_ask, sprd_bid) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                    params![now_ts, depth.stream, cur_bid5, cur_ask5, cur_aq5, cur_bq5, cur_basis_ask, cur_basis_bid, nxt_basis_ask, nxt_basis_bid, sprd_ask, sprd_bid],
                ).expect("실시간 저장 실패");

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

    let mut ask_accum = 0.0;
    for (i, ask) in depth.data.asks.iter().take(5).enumerate() {
        let price: f64 = ask[0].parse().unwrap_or(0.0);
        let qty: f64 = ask[1].parse().unwrap_or(0.0);
        ask_accum += qty;
        println!("  매도 {}호가: {} | 누적: {:>8.0}", i + 1, price, ask_accum);
    }
    println!("  -- (현재가) --");
    let mut bid_accum = 0.0;
    for (i, bid) in depth.data.bids.iter().take(5).enumerate() {
        let price: f64 = bid[0].parse().unwrap_or(0.0);
        let qty: f64 = bid[1].parse().unwrap_or(0.0);
        bid_accum += qty;
        println!("  매수 {}호가: {} | 누적: {:>8.0}", i + 1, price, bid_accum);
    }
}