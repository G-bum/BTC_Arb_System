use futures_util::StreamExt;
use serde::Deserialize;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use rusqlite::{params, Connection, ToSql};
use chrono::{Timelike, Utc};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

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

// DB 기록 명령을 위한 열거형 데이터 구조
enum DbCommand {
    InsertTrade {
        time: String,
        category: String,
        bid5: f64, ask5: f64, ask_q5: f64, bid_q5: f64,
        c_ask: f64, c_bid: f64, n_ask: f64, n_bid: f64, s_ask: f64, s_bid: f64,
    },
    InsertSnapshot(Snapshot58),
    Cleanup,
}

// 58분 정기 스냅샷용 지표 연산 함수
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
        if far > 1.0 && near > 1.0 { (far - near) / ((far + near) / 2.0) * 100.0 } else { 0.0 }
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
            let qty: f64 = v[1].parse().unwrap_or(0.0);
            ask_accum += qty; ask_q[i] = ask_accum;
        }
        let mut bid_accum = 0.0;
        for (i, v) in d.bids.iter().take(5).enumerate() {
            bid_p[i] = v[0].parse().unwrap_or(0.0);
            let qty: f64 = v[1].parse().unwrap_or(0.0);
            bid_accum += qty; bid_q[i] = bid_accum;
        }
    }
    (ask_p, ask_q, bid_p, bid_q)
}

fn init_db() -> Connection {
    let conn = Connection::open("TRADING_DATA.db").expect("DB 연결 실패");
    conn.execute("CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY, time TEXT NOT NULL, category TEXT NOT NULL, bid5_price REAL, ask5_price REAL, ask_q5 REAL, bid_q5 REAL, cur_basis_ask REAL, cur_basis_bid REAL, nxt_basis_ask REAL, nxt_basis_bid REAL, sprd_ask REAL, sprd_bid REAL)", []).expect("trades 생성 실패");
    conn.execute("CREATE TABLE IF NOT EXISTS snapshot_58min (id INTEGER PRIMARY KEY, ts_utc INTEGER NOT NULL, category TEXT NOT NULL, ask_p1 REAL, ask_p2 REAL, ask_p3 REAL, ask_p4 REAL, ask_p5 REAL, ask_q1 REAL, ask_q2 REAL, ask_q3 REAL, ask_q4 REAL, ask_q5 REAL, bid_p1 REAL, bid_p2 REAL, bid_p3 REAL, bid_p4 REAL, bid_p5 REAL, bid_q1 REAL, bid_q2 REAL, bid_q3 REAL, bid_q4 REAL, bid_q5 REAL, cur_basis_ask1 REAL, cur_basis_ask2 REAL, cur_basis_ask3 REAL, cur_basis_ask4 REAL, cur_basis_ask5 REAL, cur_basis_bid1 REAL, cur_basis_bid2 REAL, cur_basis_bid3 REAL, cur_basis_bid4 REAL, cur_basis_bid5 REAL, nxt_basis_ask1 REAL, nxt_basis_ask2 REAL, nxt_basis_ask3 REAL, nxt_basis_ask4 REAL, nxt_basis_ask5 REAL, nxt_basis_bid1 REAL, nxt_basis_bid2 REAL, nxt_basis_bid3 REAL, nxt_basis_bid4 REAL, nxt_basis_bid5 REAL, sprd_ask1 REAL, sprd_ask2 REAL, sprd_ask3 REAL, sprd_ask4 REAL, sprd_ask5 REAL, sprd_bid1 REAL, sprd_bid2 REAL, sprd_bid3 REAL, sprd_bid4 REAL, sprd_bid5 REAL)", []).expect("스냅샷 생성 실패");
    conn
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
    let (current, next) = get_active_symbols();
    println!("🚀 [시스템 가동] 비동기 채널 + 자동 재접속 + 방어 로직 모드");

    // DB 채널 설정 (버퍼 2000개)
    let (tx, mut rx) = mpsc::channel::<DbCommand>(2000);

    // [저장 담당] 백그라운드 태스크
    tokio::task::spawn_blocking(move || {
        let mut conn = init_db();
        println!("💾 [저장기] DB 엔진 연결 및 가동 완료");

        while let Some(cmd) = rx.blocking_recv() {
            match cmd {
                DbCommand::InsertTrade { time, category, bid5, ask5, ask_q5, bid_q5, c_ask, c_bid, n_ask, n_bid, s_ask, s_bid } => {
                    let _ = conn.execute(
                        "INSERT INTO trades (time, category, bid5_price, ask5_price, ask_q5, bid_q5, cur_basis_ask, cur_basis_bid, nxt_basis_ask, nxt_basis_bid, sprd_ask, sprd_bid) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                        params![time, category, bid5, ask5, ask_q5, bid_q5, c_ask, c_bid, n_ask, n_bid, s_ask, s_bid],
                    );
                },
                DbCommand::InsertSnapshot(snap) => {
                    let mut sql = String::from("INSERT INTO snapshot_58min (ts_utc, category, ask_p1, ask_p2, ask_p3, ask_p4, ask_p5, ask_q1, ask_q2, ask_q3, ask_q4, ask_q5, bid_p1, bid_p2, bid_p3, bid_p4, bid_p5, bid_q1, bid_q2, bid_q3, bid_q4, bid_q5, cur_basis_ask1, cur_basis_ask2, cur_basis_ask3, cur_basis_ask4, cur_basis_ask5, cur_basis_bid1, cur_basis_bid2, cur_basis_bid3, cur_basis_bid4, cur_basis_bid5, nxt_basis_ask1, nxt_basis_ask2, nxt_basis_ask3, nxt_basis_ask4, nxt_basis_ask5, nxt_basis_bid1, nxt_basis_bid2, nxt_basis_bid3, nxt_basis_bid4, nxt_basis_bid5, sprd_ask1, sprd_ask2, sprd_ask3, sprd_ask4, sprd_ask5, sprd_bid1, sprd_bid2, sprd_bid3, sprd_bid4, sprd_bid5) VALUES (?1, ?2");
                    for i in 3..=52 { sql.push_str(&format!(", ?{}", i)); }
                    sql.push_str(")");
                    let mut p: Vec<&dyn ToSql> = Vec::new();
                    p.push(&snap.ts_utc); p.push(&snap.category);
                    for v in &snap.ask_p { p.push(v); } for v in &snap.ask_q { p.push(v); }
                    for v in &snap.bid_p { p.push(v); } for v in &snap.bid_q { p.push(v); }
                    for v in &snap.cur_basis_ask { p.push(v); } for v in &snap.cur_basis_bid { p.push(v); }
                    for v in &snap.nxt_basis_ask { p.push(v); } for v in &snap.nxt_basis_bid { p.push(v); }
                    for v in &snap.sprd_ask { p.push(v); } for v in &snap.sprd_bid { p.push(v); }
                    let _ = conn.execute(&sql, rusqlite::params_from_iter(p));
                },
                DbCommand::Cleanup => {
                    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                    println!("🧹 [정리] 시장별 독립 샘플링 수행 중...");
                    let _ = conn.execute("DELETE FROM trades WHERE CAST(time AS INTEGER) < ?1", params![(now - 604800).to_string()]);
                    let _ = conn.execute("DELETE FROM trades WHERE CAST(time AS INTEGER) < ?1 AND CAST(time AS INTEGER) >= ?2 AND id NOT IN (SELECT MIN(id) FROM trades WHERE CAST(time AS INTEGER) < ?1 AND CAST(time AS INTEGER) >= ?2 GROUP BY (CAST(time AS INTEGER) / 600), category)", params![(now - 86400).to_string(), (now - 604800).to_string()]);
                    let _ = conn.execute("DELETE FROM trades WHERE CAST(time AS INTEGER) < ?1 AND CAST(time AS INTEGER) >= ?2 AND id NOT IN (SELECT MIN(id) FROM trades WHERE CAST(time AS INTEGER) < ?1 AND CAST(time AS INTEGER) >= ?2 GROUP BY (CAST(time AS INTEGER) / 60), category)", params![(now - 3600).to_string(), (now - 86400).to_string()]);
                    let _ = conn.execute("VACUUM", []);
                }
            }
        }
    });

    // 시작 시 정리 명령 즉시 전송
    let _ = tx.send(DbCommand::Cleanup).await;

    let url = format!("wss://dstream.binance.com/stream?streams=btcusd_perp@depth5/{}@depth5/{}@depth5", current, next);

    // [수집 담당] 메인 재접속 루프
    loop {
        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                println!("✅ 바이낸스 웹소켓 연결 성공");
                let (_, mut read) = ws_stream.split();

                let (mut last_swap_ask5, mut last_swap_bid5) = (0.0, 0.0);
                let (mut last_curr_ask5, mut last_curr_bid5) = (0.0, 0.0);
                let (mut last_next_ask5, mut last_next_bid5) = (0.0, 0.0);
                let (mut last_swap_ts, mut last_curr_ts, mut last_next_ts) = (0u64, 0u64, 0u64);
                let (mut last_swap_depth, mut last_curr_depth, mut last_next_depth) = (None, None, None);
                let mut last_snapshot_min = Utc::now().minute();
                let mut last_cleanup_hour = -1i32;
                let mut is_first_run = true;

                while let Some(Ok(msg)) = read.next().await {
                    if let Message::Text(text) = msg {
                        if let Ok(depth) = serde_json::from_str::<DepthStream>(&text) {
                            let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                            let (ap, aq, bp, bq) = get_depth_arrays(&Some(depth.data.clone()));
                            
                            if depth.stream.contains("perp") { 
                                last_swap_bid5 = bp[4]; last_swap_ask5 = ap[4];
                                last_swap_ts = now_ms; last_swap_depth = Some(depth.data.clone()); 
                            } else if depth.stream.contains(&current) { 
                                last_curr_bid5 = bp[4]; last_curr_ask5 = ap[4];
                                last_curr_ts = now_ms; last_curr_depth = Some(depth.data.clone()); 
                            } else if depth.stream.contains(&next) { 
                                last_next_bid5 = bp[4]; last_next_ask5 = ap[4];
                                last_next_ts = now_ms; last_next_depth = Some(depth.data.clone()); 
                            }

                            let now_utc = Utc::now();
                            if now_utc.minute() == 15 && last_cleanup_hour != now_utc.hour() as i32 {
                                let _ = tx.send(DbCommand::Cleanup).await;
                                last_cleanup_hour = now_utc.hour() as i32;
                            }

                            let (v_s, v_c, v_n) = (now_ms - last_swap_ts < 5000, now_ms - last_curr_ts < 5000, now_ms - last_next_ts < 5000);
                            
                            // 58분 정기 스냅샷
                            if (v_s && v_c && v_n) && (is_first_run || (now_utc.minute() == 58 && now_utc.second() == 0 && last_snapshot_min != 58)) {
                                if now_utc.minute() == 58 { last_snapshot_min = 58; }
                                let m = calculate_metrics(&last_swap_depth, &last_curr_depth, &last_next_depth);
                                let ts = now_utc.timestamp() as u64;
                                let cats = ["SWAP", &current, &next];
                                let dps = [&last_swap_depth, &last_curr_depth, &last_next_depth];
                                for (i, cat) in cats.iter().enumerate() {
                                    let (a_p, a_q, b_p, b_q) = get_depth_arrays(dps[i]);
                                    let _ = tx.send(DbCommand::InsertSnapshot(Snapshot58 {
                                        ts_utc: ts, category: cat.to_string(), ask_p: a_p, ask_q: a_q, bid_p: b_p, bid_q: b_q,
                                        cur_basis_ask: m.0, cur_basis_bid: m.1, nxt_basis_ask: m.2, nxt_basis_bid: m.3, sprd_ask: m.4, sprd_bid: m.5
                                    })).await;
                                }
                                is_first_run = false;
                            } else if now_utc.minute() != 58 { last_snapshot_min = now_utc.minute(); }

                            let calc = |far: f64, near: f64, v1: bool, v2: bool| -> f64 {
                                if v1 && v2 && far > 1.0 && near > 1.0 { (far - near) / ((far + near) / 2.0) * 100.0 } else { 0.0 }
                            };

                            let c_ask = calc(last_curr_ask5, last_swap_bid5, v_c, v_s);
                            let c_bid = calc(last_curr_bid5, last_swap_ask5, v_c, v_s);
                            let n_ask = calc(last_next_ask5, last_swap_bid5, v_n, v_s);
                            let n_bid = calc(last_next_bid5, last_swap_ask5, v_n, v_s);
                            let s_ask = calc(last_next_ask5, last_curr_bid5, v_n, v_c);
                            let s_bid = calc(last_next_bid5, last_curr_ask5, v_n, v_c);

                            // 저장 채널 전송
                            let _ = tx.send(DbCommand::InsertTrade {
                                time: (now_ms / 1000).to_string(), category: depth.stream.clone(),
                                bid5: bp[4], ask5: ap[4], ask_q5: aq[4], bid_q5: bq[4],
                                c_ask, c_bid, n_ask, n_bid, s_ask, s_bid
                            }).await;

                            print_depth_info(depth, &current, &next, v_s, v_c, v_n);
                        }
                    }
                }
                println!("⚠️ 소켓 연결 중단됨. 5초 후 재시도를 시작합니다.");
            }
            Err(e) => {
                println!("❌ 연결 실패: {}. 5초 후 다시 시도합니다.", e);
            }
        }
        sleep(Duration::from_secs(5)).await;
    }
}

fn print_depth_info(depth: DepthStream, current: &str, next: &str, v_s: bool, v_c: bool, v_n: bool) {
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let t = now_ms / 1000;
    let (role, valid) = if depth.stream.contains("perp") { ("[스왑]", v_s) }
               else if depth.stream.contains(current) { ("[당분기]", v_c) }
               else if depth.stream.contains(next) { ("[차분기]", v_n) } else { ("[기타]", true) };

    println!("\n[UTC {:02}:{:02}:{:02}] {} - {}", (t/3600)%24, (t/60)%60, t%60, role, if valid { "OK" } else { "TIMEOUT" });
    println!("--------------------------------------------------");
    let mut ask_accum = 0.0;
    for (i, ask) in depth.data.asks.iter().take(5).enumerate() {
        let p: f64 = ask[0].parse().unwrap_or(0.0);
        let q: f64 = ask[1].parse().unwrap_or(0.0);
        ask_accum += q;
        println!("  매도 {}호가: {} | 누적: {:>8.0}", i + 1, p, ask_accum);
    }
    println!("  -- (현재가) --");
    let mut bid_accum = 0.0;
    for (i, bid) in depth.data.bids.iter().take(5).enumerate() {
        let p: f64 = bid[0].parse().unwrap_or(0.0);
        let q: f64 = bid[1].parse().unwrap_or(0.0);
        bid_accum += q;
        println!("  매수 {}호가: {} | 누적: {:>8.0}", i + 1, p, bid_accum);
    }
}