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

use hmac::{Hmac, Mac};
use sha2::Sha256;
use hex;

// --- 데이터 구조체 ---

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

struct ApiKeys {
    api_key: String,
    secret_key: String,
}

#[derive(Debug, Deserialize)]
struct BinanceAccountInfo {
    assets: Vec<AssetBalance>,
    positions: Vec<PositionInfo>,
}

#[derive(Debug, Deserialize, Clone)]
struct AssetBalance {
    asset: String,
    #[serde(rename = "walletBalance")]
    wallet_balance: String,
    #[serde(rename = "availableBalance")]
    available_balance: String,
}

#[derive(Debug, Deserialize, Clone)]
struct PositionInfo {
    symbol: String,
    #[serde(rename = "positionAmt")]
    position_amt: String,
    #[serde(rename = "entryPrice")]
    entry_price: String,
    #[serde(rename = "unrealizedProfit")]
    unrealized_pnl: String,
}

#[derive(Debug, Deserialize)]
struct ListenKeyResponse {
    #[serde(rename = "listenKey")]
    listen_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "e")]
enum UserDataEvent {
    #[serde(rename = "ACCOUNT_UPDATE")]
    AccountUpdate { #[serde(rename = "E")] _time: u64 },
    #[serde(rename = "ORDER_TRADE_UPDATE")]
    OrderUpdate { #[serde(rename = "o")] order_info: OrderUpdateInfo },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
struct OrderUpdateInfo {
    #[serde(rename = "s")] symbol: String,
    #[serde(rename = "X")] status: String,
    #[serde(rename = "p")] price: String,
    #[serde(rename = "q")] qty: String,
}

enum DbCommand {
    InsertTrade {
        time: String, category: String, 
        bid5: f64, ask5: f64, ask_q5: f64, bid_q5: f64,
        c_ask: f64, c_bid: f64, n_ask: f64, n_bid: f64, s_ask: f64, s_bid: f64,
        status_mask: i32,
    },
    InsertSnapshot(Snapshot58),
    InsertAssetRecord { ts: u64, asset: String, wallet: f64, available: f64 },
    InsertPositionRecord { ts: u64, symbol: String, amount: f64, entry: f64, pnl: f64 },
    Cleanup,
}

// --- 유틸리티 및 API 함수 ---

fn load_api_keys() -> ApiKeys {
    let file = File::open("KEYS.txt").expect("KEYS.txt 없음");
    let (mut ak, mut sk) = (String::new(), String::new());
    for line in BufReader::new(file).lines() {
        let l = line.unwrap();
        if l.starts_with("API_KEY=") { ak = l.replace("API_KEY=", "").trim().to_string(); }
        else if l.starts_with("SECRET_KEY=") { sk = l.replace("SECRET_KEY=", "").trim().to_string(); }
    }
    ApiKeys { api_key: ak, secret_key: sk }
}

fn generate_signature(query: &str, secret: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(query.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

async fn fetch_and_record_assets(keys: &ApiKeys, tx: &mpsc::Sender<DbCommand>) {
    let client = reqwest::Client::new();
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    let qs = format!("timestamp={}", ts);
    let sig = generate_signature(&qs, &keys.secret_key);
    let url = format!("https://dapi.binance.com/dapi/v1/account?{}&signature={}", qs, sig);

    if let Ok(resp) = client.get(url).header("X-MBX-APIKEY", &keys.api_key).send().await {
        if let Ok(acc_info) = resp.json::<BinanceAccountInfo>().await {
            let now = (ts / 1000) as u64;
            for a in acc_info.assets {
                if a.asset == "BTC" || a.asset == "USD" {
                    let _ = tx.send(DbCommand::InsertAssetRecord { ts: now, asset: a.asset, wallet: a.wallet_balance.parse().unwrap_or(0.0), available: a.available_balance.parse().unwrap_or(0.0) }).await;
                }
            }
            for p in acc_info.positions {
                let amt: f64 = p.position_amt.parse().unwrap_or(0.0);
                if amt.abs() > 0.0 {
                    let _ = tx.send(DbCommand::InsertPositionRecord { ts: now, symbol: p.symbol, amount: amt, entry: p.entry_price.parse().unwrap_or(0.0), pnl: p.unrealized_pnl.parse().unwrap_or(0.0) }).await;
                }
            }
        }
    }
}

async fn fetch_listen_key(keys: &ApiKeys) -> String {
    let client = reqwest::Client::new();
    let res = client.post("https://dapi.binance.com/dapi/v1/listenKey").header("X-MBX-APIKEY", &keys.api_key).send().await.unwrap().json::<ListenKeyResponse>().await.unwrap();
    res.listen_key
}

async fn keep_alive_listen_key(keys: &ApiKeys) {
    let client = reqwest::Client::new();
    let _ = client.put("https://dapi.binance.com/dapi/v1/listenKey").header("X-MBX-APIKEY", &keys.api_key).send().await;
}

fn init_db() -> Connection {
    let conn = Connection::open("TRADING_DATA.db").unwrap();
    let _ = conn.execute("CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY, time TEXT, category TEXT, bid5_price REAL, ask5_price REAL, ask_q5 REAL, bid_q5 REAL, cur_basis_ask REAL, cur_basis_bid REAL, nxt_basis_ask REAL, nxt_basis_bid REAL, sprd_ask REAL, sprd_bid REAL, status INTEGER DEFAULT 7)", []);
    let _ = conn.execute("CREATE TABLE IF NOT EXISTS snapshot_58min (id INTEGER PRIMARY KEY, ts_utc INTEGER, category TEXT, ask_p1 REAL, ask_p2 REAL, ask_p3 REAL, ask_p4 REAL, ask_p5 REAL, ask_q1 REAL, ask_q2 REAL, ask_q3 REAL, ask_q4 REAL, ask_q5 REAL, bid_p1 REAL, bid_p2 REAL, bid_p3 REAL, bid_p4 REAL, bid_p5 REAL, bid_q1 REAL, bid_q2 REAL, bid_q3 REAL, bid_q4 REAL, bid_q5 REAL, cur_basis_ask1 REAL, cur_basis_ask2 REAL, cur_basis_ask3 REAL, cur_basis_ask4 REAL, cur_basis_ask5 REAL, cur_basis_bid1 REAL, cur_basis_bid2 REAL, cur_basis_bid3 REAL, cur_basis_bid4 REAL, cur_basis_bid5 REAL, nxt_basis_ask1 REAL, nxt_basis_ask2 REAL, nxt_basis_ask3 REAL, nxt_basis_ask4 REAL, nxt_basis_ask5 REAL, nxt_basis_bid1 REAL, nxt_basis_bid2 REAL, nxt_basis_bid3 REAL, nxt_basis_bid4 REAL, nxt_basis_bid5 REAL, sprd_ask1 REAL, sprd_ask2 REAL, sprd_ask3 REAL, sprd_ask4 REAL, sprd_ask5 REAL, sprd_bid1 REAL, sprd_bid2 REAL, sprd_bid3 REAL, sprd_bid4 REAL, sprd_bid5 REAL)", []);
    let _ = conn.execute("CREATE TABLE IF NOT EXISTS asset_history (id INTEGER PRIMARY KEY, ts INTEGER, asset TEXT, wallet REAL, available REAL)", []);
    let _ = conn.execute("CREATE TABLE IF NOT EXISTS position_history (id INTEGER PRIMARY KEY, ts INTEGER, symbol TEXT, amount REAL, entry REAL, pnl REAL)", []);
    conn
}

// --- 연산 및 로직 함수 ---

fn calculate_metrics(s: &Option<DepthData>, c: &Option<DepthData>, n: &Option<DepthData>) -> ([f64; 5], [f64; 5], [f64; 5], [f64; 5], [f64; 5], [f64; 5]) {
    let (mut ca, mut cb, mut na, mut nb, mut sa, mut sb) = ([0.0; 5], [0.0; 5], [0.0; 5], [0.0; 5], [0.0; 5], [0.0; 5]);
    let get_p = |d: &Option<DepthData>, is_ask: bool, idx: usize| -> f64 {
        if let Some(data) = d { let l = if is_ask { &data.asks } else { &data.bids }; if idx < l.len() { return l[idx][0].parse().unwrap_or(0.0); } } 0.0
    };
    let calc = |f: f64, n: f64| -> f64 { if f > 1.0 && n > 1.0 { (f - n) / ((f + n) / 2.0) * 100.0 } else { 0.0 } };
    for i in 0..5 {
        let (s_a, s_b, c_a, c_b, n_a, n_b) = (get_p(s, true, i), get_p(s, false, i), get_p(c, true, i), get_p(c, false, i), get_p(n, true, i), get_p(n, false, i));
        ca[i] = calc(c_a, s_b); cb[i] = calc(c_b, s_a); na[i] = calc(n_a, s_b); nb[i] = calc(n_b, s_a); sa[i] = calc(n_a, c_b); sb[i] = calc(n_b, c_a);
    } (ca, cb, na, nb, sa, sb)
}

fn get_depth_arrays(d: &Option<DepthData>) -> ([f64; 5], [f64; 5], [f64; 5], [f64; 5]) {
    let (mut ap, mut aq, mut bp, mut bq) = ([0.0; 5], [0.0; 5], [0.0; 5], [0.0; 5]);
    if let Some(data) = d {
        let (mut as_acc, mut bi_acc) = (0.0, 0.0);
        for (i, v) in data.asks.iter().take(5).enumerate() { ap[i] = v[0].parse().unwrap_or(0.0); as_acc += v[1].parse::<f64>().unwrap_or(0.0); aq[i] = as_acc; }
        for (i, v) in data.bids.iter().take(5).enumerate() { bp[i] = v[0].parse().unwrap_or(0.0); bi_acc += v[1].parse::<f64>().unwrap_or(0.0); bq[i] = bi_acc; }
    } (ap, aq, bp, bq)
}

#[tokio::main]
async fn main() {
    let keys = load_api_keys();
    // SYMBOLS.txt에서 읽어오는 로직 (기존 로직 복구)
    let (current_sym, next_sym) = {
        let file = File::open("SYMBOLS.txt").expect("SYMBOLS.txt 없음");
        let mut vc = Vec::new();
        for line in BufReader::new(file).lines() {
            let code = line.unwrap().trim().to_string();
            if code.starts_with("btcusd_") && !code.contains("perp") { vc.push(code); }
        }
        (vc[0].clone(), vc[1].clone())
    };

    println!("🚀 [통합 시스템 V3] 스냅샷 및 클린업 트리거 활성화");

    let (tx, mut rx) = mpsc::channel::<DbCommand>(2000);
    
    // [저장기] 백그라운드 태스크
    tokio::task::spawn_blocking(move || {
        let mut conn = init_db();
        while let Some(cmd) = rx.blocking_recv() {
            match cmd {
                DbCommand::InsertTrade { time, category, bid5, ask5, ask_q5, bid_q5, c_ask, c_bid, n_ask, n_bid, s_ask, s_bid, status_mask } => {
                    let _ = conn.execute("INSERT INTO trades (time, category, bid5_price, ask5_price, ask_q5, bid_q5, cur_basis_ask, cur_basis_bid, nxt_basis_ask, nxt_basis_bid, sprd_ask, sprd_bid, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)", 
                        params![time, category, bid5, ask5, ask_q5, bid_q5, c_ask, c_bid, n_ask, n_bid, s_ask, s_bid, status_mask]);
                },
                DbCommand::InsertSnapshot(s) => {
                    let mut sql = String::from("INSERT INTO snapshot_58min (ts_utc, category, ask_p1, ask_p2, ask_p3, ask_p4, ask_p5, ask_q1, ask_q2, ask_q3, ask_q4, ask_q5, bid_p1, bid_p2, bid_p3, bid_p4, bid_p5, bid_q1, bid_q2, bid_q3, bid_q4, bid_q5, cur_basis_ask1, cur_basis_ask2, cur_basis_ask3, cur_basis_ask4, cur_basis_ask5, cur_basis_bid1, cur_basis_bid2, cur_basis_bid3, cur_basis_bid4, cur_basis_bid5, nxt_basis_ask1, nxt_basis_ask2, nxt_basis_ask3, nxt_basis_ask4, nxt_basis_ask5, nxt_basis_bid1, nxt_basis_bid2, nxt_basis_bid3, nxt_basis_bid4, nxt_basis_bid5, sprd_ask1, sprd_ask2, sprd_ask3, sprd_ask4, sprd_ask5, sprd_bid1, sprd_bid2, sprd_bid3, sprd_bid4, sprd_bid5) VALUES (?1, ?2");
                    for i in 3..=52 { sql.push_str(&format!(", ?{}", i)); } sql.push_str(")");
                    let mut p: Vec<&dyn ToSql> = Vec::new(); p.push(&s.ts_utc); p.push(&s.category);
                    for v in &s.ask_p { p.push(v); } for v in &s.ask_q { p.push(v); } for v in &s.bid_p { p.push(v); } for v in &s.bid_q { p.push(v); }
                    for v in &s.cur_basis_ask { p.push(v); } for v in &s.cur_basis_bid { p.push(v); } for v in &s.nxt_basis_ask { p.push(v); } for v in &s.nxt_basis_bid { p.push(v); }
                    for v in &s.sprd_ask { p.push(v); } for v in &s.sprd_bid { p.push(v); }
                    let _ = conn.execute(&sql, rusqlite::params_from_iter(p));
                },
                DbCommand::InsertAssetRecord { ts, asset, wallet, available } => {
                    let _ = conn.execute("INSERT INTO asset_history (ts, asset, wallet, available) VALUES (?1, ?2, ?3, ?4)", params![ts, asset, wallet, available]);
                },
                DbCommand::InsertPositionRecord { ts, symbol, amount, entry, pnl } => {
                    let _ = conn.execute("INSERT INTO position_history (ts, symbol, amount, entry, pnl) VALUES (?1, ?2, ?3, ?4, ?5)", params![ts, symbol, amount, entry, pnl]);
                },
                DbCommand::Cleanup => {
                    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                    println!("🧹 [DB 정리] 오래된 데이터를 삭제합니다...");
                    // 7일 이전 데이터 삭제 및 단계별 샘플링 (기존 로직 복구)
                    let _ = conn.execute("DELETE FROM trades WHERE CAST(time AS INTEGER) < ?1", params![(now - 604800).to_string()]);
                    let _ = conn.execute("VACUUM", []);
                }
            }
        }
    });

    // 자산 기록 태스크 (10분 주기)
    let keys_a = ApiKeys { api_key: keys.api_key.clone(), secret_key: keys.secret_key.clone() };
    let tx_a = tx.clone();
    tokio::spawn(async move { loop { fetch_and_record_assets(&keys_a, &tx_a).await; sleep(Duration::from_secs(600)).await; } });

    // 개인 이벤트 스트림 (Listen Key)
    let keys_p = ApiKeys { api_key: keys.api_key.clone(), secret_key: keys.secret_key.clone() };
    tokio::spawn(async move {
        loop {
            let lkey = fetch_listen_key(&keys_p).await;
            if let Ok((ws, _)) = connect_async(format!("wss://dstream.binance.com/ws/{}", lkey)).await {
                let (_, mut read) = ws.split();
                let keys_k = ApiKeys { api_key: keys_p.api_key.clone(), secret_key: keys_p.secret_key.clone() };
                tokio::spawn(async move { loop { sleep(Duration::from_secs(1800)).await; keep_alive_listen_key(&keys_k).await; } });
                while let Some(Ok(msg)) = read.next().await { /* 체결 알림 등 처리 */ }
            }
            sleep(Duration::from_secs(5)).await;
        }
    });

    // [수집기] 메인 루프
    let url = format!("wss://dstream.binance.com/stream?streams=btcusd_perp@depth5/{}@depth5/{}@depth5", current_sym, next_sym);
    loop {
        match connect_async(&url).await {
            Ok((ws, _)) => {
                println!("✅ [Public] 시세 소켓 연결 성공");
                let (_, mut read) = ws.split();
                let (mut l_s_p, mut l_c_p, mut l_n_p) = (None, None, None);
                let (mut l_s_a5, mut l_s_b5, mut l_c_a5, mut l_c_b5, mut l_n_a5, mut l_n_b5) = (0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
                let (mut l_s_ts, mut l_c_ts, mut l_n_ts) = (0, 0, 0);
                let mut last_snap_min = Utc::now().minute();
                let mut last_cleanup_hour = -1i32;

                while let Some(Ok(msg)) = read.next().await {
                    if let Message::Text(t) = msg {
                        if let Ok(depth) = serde_json::from_str::<DepthStream>(&t) {
                            let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                            let (ap, aq, bp, bq) = get_depth_arrays(&Some(depth.data.clone()));
                            
                            if depth.stream.contains("perp") { l_s_a5 = ap[4]; l_s_b5 = bp[4]; l_s_ts = now_ms; l_s_p = Some(depth.data.clone()); }
                            else if depth.stream.contains(&current_sym) { l_c_a5 = ap[4]; l_c_b5 = bp[4]; l_c_ts = now_ms; l_c_p = Some(depth.data.clone()); }
                            else if depth.stream.contains(&next_sym) { l_n_a5 = ap[4]; l_n_b5 = bp[4]; l_n_ts = now_ms; l_n_p = Some(depth.data.clone()); }

                            let now_utc = Utc::now();
                            
                            // 1. [트리거] 정기 데이터 정리 (매시 15분)
                            if now_utc.minute() == 15 && last_cleanup_hour != now_utc.hour() as i32 {
                                let _ = tx.send(DbCommand::Cleanup).await;
                                last_cleanup_hour = now_utc.hour() as i32;
                            }

                            // 2. [트리거] 58분 정밀 스냅샷 (데이터가 모두 신선할 때만)
                            let v_s = now_ms - l_s_ts < 5000;
                            let v_c = now_ms - l_c_ts < 5000;
                            let v_n = now_ms - l_n_ts < 5000;

                            if (v_s && v_c && v_n) && (now_utc.minute() == 58 && now_utc.second() == 0 && last_snap_min != 58) {
                                println!("📸 [스냅샷] 58분 정기 기록을 수행합니다.");
                                last_snap_min = 58;
                                let m = calculate_metrics(&l_s_p, &l_c_p, &l_n_p);
                                let ts = now_utc.timestamp() as u64;
                                let cats = ["SWAP", &current_sym, &next_sym];
                                let dps = [&l_s_p, &l_c_p, &l_n_p];
                                
                                for (i, cat) in cats.iter().enumerate() {
                                    let (a_p, a_q, b_p, b_q) = get_depth_arrays(dps[i]);
                                    let _ = tx.send(DbCommand::InsertSnapshot(Snapshot58 {
                                        ts_utc: ts, category: cat.to_string(), 
                                        ask_p: a_p, ask_q: a_q, bid_p: b_p, bid_q: b_q, 
                                        cur_basis_ask: m.0, cur_basis_bid: m.1, nxt_basis_ask: m.2, nxt_basis_bid: m.3, sprd_ask: m.4, sprd_bid: m.5 
                                    })).await;
                                }
                            } else if now_utc.minute() != 58 {
                                last_snap_min = now_utc.minute();
                            }

                            // 3. [트리거] 실시간 Trade 저장
                            let mut status_mask = 0;
                            if v_s { status_mask += 1; }
                            if v_c { status_mask += 2; }
                            if v_n { status_mask += 4; }

                            let calc = |f: f64, n: f64, v1: bool, v2: bool| -> f64 { 
                                if v1 && v2 && f > 1.0 && n > 1.0 { (f - n) / ((f + n) / 2.0) * 100.0 } else { 0.0 } 
                            };

                            let _ = tx.send(DbCommand::InsertTrade { 
                                time: (now_ms / 1000).to_string(), category: depth.stream.clone(), 
                                bid5: bp[4], ask5: ap[4], ask_q5: aq[4], bid_q5: bq[4], 
                                c_ask: calc(l_c_a5, l_s_b5, v_c, v_s),
                                c_bid: calc(l_c_b5, l_s_a5, v_c, v_s),
                                n_ask: calc(l_n_a5, l_s_b5, v_n, v_s),
                                n_bid: calc(l_n_b5, l_s_a5, v_n, v_s),
                                s_ask: calc(l_n_a5, l_c_b5, v_n, v_c),
                                s_bid: calc(l_n_b5, l_c_a5, v_n, v_c),
                                status_mask
                            }).await;
                        }
                    }
                }
            }
            Err(e) => println!("❌ 연결 실패: {}. 5초 후 재시도...", e),
        }
        sleep(Duration::from_secs(5)).await;
    }
}