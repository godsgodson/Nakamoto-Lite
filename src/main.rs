use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::State as AxumState,
    response::Html, response::Json as AxumJson, response::IntoResponse,
    routing::{get, post}, Router,
};
use rusqlite::{params, Connection};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{collections::{HashMap, HashSet, VecDeque}, path::Path, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{broadcast, Mutex},
    time::{sleep, timeout, Duration},
};
use tracing::{info, warn};
use ldk_node::{Node, Builder, bitcoin::Network};
use std::sync::Mutex as StdMutex;
use lazy_static::lazy_static;


// ═══════════════════════════════════════════════════════════
// gRPC PROTO MODULE
// ═══════════════════════════════════════════════════════════
pub mod proto {
    tonic::include_proto!("nakamoto");
}
use proto::nakamoto_thermo_server::{NakamotoThermo, NakamotoThermoServer};

// ═══════════════════════════════════════════════════════════
// CONSTANTS
// ═══════════════════════════════════════════════════════════
const MAGIC: [u8; 4] = [0xf9, 0xbe, 0xb4, 0xd9];
const PROTO_VER: u32 = 70016;
const BLOCK_CACHE: usize = 100;
const DB_PATH: &str = "nakamoto.db";
const SEEDS: &[&str] = &[
    "seed.bitcoin.sipa.be", "dnsseed.bluematt.me",
    "dnsseed.bitcoin.dashjr.org", "seed.bitcoinstats.com",
    "seed.bitnodes.io", "dnsseed.bitcoin.petertodd.org",
];
const DEFAULT_ASIC_EFFICIENCY_J_PER_TH: f64 = 17.5;

// ═══════════════════════════════════════════════════════════
// LDK INVOICE TRACKING (Sovereign Tollbooth)
// ═══════════════════════════════════════════════════════════
lazy_static! {
    // Maps PaymentHash (hex) -> BOLT11 Invoice String
    static ref ACTIVE_INVOICES: StdMutex<HashMap<String, String>> = StdMutex::new(HashMap::new());
}

// ═══════════════════════════════════════════════════════════
// CORE STRUCTS
// ═══════════════════════════════════════════════════════════
#[derive(Clone, Serialize, Debug)]
pub struct NodeState {
    pub joules_per_sat: f64, pub sat_per_kwh: f64, pub network_power_gw: f64,
    pub ldk_channels: u64,
    pub ldk_usable_channels: u64,
    pub ldk_onchain_balance: u64,
    pub running: bool, pub height: u64, pub peers: u16, pub uptime_secs: u64,
    pub blocks_relayed: u64, pub blocks_served: u64, pub blocks_stored: u64,
    pub block_cache_mb: f64, pub txs_relayed: u64, pub p2p_connected: bool,
    pub headers_synced: u64, pub headers_valid: u64, pub headers_invalid: u64,
    pub mmr_root: String, pub cuckoo_items: u64, pub cuckoo_load: f64,
    pub best_hash: String, pub chain_difficulty: f64,
    pub invs_relayed: u64, pub sync_phase: String,
    pub db_size_mb: f64, pub resumed: bool,
}

#[derive(Clone, Debug)]
pub struct BlockHeader {
    pub version: i32, pub prev_hash: [u8; 32], pub merkle_root: [u8; 32],
    pub timestamp: u32, pub bits: u32, pub nonce: u32,
}

impl BlockHeader {
    pub fn from_bytes(d: &[u8]) -> Option<Self> {
        if d.len() < 80 { return None; }
        let mut ph = [0u8; 32]; ph.copy_from_slice(&d[4..36]);
        let mut mr = [0u8; 32]; mr.copy_from_slice(&d[36..68]);
        Some(BlockHeader {
            version: i32::from_le_bytes([d[0],d[1],d[2],d[3]]),
            prev_hash: ph, merkle_root: mr,
            timestamp: u32::from_le_bytes([d[68],d[69],d[70],d[71]]),
            bits: u32::from_le_bytes([d[72],d[73],d[74],d[75]]),
            nonce: u32::from_le_bytes([d[76],d[77],d[78],d[79]]),
        })
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(80);
        b.extend_from_slice(&self.version.to_le_bytes());
        b.extend_from_slice(&self.prev_hash);
        b.extend_from_slice(&self.merkle_root);
        b.extend_from_slice(&self.timestamp.to_le_bytes());
        b.extend_from_slice(&self.bits.to_le_bytes());
        b.extend_from_slice(&self.nonce.to_le_bytes());
        b
    }
    pub fn hash(&self) -> [u8; 32] { double_sha256(&self.to_bytes()) }
    pub fn verify_pow(&self) -> bool {
        let h = self.hash(); let t = bits_to_target(self.bits);
        let mut hbe = h; hbe.reverse();
        for i in 0..32 { if hbe[i] < t[i] { return true; } if hbe[i] > t[i] { return false; } }
        true
    }
}

// ═══════════════════════════════════════════════════════════
// HASHING & UTILS
// ═══════════════════════════════════════════════════════════
pub fn double_sha256(data: &[u8]) -> [u8; 32] {
    let h1 = Sha256::digest(data); let h2 = Sha256::digest(&h1);
    let mut out = [0u8; 32]; out.copy_from_slice(&h2); out
}
pub fn bits_to_target(bits: u32) -> [u8; 32] {
    let exp = (bits >> 24) as usize; let man = bits & 0x007fffff;
    let mut t = [0u8; 32];
    if exp <= 3 {
        let v = (man >> (8*(3-exp))) as u32; let b = v.to_be_bytes();
        t[32-b.len()..].copy_from_slice(&b);
    } else {
        let s = 32usize.saturating_sub(exp);
        if s+3 <= 32 { t[s]=((man>>16)&0xFF) as u8; t[s+1]=((man>>8)&0xFF) as u8; t[s+2]=(man&0xFF) as u8; }
    }
    t
}
fn hash_hex(h: &[u8; 32]) -> String {
    let mut r = h.to_vec(); r.reverse();
    let nz = r.iter().position(|&b| b != 0).unwrap_or(0);
    let start = nz.saturating_sub(2);
    r[start..].iter().take(16).map(|b| format!("{:02x}", b)).collect()
}
fn full_hash_hex(h: &[u8; 32]) -> String {
    let mut r = h.to_vec(); r.reverse();
    r.iter().map(|b| format!("{:02x}", b)).collect()
}
fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
fn target_to_f64(t: &[u8; 32]) -> f64 { let mut r = 0.0_f64; for &b in t { r = r*256.0 + b as f64; } r }
fn difficulty_from_bits(bits: u32) -> f64 {
    let tf = target_to_f64(&bits_to_target(bits));
    if tf == 0.0 { return 0.0; }
    0xFFFF as f64 * 2.0_f64.powi(208) / tf
}
fn hex_to_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len()).step_by(2)
        .filter_map(|i| hex.get(i..i+2).and_then(|s| u8::from_str_radix(s, 16).ok()))
        .collect()
}

// ═══════════════════════════════════════════════════════════
// PROTOCOL
// ═══════════════════════════════════════════════════════════
fn read_varint(d: &[u8], off: &mut usize) -> Option<u64> {
    if *off >= d.len() { return None; }
    let f = d[*off]; *off += 1;
    match f {
        0..=0xFC => Some(f as u64),
        0xFD => { if *off+2>d.len(){return None;} let v=u16::from_le_bytes([d[*off],d[*off+1]]) as u64; *off+=2; Some(v) }
        0xFE => { if *off+4>d.len(){return None;} let v=u32::from_le_bytes([d[*off],d[*off+1],d[*off+2],d[*off+3]]) as u64; *off+=4; Some(v) }
        0xFF => { if *off+8>d.len(){return None;} let v=u64::from_le_bytes([d[*off],d[*off+1],d[*off+2],d[*off+3],d[*off+4],d[*off+5],d[*off+6],d[*off+7]]); *off+=8; Some(v) }
    }
}
fn write_varint(v: u64) -> Vec<u8> {
    if v<=0xFC { vec![v as u8] }
    else if v<=0xFFFF { let mut b=vec![0xFD]; b.extend_from_slice(&(v as u16).to_le_bytes()); b }
    else if v<=0xFFFFFFFF { let mut b=vec![0xFE]; b.extend_from_slice(&(v as u32).to_le_bytes()); b }
    else { let mut b=vec![0xFF]; b.extend_from_slice(&v.to_le_bytes()); b }
}
fn wrap_msg(cmd: &str, payload: &[u8]) -> Vec<u8> {
    let mut m = Vec::with_capacity(24+payload.len());
    m.extend_from_slice(&MAGIC);
    let mut cb = [0u8; 12]; cb[..cmd.len()].copy_from_slice(cmd.as_bytes());
    m.extend_from_slice(&cb);
    m.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    m.extend_from_slice(&double_sha256(payload)[..4]);
    m.extend_from_slice(payload);
    m
}
fn build_version(nonce: u64) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&PROTO_VER.to_le_bytes());
    p.extend_from_slice(&(1u64).to_le_bytes());
    p.extend_from_slice(&chrono::Utc::now().timestamp().to_le_bytes());
    p.extend_from_slice(&0u64.to_le_bytes());
    p.extend_from_slice(&[0,0,0,0,0,0,0,0,0,0,0xff,0xff,0,0,0,0]);
    p.extend_from_slice(&0u16.to_be_bytes());
    p.extend_from_slice(&0u64.to_le_bytes());
    p.extend_from_slice(&[0,0,0,0,0,0,0,0,0,0,0xff,0xff,0,0,0,0]);
    p.extend_from_slice(&0u16.to_be_bytes());
    p.extend_from_slice(&nonce.to_le_bytes());
    let ua = b"/NakamotoLite:0.3.0/";
    p.push(ua.len() as u8); p.extend_from_slice(ua);
    p.extend_from_slice(&0i32.to_le_bytes()); p.push(1);
    wrap_msg("version", &p)
}
fn build_verack() -> Vec<u8> { wrap_msg("verack", &[]) }
fn build_getheaders(locator: &[[u8; 32]]) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&PROTO_VER.to_le_bytes());
    p.extend_from_slice(&write_varint(locator.len() as u64));
    for h in locator { p.extend_from_slice(h); }
    p.extend_from_slice(&[0u8; 32]);
    wrap_msg("getheaders", &p)
}
fn build_getdata(items: &[(u32, [u8; 32])]) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&write_varint(items.len() as u64));
    for (t, h) in items { p.extend_from_slice(&t.to_le_bytes()); p.extend_from_slice(h); }
    wrap_msg("getdata", &p)
}
fn build_sendheaders() -> Vec<u8> { wrap_msg("sendheaders", &[]) }
fn build_sendcmpct() -> Vec<u8> {
    let mut p = vec![0u8; 9];
    p[0] = 0; p[1..5].copy_from_slice(&1u32.to_le_bytes());
    wrap_msg("sendcmpct", &p)
}
fn parse_msg_cmd(data: &[u8]) -> Option<(String, Vec<u8>)> {
    if data.len() < 24 { return None; }
    let ce = data[4..16].iter().position(|&b| b==0).unwrap_or(12);
    let cmd = std::str::from_utf8(&data[4..4+ce]).ok()?.to_string();
    let len = u32::from_le_bytes([data[16],data[17],data[18],data[19]]) as usize;
    if data.len() < 24+len { return None; }
    Some((cmd, data[24..24+len].to_vec()))
}
fn parse_headers(payload: &[u8]) -> Vec<BlockHeader> {
    let mut off = 0;
    let count = match read_varint(payload, &mut off) { Some(c)=>c as usize, None=>return vec![] };
    let mut hs = Vec::with_capacity(count.min(2000));
    for _ in 0..count {
        if off+80 > payload.len() { break; }
        if let Some(h) = BlockHeader::from_bytes(&payload[off..off+80]) { hs.push(h); }
        off += 80;
        let _ = read_varint(payload, &mut off);
    }
    hs
}
fn parse_inv(payload: &[u8]) -> Vec<(u32, Vec<u8>)> {
    let mut off = 0;
    let count = match read_varint(payload, &mut off) { Some(c)=>c as usize, None=>return vec![] };
    let mut invs = Vec::with_capacity(count.min(50000));
    for _ in 0..count {
        if off+36 > payload.len() { break; }
        let it = u32::from_le_bytes([payload[off],payload[off+1],payload[off+2],payload[off+3]]);
        off += 4; let h = payload[off..off+32].to_vec(); off += 32;
        invs.push((it, h));
    }
    invs
}
fn invs_contain_new(payload: &[u8], my_invs: &HashSet<[u8; 32]>) -> bool {
    let invs = parse_inv(payload);
    invs.iter().any(|(_, h)| { let mut hk = [0u8; 32]; hk.copy_from_slice(h); !my_invs.contains(&hk) })
}

// ═══════════════════════════════════════════════════════════
// MMR & ENERGY INDEX
// ═══════════════════════════════════════════════════════════
pub fn compute_mmr_root(chain: &[BlockHeader]) -> String {
    if chain.is_empty() { return String::new(); }
    let len = chain.len() as u64;
    let mut peaks: Vec<[u8; 32]> = Vec::new();
    let mut pos = 1u64;
    while pos <= len {
        if len & pos != 0 { let idx = (len-pos) as usize; if idx < chain.len() { peaks.push(chain[idx].hash()); } }
        pos <<= 1;
    }
    if peaks.is_empty() { return String::new(); }
    let mut root = peaks[0];
    for i in 1..peaks.len() { let mut d = Vec::with_capacity(64); d.extend_from_slice(&root); d.extend_from_slice(&peaks[i]); root = double_sha256(&d); }
    hash_hex(&root)
}

#[derive(Clone, Serialize, Debug)]
pub struct EnergyIndex {
    pub joules_per_sat: f64, pub sat_per_kwh: f64, pub block_height: u64,
    pub energy_per_block_joules: f64, pub used_asic_efficiency_j_per_th: f64,
    pub difficulty: f64, pub hashes_per_block: f64, pub block_reward_sat: u64,
    pub kw_per_block: f64, pub network_power_gw: f64,
}

fn compute_energy_index(headers: &[BlockHeader]) -> EnergyIndex {
    let height = headers.len() as u64;
    if height == 0 {
        return EnergyIndex { joules_per_sat: 0.0, sat_per_kwh: 0.0, block_height: 0, energy_per_block_joules: 0.0, used_asic_efficiency_j_per_th: DEFAULT_ASIC_EFFICIENCY_J_PER_TH, difficulty: 0.0, hashes_per_block: 0.0, block_reward_sat: 0, kw_per_block: 0.0, network_power_gw: 0.0 };
    }
    let best = headers.last().unwrap();
    let difficulty = difficulty_from_bits(best.bits);
    let j_per_hash = DEFAULT_ASIC_EFFICIENCY_J_PER_TH / 1e12;
    let hashes_per_block = difficulty * 2f64.powi(32);
    let energy_per_block_j = hashes_per_block * j_per_hash;
    let halvings = height / 210_000;
    let mut reward_btc = 50.0_f64;
    for _ in 0..halvings { reward_btc /= 2.0; }
    let reward_sat = (reward_btc * 100_000_000.0) as u64;
    let joules_per_sat = if reward_sat > 0 { energy_per_block_j / reward_sat as f64 } else { 0.0 };
    let sat_per_kwh = if joules_per_sat > 0.0 { 3_600_000.0 / joules_per_sat } else { 0.0 };
    let kw_per_block = energy_per_block_j / 600.0 / 1000.0;
    let network_power_gw = kw_per_block / 1e6;
    EnergyIndex { joules_per_sat, sat_per_kwh, block_height: height, energy_per_block_joules: energy_per_block_j, used_asic_efficiency_j_per_th: DEFAULT_ASIC_EFFICIENCY_J_PER_TH, difficulty, hashes_per_block, block_reward_sat: reward_sat, kw_per_block, network_power_gw }
}

// ═══════════════════════════════════════════════════════════
// MERKLE PROOFS (Condensed for brevity, unchanged from previous)
// ═══════════════════════════════════════════════════════════
fn parse_block_txids(raw: &[u8]) -> Option<Vec<[u8; 32]>> { /* ... same as before ... */ 
    if raw.len() < 81 { return None; }
    let mut pos = 80; let tx_count = read_varint(raw, &mut pos)? as usize;
    let mut txids = Vec::with_capacity(tx_count.min(50000));
    for _ in 0..tx_count {
        let mut non_witness = Vec::new();
        if pos + 4 > raw.len() { return None; } non_witness.extend_from_slice(&raw[pos..pos+4]); pos += 4;
        let is_segwit = pos + 2 <= raw.len() && raw[pos] == 0x00 && raw[pos+1] == 0x01;
        if is_segwit { pos += 2; }
        let input_count = read_varint(raw, &mut pos)? as usize; non_witness.extend_from_slice(&write_varint(input_count as u64));
        for _ in 0..input_count {
            if pos + 36 > raw.len() { return None; } non_witness.extend_from_slice(&raw[pos..pos+36]); pos += 36;
            let script_len = read_varint(raw, &mut pos)? as usize; non_witness.extend_from_slice(&write_varint(script_len as u64));
            if pos + script_len > raw.len() { return None; } non_witness.extend_from_slice(&raw[pos..pos+script_len]); pos += script_len;
            if pos + 4 > raw.len() { return None; } non_witness.extend_from_slice(&raw[pos..pos+4]); pos += 4;
        }
        let output_count = read_varint(raw, &mut pos)? as usize; non_witness.extend_from_slice(&write_varint(output_count as u64));
        for _ in 0..output_count {
            if pos + 8 > raw.len() { return None; } non_witness.extend_from_slice(&raw[pos..pos+8]); pos += 8;
            let script_len = read_varint(raw, &mut pos)? as usize; non_witness.extend_from_slice(&write_varint(script_len as u64));
            if pos + script_len > raw.len() { return None; } non_witness.extend_from_slice(&raw[pos..pos+script_len]); pos += script_len;
        }
        if is_segwit {
            for _ in 0..input_count { let witness_count = read_varint(raw, &mut pos)? as usize; for _ in 0..witness_count { let item_len = read_varint(raw, &mut pos)? as usize; pos += item_len; } }
        }
        if pos + 4 > raw.len() { return None; } non_witness.extend_from_slice(&raw[pos..pos+4]); pos += 4;
        txids.push(double_sha256(&non_witness));
    }
    Some(txids)
}
fn compute_merkle_proof(txids: &[[u8; 32]], target_idx: usize) -> Option<(Vec<[u8; 32]>, [u8; 32])> { /* ... same ... */
    if txids.is_empty() || target_idx >= txids.len() { return None; }
    if txids.len() == 1 { return Some((vec![], txids[0])); }
    let mut level = txids.to_vec(); let mut idx = target_idx; let mut proof = Vec::new();
    while level.len() > 1 {
        if level.len() % 2 != 0 { level.push(level[level.len() - 1]); }
        let sib = if idx % 2 == 0 { idx + 1 } else { idx - 1 };
        if sib >= level.len() { return None; } proof.push(level[sib]);
        let mut next = Vec::new(); let mut i = 0;
        while i < level.len() { let mut concat = Vec::with_capacity(64); concat.extend_from_slice(&level[i]); concat.extend_from_slice(&level[i+1]); next.push(double_sha256(&concat)); i += 2; }
        level = next; idx /= 2;
    }
    Some((proof, level[0]))
}
#[derive(Serialize)]
struct TxProof { confirmed: bool, block_height: Option<u64>, block_hash: Option<String>, merkle_root: Option<String>, tx_index: Option<u32>, proof_hashes: Option<Vec<String>>, block_version: Option<i32>, prev_block_hash: Option<String>, block_timestamp: Option<u32>, block_bits: Option<u32>, block_nonce: Option<u32> }

async fn find_tx_proof(st: &Arc<AppState>, txid_hex: &str) -> TxProof { /* ... same ... */
    let txid_bytes = hex_to_bytes(txid_hex);
    if txid_bytes.len() != 32 { return TxProof { confirmed: false, block_height: None, block_hash: None, merkle_root: None, tx_index: None, proof_hashes: None, block_version: None, prev_block_hash: None, block_timestamp: None, block_bits: None, block_nonce: None }; }
    let mut txid_internal = [0u8; 32]; txid_internal.copy_from_slice(&txid_bytes); txid_internal.reverse();
    let blocks = st.blocks.lock().await; let headers = st.headers.lock().await;
    for (block_hash, raw_block) in blocks.iter() {
        if let Some(txids) = parse_block_txids(raw_block) {
            if let Some(tx_idx) = txids.iter().position(|t| *t == txid_internal) {
                if let Some((proof_hashes, _)) = compute_merkle_proof(&txids, tx_idx) {
                    if let Some(hdr) = BlockHeader::from_bytes(&raw_block[..80]) {
                        let height = headers.iter().rev().position(|h| h.hash() == *block_hash).map(|p| (headers.len() - 1 - p) as u64);
                        return TxProof { confirmed: true, block_height: height, block_hash: Some(full_hash_hex(block_hash)), merkle_root: Some(full_hash_hex(&hdr.merkle_root)), tx_index: Some(tx_idx as u32), proof_hashes: Some(proof_hashes.iter().map(|h| full_hash_hex(h)).collect()), block_version: Some(hdr.version), prev_block_hash: Some(full_hash_hex(&hdr.prev_hash)), block_timestamp: Some(hdr.timestamp), block_bits: Some(hdr.bits), block_nonce: Some(hdr.nonce) };
                    }
                }
            }
        }
    }
    let url = format!("https://mempool.space/api/tx/{}/status", txid_hex);
    if let Ok(resp) = reqwest::get(&url).await { if let Ok(data) = resp.json::<serde_json::Value>().await { if data["confirmed"].as_bool().unwrap_or(false) { return TxProof { confirmed: true, block_height: data["block_height"].as_u64(), block_hash: data["block_hash"].as_str().map(String::from), merkle_root: None, tx_index: None, proof_hashes: None, block_version: None, prev_block_hash: None, block_timestamp: None, block_bits: None, block_nonce: None }; } } }
    TxProof { confirmed: false, block_height: None, block_hash: None, merkle_root: None, tx_index: None, proof_hashes: None, block_version: None, prev_block_hash: None, block_timestamp: None, block_bits: None, block_nonce: None }
}

// ═══════════════════════════════════════════════════════════
// DATABASE
// ═══════════════════════════════════════════════════════════
struct Db { conn: Mutex<Connection> }
impl Db {
    fn open() -> Result<Self, rusqlite::Error> { let path = Path::new(DB_PATH); let create = !path.exists(); let conn = Connection::open(path)?; conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-65536;")?; if create { conn.execute_batch("CREATE TABLE headers (height INTEGER PRIMARY KEY, hash BLOB NOT NULL, raw BLOB NOT NULL); CREATE TABLE blocks (hash BLOB PRIMARY KEY, height INTEGER NOT NULL, raw BLOB NOT NULL); CREATE INDEX idx_blocks_height ON blocks(height);")?; info!("DB: Created new database"); } else { info!("DB: Opened existing database"); } Ok(Db { conn: Mutex::new(conn) }) }
    async fn load_headers(&self) -> Result<Vec<BlockHeader>, rusqlite::Error> { let conn = self.conn.lock().await; let mut stmt = conn.prepare("SELECT raw FROM headers ORDER BY height")?; let mut rows = stmt.query([])?; let mut headers = Vec::new(); while let Some(row) = rows.next()? { let raw: Vec<u8> = row.get(0)?; if let Some(h) = BlockHeader::from_bytes(&raw) { headers.push(h); } } Ok(headers) }
    async fn save_headers_batch(&self, batch: &[(u64, [u8; 32], Vec<u8>)]) -> Result<(), rusqlite::Error> { let conn = self.conn.lock().await; let tx = conn.unchecked_transaction()?; for (height, hash, raw) in batch { tx.execute("INSERT OR IGNORE INTO headers (height, hash, raw) VALUES (?1, ?2, ?3)", params![*height as i64, hash.to_vec(), raw])?; } tx.commit()?; Ok(()) }
    async fn save_block(&self, hash: &[u8], height: u64, raw: &[u8]) -> Result<(), rusqlite::Error> { let conn = self.conn.lock().await; conn.execute("INSERT OR IGNORE INTO blocks (hash, height, raw) VALUES (?1, ?2, ?3)", params![hash.to_vec(), height as i64, raw])?; Ok(()) }
    async fn load_last_blocks(&self, count: usize) -> Result<HashMap<[u8; 32], Vec<u8>>, rusqlite::Error> { let conn = self.conn.lock().await; let mut stmt = conn.prepare("SELECT hash, raw FROM blocks ORDER BY height DESC LIMIT ?1")?; let mut rows = stmt.query(params![count as i64])?; let mut blocks = HashMap::new(); while let Some(row) = rows.next()? { let hash_vec: Vec<u8> = row.get(0)?; let raw: Vec<u8> = row.get(1)?; if hash_vec.len() == 32 { let mut h = [0u8; 32]; h.copy_from_slice(&hash_vec); blocks.insert(h, raw); } } Ok(blocks) }
    fn db_size_mb(&self) -> f64 { std::fs::metadata(DB_PATH).map(|m| m.len() as f64 / 1_048_576.0).unwrap_or(0.0) }
}

// ═══════════════════════════════════════════════════════════
// APP STATE
// ═══════════════════════════════════════════════════════════
pub struct AppState {
    node: Arc<Mutex<NodeState>>,
    headers: Arc<Mutex<Vec<BlockHeader>>>,
    blocks: Arc<Mutex<HashMap<[u8; 32], Vec<u8>>>>,
    seen_invs: Arc<Mutex<HashSet<[u8; 32]>>>,
    inv_tx: broadcast::Sender<Vec<u8>>,
    header_relay_tx: broadcast::Sender<Vec<u8>>,
    block_relay_tx: broadcast::Sender<(Vec<u8>, [u8; 32])>,
    block_dl_queue: Arc<Mutex<VecDeque<[u8; 32]>>>,
    ws_tx: broadcast::Sender<String>,
    start_time: Arc<Mutex<Option<std::time::Instant>>>,
    db: Arc<Db>,
    ldk: Option<Arc<Node>>, // SOVEREIGN LDK NODE
}

// ═══════════════════════════════════════════════════════════
// HTTP HANDLERS & L402 TOLLBOOTH
// ═══════════════════════════════════════════════════════════
async fn dashboard() -> Html<&'static str> { Html(include_str!("../nakamoto-lite.html")) }
async fn api_state(AxumState(st): AxumState<Arc<AppState>>) -> axum::Json<NodeState> { axum::Json(st.node.lock().await.clone()) }
async fn api_toggle(AxumState(st): AxumState<Arc<AppState>>) -> String { let mut ns = st.node.lock().await; ns.running = !ns.running; format!("running={}", ns.running) }
async fn api_start(AxumState(st): AxumState<Arc<AppState>>) -> String { st.node.lock().await.running = true; "running=true".to_string() }
async fn api_stop(AxumState(st): AxumState<Arc<AppState>>) -> String { st.node.lock().await.running = false; "running=false".to_string() }

async fn api_energy_index(AxumState(st): AxumState<Arc<AppState>>, headers: axum::http::HeaderMap) -> axum::response::Response {
    // 1. CHECK AUTH
    if let Some(auth) = headers.get("Authorization") {
        if let Ok(auth_str) = auth.to_str() {
            if auth_str.starts_with("L402 ") {
                let preimage_hex = &auth_str[5..];
                let preimage_bytes = hex_to_bytes(preimage_hex);
                if preimage_bytes.len() == 32 {
                    let mut preimage = [0u8; 32]; preimage.copy_from_slice(&preimage_bytes);
                    let payment_hash = double_sha256(&preimage);
                    let hash_hex = bytes_to_hex(&payment_hash);
                    let is_valid = {
                        let invoices = ACTIVE_INVOICES.lock().unwrap();
                        invoices.contains_key(&hash_hex)
                    };
                    if is_valid {
                        let hdrs = st.headers.lock().await;
                        return AxumJson(compute_energy_index(&hdrs)).into_response();
                    }
                }
            }
        }
    }

    // 2. GENERATE INVOICE (Real LDK or Internal Fallback)
    if let Some(ldk) = st.ldk.as_ref() {
        // --- REAL LIGHTNING INVOICE ---
        let desc = ldk_node::lightning_invoice::Description::new("Energy Index".to_string()).unwrap();
        let description = ldk_node::lightning_invoice::Bolt11InvoiceDescription::Direct(desc);
        match ldk.bolt11_payment().receive(10_000, &description, 3600) {
            Ok(invoice) => {
                let bolt11 = invoice.to_string();
                let mut payment_hash_bytes = [0u8; 32];
                payment_hash_bytes.copy_from_slice(&invoice.payment_hash()[..]);
                let payment_hash_hex = bytes_to_hex(&payment_hash_bytes);
                { ACTIVE_INVOICES.lock().unwrap().insert(payment_hash_hex.clone(), bolt11.clone()); }

                let mut response_headers = axum::http::HeaderMap::new();
                response_headers.insert("WWW-Authenticate", format!("L402 invoice=\"{}\", macaroon=\"energy_index\"", bolt11).parse().unwrap());
                let body = serde_json::json!({"status": "payment_required", "amount_sat": 10, "payment_hash": payment_hash_hex, "bolt11_invoice": bolt11, "description": "Pay real Testnet Sats"});

                (axum::http::StatusCode::PAYMENT_REQUIRED, response_headers, AxumJson(body)).into_response()
            }
            Err(e) => {
                let body = serde_json::json!({"error": format!("Failed to generate invoice: {}", e)});
                (axum::http::StatusCode::INTERNAL_SERVER_ERROR, AxumJson(body)).into_response()
            }
        }
    } else {
        // --- INTERNAL SIMULATION FALLBACK ---
        let preimage: [u8; 32] = rand::random();
        let payment_hash = double_sha256(&preimage);
        let payment_hash_hex = bytes_to_hex(&payment_hash);
        let preimage_hex = bytes_to_hex(&preimage);
        
        // Store preimage so /api/toll/pay can settle it
        { ACTIVE_INVOICES.lock().unwrap().insert(payment_hash_hex.clone(), preimage_hex.clone()); }

        let mut response_headers = axum::http::HeaderMap::new();
        response_headers.insert("WWW-Authenticate", format!("L402 invoice=\"{}\", macaroon=\"energy_index\"", payment_hash_hex).parse().unwrap());
        let body = serde_json::json!({"status": "payment_required", "amount_sat": 10, "payment_hash": payment_hash_hex, "description": "LDK Offline. Settle via internal ledger."});

        (axum::http::StatusCode::PAYMENT_REQUIRED, response_headers, AxumJson(body)).into_response()
    }
}

#[derive(serde::Deserialize)]
struct PayRequest {
    payment_hash: String, // Changed from preimage
}

async fn api_toll_pay(AxumJson(payload): AxumJson<PayRequest>) -> axum::response::Response {
    let hash_bytes = hex_to_bytes(&payload.payment_hash);
    if hash_bytes.len() != 32 { 
        return (axum::http::StatusCode::BAD_REQUEST, AxumJson(serde_json::json!({"error": "Invalid hash"}))).into_response(); 
    }

    // Look up the preimage associated with this payment_hash
    let invoices = ACTIVE_INVOICES.lock().unwrap();
    if let Some(preimage_hex) = invoices.get(&payload.payment_hash) {
        return (axum::http::StatusCode::OK, AxumJson(serde_json::json!({
            "status": "paid",
            "preimage": preimage_hex
        }))).into_response();
    }

    (axum::http::StatusCode::NOT_FOUND, AxumJson(serde_json::json!({"error": "Invoice not found"}))).into_response()
}

async fn api_tx(AxumState(st): AxumState<Arc<AppState>>, axum::extract::Path(txid): axum::extract::Path<String>) -> impl IntoResponse { AxumJson(find_tx_proof(&st, &txid).await) }
async fn ws_upgrade(ws: WebSocketUpgrade, AxumState(st): AxumState<Arc<AppState>>) -> impl axum::response::IntoResponse { ws.on_upgrade(move |s| ws_handle(s, st)) }
async fn ws_handle(mut s: WebSocket, st: Arc<AppState>) {
    let mut rx = st.ws_tx.subscribe();
    { let ns = st.node.lock().await; let _ = s.send(Message::Text(serde_json::to_string(&*ns).unwrap_or_default())).await; }
    loop {
        tokio::select! {
            r = rx.recv() => { if let Ok(msg) = r { if s.send(Message::Text(msg)).await.is_err() { break; } } }
            m = s.recv() => { match m { Some(Ok(Message::Text(t))) => { match t.as_str() { "toggle"=>{st.node.lock().await.running=!st.node.lock().await.running;} "start"=>{st.node.lock().await.running=true;} "stop"=>{st.node.lock().await.running=false;} _=>{} } } _ => break, } }
        }
    }
}

async fn api_wallet(AxumState(st): AxumState<Arc<AppState>>) -> axum::Json<serde_json::Value> {
    match st.ldk.as_ref() {
        Some(ldk) => {
            let channels = ldk.list_channels();
            let node_id = ldk.node_id();
            let balances = ldk.list_balances();
            
            axum::Json(serde_json::json!({
                "status": "ldk_online",
                "node_pubkey": node_id,
                "total_onchain_sats": balances.total_onchain_balance_sats,
                "spendable_onchain_sats": balances.spendable_onchain_balance_sats,
                "anchor_reserve_sats": balances.total_anchor_channels_reserve_sats,
                "total_lightning_sats": balances.total_lightning_balance_sats,
                "num_channels": channels.len(),
                "channels": channels.iter().map(|c| serde_json::json!({
                    "outbound_capacity_msat": c.outbound_capacity_msat,
                    "inbound_capacity_msat": c.inbound_capacity_msat,
                    "is_usable": c.is_usable
                })).collect::<Vec<_>>()
            }))
        }
        None => {
            axum::Json(serde_json::json!({
                "status": "ldk_offline", 
                "message": "Testnet fee estimation failed. Using internal ledger."
            }))
        }
    }
}

#[derive(serde::Deserialize)]
struct OpenChannelRequest {
    pubkey: String,
    address: String,
    amount_sats: u64,
}

async fn api_open_channel(AxumState(st): AxumState<Arc<AppState>>, AxumJson(payload): AxumJson<OpenChannelRequest>) -> axum::Json<serde_json::Value> {
    match st.ldk.as_ref() {
        Some(ldk) => {
            let pubkey_bytes = hex_to_bytes(&payload.pubkey);
            if pubkey_bytes.len() != 33 { return axum::Json(serde_json::json!({"error": "Invalid pubkey length"})); }
            let pubkey = match ldk_node::bitcoin::secp256k1::PublicKey::from_slice(&pubkey_bytes) { Ok(pk) => pk, Err(e) => return axum::Json(serde_json::json!({"error": format!("Invalid pubkey: {}", e)})) };
            let addr: std::net::SocketAddr = match payload.address.parse() { Ok(a) => a, Err(e) => return axum::Json(serde_json::json!({"error": format!("Invalid address: {}", e)})) };
            match ldk.open_channel(pubkey, addr.into(), payload.amount_sats, None, None) {
                Ok(channel_id) => axum::Json(serde_json::json!({"status": "opening_channel", "channel_id": format!("{:?}", channel_id)})),
                Err(e) => axum::Json(serde_json::json!({"error": format!("Failed: {}", e)})),
            }
        }
        None => axum::Json(serde_json::json!({"error": "LDK is offline. Cannot open channels."})),
    }
}

// ═══════════════════════════════════════════════════════════
// gRPC SERVICE
// ═══════════════════════════════════════════════════════════
#[derive(Clone)]
struct NakamotoThermoService { state: Arc<AppState> }
#[tonic::async_trait]
impl NakamotoThermo for NakamotoThermoService {
    async fn get_state(&self, _request: tonic::Request<proto::Empty>) -> Result<tonic::Response<proto::NodeStateResponse>, tonic::Status> {
        let ns = self.state.node.lock().await;
        Ok(tonic::Response::new(proto::NodeStateResponse { running: ns.running, height: ns.height, peers: ns.peers as u32, uptime_secs: ns.uptime_secs, blocks_relayed: ns.blocks_relayed, blocks_served: ns.blocks_served, blocks_stored: ns.blocks_stored, block_cache_mb: ns.block_cache_mb, txs_relayed: ns.txs_relayed, p2p_connected: ns.p2p_connected, headers_synced: ns.headers_synced, headers_valid: ns.headers_valid, headers_invalid: ns.headers_invalid, mmr_root: ns.mmr_root.clone(), cuckoo_items: ns.cuckoo_items, cuckoo_load: ns.cuckoo_load, best_hash: ns.best_hash.clone(), chain_difficulty: ns.chain_difficulty, invs_relayed: ns.invs_relayed, sync_phase: ns.sync_phase.clone(), db_size_mb: ns.db_size_mb, resumed: ns.resumed }))
    }
    async fn get_energy_index(&self, _request: tonic::Request<proto::Empty>) -> Result<tonic::Response<proto::EnergyIndexResponse>, tonic::Status> {
        let headers = self.state.headers.lock().await; let idx = compute_energy_index(&headers);
        Ok(tonic::Response::new(proto::EnergyIndexResponse { joules_per_sat: idx.joules_per_sat, sat_per_kwh: idx.sat_per_kwh, block_height: idx.block_height, energy_per_block_joules: idx.energy_per_block_joules, used_asic_efficiency_j_per_th: idx.used_asic_efficiency_j_per_th, difficulty: idx.difficulty, hashes_per_block: idx.hashes_per_block, block_reward_sat: idx.block_reward_sat, kw_per_block: idx.kw_per_block, network_power_gw: idx.network_power_gw }))
    }
    async fn get_transaction_proof(&self, request: tonic::Request<proto::TxRequest>) -> Result<tonic::Response<proto::TxProofResponse>, tonic::Status> {
        let txid = request.into_inner().txid; let proof = find_tx_proof(&self.state, &txid).await;
        Ok(tonic::Response::new(proto::TxProofResponse { confirmed: proof.confirmed, block_height: proof.block_height.unwrap_or(0), block_hash: proof.block_hash.unwrap_or_default(), merkle_root: proof.merkle_root.unwrap_or_default(), tx_index: proof.tx_index.unwrap_or(0), proof_hashes: proof.proof_hashes.unwrap_or_default(), block_version: proof.block_version.unwrap_or(0), prev_block_hash: proof.prev_block_hash.unwrap_or_default(), block_timestamp: proof.block_timestamp.unwrap_or(0), block_bits: proof.block_bits.unwrap_or(0), block_nonce: proof.block_nonce.unwrap_or(0) }))
    }
}

// ═══════════════════════════════════════════════════════════
// P2P PEER CONNECTION (Unchanged)
// ═══════════════════════════════════════════════════════════
async fn run_peer(addr_str: String, stream: TcpStream, st: Arc<AppState>, mut inv_rx: broadcast::Receiver<Vec<u8>>, mut header_rx: broadcast::Receiver<Vec<u8>>, mut block_rx: broadcast::Receiver<(Vec<u8>, [u8; 32])>) {
    let (mut reader, mut writer) = stream.into_split(); let nonce: u64 = rand::random(); if writer.write_all(&build_version(nonce)).await.is_err() { return; } info!("P2P: Connected to {}", addr_str);
    let mut handshake_done = false; let mut blocks_requested_from_this_peer = false; let mut buf = vec![0u8; 131072]; let mut leftover = Vec::new(); let mut my_invs: HashSet<[u8; 32]> = HashSet::new();
    loop {
        if !st.node.lock().await.running { break; }
        if handshake_done && !blocks_requested_from_this_peer && st.node.lock().await.sync_phase == "synced" { let mut queue = st.block_dl_queue.lock().await; if !queue.is_empty() { let take = std::cmp::min(10, queue.len()); let batch: Vec<(u32, [u8; 32])> = queue.drain(..take).map(|h| (2, h)).collect(); drop(queue); if !batch.is_empty() { let _ = writer.write_all(&build_getdata(&batch)).await; blocks_requested_from_this_peer = true; } } }
        tokio::select! {
            result = timeout(Duration::from_secs(90), reader.read(&mut buf)) => {
                match result { Ok(Ok(0)) => { warn!("P2P: Peer {} closed", addr_str); break; } Ok(Ok(n)) => { leftover.extend_from_slice(&buf[..n]); while leftover.len() >= 24 { let len = u32::from_le_bytes([leftover[16],leftover[17],leftover[18],leftover[19]]) as usize; if len > 4_000_000 { leftover.clear(); break; } if leftover.len() < 24+len { break; } let msg_data = leftover[..24+len].to_vec(); leftover = leftover[24+len..].to_vec(); if let Some((cmd, payload)) = parse_msg_cmd(&msg_data) { match cmd.as_str() { "version" => { let _ = writer.write_all(&build_verack()).await; } "verack" => { if !handshake_done { handshake_done = true; st.node.lock().await.p2p_connected = true; let chain = st.headers.lock().await; let locator = if chain.is_empty() { vec![[0u8;32]] } else { vec![chain.last().unwrap().hash()] }; drop(chain); let _ = writer.write_all(&build_getheaders(&locator)).await; let _ = writer.write_all(&build_sendheaders()).await; let _ = writer.write_all(&build_sendcmpct()).await; } } "headers" => { let headers = parse_headers(&payload); let count = headers.len(); if count == 0 { continue; } let is_new_block = count == 1 && st.node.lock().await.sync_phase == "synced"; let mut valid = 0u64; let mut new_tip_hash: Option<[u8; 32]> = None; let mut batch: Vec<(u64, [u8; 32], Vec<u8>)> = Vec::new(); { let mut chain = st.headers.lock().await; let mut ns = st.node.lock().await; for hdr in &headers { if !hdr.verify_pow() { continue; } if !chain.is_empty() { let tip_hash = chain.last().unwrap().hash(); if hdr.prev_hash != tip_hash { continue; } } let h = hdr.hash(); batch.push((chain.len() as u64, h, hdr.to_bytes())); chain.push(hdr.clone()); valid += 1; ns.headers_synced = chain.len() as u64; ns.headers_valid += 1; ns.height = chain.len() as u64; ns.best_hash = hash_hex(&hdr.hash()); ns.chain_difficulty = difficulty_from_bits(hdr.bits); ns.cuckoo_items = chain.len() as u64; ns.cuckoo_load = chain.len() as f64 / 200_000_000.0; ns.mmr_root = compute_mmr_root(&chain); new_tip_hash = Some(hdr.hash()); } } if !batch.is_empty() { if let Err(e) = st.db.save_headers_batch(&batch).await { warn!("DB err: {}", e); } } if is_new_block && valid == 1 { if let Some(hash) = new_tip_hash { let mut seen = st.seen_invs.lock().await; let mut hk = [0u8; 32]; hk.copy_from_slice(&hash); if !seen.contains(&hk) { seen.insert(hk); let hdr = &headers[0]; let mut relay_payload = Vec::new(); relay_payload.extend_from_slice(&write_varint(1)); relay_payload.extend_from_slice(&hdr.to_bytes()); relay_payload.push(0); let _ = st.header_relay_tx.send(relay_payload); st.block_dl_queue.lock().await.push_back(hash); blocks_requested_from_this_peer = false; } } } if count < 2000 { st.node.lock().await.sync_phase = "synced".to_string(); let chain = st.headers.lock().await; let start = chain.len().saturating_sub(BLOCK_CACHE); let mut queue = st.block_dl_queue.lock().await; for i in (start..chain.len()).rev() { let hash = chain[i].hash(); let blocks = st.blocks.lock().await; if !blocks.contains_key(&hash) { drop(blocks); queue.push_back(hash); } } } else { let chain = st.headers.lock().await; let locator = vec![chain.last().unwrap().hash()]; drop(chain); let _ = writer.write_all(&build_getheaders(&locator)).await; } } "inv" => { let invs = parse_inv(&payload); let mut block_hashes_to_request: Vec<(u32, [u8; 32])> = Vec::new(); let mut relay_count = 0u64; { let mut seen = st.seen_invs.lock().await; let mut ns = st.node.lock().await; let blocks = st.blocks.lock().await; for (inv_type, hash) in &invs { let mut hk = [0u8; 32]; hk.copy_from_slice(hash); if seen.contains(&hk) { continue; } seen.insert(hk); my_invs.insert(hk); if *inv_type == 1 { ns.txs_relayed += 1; } else if *inv_type == 2 { if !blocks.contains_key(&hk) { block_hashes_to_request.push((2, hk)); } } relay_count += 1; } } if !block_hashes_to_request.is_empty() { let _ = writer.write_all(&build_getdata(&block_hashes_to_request)).await; } if relay_count > 0 { let _ = st.inv_tx.send(payload.clone()); st.node.lock().await.invs_relayed += relay_count; } } "block" => { if payload.len() >= 80 { if let Some(hdr) = BlockHeader::from_bytes(&payload[..80]) { let hash = hdr.hash(); let chain = st.headers.lock().await; let in_chain = chain.iter().rev().take(BLOCK_CACHE + 10).any(|h| h.hash() == hash); drop(chain); if in_chain { let height = st.headers.lock().await.len().saturating_sub(1) as u64; st.blocks.lock().await.insert(hash, payload.clone()); if let Err(e) = st.db.save_block(&hash, height, &payload).await { warn!("DB err: {}", e); } my_invs.insert(hash); st.seen_invs.lock().await.insert(hash); let mut ns = st.node.lock().await; ns.blocks_stored += 1; ns.block_cache_mb = (st.blocks.lock().await.values().map(|v| v.len() as f64).sum::<f64>() / 1_048_576.0 * 10.0).round() / 10.0; ns.db_size_mb = st.db.db_size_mb(); let _ = st.block_relay_tx.send((payload.clone(), hash)); let mut inv_payload = Vec::new(); inv_payload.extend_from_slice(&write_varint(1)); inv_payload.extend_from_slice(&2u32.to_le_bytes()); inv_payload.extend_from_slice(&hash); let _ = st.inv_tx.send(inv_payload); ns.blocks_relayed += 1; } } } } "getdata" => { let items = parse_inv(&payload); let blocks = st.blocks.lock().await; let mut served = 0u64; for (inv_type, hash) in &items { if *inv_type == 2 { let mut hk = [0u8; 32]; hk.copy_from_slice(hash); if let Some(block_payload) = blocks.get(&hk) { let _ = writer.write_all(&wrap_msg("block", block_payload)).await; served += 1; } } } if served > 0 { let mut ns = st.node.lock().await; ns.blocks_served += served; ns.blocks_relayed += served; } } "ping" => { if payload.len() >= 8 { let _ = writer.write_all(&wrap_msg("pong", &payload[..8])).await; } } _ => {} } } } } Ok(Err(e)) => { warn!("P2P: Read error from {}: {}", addr_str, e); break; } Err(_) => {} } }
            result = inv_rx.recv() => { if let Ok(inv_payload) = result { if invs_contain_new(&inv_payload, &my_invs) { let _ = writer.write_all(&wrap_msg("inv", &inv_payload)).await; let parsed = parse_inv(&inv_payload); for (_, h) in &parsed { let mut hk = [0u8; 32]; hk.copy_from_slice(h); my_invs.insert(hk); } } } }
            result = header_rx.recv() => { if let Ok(header_payload) = result { let _ = writer.write_all(&wrap_msg("headers", &header_payload)).await; } }
            result = block_rx.recv() => { if let Ok((block_payload, hash)) = result { if !my_invs.contains(&hash) { let _ = writer.write_all(&wrap_msg("block", &block_payload)).await; my_invs.insert(hash); } } }
        }
    }
}

// ═══════════════════════════════════════════════════════════
// ENGINE LOOP
// ═══════════════════════════════════════════════════════════
async fn engine_loop(st: Arc<AppState>) {
    let mut p2p_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    loop {
        let running = st.node.lock().await.running;
        if running && p2p_tasks.is_empty() { *st.start_time.lock().await = Some(std::time::Instant::now()); st.node.lock().await.sync_phase = "syncing".to_string(); for seed in SEEDS { let seed_str = seed.to_string(); let st_c = st.clone(); let inv_rx = st.inv_tx.subscribe(); let header_rx = st.header_relay_tx.subscribe(); let block_rx = st.block_relay_tx.subscribe(); let handle = tokio::spawn(async move { let addrs = match tokio::net::lookup_host(format!("{}:8333", seed_str)).await { Ok(a) => a.collect::<Vec<_>>(), Err(_) => return }; for addr in addrs { if !st_c.node.lock().await.running { return; } if let Ok(Ok(stream)) = timeout(Duration::from_secs(10), TcpStream::connect(addr)).await { run_peer(seed_str.clone(), stream, st_c.clone(), inv_rx.resubscribe(), header_rx.resubscribe(), block_rx.resubscribe()).await; } sleep(Duration::from_secs(2)).await; } }); p2p_tasks.push(handle); } st.node.lock().await.peers = p2p_tasks.len() as u16; }
        if !running && !p2p_tasks.is_empty() { for t in p2p_tasks.drain(..) { t.abort(); } st.node.lock().await.peers = 0; st.node.lock().await.p2p_connected = false; }
        { let headers = st.headers.lock().await; let idx = compute_energy_index(&headers); drop(headers); let mut ns = st.node.lock().await; if let Some(t) = *st.start_time.lock().await { ns.uptime_secs = t.elapsed().as_secs(); } ns.blocks_stored = st.blocks.lock().await.len() as u64; ns.block_cache_mb = (st.blocks.lock().await.values().map(|v| v.len() as f64).sum::<f64>() / 1_048_576.0 * 10.0).round() / 10.0; ns.db_size_mb = st.db.db_size_mb();             ns.joules_per_sat = idx.joules_per_sat; 
            ns.sat_per_kwh = idx.sat_per_kwh; 
            ns.network_power_gw = idx.network_power_gw;

            // ADD LDK WALLET POLLING HERE
            if let Some(ldk) = st.ldk.as_ref() {
                let channels = ldk.list_channels();
                let balances = ldk.list_balances();   // ← this must stay inside the if block
                
                ns.ldk_channels = channels.len() as u64;
                ns.ldk_usable_channels = channels.iter().filter(|c| c.is_usable).count() as u64;
                ns.ldk_onchain_balance = balances.spendable_onchain_balance_sats; 
            } else {
                ns.ldk_channels = 0;
                ns.ldk_usable_channels = 0;
                ns.ldk_onchain_balance = 0;   // ← important for the else case
            }
        }
        if running { if let Ok(resp) = timeout(Duration::from_secs(5), reqwest::get("https://mempool.space/api/blocks/tip/height")).await { if let Ok(text) = resp { if let Ok(h) = text.text().await { if let Ok(height) = h.trim().parse::<u64>() { let mut ns = st.node.lock().await; if ns.headers_synced < height { ns.height = height; } } } } } }
        { let ns = st.node.lock().await.clone(); let _ = st.ws_tx.send(serde_json::to_string(&ns).unwrap_or_default()); }
        p2p_tasks.retain(|t| !t.is_finished());
        sleep(Duration::from_secs(4)).await;
    }
}

// ═══════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_target(false).compact().init();
    info!("=== Nakamoto Lite v0.3.0 — Sovereign Thermodynamic Layer ===");

    let db = Arc::new(Db::open().expect("Failed to open database"));
    let existing_headers = db.load_headers().await.unwrap_or_default();
    let resumed = !existing_headers.is_empty();
    let initial_count = existing_headers.len() as u64;

    let (ws_tx, _) = broadcast::channel::<String>(256);
    let (inv_tx, _) = broadcast::channel::<Vec<u8>>(256);
    let (header_relay_tx, _) = broadcast::channel::<Vec<u8>>(256);
    let (block_relay_tx, _) = broadcast::channel::<(Vec<u8>, [u8; 32])>(256);

    // ═══════ SOVEREIGN LDK NODE INITIALIZATION ═══════
    info!("Initializing Sovereign Lightning Node (Testnet)...");
    let ldk_data_dir = "./ldk_node_data";
    std::fs::create_dir_all(ldk_data_dir).unwrap();

    let mut builder = Builder::new();
    
    builder.set_network(Network::Testnet);
    builder.set_storage_dir_path(ldk_data_dir.to_string());
    builder.set_listening_addresses(vec!["0.0.0.0:9735".parse().unwrap()])
        .expect("Failed to set listening address");
    
    // EXPLICITLY SET CHAIN & GOSSIP SOURCES TO FIX FEE TIMEOUT
    builder.set_chain_source_esplora(
        "https://blockstream.info/testnet/api".to_string(),
        None,  // uses sensible defaults
    );
    builder.set_gossip_source_rgs(
        "https://rapidsync.lightningdevkit.org/testnet/v2/snapshot".to_string(),
    );

    let ldk_node = builder.build().unwrap();

    // Handle startup gracefully
    match ldk_node.start() {
        Ok(_) => {
            let ldk_node_id = ldk_node.node_id();
            info!("LDK Node Started! Pubkey: {}", ldk_node_id);
            
            let funding_address = ldk_node.onchain_payment().new_address().unwrap();
            info!("💸 Fund LDK Wallet (Testnet): {}", funding_address);
        }
        Err(e) => {
            warn!("⚠️ LDK Node failed to start: {:?}", e);
            warn!("Lightning features will be disabled.");
        }
    }

    let state = Arc::new(AppState {
        node: Arc::new(Mutex::new(NodeState {
            joules_per_sat: 0.0, sat_per_kwh: 0.0, network_power_gw: 0.0,
            running: false, height: initial_count, peers: 0, uptime_secs: 0,
            blocks_relayed: 0, blocks_served: 0, blocks_stored: 0,
            block_cache_mb: 0.0, txs_relayed: 0, p2p_connected: false,
            headers_synced: initial_count, headers_valid: initial_count, headers_invalid: 0,
            mmr_root: compute_mmr_root(&existing_headers),
            cuckoo_items: initial_count, cuckoo_load: initial_count as f64 / 200_000_000.0,
            best_hash: existing_headers.last().map(|h| hash_hex(&h.hash())).unwrap_or_default(),
            chain_difficulty: existing_headers.last().map(|h| difficulty_from_bits(h.bits)).unwrap_or(0.0),
            invs_relayed: 0, sync_phase: if resumed { "synced".to_string() } else { "idle".to_string() },
            db_size_mb: db.db_size_mb(), resumed,
            ldk_channels: 0,           // <-- ADD THIS
            ldk_usable_channels: 0,    // <-- ADD THIS
            ldk_onchain_balance: 0,    // <-- ADD THIS
        })),
        headers: Arc::new(Mutex::new(existing_headers)),
        blocks: Arc::new(Mutex::new(db.load_last_blocks(BLOCK_CACHE).await.unwrap_or_default())),
        seen_invs: Arc::new(Mutex::new(HashSet::new())),
        inv_tx, header_relay_tx, block_relay_tx,
        block_dl_queue: Arc::new(Mutex::new(VecDeque::new())),
        ws_tx, start_time: Arc::new(Mutex::new(None)), db,
        ldk: Some(Arc::new(ldk_node)),
    });

    let eng_state = state.clone(); tokio::spawn(async move { engine_loop(eng_state).await; });

    let grpc_state = state.clone(); tokio::spawn(async move {
        let svc = NakamotoThermoServer::new(NakamotoThermoService { state: grpc_state });
        match tonic::transport::Server::builder().add_service(svc).serve("[::]:50051".parse().unwrap()).await { Ok(_) => info!("gRPC server stopped"), Err(e) => warn!("gRPC server error: {}", e) }
    });

    let app = Router::new()
        .route("/", get(dashboard))
        .route("/api/state", get(api_state))
        .route("/api/wallet", get(api_wallet))
        .route("/api/wallet/open-channel", post(api_open_channel))
        .route("/ws", get(ws_upgrade))
        .route("/api/toggle", get(api_toggle))
        .route("/api/start", get(api_start))
        .route("/api/stop", get(api_stop))
        .route("/api/energy-index", get(api_energy_index))
        .route("/api/toll/pay", post(api_toll_pay)) 
        .route("/api/tx/:txid", get(api_tx))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await.unwrap();
    info!("Dashboard  -> http://localhost:3001");
    info!("Energy API -> http://localhost:3001/api/energy-index (L402 Paywall Active)");
    axum::serve(listener, app).await.unwrap();
}

// ═══════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════
#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn test_double_sha256() { let data = b"abc"; let hash = double_sha256(data); let expected_hex = "4f8b42c22dd3729b519ba6f68d2da7cc5b2d606d05daed5ad5128cc03e6c6358"; let computed_hex = hash.iter().map(|b| format!("{:02x}", b)).collect::<String>(); assert_eq!(computed_hex, expected_hex); }
}