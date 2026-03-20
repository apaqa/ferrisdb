// =============================================================================
// transaction/keyutil.rs — MVCC key 編碼工具
// =============================================================================
//
// 我們把 user key 編碼成：
//   user_key + (u64::MAX - ts).to_be_bytes()
//
// 這樣的好處：
// - 同一個 user_key 的所有版本會排在一起
// - ts 越大（越新）的版本，suffix 越小
// - 在字典序排序下，新的版本會排在前面
//
// 範例：
// - user key = "name"
// - ts = 10、11、12
// - 編碼後排序會是 ts=12, ts=11, ts=10
//
// 這讓我們在 scan 某個 key 的版本區間時，可以很快找到「最新可見版本」。

pub fn encode_key(user_key: &[u8], ts: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(user_key.len() + 8);
    out.extend_from_slice(user_key);
    out.extend_from_slice(&(u64::MAX - ts).to_be_bytes());
    out
}

pub fn decode_key(encoded: &[u8]) -> (&[u8], u64) {
    assert!(encoded.len() >= 8, "encoded mvcc key must be at least 8 bytes");
    let split = encoded.len() - 8;
    let user_key = &encoded[..split];
    let mut ts_bytes = [0_u8; 8];
    ts_bytes.copy_from_slice(&encoded[split..]);
    let inverted = u64::from_be_bytes(ts_bytes);
    let ts = u64::MAX - inverted;
    (user_key, ts)
}

pub fn encode_key_prefix_start(user_key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(user_key.len() + 8);
    out.extend_from_slice(user_key);
    out.extend_from_slice(&[0_u8; 8]);
    out
}

pub fn encode_key_prefix_end(user_key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(user_key.len() + 8);
    out.extend_from_slice(user_key);
    out.extend_from_slice(&[0xFF_u8; 8]);
    out
}
