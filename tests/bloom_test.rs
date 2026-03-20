// =============================================================================
// tests/bloom_test.rs — Bloom Filter 測試
// =============================================================================

use ferrisdb::storage::bloom::BloomFilter;

#[test]
fn test_inserted_keys_all_may_contain() {
    let mut bloom = BloomFilter::new(1000, 0.01);

    for i in 0..1000_u32 {
        let key = format!("key:{:04}", i);
        bloom.insert(key.as_bytes());
    }

    for i in 0..1000_u32 {
        let key = format!("key:{:04}", i);
        assert!(bloom.may_contain(key.as_bytes()), "missing inserted key {}", key);
    }
}

#[test]
fn test_false_positive_rate_below_five_percent() {
    let mut bloom = BloomFilter::new(1000, 0.01);
    for i in 0..1000_u32 {
        bloom.insert(format!("exists:{:04}", i).as_bytes());
    }

    let mut false_positives = 0_u32;
    for i in 0..1000_u32 {
        if bloom.may_contain(format!("missing:{:04}", i).as_bytes()) {
            false_positives += 1;
        }
    }

    let rate = false_positives as f64 / 1000.0;
    assert!(rate < 0.05, "false positive rate too high: {}", rate);
}

#[test]
fn test_serialize_deserialize_roundtrip() {
    let mut bloom = BloomFilter::new(100, 0.01);
    for i in 0..100_u32 {
        bloom.insert(format!("round:{:03}", i).as_bytes());
    }

    let encoded = bloom.to_bytes();
    let decoded = BloomFilter::from_bytes(&encoded).expect("decode bloom");

    for i in 0..100_u32 {
        let key = format!("round:{:03}", i);
        assert_eq!(bloom.may_contain(key.as_bytes()), decoded.may_contain(key.as_bytes()));
    }

    for i in 0..100_u32 {
        let key = format!("not-round:{:03}", i);
        assert_eq!(bloom.may_contain(key.as_bytes()), decoded.may_contain(key.as_bytes()));
    }
}
