// =============================================================================
// storage/sstable/format.rs — SSTable 檔案格式定義
// =============================================================================
//
// 這個檔案專門處理「格式」本身，不處理業務邏輯。
// 我們把所有 magic/version/section 大小與編解碼 helper 放在這裡，
// 讓 writer / reader 可以共用，避免規格分散導致不一致。
//
// 簡化版 SSTable 格式：
// 1. Header
//    - magic: "FRDB" (4 bytes)
//    - version: u32 little-endian (4 bytes)
//
// 2. Data Section（多筆 entry）
//    - key_len: u32 LE
//    - value_len: u32 LE
//    - key bytes
//    - value bytes
//
// 3. Index Section（多筆 entry，key -> offset）
//    - key_len: u32 LE
//    - value_len: u32 LE（固定應為 8）
//    - key bytes
//    - offset bytes（u64 LE，8 bytes）
//
// 4. Footer（固定 24 bytes）
//    - index_offset: u64 LE（index section 起點）
//    - index_count: u64 LE（index 筆數）
//    - magic: "FRDB" (4 bytes)
//    - version: u32 LE (4 bytes)

use std::io::{Read, Write};

pub const MAGIC: [u8; 4] = *b"FRDB";
pub const VERSION: u32 = 1;

pub const HEADER_SIZE: u64 = 8;
pub const FOOTER_SIZE: u64 = 24;
pub const INDEX_VALUE_SIZE: u32 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Footer {
    pub index_offset: u64,
    pub index_count: u64,
}

pub fn write_header<W: Write>(mut w: W) -> std::io::Result<()> {
    w.write_all(&MAGIC)?;
    w.write_all(&VERSION.to_le_bytes())?;
    Ok(())
}

pub fn read_and_validate_header<R: Read>(mut r: R) -> std::io::Result<()> {
    let mut magic = [0_u8; 4];
    r.read_exact(&mut magic)?;
    let version = read_u32(&mut r)?;

    if magic != MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid sstable header magic: {:?}", magic),
        ));
    }
    if version != VERSION {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unsupported sstable version: {}", version),
        ));
    }
    Ok(())
}

pub fn write_footer<W: Write>(mut w: W, footer: Footer) -> std::io::Result<()> {
    w.write_all(&footer.index_offset.to_le_bytes())?;
    w.write_all(&footer.index_count.to_le_bytes())?;
    w.write_all(&MAGIC)?;
    w.write_all(&VERSION.to_le_bytes())?;
    Ok(())
}

pub fn read_and_validate_footer<R: Read>(mut r: R) -> std::io::Result<Footer> {
    let index_offset = read_u64(&mut r)?;
    let index_count = read_u64(&mut r)?;

    let mut magic = [0_u8; 4];
    r.read_exact(&mut magic)?;
    let version = read_u32(&mut r)?;

    if magic != MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid sstable footer magic: {:?}", magic),
        ));
    }
    if version != VERSION {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unsupported sstable footer version: {}", version),
        ));
    }

    Ok(Footer {
        index_offset,
        index_count,
    })
}

/// 寫一筆可變長 entry（data section / index section 都共用）
pub fn write_entry<W: Write>(mut w: W, key: &[u8], value: &[u8]) -> std::io::Result<()> {
    let key_len = u32::try_from(key.len()).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "key length exceeds u32")
    })?;
    let value_len = u32::try_from(value.len()).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "value length exceeds u32")
    })?;

    w.write_all(&key_len.to_le_bytes())?;
    w.write_all(&value_len.to_le_bytes())?;
    w.write_all(key)?;
    w.write_all(value)?;
    Ok(())
}

/// 讀一筆可變長 entry，回傳 (key, value)
pub fn read_entry<R: Read>(mut r: R) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
    let key_len = read_u32(&mut r)? as usize;
    let value_len = read_u32(&mut r)? as usize;

    let mut key = vec![0_u8; key_len];
    let mut value = vec![0_u8; value_len];
    r.read_exact(&mut key)?;
    r.read_exact(&mut value)?;
    Ok((key, value))
}

pub fn read_u32<R: Read>(mut r: R) -> std::io::Result<u32> {
    let mut buf = [0_u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

pub fn read_u64<R: Read>(mut r: R) -> std::io::Result<u64> {
    let mut buf = [0_u8; 8];
    r.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}
