//! Redis-compatible HyperLogLog implementation (dense and sparse encoding).

use bytes::Bytes;

pub const HLL_HDR_SIZE: usize = 16;
pub const HLL_DENSE: u8 = 0;
pub const HLL_SPARSE: u8 = 1;
pub const HLL_REGISTERS: usize = 16384;
pub const HLL_DENSE_SIZE: usize = HLL_HDR_SIZE + 12288; // 12304 bytes
pub const HLL_SPARSE_MAX_BYTES: usize = 3000;

pub const ERR_WRONGTYPE: &str = "WRONGTYPE Key is not a valid HyperLogLog string value.";
pub const ERR_INVALIDOBJ: &str = "INVALIDOBJ Corrupted HLL object detected";

/// 64-bit MurmurHash2 implementation with seed, exactly matching Redis MurmurHash64A.
pub fn murmur_hash_64a(key: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u32 = 47;

    let len = key.len();
    let mut h: u64 = seed ^ ((len as u64).wrapping_mul(M));

    let n_blocks = len / 8;
    for i in 0..n_blocks {
        let chunk = &key[i * 8..(i + 1) * 8];
        let mut k = u64::from_le_bytes(chunk.try_into().unwrap());
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);

        h ^= k;
        h = h.wrapping_mul(M);
    }

    let rem = &key[n_blocks * 8..];
    if !rem.is_empty() {
        let mut k: u64 = 0;
        for (i, &b) in rem.iter().enumerate() {
            k |= (b as u64) << (i * 8);
        }
        h ^= k;
        h = h.wrapping_mul(M);
    }

    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;

    h
}

/// Compute leading pattern length for an element (matches Redis hllPatLen).
#[inline]
pub fn hll_pat_len(ele: &[u8]) -> (usize, u8) {
    let hash = murmur_hash_64a(ele, 0xadc83b19);
    let index = (hash & 0x3FFF) as usize;
    let pat = (hash >> 14) | (1u64 << 50);
    let count = (pat.trailing_zeros() + 1) as u8;
    (index, count)
}

#[inline]
pub fn hll_dense_get_register(registers: &[u8], reg_num: usize) -> u8 {
    let bit = reg_num * 6;
    let byte = bit / 8;
    let fb = bit % 8;
    let b0 = registers[byte] as u16;
    let b1 = if byte + 1 < registers.len() {
        registers[byte + 1] as u16
    } else {
        0
    };
    let word = b0 | (b1 << 8);
    ((word >> fb) & 0x3F) as u8
}

#[inline]
pub fn hll_dense_set_register(registers: &mut [u8], reg_num: usize, val: u8) {
    let bit = reg_num * 6;
    let byte = bit / 8;
    let fb = bit % 8;
    let b0 = registers[byte] as u16;
    let b1 = if byte + 1 < registers.len() {
        registers[byte + 1] as u16
    } else {
        0
    };
    let mut word = b0 | (b1 << 8);
    word &= !(0x3F << fb);
    word |= ((val as u16) & 0x3F) << fb;
    let bytes = word.to_le_bytes();
    registers[byte] = bytes[0];
    if byte + 1 < registers.len() {
        registers[byte + 1] = bytes[1];
    }
}

pub fn hll_validate(bytes: &[u8]) -> Result<u8, &'static str> {
    if bytes.len() < HLL_HDR_SIZE {
        return Err(ERR_WRONGTYPE);
    }
    if &bytes[0..4] != b"HYLL" {
        return Err(ERR_WRONGTYPE);
    }
    let encoding = bytes[4];
    if encoding == HLL_DENSE {
        if bytes.len() != HLL_DENSE_SIZE {
            return Err(ERR_WRONGTYPE);
        }
        Ok(HLL_DENSE)
    } else if encoding == HLL_SPARSE {
        let mut count: usize = 0;
        let mut i = HLL_HDR_SIZE;
        while i < bytes.len() {
            let b = bytes[i];
            if (b & 0x80) != 0 {
                let len = (b & 0x03) as usize + 1;
                count = count.checked_add(len).ok_or(ERR_INVALIDOBJ)?;
                if count > HLL_REGISTERS {
                    return Err(ERR_INVALIDOBJ);
                }
                i += 1;
            } else if (b & 0x40) != 0 {
                if i + 1 >= bytes.len() {
                    return Err(ERR_INVALIDOBJ);
                }
                let len = ((((b & 0x3F) as usize) << 8) | (bytes[i + 1] as usize)) + 1;
                count = count.checked_add(len).ok_or(ERR_INVALIDOBJ)?;
                if count > HLL_REGISTERS {
                    return Err(ERR_INVALIDOBJ);
                }
                i += 2;
            } else {
                let len = (b & 0x3F) as usize + 1;
                count = count.checked_add(len).ok_or(ERR_INVALIDOBJ)?;
                if count > HLL_REGISTERS {
                    return Err(ERR_INVALIDOBJ);
                }
                i += 1;
            }
        }
        if count != HLL_REGISTERS {
            return Err(ERR_INVALIDOBJ);
        }
        Ok(HLL_SPARSE)
    } else {
        Err(ERR_WRONGTYPE)
    }
}

pub fn hll_decode_registers(bytes: &[u8]) -> Result<[u8; 16384], &'static str> {
    let encoding = hll_validate(bytes)?;
    let mut regs = [0u8; 16384];
    if encoding == HLL_DENSE {
        let dense_data = &bytes[HLL_HDR_SIZE..];
        for i in 0..16384 {
            regs[i] = hll_dense_get_register(dense_data, i);
        }
    } else {
        let mut reg_idx = 0;
        let mut i = HLL_HDR_SIZE;
        while i < bytes.len() {
            let b = bytes[i];
            if (b & 0x80) != 0 {
                let val = ((b >> 2) & 0x1F) + 1;
                let len = (b & 0x03) as usize + 1;
                for _ in 0..len {
                    regs[reg_idx] = val;
                    reg_idx += 1;
                }
                i += 1;
            } else if (b & 0x40) != 0 {
                let len = ((((b & 0x3F) as usize) << 8) | (bytes[i + 1] as usize)) + 1;
                reg_idx += len;
                i += 2;
            } else {
                let len = (b & 0x3F) as usize + 1;
                reg_idx += len;
                i += 1;
            }
        }
    }
    Ok(regs)
}

pub fn hll_encode_sparse(regs: &[u8; 16384]) -> Option<Vec<u8>> {
    if regs.iter().any(|&r| r > 32) {
        return None;
    }
    let mut buf = Vec::with_capacity(64);
    let mut i = 0;
    while i < 16384 {
        if regs[i] == 0 {
            let mut run_len = 0;
            while i < 16384 && regs[i] == 0 {
                run_len += 1;
                i += 1;
            }
            while run_len > 0 {
                if run_len <= 64 {
                    buf.push((run_len - 1) as u8);
                    run_len = 0;
                } else {
                    let chunk = run_len.min(16384);
                    let val = (chunk - 1) as u16;
                    buf.push(0x40 | ((val >> 8) as u8 & 0x3F));
                    buf.push((val & 0xFF) as u8);
                    run_len -= chunk;
                }
            }
        } else {
            let val = regs[i];
            let mut run_len = 0;
            while i < 16384 && regs[i] == val && run_len < 4 {
                run_len += 1;
                i += 1;
            }
            buf.push(0x80 | ((val - 1) << 2) | (run_len - 1) as u8);
        }
        if buf.len() > HLL_SPARSE_MAX_BYTES {
            return None;
        }
    }
    Some(buf)
}

pub fn hll_encode_dense(regs: &[u8; 16384]) -> Vec<u8> {
    let mut data = vec![0u8; 12288];
    for (i, &r) in regs.iter().enumerate() {
        if r > 0 {
            hll_dense_set_register(&mut data, i, r);
        }
    }
    data
}

pub fn hll_create_sparse_empty() -> Vec<u8> {
    let mut hll = vec![0u8; HLL_HDR_SIZE + 2];
    hll[0..4].copy_from_slice(b"HYLL");
    hll[4] = HLL_SPARSE;
    // card is 0, valid bit 63 is 0 (byte 15 is 0)
    // XZERO covering 16384 zeros: 0x40 | 0x3F = 0x7F, 0xFF
    hll[16] = 0x7F;
    hll[17] = 0xFF;
    hll
}

pub fn hll_create_from_regs(regs: &[u8; 16384], card_cache: Option<u64>) -> Vec<u8> {
    let sparse_payload = hll_encode_sparse(regs);
    let (encoding, payload) = if let Some(sp) = sparse_payload {
        (HLL_SPARSE, sp)
    } else {
        (HLL_DENSE, hll_encode_dense(regs))
    };
    let mut out = Vec::with_capacity(HLL_HDR_SIZE + payload.len());
    out.extend_from_slice(b"HYLL");
    out.push(encoding);
    out.push(0);
    out.push(0);
    out.push(0);
    if let Some(card) = card_cache {
        let mut card_bytes = card.to_le_bytes();
        card_bytes[7] &= 0x7F; // bit 63 is 0 (valid)
        out.extend_from_slice(&card_bytes);
    } else {
        let mut card_bytes = [0u8; 8];
        card_bytes[7] = 0x80; // bit 63 is 1 (invalid)
        out.extend_from_slice(&card_bytes);
    }
    out.extend_from_slice(&payload);
    out
}

pub fn hll_compute_card(regs: &[u8; 16384]) -> u64 {
    const M: f64 = 16384.0;
    const ALPHA: f64 = 0.7213475204444817;
    let mut sum = 0.0;
    let mut zeros = 0;
    for &val in regs.iter() {
        sum += 2.0_f64.powi(-(val as i32));
        if val == 0 {
            zeros += 1;
        }
    }

    let raw_estimate = ALPHA * M * M / sum;
    if raw_estimate <= 2.5 * M && zeros > 0 {
        let count = M * (M / zeros as f64).ln();
        count.round() as u64
    } else if raw_estimate <= (1.0 / 30.0) * 4294967296.0 {
        raw_estimate.round() as u64
    } else {
        let two_to_32 = 4294967296.0;
        (-two_to_32 * (1.0 - raw_estimate / two_to_32).ln()).round() as u64
    }
}

pub fn hll_count(bytes: &mut [u8]) -> Result<u64, &'static str> {
    hll_validate(bytes)?;
    // Check if cached cardinality is valid (bit 63 == 0)
    let card_bytes: [u8; 8] = bytes[8..16].try_into().unwrap();
    if (card_bytes[7] & 0x80) == 0 {
        let card = u64::from_le_bytes(card_bytes);
        return Ok(card);
    }
    let regs = hll_decode_registers(bytes)?;
    let card = hll_compute_card(&regs);
    let mut new_card = card.to_le_bytes();
    new_card[7] &= 0x7F; // bit 63 = 0 (valid)
    bytes[8..16].copy_from_slice(&new_card);
    Ok(card)
}

pub fn hll_add(bytes: &mut Vec<u8>, elements: &[Bytes]) -> Result<bool, &'static str> {
    let mut regs = hll_decode_registers(bytes)?;
    let mut modified = false;
    for elem in elements {
        let (index, count) = hll_pat_len(elem.as_ref());
        if count > regs[index] {
            regs[index] = count;
            modified = true;
        }
    }
    if modified {
        *bytes = hll_create_from_regs(&regs, None);
    }
    Ok(modified)
}

pub fn hll_merge(dest: &mut Vec<u8>, sources: &[&[u8]]) -> Result<(), &'static str> {
    let mut max_regs = if dest.is_empty() {
        [0u8; 16384]
    } else {
        hll_decode_registers(dest)?
    };
    for src in sources {
        if !src.is_empty() {
            let regs = hll_decode_registers(src)?;
            for i in 0..16384 {
                if regs[i] > max_regs[i] {
                    max_regs[i] = regs[i];
                }
            }
        }
    }
    *dest = hll_create_from_regs(&max_regs, None);
    Ok(())
}
