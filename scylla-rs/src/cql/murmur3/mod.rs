// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This crates implements the murmur3, which is used to calculate the partition key in scyllaDB.
use std::convert::TryInto;

fn copy_into_array<A, T>(slice: &[T]) -> A
where
    A: Default + AsMut<[T]>,
    T: Copy,
{
    let mut a = A::default();
    <A as AsMut<[T]>>::as_mut(&mut a).copy_from_slice(slice);
    a
}

/// Modified from https://github.com/stusmall/murmur3
///
/// Use the x64 variant of the 128 bit murmur3 to hash some [Read] implementation.
///
/// # Example
/// ```
/// use scylla_rs::cql::murmur3_cassandra_x64_128;
/// let hash_pair = murmur3_cassandra_x64_128(
///     "EHUHSJRCMDJSZUQMNLDBSRFC9O9XCI9SMHFWWHNDYOOOWMSOJQHCC9GFUEGECEVVXCSXYTHSRJ9TZ9999".as_bytes(),
///     0,
/// );
/// ```
pub fn murmur3_cassandra_x64_128(source: &[u8], seed: u32) -> (i64, i64) {
    const C1: i64 = -8_663_945_395_140_668_459_i64; // 0x87c3_7b91_1142_53d5;
    const C2: i64 = 0x4cf5_ad43_2745_937f;
    const C3: i64 = 0x52dc_e729;
    const C4: i64 = 0x3849_5ab5;
    const R1: u32 = 27;
    const R2: u32 = 31;
    const R3: u32 = 33;
    const M: i64 = 5;
    let mut h1: i64 = seed as i64;
    let mut h2: i64 = seed as i64;
    let mut chunks_iter = source.chunks_exact(16);
    let rem = chunks_iter.remainder();
    for chunk in chunks_iter {
        let k1 = i64::from_le_bytes((&chunk[..8]).try_into().unwrap());
        let k2 = i64::from_le_bytes((&chunk[8..]).try_into().unwrap());
        h1 ^= k1.wrapping_mul(C1).rotate_left(R2).wrapping_mul(C2);
        h1 = h1.rotate_left(R1).wrapping_add(h2).wrapping_mul(M).wrapping_add(C3);
        h2 ^= k2.wrapping_mul(C2).rotate_left(R3).wrapping_mul(C1);
        h2 = h2.rotate_left(R2).wrapping_add(h1).wrapping_mul(M).wrapping_add(C4);
    }
    let read = rem.len();
    if read > 0 {
        let mut k1 = 0;
        let mut k2 = 0;
        if read >= 15 {
            k2 ^= (rem[14] as i64) << 48;
        }
        if read >= 14 {
            k2 ^= (rem[13] as i64) << 40;
        }
        if read >= 13 {
            k2 ^= (rem[12] as i64) << 32;
        }
        if read >= 12 {
            k2 ^= (rem[11] as i64) << 24;
        }
        if read >= 11 {
            k2 ^= (rem[10] as i64) << 16;
        }
        if read >= 10 {
            k2 ^= (rem[9] as i64) << 8;
        }
        if read >= 9 {
            k2 ^= rem[8] as i64;
            k2 = k2.wrapping_mul(C2).rotate_left(33).wrapping_mul(C1);
            h2 ^= k2;
        }
        if read >= 8 {
            k1 ^= (rem[7] as i64) << 56;
        }
        if read >= 7 {
            k1 ^= (rem[6] as i64) << 48;
        }
        if read >= 6 {
            k1 ^= (rem[5] as i64) << 40;
        }
        if read >= 5 {
            k1 ^= (rem[4] as i64) << 32;
        }
        if read >= 4 {
            k1 ^= (rem[3] as i64) << 24;
        }
        if read >= 3 {
            k1 ^= (rem[2] as i64) << 16;
        }
        if read >= 2 {
            k1 ^= (rem[1] as i64) << 8;
        }
        if read >= 1 {
            k1 ^= rem[0] as i64;
        }
        k1 = k1.wrapping_mul(C1).rotate_left(31).wrapping_mul(C2);
        h1 ^= k1;
    }

    h1 ^= source.len() as i64;
    h2 ^= source.len() as i64;
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    h1 = fmix64_i64(h1);
    h2 = fmix64_i64(h2);
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    (h1, h2)
}

#[allow(unused)]
pub fn old_modified_murmur3_cassandra_x64_128(source: &[u8], seed: u32) -> anyhow::Result<(i64, i64)> {
    const C1: i64 = -8_663_945_395_140_668_459_i64; // 0x87c3_7b91_1142_53d5;
    const C2: i64 = 0x4cf5_ad43_2745_937f;
    const C3: i64 = 0x52dc_e729;
    const C4: i64 = 0x3849_5ab5;
    const R1: u32 = 27;
    const R2: u32 = 31;
    const R3: u32 = 33;
    const M: i64 = 5;
    let mut h1: i64 = seed as i64;
    let mut h2: i64 = seed as i64;
    let mut chunks_iter = source.chunks_exact(16);
    let rem = chunks_iter.remainder();
    for chunk in chunks_iter {
        let k1 = i64::from_le_bytes(copy_into_array(&chunk[..8]));
        let k2 = i64::from_le_bytes(copy_into_array(&chunk[8..]));
        h1 ^= k1.wrapping_mul(C1).rotate_left(R2).wrapping_mul(C2);
        h1 = h1.rotate_left(R1).wrapping_add(h2).wrapping_mul(M).wrapping_add(C3);
        h2 ^= k2.wrapping_mul(C2).rotate_left(R3).wrapping_mul(C1);
        h2 = h2.rotate_left(R2).wrapping_add(h1).wrapping_mul(M).wrapping_add(C4);
    }
    let read = rem.len();
    if read > 0 {
        let mut k1 = 0;
        let mut k2 = 0;
        if read >= 15 {
            k2 ^= (rem[14] as i64) << 48;
        }
        if read >= 14 {
            k2 ^= (rem[13] as i64) << 40;
        }
        if read >= 13 {
            k2 ^= (rem[12] as i64) << 32;
        }
        if read >= 12 {
            k2 ^= (rem[11] as i64) << 24;
        }
        if read >= 11 {
            k2 ^= (rem[10] as i64) << 16;
        }
        if read >= 10 {
            k2 ^= (rem[9] as i64) << 8;
        }
        if read >= 9 {
            k2 ^= rem[8] as i64;
            k2 = k2.wrapping_mul(C2).rotate_left(33).wrapping_mul(C1);
            h2 ^= k2;
        }
        if read >= 8 {
            k1 ^= (rem[7] as i64) << 56;
        }
        if read >= 7 {
            k1 ^= (rem[6] as i64) << 48;
        }
        if read >= 6 {
            k1 ^= (rem[5] as i64) << 40;
        }
        if read >= 5 {
            k1 ^= (rem[4] as i64) << 32;
        }
        if read >= 4 {
            k1 ^= (rem[3] as i64) << 24;
        }
        if read >= 3 {
            k1 ^= (rem[2] as i64) << 16;
        }
        if read >= 2 {
            k1 ^= (rem[1] as i64) << 8;
        }
        if read >= 1 {
            k1 ^= rem[0] as i64;
        }
        k1 = k1.wrapping_mul(C1).rotate_left(31).wrapping_mul(C2);
        h1 ^= k1;
    }

    h1 ^= source.len() as i64;
    h2 ^= source.len() as i64;
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    h1 = fmix64_i64(h1);
    h2 = fmix64_i64(h2);
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    return Ok((h1, h2));
}

use std::ops::Shl;
#[allow(unused)]
pub fn old_murmur3_cassandra_x64_128<T: std::io::Read>(source: &mut T, seed: u32) -> std::io::Result<i64> {
    const C1: i64 = -8_663_945_395_140_668_459_i64; // 0x87c3_7b91_1142_53d5;
    const C2: i64 = 0x4cf5_ad43_2745_937f;
    const C3: i64 = 0x52dc_e729;
    const C4: i64 = 0x3849_5ab5;
    const R1: u32 = 27;
    const R2: u32 = 31;
    const R3: u32 = 33;
    const M: i64 = 5;
    let mut h1: i64 = seed as i64;
    let mut h2: i64 = seed as i64;
    let mut buf = [0; 16];
    let mut processed: usize = 0;
    loop {
        let read = source.read(&mut buf[..])?;
        processed += read;
        if read == 16 {
            let k1 = i64::from_le_bytes(copy_into_array(&buf[0..8]));
            let k2 = i64::from_le_bytes(copy_into_array(&buf[8..]));
            h1 ^= k1.wrapping_mul(C1).rotate_left(R2).wrapping_mul(C2);
            h1 = h1.rotate_left(R1).wrapping_add(h2).wrapping_mul(M).wrapping_add(C3);
            h2 ^= k2.wrapping_mul(C2).rotate_left(R3).wrapping_mul(C1);
            h2 = h2.rotate_left(R2).wrapping_add(h1).wrapping_mul(M).wrapping_add(C4);
        } else if read == 0 {
            h1 ^= processed as i64;
            h2 ^= processed as i64;
            h1 = h1.wrapping_add(h2);
            h2 = h2.wrapping_add(h1);
            h1 = fmix64_i64(h1);
            h2 = fmix64_i64(h2);
            h1 = h1.wrapping_add(h2);
            // This is the original output
            // h2 = h2.wrapping_add(h1);
            // let x = ((h2 as i128) << 64) | (h1 as u64 as i128);
            let x = h1 as i64;
            return Ok(x);
        } else {
            let mut k1 = 0;
            let mut k2 = 0;
            if read >= 15 {
                k2 ^= (buf[14] as i8 as i64).shl(48);
            }
            if read >= 14 {
                k2 ^= (buf[13] as i8 as i64).shl(40);
            }
            if read >= 13 {
                k2 ^= (buf[12] as i8 as i64).shl(32);
            }
            if read >= 12 {
                k2 ^= (buf[11] as i8 as i64).shl(24);
            }
            if read >= 11 {
                k2 ^= (buf[10] as i8 as i64).shl(16);
            }
            if read >= 10 {
                k2 ^= (buf[9] as i8 as i64).shl(8);
            }
            if read >= 9 {
                k2 ^= buf[8] as i8 as i64;
                k2 = k2.wrapping_mul(C2).rotate_left(33).wrapping_mul(C1);
                h2 ^= k2;
            }
            if read >= 8 {
                k1 ^= (buf[7] as i8 as i64).shl(56);
            }
            if read >= 7 {
                k1 ^= (buf[6] as i8 as i64).shl(48);
            }
            if read >= 6 {
                k1 ^= (buf[5] as i8 as i64).shl(40);
            }
            if read >= 5 {
                k1 ^= (buf[4] as i8 as i64).shl(32);
            }
            if read >= 4 {
                k1 ^= (buf[3] as i8 as i64).shl(24);
            }
            if read >= 3 {
                k1 ^= (buf[2] as i8 as i64).shl(16);
            }
            if read >= 2 {
                k1 ^= (buf[1] as i8 as i64).shl(8);
            }
            if read >= 1 {
                k1 ^= buf[0] as i8 as i64;
            }
            k1 = k1.wrapping_mul(C1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(C2);
            h1 ^= k1;
        }
    }
}

fn fmix64_i64(k: i64) -> i64 {
    const C1: u64 = 0xff51_afd7_ed55_8ccd;
    const C2: u64 = 0xc4ce_b9fe_1a85_ec53;
    const R: u32 = 33;
    let mut tmp = k as u64;
    tmp ^= tmp >> R;
    tmp = tmp.wrapping_mul(C1);
    tmp ^= tmp >> R;
    tmp = tmp.wrapping_mul(C2);
    tmp ^= tmp >> R;
    tmp as i64
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{
        BufRead,
        BufReader,
        Cursor,
    };
    #[test]
    fn test_tx_murmur3_token_generation() {
        let tx = "EHUHSJRCMDJSZUQMNLDBSRFC9O9XCI9SMHFWWHNDYOOOWMSOJQHCC9GFUEGECEVVXCSXYTHSRJ9TZ9999";
        let hash_pair = murmur3_cassandra_x64_128(tx.as_bytes(), 0);
        assert_eq!(hash_pair.0, -7733304998189415164);
    }

    #[test]
    fn test_address_murmur3_token_generation() {
        let addr = "NBBM9QWTLPXDQPISXWRJSMOKJQVHCIYBZTWPPAXJSRNRDWQOJDQNX9BZ9RQVLNVTOJBHKBDPP9NPGPGYAQGFDYOHLA";
        let hash_pair = murmur3_cassandra_x64_128(addr.as_bytes(), 0);
        assert_eq!(hash_pair.0, -5381343058315604526);
    }

    #[test]
    fn test_1000() {
        let mut file = BufReader::new(Cursor::new(std::include_str!("murmur3_tests.txt")));
        let mut buf = String::new();
        while let Ok(n) = file.read_line(&mut buf) {
            if n == 0 {
                break;
            }
            let mut vals = buf.split_whitespace();
            let key = vals.next().unwrap();
            let hash1 = vals.next().unwrap().parse::<i64>().unwrap();
            let hash2 = vals.next().unwrap().parse::<i64>().unwrap();
            let (h1, h2) = murmur3_cassandra_x64_128(key.as_bytes(), 0);
            assert_eq!((h1, h2), (hash1, hash2));
            buf.clear();
        }
    }

    #[test]
    fn perf_test() {
        struct Item {
            key: String,
            hash1: i64,
            hash2: i64,
        }

        let file_str = std::include_str!("murmur3_tests.txt");
        let n = 10000;
        let mut buf = String::new();
        let mut items: Vec<Item> = Vec::new();
        let mut file = BufReader::new(Cursor::new(file_str));
        while let Ok(read) = file.read_line(&mut buf) {
            if read == 0 {
                break;
            }
            let mut vals = buf.split_whitespace();
            let key = vals.next().unwrap();
            let hash1 = vals.next().unwrap().parse::<i64>().unwrap();
            let hash2 = vals.next().unwrap().parse::<i64>().unwrap();
            items.push(Item {
                key: key.to_string(),
                hash1,
                hash2,
            });
            buf.clear();
        }

        let now = std::time::SystemTime::now();
        for _ in 0..n {
            for item in &items {
                let (h1, h2) = murmur3_cassandra_x64_128(item.key.as_bytes(), 0);
                assert_eq!((h1, h2), (item.hash1, item.hash2));
            }
        }
        println!(
            "New Method: {} runs completed in {} ms",
            1000_i64 * n,
            now.elapsed().unwrap().as_millis()
        );

        let now = std::time::SystemTime::now();
        for _ in 0..n {
            for item in &items {
                let (h1, h2) = old_modified_murmur3_cassandra_x64_128(item.key.as_bytes(), 0).unwrap();
                assert_eq!((h1, h2), (item.hash1, item.hash2));
            }
        }
        println!(
            "Old Modified Method: {} runs completed in {} ms",
            1000_i64 * n,
            now.elapsed().unwrap().as_millis()
        );

        let now = std::time::SystemTime::now();
        for _ in 0..n {
            for item in &items {
                let mut key = Cursor::new(item.key.clone());
                let h1 = old_murmur3_cassandra_x64_128(&mut key, 0).unwrap();
                assert_eq!(h1, item.hash1);
            }
        }
        let mut total_time = now.elapsed().unwrap().as_millis();
        // We exclute the Cursor cloning time to compare the calculation speed
        let now = std::time::SystemTime::now();
        for _ in 0..n {
            for item in &items {
                let _ = Cursor::new(item.key.clone());
            }
        }
        total_time -= now.elapsed().unwrap().as_millis();
        println!("Old Method: {} runs completed in {} ms", 1000_i64 * n, total_time);
    }
}
