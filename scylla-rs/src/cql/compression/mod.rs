// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This crates implements the uncompressed, LZ4, and snappy compression methods for Cassandra.

use std::convert::TryInto;

use serde::{
    Deserialize,
    Serialize,
};
use thiserror::Error;

/// This compression thread provides the buffer compression/decompression methods for uncompressed/Lz4/snappy.
pub trait Compression: Sync {
    const FLAG: u8;
    const KIND: Option<&'static str>;
    /// Accepts a buffer with a header and decompresses it.
    fn decompress(mut buffer: Vec<u8>) -> Result<Vec<u8>, CompressionError> {
        if buffer.len() < 9 {
            return Err(CompressionError::SmallBuffer);
        }
        if buffer[1] & Self::FLAG == 0 {
            return Ok(buffer);
        }
        // Decompress the body
        let decompressed_buffer = Self::decompress_body(&buffer[5..])?;
        buffer.resize(decompressed_buffer.len() + 5, 0);
        buffer[5..].copy_from_slice(&decompressed_buffer);
        Ok(buffer)
    }
    /// Accepts a body buffer with four byte length prepended
    fn decompress_body(buffer: &[u8]) -> Result<Vec<u8>, CompressionError>;
    /// Accepts a buffer with a header and compresses it.
    fn compress(mut buffer: Vec<u8>) -> Result<Vec<u8>, CompressionError> {
        if buffer.len() < 9 {
            return Err(CompressionError::SmallBuffer);
        }
        // Compress the body
        let compressed_buffer = Self::compress_body(&buffer[5..])?;
        buffer[1] |= Self::FLAG;
        buffer.resize(compressed_buffer.len() + 5, 0);
        buffer[5..].copy_from_slice(&compressed_buffer);
        Ok(buffer)
    }
    /// Accepts a body buffer with four byte length prepended
    fn compress_body(buffer: &[u8]) -> Result<Vec<u8>, CompressionError>;
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum CompressionType {
    #[serde(rename = "snappy")]
    Snappy,
    #[serde(rename = "lz4")]
    Lz4,
}

#[derive(Debug, Error)]
pub enum CompressionError {
    #[error("Failed to compress the frame: {0}")]
    BadCompression(anyhow::Error),
    #[error("Failed to decompress the frame: {0}")]
    BadDecompression(anyhow::Error),
    #[error("Buffer is too small")]
    SmallBuffer,
}
/// LZ4 unit structure which implements compression trait.
#[derive(Debug, Copy, Clone)]
pub struct Lz4;
impl Compression for Lz4 {
    const FLAG: u8 = 1;
    const KIND: Option<&'static str> = Some("lz4");
    fn decompress_body(buffer: &[u8]) -> Result<Vec<u8>, CompressionError> {
        let size = i32::from_be_bytes(buffer[4..8].try_into().unwrap());
        // lz4 will fail if we get a zero-sized body, just just skip it
        if size == 0 {
            return Ok(vec![0; 4]);
        }
        let mut body = lz4::block::decompress(&buffer[8..], Some(size))
            .map_err(|e| CompressionError::BadDecompression(e.into()))?;
        body.extend(&i32::to_be_bytes(body.len() as i32));
        body.rotate_right(4);
        Ok(body)
    }
    fn compress_body(buffer: &[u8]) -> Result<Vec<u8>, CompressionError> {
        let mut body =
            lz4::block::compress(&buffer[4..], None, true).map_err(|e| CompressionError::BadCompression(e.into()))?;
        body.extend(&i32::to_be_bytes(body.len() as i32));
        body.rotate_right(4);
        Ok(body)
    }
}

/// Snappy unit structure which implements compression trait.
#[derive(Debug, Copy, Clone)]
pub struct Snappy;
impl Compression for Snappy {
    const FLAG: u8 = 1;
    const KIND: Option<&'static str> = Some("snappy");
    fn decompress_body(buffer: &[u8]) -> Result<Vec<u8>, CompressionError> {
        let mut body = snap::raw::Decoder::new()
            .decompress_vec(&buffer[4..])
            .map_err(|e| CompressionError::BadDecompression(e.into()))?;
        body.extend(&i32::to_be_bytes(body.len() as i32));
        body.rotate_right(4);
        Ok(body)
    }
    fn compress_body(buffer: &[u8]) -> Result<Vec<u8>, CompressionError> {
        let mut body = snap::raw::Encoder::new()
            .compress_vec(&buffer[4..])
            .map_err(|e| CompressionError::BadCompression(e.into()))?;
        body.extend(&i32::to_be_bytes(body.len() as i32));
        body.rotate_right(4);
        Ok(body)
    }
}
/// Uncompressed unit structure which implements compression trait.
#[derive(Debug, Copy, Clone)]
pub struct Uncompressed;
impl Compression for Uncompressed {
    const FLAG: u8 = 0;
    const KIND: Option<&'static str> = None;
    fn decompress_body(buffer: &[u8]) -> Result<Vec<u8>, CompressionError> {
        Ok(buffer.to_vec())
    }
    fn compress_body(buffer: &[u8]) -> Result<Vec<u8>, CompressionError> {
        Ok(buffer.to_vec())
    }
}
