// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This crates implements the uncompressed, LZ4, and snappy compression methods for Cassandra.

use serde::{
    Deserialize,
    Serialize,
};
use std::convert::TryInto;
use thiserror::Error;

/// This compression thread provides the buffer compression/decompression methods for uncompressed/Lz4/snappy.
pub trait Compression: Sync {
    /// The flag used in the frame for this compression, either 1 or 0
    const FLAG: u8;
    /// The compression type enum variant representation
    const KIND: Option<CompressionType>;
    /// Accepts a buffer with a header and decompresses it.
    fn decompress(mut buffer: Vec<u8>) -> Result<Vec<u8>, CompressionError> {
        if buffer.len() < 9 {
            return Err(CompressionError::SmallBuffer);
        }
        if buffer[1] & Self::FLAG == 0 {
            return Ok(buffer);
        }
        // Decompress the body
        if let Some(decompressed_buffer) = Self::decompress_body(&buffer[5..])? {
            buffer.resize(decompressed_buffer.len() + 5, 0);
            buffer[5..].copy_from_slice(&decompressed_buffer);
        }
        Ok(buffer)
    }
    /// Accepts a body buffer with four byte length prepended
    fn decompress_body(buffer: &[u8]) -> Result<Option<Vec<u8>>, CompressionError>;
    /// Accepts a buffer with a header and compresses it.
    fn compress(mut buffer: Vec<u8>) -> Result<Vec<u8>, CompressionError> {
        if buffer.len() < 9 {
            return Err(CompressionError::SmallBuffer);
        }
        if Self::FLAG == 0 {
            return Ok(buffer);
        }
        // Compress the body
        if let Some(compressed_buffer) = Self::compress_body(&buffer[5..])? {
            buffer[1] |= Self::FLAG;
            buffer.resize(compressed_buffer.len() + 5, 0);
            buffer[5..].copy_from_slice(&compressed_buffer);
        }
        Ok(buffer)
    }
    /// Accepts a body buffer with four byte length prepended
    fn compress_body(buffer: &[u8]) -> Result<Option<Vec<u8>>, CompressionError>;
}

/// The available compression types usable by scylla
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
    /// Snappy compression, provided by [`snap`](https://docs.rs/snap/latest/snap/)
    #[serde(rename = "snappy")]
    Snappy = 0,
    /// LZ4 compression, provided by [`lz4_flex`](https://docs.rs/lz4_flex/latest/lz4_flex/)
    #[serde(rename = "lz4")]
    Lz4 = 1,
}

impl core::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            CompressionType::Snappy => write!(f, "snappy"),
            CompressionType::Lz4 => write!(f, "lz4"),
        }
    }
}

#[allow(missing_docs)]
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
    const KIND: Option<CompressionType> = Some(CompressionType::Lz4);
    fn decompress_body(buffer: &[u8]) -> Result<Option<Vec<u8>>, CompressionError> {
        let size = i32::from_be_bytes(buffer[4..8].try_into().unwrap()) as usize;
        let mut body =
            lz4_flex::decompress(&buffer[8..], size).map_err(|e| CompressionError::BadDecompression(e.into()))?;
        body.extend(&i32::to_be_bytes(body.len() as i32));
        body.rotate_right(4);
        Ok(Some(body))
    }
    fn compress_body(buffer: &[u8]) -> Result<Option<Vec<u8>>, CompressionError> {
        let mut body = lz4_flex::compress_prepend_size(&buffer[4..]);
        // Don't use the compressed bytes if they're BIGGER than the uncompressed ones...
        if body.len() > buffer[4..].len() {
            return Ok(None);
        }
        body.extend(&i32::to_be_bytes(body.len() as i32));
        body.rotate_right(4);
        Ok(Some(body))
    }
}

/// Snappy unit structure which implements compression trait.
#[derive(Debug, Copy, Clone)]
pub struct Snappy;
impl Compression for Snappy {
    const FLAG: u8 = 1;
    const KIND: Option<CompressionType> = Some(CompressionType::Snappy);
    fn decompress_body(buffer: &[u8]) -> Result<Option<Vec<u8>>, CompressionError> {
        let mut body = snap::raw::Decoder::new()
            .decompress_vec(&buffer[4..])
            .map_err(|e| CompressionError::BadDecompression(e.into()))?;
        body.extend(&i32::to_be_bytes(body.len() as i32));
        body.rotate_right(4);
        Ok(Some(body))
    }
    fn compress_body(buffer: &[u8]) -> Result<Option<Vec<u8>>, CompressionError> {
        let mut body = snap::raw::Encoder::new()
            .compress_vec(&buffer[4..])
            .map_err(|e| CompressionError::BadCompression(e.into()))?;
        // Don't use the compressed bytes if they're BIGGER than the uncompressed ones...
        if body.len() > buffer[4..].len() {
            return Ok(None);
        }
        body.extend(&i32::to_be_bytes(body.len() as i32));
        body.rotate_right(4);
        Ok(Some(body))
    }
}
/// Uncompressed unit structure which implements compression trait.
#[derive(Debug, Copy, Clone)]
pub struct Uncompressed;
impl Compression for Uncompressed {
    const FLAG: u8 = 0;
    const KIND: Option<CompressionType> = None;
    fn decompress_body(_: &[u8]) -> Result<Option<Vec<u8>>, CompressionError> {
        Ok(None)
    }
    fn compress_body(_: &[u8]) -> Result<Option<Vec<u8>>, CompressionError> {
        Ok(None)
    }
}
