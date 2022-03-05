// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the cql error decoder.

use super::*;
use anyhow::bail;
use std::{
    convert::{
        TryFrom,
        TryInto,
    },
    fmt::Display,
};

#[derive(Error, Debug, Clone)]
/// The CQL error structure.
pub struct ErrorFrame {
    /// The Error code.
    pub(crate) code: ErrorCode,
    /// The message string.
    pub(crate) message: String,
    /// The additional Error information.
    pub(crate) additional: Option<Additional>,
}

impl Display for ErrorFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(additional) = &self.additional {
            write!(f, "{} ({})", self.message, additional)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

impl ErrorFrame {
    pub fn code(&self) -> ErrorCode {
        self.code
    }
    pub fn message(&self) -> &String {
        &self.message
    }
    pub fn additional(&self) -> &Option<Additional> {
        &self.additional
    }
    pub fn is_server_error(&self) -> bool {
        self.code == ErrorCode::ServerError
    }
    pub fn is_protocol_error(&self) -> bool {
        self.code == ErrorCode::ProtocolError
    }
    pub fn is_authentication_error(&self) -> bool {
        self.code == ErrorCode::AuthenticationError
    }
    pub fn is_unavailable_exception(&self) -> bool {
        self.code == ErrorCode::UnavailableException
    }
    pub fn is_overloaded(&self) -> bool {
        self.code == ErrorCode::Overloaded
    }
    pub fn is_bootstrapping(&self) -> bool {
        self.code == ErrorCode::IsBootstrapping
    }
    pub fn is_truncate_error(&self) -> bool {
        self.code == ErrorCode::TruncateError
    }
    pub fn is_write_timeout(&self) -> bool {
        self.code == ErrorCode::WriteTimeout
    }
    pub fn is_read_timeout(&self) -> bool {
        self.code == ErrorCode::ReadTimeout
    }
    pub fn is_read_failure(&self) -> bool {
        self.code == ErrorCode::ReadFailure
    }
    pub fn is_function_failure(&self) -> bool {
        self.code == ErrorCode::FunctionFailure
    }
    pub fn is_write_failure(&self) -> bool {
        self.code == ErrorCode::WriteFailure
    }
    pub fn is_syntax_error(&self) -> bool {
        self.code == ErrorCode::SyntaxError
    }
    pub fn is_unauthorized(&self) -> bool {
        self.code == ErrorCode::Unauthorized
    }
    pub fn is_invalid(&self) -> bool {
        self.code == ErrorCode::Invalid
    }
    pub fn is_config_error(&self) -> bool {
        self.code == ErrorCode::ConfigError
    }
    pub fn is_already_exists(&self) -> bool {
        self.code == ErrorCode::AlreadyExists
    }
    pub fn is_unprepared(&self) -> bool {
        self.code == ErrorCode::Unprepared
    }
    pub fn unprepared_id(&self) -> Option<[u8; 16]> {
        if let Some(Additional::Unprepared(u)) = &self.additional {
            Some(u.id)
        } else {
            None
        }
    }
}

impl FromPayload for ErrorFrame {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        let code = ErrorCode::try_from(read_int(start, payload)?)?;
        let message = read_string(start, payload)?;
        let additional = match code {
            ErrorCode::UnavailableException => Some(Additional::UnavailableException(
                UnavailableException::from_payload(start, payload)?,
            )),
            ErrorCode::WriteTimeout => Some(Additional::WriteTimeout(WriteTimeout::from_payload(start, payload)?)),
            ErrorCode::ReadTimeout => Some(Additional::ReadTimeout(ReadTimeout::from_payload(start, payload)?)),
            ErrorCode::ReadFailure => Some(Additional::ReadFailure(ReadFailure::from_payload(start, payload)?)),
            ErrorCode::FunctionFailure => Some(Additional::FunctionFailure(FunctionFailure::from_payload(
                start, payload,
            )?)),
            ErrorCode::WriteFailure => Some(Additional::WriteFailure(WriteFailure::from_payload(start, payload)?)),
            ErrorCode::AlreadyExists => Some(Additional::AlreadyExists(AlreadyExists::from_payload(start, payload)?)),
            ErrorCode::Unprepared => Some(Additional::Unprepared(Unprepared::from_payload(start, payload)?)),
            _ => None,
        };
        Ok(ErrorFrame {
            code,
            message,
            additional,
        })
    }
}

// ErrorCodes as consts
/// The Error code of `SERVER_ERROR`.
pub const SERVER_ERROR: i32 = 0x0000;
/// The Error code of `PROTOCOL_ERROR`.
pub const PROTOCOL_ERROR: i32 = 0x000A;
/// The Error code of `AUTHENTICATION_ERROR`.
pub const AUTHENTICATION_ERROR: i32 = 0x0100;
/// The Error code of `UNAVAILABLE_EXCEPTION`.
pub const UNAVAILABLE_EXCEPTION: i32 = 0x1000;
/// The Error code of `OVERLOADED`.
pub const OVERLOADED: i32 = 0x1001;
/// The Error code of `IS_BOOTSTRAPPING`.
pub const IS_BOOTSTRAPPING: i32 = 0x1002;
/// The Error code of `TRUNCATE_ERROR`.
pub const TRUNCATE_ERROR: i32 = 0x1003;
/// The Error code of `WRITE_TIMEOUT`.
pub const WRITE_TIMEOUT: i32 = 0x1100;
/// The Error code of `READ_TIMEOUT`.
pub const READ_TIMEOUT: i32 = 0x1200;
/// The Error code of `READ_FAILURE`.
pub const READ_FAILURE: i32 = 0x1300;
/// The Error code of `FUNCTION_FAILURE`.
pub const FUNCTION_FAILURE: i32 = 0x1400;
/// The Error code of `WRITE_FAILURE`.
pub const WRITE_FAILURE: i32 = 0x1500;
/// The Error code of `SYNTAX_ERROR`.
pub const SYNTAX_ERROR: i32 = 0x2000;
/// The Error code of `UNAUTHORIZED`.
pub const UNAUTHORIZED: i32 = 0x2100;
/// The Error code of `INVALID`.
pub const INVALID: i32 = 0x2200;
/// The Error code of `CONFIGURE_ERROR`.
pub const CONFIG_ERROR: i32 = 0x2300;
/// The Error code of `ALREADY_EXISTS`.
pub const ALREADY_EXISTS: i32 = 0x2400;
/// The Error code of `UNPREPARED`.
pub const UNPREPARED: i32 = 0x2500;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
/// The Error code enum.
pub enum ErrorCode {
    /// The Error code is `SERVER_ERROR`.
    ServerError = 0x0000,
    /// The Error code is `PROTOCOL_ERROR`.
    ProtocolError = 0x000A,
    /// The Error code is `AUTHENTICATION_ERROR`.
    AuthenticationError = 0x0100,
    /// The Error code is `UNAVAILABLE_EXCEPTION`.
    UnavailableException = 0x1000,
    /// The Error code is `OVERLOADED`.
    Overloaded = 0x1001,
    /// The Error code is `IS_BOOTSTRAPPING`.
    IsBootstrapping = 0x1002,
    /// The Error code is `TRUNCATE_ERROR`.
    TruncateError = 0x1003,
    /// The Error code is `WRITE_TIMEOUT`.
    WriteTimeout = 0x1100,
    /// The Error code is `READ_TIMEOUT`.
    ReadTimeout = 0x1200,
    /// The Error code is `READ_FAILURE`.
    ReadFailure = 0x1300,
    /// The Error code is `FUNCTION_FAILURE`.
    FunctionFailure = 0x1400,
    /// The Error code is `WRITE_FAILURE`.
    WriteFailure = 0x1500,
    /// The Error code is `SYNTAX_ERROR`.
    SyntaxError = 0x2000,
    /// The Error code is `UNAUTHORIZED`.
    Unauthorized = 0x2100,
    /// The Error code is `INVALID`.
    Invalid = 0x2200,
    /// The Error code is `CONFIG_ERROR`.
    ConfigError = 0x2300,
    /// The Error code is `ALREADY_EXISTS`.
    AlreadyExists = 0x2400,
    /// The Error code is `UNPREPARED`.
    Unprepared = 0x2500,
}

impl TryFrom<i32> for ErrorCode {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        Ok(match value {
            0x0000 => ErrorCode::ServerError,
            0x000A => ErrorCode::ProtocolError,
            0x0100 => ErrorCode::AuthenticationError,
            0x1000 => ErrorCode::UnavailableException,
            0x1001 => ErrorCode::Overloaded,
            0x1002 => ErrorCode::IsBootstrapping,
            0x1003 => ErrorCode::TruncateError,
            0x1100 => ErrorCode::WriteTimeout,
            0x1200 => ErrorCode::ReadTimeout,
            0x1300 => ErrorCode::ReadFailure,
            0x1400 => ErrorCode::FunctionFailure,
            0x1500 => ErrorCode::WriteFailure,
            0x2000 => ErrorCode::SyntaxError,
            0x2100 => ErrorCode::Unauthorized,
            0x2200 => ErrorCode::Invalid,
            0x2300 => ErrorCode::ConfigError,
            0x2400 => ErrorCode::AlreadyExists,
            0x2500 => ErrorCode::Unprepared,
            _ => anyhow::bail!("Unknown error code: {}", value),
        })
    }
}

#[derive(Clone, Error, Debug)]
#[error(transparent)]
/// The additional error information enum.
pub enum Additional {
    /// The additional error information is `UnavailableException`.
    UnavailableException(UnavailableException),
    /// The additional error information is `WriteTimeout`.
    WriteTimeout(WriteTimeout),
    /// The additional error information is `ReadTimeout`.
    ReadTimeout(ReadTimeout),
    /// The additional error information is `ReadFailure`.
    ReadFailure(ReadFailure),
    /// The additional error information is `FunctionFailure`.
    FunctionFailure(FunctionFailure),
    /// The additional error information is `WriteFailure`.
    WriteFailure(WriteFailure),
    /// The additional error information is `AlreadyExists`.
    AlreadyExists(AlreadyExists),
    /// The additional error information is `Unprepared`.
    Unprepared(Unprepared),
}
#[derive(Clone, Error, Debug)]
#[error("UnavailableException: consistency level: {cl}, required: {required}, alive: {alive}")]
/// The unavailable exception structure.
pub struct UnavailableException {
    /// The consistency level.
    pub cl: Consistency,
    /// The number of nodes that should be alive to respect the consistency levels.
    pub required: i32,
    /// The number of replicas that were known to be alive when the request had been processed.
    pub alive: i32,
}
impl FromPayload for UnavailableException {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            cl: Consistency::try_from(read_short(start, payload)?)?,
            required: read_int(start, payload)?,
            alive: read_int(start, payload)?,
        })
    }
}
#[derive(Clone, Error, Debug)]
#[error(
    "WriteTimeout: consistency level: {cl}, received: {received}, block for: {block_for}, write type: {write_type}"
)]
/// The additional error information, `WriteTimeout`, stucture.
pub struct WriteTimeout {
    /// The consistency level of the query having triggered the exception.
    pub cl: Consistency,
    /// Representing the number of nodes having acknowledged the request.
    pub received: i32,
    /// Representing the number of replicas whose acknowledgement is required to achieve `cl`.
    pub block_for: i32,
    /// That describe the type of the write that timed out.
    pub write_type: WriteType,
}
impl FromPayload for WriteTimeout {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            cl: Consistency::try_from(read_short(start, payload)?)?,
            received: read_int(start, payload)?,
            block_for: read_int(start, payload)?,
            write_type: WriteType::from_payload(start, payload)?,
        })
    }
}
#[derive(Clone, Error, Debug)]
#[error(
    "ReadTimeout: consistency level: {cl}, received: {received}, block for: {block_for}, data present: {data_present}"
)]
/// The additional error information, `ReadTimeout`, stucture.
pub struct ReadTimeout {
    /// The consistency level of the query having triggered the exception.
    pub cl: Consistency,
    /// Representing the number of nodes having answered the request.
    pub received: i32,
    /// Representing the number of replicas whose response is required to achieve `cl`.
    pub block_for: i32,
    /// If its value is 0, it means the replica that was asked for data has not responded.
    /// Otherwise, the value is != 0.
    pub data_present: u8,
}
impl ReadTimeout {
    /// Check whether the the replica that was asked for data had not responded.
    pub fn replica_had_not_responded(&self) -> bool {
        self.data_present == 0
    }
}
impl FromPayload for ReadTimeout {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            cl: Consistency::try_from(read_short(start, payload)?)?,
            received: read_int(start, payload)?,
            block_for: read_int(start, payload)?,
            data_present: read_byte(start, payload)?,
        })
    }
}
#[derive(Clone, Error, Debug)]
#[error("ReadFailure: consistency level: {cl}, received: {received}, block for: {block_for}, num failures: {num_failures}, data present: {data_present}")]
/// The additional error information, `ReadFailure`, stucture.
pub struct ReadFailure {
    /// The consistency level of the query having triggered the exception.
    pub cl: Consistency,
    /// Representing the number of nodes having answered the request.
    pub received: i32,
    /// Representing the number of replicas whose acknowledgement is required to
    /// achieve <cl>.
    pub block_for: i32,
    /// The number of nodes that experience a failure while executing the request.
    pub num_failures: i32,
    /// If its value is 0, it means the replica that was asked for data had not
    /// responded. Otherwise, the value is != 0.
    pub data_present: u8,
}
impl ReadFailure {
    /// Check whether the the replica that was asked for data had not responded.
    pub fn replica_had_not_responded(&self) -> bool {
        self.data_present == 0
    }
}
impl FromPayload for ReadFailure {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            cl: Consistency::try_from(read_short(start, payload)?)?,
            received: read_int(start, payload)?,
            block_for: read_int(start, payload)?,
            num_failures: read_int(start, payload)?,
            data_present: read_byte(start, payload)?,
        })
    }
}
#[derive(Clone, Error, Debug)]
#[error("FunctionFailure: keyspace: {keyspace}, function: {function}, arg_types: {arg_types:?}")]
/// The additional error information, `FunctionFailure`, stucture.
pub struct FunctionFailure {
    /// The keyspace of the failed function.
    pub keyspace: String,
    /// The name of the failed function.
    pub function: String,
    /// One string for each argument type (as CQL type) of the failed function.
    pub arg_types: Vec<String>,
}

impl FromPayload for FunctionFailure {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            keyspace: read_string(start, payload)?,
            function: read_string(start, payload)?,
            arg_types: read_string_list(start, payload)?,
        })
    }
}
#[derive(Clone, Error, Debug)]
#[error("WriteFailure: consistency level: {cl}, received: {received}, block for: {block_for}, num failures: {num_failures}, write type: {write_type}")]
/// The additional error information, `WriteFailure`, stucture.
pub struct WriteFailure {
    /// The consistency level of the query having triggered the exception.
    pub cl: Consistency,
    /// Representing the number of nodes having answered the request.
    pub received: i32,
    /// Representing the number of replicas whose acknowledgement is required to achieve `cl`.
    pub block_for: i32,
    /// Representing the number of nodes that experience a failure while executing the request.
    pub num_failures: i32,
    /// Describes the type of the write that timed out.
    pub write_type: WriteType,
}

impl FromPayload for WriteFailure {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            cl: Consistency::try_from(read_short(start, payload)?)?,
            received: read_int(start, payload)?,
            block_for: read_int(start, payload)?,
            num_failures: read_int(start, payload)?,
            write_type: WriteType::from_payload(start, payload)?,
        })
    }
}
#[derive(Clone, Error, Debug)]
#[error("AlreadyExists: keyspace: {ks}, table: {table}")]
/// The additional error information, `AlreadyExists`, stucture.
pub struct AlreadyExists {
    /// Representing either the keyspace that already exists, or the keyspace in which the table that
    /// already exists is.
    pub ks: String,
    /// Representing the name of the table that already exists. If the query was attempting to create a
    /// keyspace, <table> will be present but will be the empty string.
    pub table: String,
}

impl FromPayload for AlreadyExists {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            ks: read_string(start, payload)?,
            table: read_string(start, payload)?,
        })
    }
}
#[derive(Clone, Error, Debug)]
#[error("Unprepared: id: {id:?}")]
/// The additional error information, `Unprepared`, stucture.
pub struct Unprepared {
    /// The unprepared id.
    pub id: [u8; 16],
}

impl FromPayload for Unprepared {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            id: read_prepared_id(start, payload)?,
        })
    }
}
#[derive(Clone, Debug)]
/// The type of the write that timed out.
pub enum WriteType {
    /// Simple write type.
    Simple,
    /// Batch write type.
    Batch,
    /// UnloggedBatch write type.
    UnloggedBatch,
    /// Counter write type.
    Counter,
    /// BatchLog write type.
    BatchLog,
    /// Cas write type.
    Cas,
    /// View write type.
    View,
    /// Cdc write type.
    Cdc,
}

impl Display for WriteType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteType::Simple => write!(f, "SIMPLE"),
            WriteType::Batch => write!(f, "BATCH"),
            WriteType::UnloggedBatch => write!(f, "UNLOGGED_BATCH"),
            WriteType::Counter => write!(f, "COUNTER"),
            WriteType::BatchLog => write!(f, "BATCH_LOG"),
            WriteType::Cas => write!(f, "CAS"),
            WriteType::View => write!(f, "VIEW"),
            WriteType::Cdc => write!(f, "CDC"),
        }
    }
}

impl FromPayload for WriteType {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(match read_str(start, payload)? {
            "SIMPLE" => WriteType::Simple,
            "BATCH" => WriteType::Batch,
            "UNLOGGED_BATCH" => WriteType::UnloggedBatch,
            "COUNTER" => WriteType::Counter,
            "BATCH_LOG" => WriteType::BatchLog,
            "CAS" => WriteType::Cas,
            "VIEW" => WriteType::View,
            "CDC" => WriteType::Cdc,
            _ => bail!("unexpected write_type error"),
        })
    }
}

impl FromPayload for ErrorCode {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(read_int(start, payload)?.try_into()?)
    }
}
