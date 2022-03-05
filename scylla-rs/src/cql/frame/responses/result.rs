// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the information in result frame.

use super::*;
use scylla_parse::{
    CollectionType,
    CqlType,
    KeyspaceQualifyExt,
    NativeType,
};
use std::convert::TryFrom;

pub const VOID: i32 = 0x0001;
pub const ROWS: i32 = 0x0002;
pub const SETKEYSPACE: i32 = 0x0003;
pub const PREPARED: i32 = 0x0004;
pub const SCHEMACHANGE: i32 = 0x0005;

#[derive(Clone, Debug)]
pub struct ResultFrame {
    pub kind: ResultBodyKind,
}

impl ResultFrame {
    pub fn kind(&self) -> &ResultBodyKind {
        &self.kind
    }
}

impl TryInto<RowsResult> for ResultFrame {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<RowsResult, Self::Error> {
        match self.kind {
            ResultBodyKind::Rows(rows) => Ok(rows),
            _ => anyhow::bail!("Result Frame is not Rows Result"),
        }
    }
}

impl FromPayload for ResultFrame {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        let kind = read_int(start, payload)?.try_into()?;
        Ok(Self {
            kind: match kind {
                ResultKind::Void => ResultBodyKind::Void,
                ResultKind::Rows => ResultBodyKind::Rows(RowsResult::from_payload(start, payload)?),
                ResultKind::SetKeyspace => ResultBodyKind::SetKeyspace(read_string(start, payload)?),
                ResultKind::Prepared => ResultBodyKind::Prepared(PreparedResult::from_payload(start, payload)?),
                ResultKind::SchemaChange => {
                    ResultBodyKind::SchemaChange(SchemaChangeResult::from_payload(start, payload)?)
                }
            },
        })
    }
}

#[derive(Copy, Clone, Debug)]
#[repr(i32)]
pub enum ResultKind {
    Void = 0x0001,
    Rows = 0x0002,
    SetKeyspace = 0x0003,
    Prepared = 0x0004,
    SchemaChange = 0x0005,
}

impl TryFrom<i32> for ResultKind {
    type Error = anyhow::Error;

    fn try_from(kind: i32) -> anyhow::Result<Self> {
        match kind {
            VOID => Ok(Self::Void),
            ROWS => Ok(Self::Rows),
            SETKEYSPACE => Ok(Self::SetKeyspace),
            PREPARED => Ok(Self::Prepared),
            SCHEMACHANGE => Ok(Self::SchemaChange),
            _ => anyhow::bail!("Unknown result kind: {:x}", kind),
        }
    }
}

#[derive(Clone, Debug, From, TryInto)]
pub enum ResultBodyKind {
    Void,
    Rows(RowsResult),
    SetKeyspace(String),
    Prepared(PreparedResult),
    SchemaChange(SchemaChangeResult),
}

#[derive(Copy, Clone, Debug)]
pub struct ResultFlags(i32);

impl ResultFlags {
    pub fn global_tables_spec(&self) -> bool {
        self.0 & 0x0001 != 0
    }

    pub fn has_more_pages(&self) -> bool {
        self.0 & 0x0002 != 0
    }

    pub fn no_metadata(&self) -> bool {
        self.0 & 0x0004 != 0
    }
}

#[derive(Clone, Debug)]
pub struct GlobalTableSpec {
    keyspace: String,
    table: String,
}

impl FromPayload for GlobalTableSpec {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            keyspace: read_string(start, payload)?,
            table: read_string(start, payload)?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ColumnSpec {
    keyspace: Option<String>,
    table: Option<String>,
    name: String,
    kind: CqlType,
}

#[derive(Clone, Debug)]
pub struct RowsResultMetadata {
    flags: ResultFlags,
    columns_count: i32,
    paging_state: Option<Vec<u8>>,
    global_table_spec: Option<GlobalTableSpec>,
    column_specs: Option<Vec<ColumnSpec>>,
}

impl RowsResultMetadata {
    pub fn flags(&self) -> ResultFlags {
        self.flags
    }

    pub fn columns_count(&self) -> i32 {
        self.columns_count
    }

    pub fn paging_state(&self) -> &Option<Vec<u8>> {
        &self.paging_state
    }

    pub fn global_table_spec(&self) -> &Option<GlobalTableSpec> {
        &self.global_table_spec
    }

    pub fn column_specs(&self) -> &Option<Vec<ColumnSpec>> {
        &self.column_specs
    }
}

impl FromPayload for RowsResultMetadata {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        let flags = ResultFlags(read_int(start, payload)?);
        let columns_count = read_int(start, payload)?;
        let paging_state = if flags.has_more_pages() {
            Some(read_bytes(start, payload)?.to_vec())
        } else {
            None
        };
        let global_table_spec = if flags.global_tables_spec() {
            Some(GlobalTableSpec::from_payload(start, payload)?)
        } else {
            None
        };
        let column_specs = if columns_count > 0 {
            let mut column_specs = Vec::with_capacity(columns_count as usize);
            for _ in 0..columns_count {
                let (keyspace, table) = if !flags.global_tables_spec() {
                    (Some(read_string(start, payload)?), Some(read_string(start, payload)?))
                } else {
                    (None, None)
                };
                let name = read_string(start, payload)?;
                let kind = read_cql_type(start, payload)?;
                column_specs.push(ColumnSpec {
                    keyspace,
                    table,
                    name,
                    kind,
                });
            }
            Some(column_specs)
        } else {
            None
        };
        Ok(Self {
            flags,
            columns_count,
            paging_state,
            global_table_spec,
            column_specs,
        })
    }
}

fn read_cql_type(start: &mut usize, payload: &[u8]) -> anyhow::Result<CqlType> {
    let kind = read_short(start, payload)?;
    Ok(match kind {
        0x0000 => CqlType::Custom(read_string(start, payload)?.into()),
        0x0001 => CqlType::Native(NativeType::Ascii),
        0x0002 => CqlType::Native(NativeType::Bigint),
        0x0003 => CqlType::Native(NativeType::Blob),
        0x0004 => CqlType::Native(NativeType::Boolean),
        0x0005 => CqlType::Native(NativeType::Counter),
        0x0006 => CqlType::Native(NativeType::Decimal),
        0x0007 => CqlType::Native(NativeType::Double),
        0x0008 => CqlType::Native(NativeType::Float),
        0x0009 => CqlType::Native(NativeType::Int),
        0x000B => CqlType::Native(NativeType::Timestamp),
        0x000C => CqlType::Native(NativeType::Uuid),
        0x000D => CqlType::Native(NativeType::Varchar),
        0x000E => CqlType::Native(NativeType::Varint),
        0x000F => CqlType::Native(NativeType::Timeuuid),
        0x0010 => CqlType::Native(NativeType::Inet),
        0x0011 => CqlType::Native(NativeType::Date),
        0x0012 => CqlType::Native(NativeType::Time),
        0x0013 => CqlType::Native(NativeType::Smallint),
        0x0014 => CqlType::Native(NativeType::Tinyint),
        0x0020 => CqlType::Collection(Box::new(CollectionType::List(read_cql_type(start, payload)?))),
        0x0021 => CqlType::Collection(Box::new(CollectionType::Map(
            read_cql_type(start, payload)?,
            read_cql_type(start, payload)?,
        ))),
        0x0022 => CqlType::Collection(Box::new(CollectionType::Set(read_cql_type(start, payload)?))),
        0x0030 => {
            let res = CqlType::UserDefined(read_string(start, payload)?.dot(read_string(start, payload)?));
            let n = read_short(start, payload)? as usize;
            // TODO: Maybe use a self defined version of CqlType because this does not match the parser version
            // Couldn't be that convenient huh ¯\_(ツ)_/¯
            for _ in 0..n {
                read_string(start, payload)?;
                read_cql_type(start, payload)?;
            }
            res
        }
        0x0031 => {
            let n = read_short(start, payload)? as usize;
            let mut types = Vec::with_capacity(n);
            for _ in 0..n {
                types.push(read_cql_type(start, payload)?);
            }
            CqlType::Tuple(types)
        }
        _ => anyhow::bail!("Unknown CQL type {}", kind),
    })
}

#[derive(Clone)]
pub struct RowsResult {
    metadata: RowsResultMetadata,
    rows_count: i32,
    rows: Vec<u8>,
}

impl std::fmt::Debug for RowsResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowsResult")
            .field("metadata", &self.metadata)
            .field("rows_count", &self.rows_count)
            .finish()
    }
}

impl RowsResult {
    pub fn metadata(&self) -> &RowsResultMetadata {
        &self.metadata
    }

    pub fn rows_count(&self) -> i32 {
        self.rows_count
    }

    pub fn rows(&self) -> &[u8] {
        &self.rows
    }

    pub fn iter<R: RowDecoder>(&self) -> Iter<R> {
        Iter::new(&self)
    }

    pub fn into_iter<R: RowDecoder>(self) -> IntoIter<R> {
        IntoIter::new(self)
    }
}

impl FromPayload for RowsResult {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        let metadata = RowsResultMetadata::from_payload(start, payload)?;
        let rows_count = read_int(start, payload)?;
        let rows = &payload[*start..];
        *start += rows.len();
        Ok(Self {
            metadata,
            rows_count,
            rows: rows.to_vec(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct PreparedResultMetadata {
    flags: ResultFlags,
    columns_count: i32,
    pk_count: i32,
    pk_indexes: Option<Vec<i16>>,
    global_table_spec: Option<GlobalTableSpec>,
    column_specs: Option<Vec<ColumnSpec>>,
}

impl FromPayload for PreparedResultMetadata {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        let flags = ResultFlags(read_int(start, payload)?);
        let columns_count = read_int(start, payload)?;
        let pk_count = read_int(start, payload)?;
        let pk_indixes = if pk_count > 0 {
            let mut pk_indixes = Vec::with_capacity(pk_count as usize);
            for _ in 0..pk_count {
                pk_indixes.push(read_short(start, payload)?);
            }
            Some(pk_indixes)
        } else {
            None
        };
        let global_table_spec = if flags.global_tables_spec() {
            Some(GlobalTableSpec {
                keyspace: read_string(start, payload)?,
                table: read_string(start, payload)?,
            })
        } else {
            None
        };
        let column_specs = if columns_count > 0 {
            let mut column_specs = Vec::with_capacity(columns_count as usize);
            for _ in 0..columns_count {
                let (keyspace, table) = if !flags.global_tables_spec() {
                    (Some(read_string(start, payload)?), Some(read_string(start, payload)?))
                } else {
                    (None, None)
                };
                let name = read_string(start, payload)?;
                let kind = read_cql_type(start, payload)?;
                column_specs.push(ColumnSpec {
                    keyspace,
                    table,
                    name,
                    kind,
                });
            }
            Some(column_specs)
        } else {
            None
        };
        Ok(Self {
            flags,
            columns_count,
            pk_count,
            pk_indexes: pk_indixes,
            global_table_spec,
            column_specs,
        })
    }
}

#[derive(Clone, Debug)]
pub struct PreparedResult {
    id: [u8; 16],
    metadata: PreparedResultMetadata,
    result_metadata: Option<RowsResultMetadata>,
}

impl FromPayload for PreparedResult {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        let id = read_prepared_id(start, payload)?;
        let metadata = PreparedResultMetadata::from_payload(start, payload)?;
        let result_metadata = if !metadata.flags.no_metadata() {
            Some(RowsResultMetadata::from_payload(start, payload)?)
        } else {
            None
        };
        Ok(Self {
            id,
            metadata,
            result_metadata,
        })
    }
}

#[derive(Clone, Debug)]
pub struct SchemaChangeResult {
    change_type: SchemaChangeType,
    target: SchemaChangeTarget,
}

impl FromPayload for SchemaChangeResult {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        let change_type = SchemaChangeType::from_payload(start, payload)?;
        let target = SchemaChangeTarget::from_payload(start, payload)?;
        Ok(Self { change_type, target })
    }
}

impl FromPayload for SchemaChangeTarget {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        let target_type = read_string(start, payload)?;
        Ok(match target_type.as_str() {
            "KEYSPACE" => {
                let keyspace = read_string(start, payload)?;
                SchemaChangeTarget::Keyspace(keyspace)
            }
            "TABLE" => {
                let keyspace = read_string(start, payload)?;
                let table = read_string(start, payload)?;
                SchemaChangeTarget::Table { keyspace, table }
            }
            "TYPE" => {
                let keyspace = read_string(start, payload)?;
                let table = read_string(start, payload)?;
                SchemaChangeTarget::Type { keyspace, table }
            }
            "FUNCTION" => {
                let keyspace = read_string(start, payload)?;
                let name = read_string(start, payload)?;
                let args = read_string_list(start, payload)?;
                SchemaChangeTarget::Function { keyspace, name, args }
            }
            "AGGREGATE" => {
                let keyspace = read_string(start, payload)?;
                let name = read_string(start, payload)?;
                let args = read_string_list(start, payload)?;
                SchemaChangeTarget::Aggregate { keyspace, name, args }
            }
            _ => anyhow::bail!("Unknown schema change target type"),
        })
    }
}
