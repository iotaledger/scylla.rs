// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the RESULT frame.

use super::*;
use scylla_parse::{
    CollectionType,
    CqlType,
    KeyspaceQualifyExt,
    NativeType,
};
use std::convert::TryFrom;

/// The result of a query ([`QueryFrame`], [`PrepareFrame`], [`ExecuteFrame`] or [`BatchFrame`]).
///
/// The first element of the body of a RESULT message is an `[int]` representing the
/// `kind` of result. The rest of the body depends on the kind.
#[derive(Clone, Debug)]
pub struct ResultFrame {
    /// The body kind
    pub kind: ResultBodyKind,
}

impl ResultFrame {
    /// Get the body kind
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

#[allow(missing_docs)]
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
            0x0001 => Ok(Self::Void),
            0x0002 => Ok(Self::Rows),
            0x0003 => Ok(Self::SetKeyspace),
            0x0004 => Ok(Self::Prepared),
            0x0005 => Ok(Self::SchemaChange),
            _ => anyhow::bail!("Unknown result kind: {:x}", kind),
        }
    }
}

/// The result body kind. Can be one of:
///     - `0x0001`: **Void**: result carrying no information.
///     - `0x0002`: **Rows**: the result of select queries, returning a set of rows.
///     - `0x0003`: **Set keyspace**: the result of a `use` query.
///     - `0x0004`: **Prepared**: result of a [`PrepareFrame`].
///     - `0x0005`: **Schema change**: the result to a schema altering query.
#[allow(missing_docs)]
#[derive(Clone, Debug, From, TryInto)]
pub enum ResultBodyKind {
    Void,
    Rows(RowsResult),
    SetKeyspace(String),
    Prepared(PreparedResult),
    SchemaChange(SchemaChangeResult),
}

/// Result flags. A flag is set if the bit corresponding to its `mask` is set.
/// Supported flags are, given their mask:
///     - `0x0001`: **Global tables spec:** if set, only one table spec (keyspace and table name) is provided as
///       `<global_table_spec>`. If not set, `<global_table_spec>` is not present.
///     - `0x0002`: **Has more pages:** indicates whether this is not the last page of results and more should be
///       retrieved. If set, the `<paging_state>` will be present. The `<paging_state>` is a `[bytes]` value that should
///       be used in QUERY/EXECUTE to continue paging and retrieve the remainder of the result for this query.
///     - `0x0004`: **No metadata:** if set, the `<metadata>` is only composed of these `<flags>`, the `<column_count>`
///       and optionally the `<paging_state>` (depending on the Has more pages flag) but no other information. This will
///       only ever be the case if this was requested during the query.
#[derive(Copy, Clone, Debug)]
#[repr(transparent)]
pub struct ResultFlags(i32);

impl ResultFlags {
    /// The global tables spec flag.
    pub const GLOBAL_TABLES_SPEC: i32 = 0x0001;
    /// The has more pages flag.
    pub const HAS_MORE_PAGES: i32 = 0x0002;
    /// The no metadata flag.
    pub const NO_METADATA: i32 = 0x0004;

    /// Get the global tables spec flag
    pub fn global_tables_spec(&self) -> bool {
        self.0 & Self::GLOBAL_TABLES_SPEC != 0
    }

    /// Get the has more pages flag
    pub fn has_more_pages(&self) -> bool {
        self.0 & Self::HAS_MORE_PAGES != 0
    }

    /// Get the no metadata flag
    pub fn no_metadata(&self) -> bool {
        self.0 & Self::NO_METADATA != 0
    }
}
#[allow(missing_docs)]
#[derive(Clone, Debug)]
pub struct GlobalTableSpec {
    pub keyspace: String,
    pub table: String,
}

impl GlobalTableSpec {
    /// Get the keyspace
    pub fn keyspace(&self) -> &String {
        &self.keyspace
    }

    /// Get the table
    pub fn table(&self) -> &String {
        &self.table
    }
}

impl FromPayload for GlobalTableSpec {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            keyspace: read_string(start, payload)?,
            table: read_string(start, payload)?,
        })
    }
}

/// Specifies a bind marker in a prepared statement.
#[derive(Clone, Debug)]
pub struct ColumnSpec {
    /// The keyspace, only present if the Global tables spec flag is not set.
    pub keyspace: Option<String>,
    /// The table, only present if the Global tables spec flag is not set.
    pub table: Option<String>,
    /// The name of the bind marker (if named), or the name of the column, field, or expression that the bind marker
    /// corresponds to (if the bind marker is "anonymous").
    pub name: String,
    /// The expected type of values for the bind marker
    pub cql_type: CqlType,
}

impl ColumnSpec {
    /// Get the keyspace
    pub fn keyspace(&self) -> &Option<String> {
        &self.keyspace
    }

    /// Get the table
    pub fn table(&self) -> &Option<String> {
        &self.table
    }

    /// Get the name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the cql type
    pub fn cql_type(&self) -> &CqlType {
        &self.cql_type
    }
}

#[derive(Clone, Debug)]
#[allow(missing_docs)]
pub struct RowsResultMetadata {
    pub flags: ResultFlags,
    /// The number of columns selected by the query that produced this result.
    pub columns_count: i32,
    pub paging_state: Option<Vec<u8>>,
    pub global_table_spec: Option<GlobalTableSpec>,
    pub column_specs: Option<Vec<ColumnSpec>>,
}

impl RowsResultMetadata {
    /// Get the flags
    pub fn flags(&self) -> ResultFlags {
        self.flags
    }

    /// Get the columns count
    pub fn columns_count(&self) -> i32 {
        self.columns_count
    }

    /// Get the paging state
    pub fn paging_state(&self) -> &Option<Vec<u8>> {
        &self.paging_state
    }

    /// Get the global table spec
    pub fn global_table_spec(&self) -> &Option<GlobalTableSpec> {
        &self.global_table_spec
    }

    /// Get the column specs
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
                    cql_type: kind,
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

/// Indicates a set of rows.
#[derive(Clone)]
#[allow(missing_docs)]
pub struct RowsResult {
    pub metadata: RowsResultMetadata,
    pub rows_count: i32,
    pub rows: Vec<u8>,
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
    /// Get the rows metadata
    pub fn metadata(&self) -> &RowsResultMetadata {
        &self.metadata
    }

    /// Get the number of rows
    pub fn rows_count(&self) -> i32 {
        self.rows_count
    }

    /// Get the rows buffer
    pub fn rows(&self) -> &[u8] {
        &self.rows
    }

    /// Get an iterator over the rows
    pub fn iter<R: RowDecoder>(&self) -> Iter<R> {
        Iter::new(&self)
    }

    /// Consume the result and get an iterator over the rows
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

#[allow(missing_docs)]
#[derive(Clone, Debug)]
pub struct PreparedResultMetadata {
    pub flags: ResultFlags,
    /// An `[int]` representing the number of bind markers in the prepared statement.
    pub columns_count: i32,
    /// Represents the indexes of the bind markers
    /// that corresponds to the partition key columns in the query.
    ///
    /// For example, a sequence of [2, 0, 1] indicates that the table has three partition key columns; the full
    /// partition key can be constructed by creating a composite of the values for the bind markers at index 2, at
    /// index 0, and at index 1. This allows implementations with token-aware routing to correctly
    /// construct the partition key without needing to inspect table metadata.
    pub pk_indexes: Vec<u16>,
    pub global_table_spec: Option<GlobalTableSpec>,
    pub column_specs: Option<Vec<ColumnSpec>>,
}

impl PreparedResultMetadata {
    /// Get the result flags
    pub fn flags(&self) -> &ResultFlags {
        &self.flags
    }

    /// Get the number of columns in the result
    pub fn columns_count(&self) -> i32 {
        self.columns_count
    }

    /// Get the primary key indexes list
    pub fn pk_indexes(&self) -> &Vec<u16> {
        &self.pk_indexes
    }

    /// Get the global table spec
    pub fn global_table_spec(&self) -> &Option<GlobalTableSpec> {
        &self.global_table_spec
    }

    /// Get the column specs
    pub fn column_specs(&self) -> &Option<Vec<ColumnSpec>> {
        &self.column_specs
    }
}

impl FromPayload for PreparedResultMetadata {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        let flags = ResultFlags(read_int(start, payload)?);
        let columns_count = read_int(start, payload)?;
        let pk_count = read_int(start, payload)?;
        let mut pk_indexes = Vec::with_capacity(pk_count as usize);
        for _ in 0..pk_count {
            pk_indexes.push(read_short(start, payload)?);
        }
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
                    cql_type: kind,
                });
            }
            Some(column_specs)
        } else {
            None
        };
        Ok(Self {
            flags,
            columns_count,
            pk_indexes,
            global_table_spec,
            column_specs,
        })
    }
}

/// The result of a [`PrepareFrame`].
///
/// Note that the prepared query ID returned is global to the node on which the query has been prepared.
#[derive(Clone, Debug)]
#[allow(missing_docs)]
pub struct PreparedResult {
    pub id: [u8; 16],
    pub metadata: PreparedResultMetadata,
    /// This describes the metadata for the result set that will be returned
    /// when this prepared statement is executed.
    pub result_metadata: Option<RowsResultMetadata>,
}

impl PreparedResult {
    /// Get the id of the prepared statement.
    pub fn id(&self) -> &[u8; 16] {
        &self.id
    }

    /// Get the metadata of the prepared statement.
    pub fn metadata(&self) -> &PreparedResultMetadata {
        &self.metadata
    }

    /// Get the metadata of the result set that will be returned when this
    /// prepared statement is executed.
    pub fn result_metadata(&self) -> &Option<RowsResultMetadata> {
        &self.result_metadata
    }
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

/// The result of a schema altering query (creation/update/drop of a
/// keyspace/table/index).
///
/// Note that a query to create or drop an index is considered to be a change
/// to the table the index is on.
#[derive(Clone, Debug)]
#[allow(missing_docs)]
pub struct SchemaChangeResult {
    pub change_type: SchemaChangeType,
    pub target: SchemaChangeTarget,
}

impl SchemaChangeResult {
    /// Get the change type
    pub fn change_type(&self) -> &SchemaChangeType {
        &self.change_type
    }

    /// Get the target
    pub fn target(&self) -> &SchemaChangeTarget {
        &self.target
    }
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
