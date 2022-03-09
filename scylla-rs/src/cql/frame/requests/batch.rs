// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the BATCH frame.

use super::*;

/**
   Allows executing a list of queries (prepared or not) as a batch (note that
   only DML statements are accepted in a batch). The body of the message must
   be:
   <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>]
   where:
   - <type> is a [byte] indicating the type of batch to use:
       - If <type> == 0, the batch will be "logged". This is equivalent to a
           normal CQL3 batch statement.
       - If <type> == 1, the batch will be "unlogged".
       - If <type> == 2, the batch will be a "counter" batch (and non-counter
           statements will be rejected).
   - <flags> is a [byte] whose bits define the options for this query and
       in particular influence what the remainder of the message contains. See [`BatchFlags`].
   - <n> is a [short] indicating the number of following queries.
   - <query_1>...<query_n> are the queries to execute. A <query_i> must be of the
       form:
       <kind><string_or_id><n>[<name_1>]<value_1>...[<name_n>]<value_n>
       where:
       - <kind> is a [byte] indicating whether the following query is a prepared
           one or not. <kind> value must be either 0 or 1.
       - <string_or_id> depends on the value of <kind>. If <kind> == 0, it should be
           a [long string] query string (as in QUERY, the query string might contain
           bind markers). Otherwise (that is, if <kind> == 1), it should be a
           [short bytes] representing a prepared query ID.
       - <n> is a [short] indicating the number (possibly 0) of following values.
       - <name_i> is the optional name of the following <value_i>. It must be present
           if and only if the 0x40 flag is provided for the batch.
       - <value_i> is the [value] to use for bound variable i (of bound variable <name_i>
           if the 0x40 flag is used).
   - <consistency> is the [consistency] level for the operation.
   - <serial_consistency> is only present if the 0x10 flag is set. In that case,
       <serial_consistency> is the [consistency] level for the serial phase of
       conditional updates. That consitency can only be either SERIAL or
       LOCAL_SERIAL and if not present will defaults to SERIAL. This option will
       be ignored for anything else other than a conditional update/insert.

   The server will respond with a RESULT message.
*/
#[derive(Clone, Debug, Builder)]
#[builder(derive(Clone, Debug))]
#[builder(pattern = "owned", setter(strip_option))]
pub struct BatchFrame {
    /// Batch type: 0 = logged, 1 = unlogged, 2 = counter
    #[builder(default)]
    pub(crate) batch_type: BatchType,
    /// The batch queries
    #[builder(private)]
    pub(crate) queries: Vec<BatchQuery>,
    /// Batch consistency
    pub(crate) consistency: Consistency,
    /// Batch flags
    #[builder(default)]
    pub(crate) flags: BatchFlags,
    /// Batch Serial consistency
    #[builder(default)]
    pub(crate) serial_consistency: Option<Consistency>,
    /// Batch timestamp
    #[builder(default)]
    pub(crate) timestamp: Option<i64>,
}

impl BatchFrame {
    /// Get the batch type.
    pub fn batch_type(&self) -> BatchType {
        self.batch_type
    }

    /// Get the batch queries.
    pub fn queries(&self) -> &Vec<BatchQuery> {
        &self.queries
    }

    /// Get the consistency.
    pub fn consistency(&self) -> Consistency {
        self.consistency
    }

    /// Get the flags.
    pub fn flags(&self) -> BatchFlags {
        self.flags
    }

    /// Get the serial consistency.
    pub fn serial_consistency(&self) -> Option<Consistency> {
        self.serial_consistency
    }

    /// Get the timestamp.
    pub fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }
}

impl FromPayload for BatchFrame {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        let batch_type = read_byte(start, payload)?.try_into()?;
        let query_count = read_short(start, payload)? as usize;
        let mut queries = Vec::with_capacity(query_count);
        for _ in 0..query_count {
            let query_kind = read_byte(start, payload)?;
            queries.push(match query_kind {
                0 => {
                    let statement = read_long_string(start, payload)?;
                    BatchQuery::Query {
                        statement,
                        values: read_values(start, payload)?,
                    }
                }
                1 => {
                    let id = read_prepared_id(start, payload)?;
                    BatchQuery::Prepared {
                        id,
                        values: read_values(start, payload)?,
                    }
                }
                _ => anyhow::bail!("Invalid query kind: {}", query_kind),
            });
        }
        let consistency = read_short(start, payload)?.try_into()?;
        let flags = BatchFlags(read_byte(start, payload)?);
        let serial_consistency = if flags.serial_consistency() {
            Some(read_short(start, payload)?.try_into()?)
        } else {
            None
        };
        let timestamp = if flags.default_timestamp() {
            Some(read_long(start, payload)?)
        } else {
            None
        };
        Ok(Self {
            batch_type,
            queries,
            consistency,
            flags,
            serial_consistency,
            timestamp,
        })
    }
}

impl ToPayload for BatchFrame {
    fn to_payload(self, payload: &mut Vec<u8>) {
        // add batch type
        write_byte(self.batch_type as u8, payload);
        // add query count
        write_short(self.queries.len() as u16, payload);
        for query in self.queries {
            match &query {
                BatchQuery::Query { statement, values: _ } => {
                    // add query flag
                    write_byte(0, payload);
                    // add query statement
                    write_long_string(statement, payload);
                }
                BatchQuery::Prepared { id, values: _ } => {
                    // add prepared flag
                    write_byte(1, payload);
                    // add prepared id
                    write_prepared_id(*id, payload);
                }
            }
            let (BatchQuery::Query { statement: _, values } | BatchQuery::Prepared { id: _, values }) = query;
            // add query values
            write_short(values.len() as u16, payload);
            payload.extend(values.payload());
        }
        // add consistency
        write_short(self.consistency as u16, payload);
        // add flags
        write_byte(self.flags.0, payload);
        // add serial consistency
        if let Some(consistency) = self.serial_consistency {
            if self.flags.serial_consistency() {
                write_short(consistency as u16, payload);
            }
        }
        if let Some(timestamp) = self.timestamp {
            if self.flags.default_timestamp() {
                write_long(timestamp, payload);
            }
        }
    }
}

/// A query in a batch.
#[derive(Clone, Debug)]
pub enum BatchQuery {
    /// An unprepared query
    Query {
        /// The query statement
        statement: String,
        /// The query bound values
        values: Values,
    },
    /// A prepared query
    Prepared {
        /// The query's prepared id
        id: [u8; 16],
        /// The query bound values
        values: Values,
    },
}

#[repr(u8)]
/// The batch type enum.
pub enum BatchTypes {
    /// The batch will be logged.
    Logged = 0,
    /// The batch will be unlogged.
    Unlogged = 1,
    /// The batch will be a "counter" batch.
    Counter = 2,
}

/// Gating type for batch type
#[derive(Copy, Clone, Debug)]
#[repr(u8)]
pub enum BatchType {
    /// The batch will be logged.
    Logged = 0,
    /// The batch will be unlogged.
    Unlogged = 1,
    /// The batch will be a "counter" batch.
    Counter = 2,
}

impl TryFrom<u8> for BatchType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Logged),
            1 => Ok(Self::Unlogged),
            2 => Ok(Self::Counter),
            _ => Err(anyhow::anyhow!("Invalid batch type: {}", value)),
        }
    }
}

impl Default for BatchType {
    fn default() -> Self {
        BatchType::Logged
    }
}

impl BatchFrameBuilder {
    /// Set the batch type to logged. See https://cassandra.apache.org/doc/latest/cql/dml.html#batch
    pub fn logged(mut self) -> Self {
        self.batch_type.replace(BatchType::Logged);
        self
    }
    /// Set the batch type to unlogged. See https://cassandra.apache.org/doc/latest/cql/dml.html#unlogged-batches
    pub fn unlogged(mut self) -> Self {
        self.batch_type.replace(BatchType::Unlogged);
        self
    }
    /// Set the batch type to counter. See https://cassandra.apache.org/doc/latest/cql/dml.html#counter-batches
    pub fn counter(mut self) -> Self {
        self.batch_type.replace(BatchType::Counter);
        self
    }

    /// Add a query statement to the Batch frame.
    pub fn statement(mut self, statement: &str) -> Self {
        if self.queries.is_none() {
            self.queries = Some(Vec::new());
        }
        let queries = self.queries.as_mut().unwrap();
        queries.push(BatchQuery::Query {
            statement: statement.to_string(),
            values: Default::default(),
        });
        self
    }
    /// Add a prepared id to the Batch frame.
    pub fn id(mut self, id: [u8; 16]) -> Self {
        if self.queries.is_none() {
            self.queries = Some(Vec::new());
        }
        let queries = self.queries.as_mut().unwrap();
        queries.push(BatchQuery::Prepared {
            id,
            values: Default::default(),
        });
        self
    }
}

#[allow(missing_docs)]
#[derive(Debug, Error)]
pub enum BatchBindError {
    #[error("No statements to bind values for")]
    NoStatements,
    #[error("Batch encode error: {0}")]
    EncodeError(#[from] anyhow::Error),
}

impl Binder for BatchFrameBuilder {
    type Error = BatchBindError;
    /// Set the value in the Batch frame.
    fn value<V: ColumnEncoder>(mut self, value: &V) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        if let Some(flags) = &mut self.flags {
            flags.set_named_values(false);
        } else {
            self.flags.replace(BatchFlags::default());
        }
        if let Some(query) = self.queries.as_mut().and_then(|q| q.last_mut()) {
            let value_buf = value.encode_new();
            match query {
                BatchQuery::Query { statement: _, values } | BatchQuery::Prepared { id: _, values } => {
                    values.push(None, value_buf.as_slice());
                }
            }
            Ok(self)
        } else {
            Err(BatchBindError::NoStatements)
        }
    }

    /// Set the value to be unset in the Batch frame.
    fn unset_value(mut self) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        if let Some(query) = self.queries.as_mut().and_then(|q| q.last_mut()) {
            match query {
                BatchQuery::Query { statement: _, values } | BatchQuery::Prepared { id: _, values } => {
                    values.push_unset(None);
                }
            }
            Ok(self)
        } else {
            Err(BatchBindError::NoStatements)
        }
    }

    /// Set the value to be null in the Batch frame.
    fn null_value(mut self) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        if let Some(query) = self.queries.as_mut().and_then(|q| q.last_mut()) {
            match query {
                BatchQuery::Query { statement: _, values } | BatchQuery::Prepared { id: _, values } => {
                    values.push_null(None);
                }
            }
            Ok(self)
        } else {
            Err(BatchBindError::NoStatements)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::Uncompressed;

    #[test]
    // note: junk data
    fn simple_query_builder_test() {
        let _payload = BatchFrameBuilder::default()
            .logged()
            .statement("INSERT_TX_QUERY")
            .value(&"HASH_VALUE")
            .unwrap()
            .value(&"PAYLOAD_VALUE")
            .unwrap()
            .id([0; 16]) // add second query(prepared one) to the batch
            .value(&"JUNK_VALUE")
            .unwrap() // junk value
            .consistency(Consistency::One)
            .build()
            .unwrap()
            .encode::<Uncompressed>()
            .unwrap();
    }
}
