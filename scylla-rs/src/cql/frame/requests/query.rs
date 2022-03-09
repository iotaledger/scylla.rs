// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the QUERY frame.

use super::*;

/**
    Performs a CQL query. The body of the message must be:

    `<query><query_parameters>`

    where <query> is a [long string] representing the query and

    `<query_parameters>` must be

    `<consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]`

    where:
    - `<consistency>` is the `[consistency]` level for the operation.
    - `<flags>` is a `[byte]` whose bits define the options for this query and
        in particular influence what the remainder of the message contains. See [`QueryFlags`].

    Note that the consistency is ignored by some queries (USE, CREATE, ALTER,
    TRUNCATE, ...).

    The server will respond to a QUERY message with a RESULT message, the content
    of which depends on the query.
*/
#[derive(Clone, Debug, Builder)]
#[builder(derive(Clone, Debug))]
#[builder(pattern = "owned", setter(strip_option))]
pub struct QueryFrame {
    /// The query statement
    pub(crate) statement: String,
    /// The consistency level
    pub(crate) consistency: Consistency,
    #[builder(default)]
    /// The query flags
    pub(crate) flags: QueryFlags,
    #[builder(private, default)]
    /// The bound values list
    pub(crate) values: Values,
    #[builder(default)]
    /// The page size
    pub(crate) page_size: Option<i32>,
    #[builder(default)]
    /// The paging state
    pub(crate) paging_state: Option<Vec<u8>>,
    #[builder(default)]
    /// The serial consistency level
    pub(crate) serial_consistency: Option<Consistency>,
    #[builder(default)]
    /// The timestamp
    pub(crate) timestamp: Option<i64>,
}

impl QueryFrame {
    /// Get the query statement.
    pub fn statement(&self) -> &String {
        &self.statement
    }

    /// Get the consistency level.
    pub fn consistency(&self) -> Consistency {
        self.consistency
    }

    /// Get the query flags.
    pub fn flags(&self) -> QueryFlags {
        self.flags
    }

    /// Get the bound values.
    pub fn values(&self) -> &Values {
        &self.values
    }

    /// Get the page size.
    pub fn page_size(&self) -> Option<i32> {
        self.page_size
    }

    /// Get the paging state.
    pub fn paging_state(&self) -> &Option<Vec<u8>> {
        &self.paging_state
    }

    /// Get the serial consistency level.
    pub fn serial_consistency(&self) -> Option<Consistency> {
        self.serial_consistency
    }

    /// Get the timestamp.
    pub fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    /// Convert into a QUERY frame from an EXECUTE frame using a given statement.
    pub fn from_execute(ef: ExecuteFrame, statement: String) -> Self {
        Self {
            statement,
            consistency: ef.consistency,
            flags: ef.flags,
            values: ef.values,
            page_size: ef.page_size,
            paging_state: ef.paging_state,
            serial_consistency: ef.serial_consistency,
            timestamp: ef.timestamp,
        }
    }
}

impl FromPayload for QueryFrame {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        let statement = read_long_string(start, payload)?;
        let consistency = Consistency::try_from(read_short(start, payload)?)?;
        let flags = QueryFlags(read_byte(start, payload)?);
        let values = if flags.named_values() {
            read_named_values(start, payload)?
        } else {
            read_values(start, payload)?
        };
        let page_size = if flags.page_size() {
            Some(read_int(start, payload)?)
        } else {
            None
        };
        let paging_state = if flags.paging_state() {
            Some(read_bytes(start, payload)?.to_vec())
        } else {
            None
        };
        let serial_consistency = if flags.serial_consistency() {
            Some(Consistency::try_from(read_short(start, payload)?)?)
        } else {
            None
        };
        let timestamp = if flags.default_timestamp() {
            Some(read_long(start, payload)?)
        } else {
            None
        };
        Ok(Self {
            statement,
            consistency,
            flags,
            values,
            page_size,
            paging_state,
            serial_consistency,
            timestamp,
        })
    }
}

impl ToPayload for QueryFrame {
    fn to_payload(self, payload: &mut Vec<u8>) {
        payload.reserve(
            self.statement.len()
                + self.values.payload().len()
                + self.paging_state.as_ref().map(|s| s.len()).unwrap_or_default()
                + 23,
        );
        write_long_string(&self.statement, payload);
        write_short(self.consistency as u16, payload);
        write_byte(self.flags.0, payload);
        if self.flags.values() {
            write_short(self.values.len() as u16, payload);
            payload.extend(self.values.payload());
        }
        if let Some(page_size) = self.page_size {
            if self.flags.page_size() {
                write_int(page_size, payload);
            }
        }
        if let Some(paging_state) = self.paging_state {
            if self.flags.paging_state() {
                write_bytes(&paging_state, payload);
            }
        }
        if let Some(serial_consistency) = self.serial_consistency {
            if self.flags.serial_consistency() {
                write_short(serial_consistency as u16, payload);
            }
        }
        if let Some(timestamp) = self.timestamp {
            if self.flags.default_timestamp() {
                write_long(timestamp, payload);
            }
        }
    }
}

#[allow(missing_docs)]
#[derive(Debug, Error)]
pub enum QueryBindError {
    #[error("Query encode error: {0}")]
    EncodeError(#[from] anyhow::Error),
}

impl Binder for QueryFrameBuilder {
    type Error = QueryBindError;
    /// Set the next value in the query frame.
    fn value<V: ColumnEncoder>(mut self, value: &V) -> Result<Self, Self::Error> {
        if let Some(flags) = &mut self.flags {
            flags.set_values(true);
            flags.set_named_values(false);
        } else {
            let mut flags = QueryFlags::default();
            flags.set_values(true);
            self.flags.replace(flags);
        }
        // apply value
        let value_buf = value.encode_new();
        if self.values.is_none() {
            self.values = Some(Values::default());
        }
        let values = self.values.as_mut().unwrap();
        values.push(None, value_buf.as_slice());
        Ok(self)
    }
    /// Set the value to be unset in the query frame.
    fn unset_value(mut self) -> Result<Self, Self::Error> {
        // apply value
        if self.values.is_none() {
            self.values = Some(Values::default());
        }
        let values = self.values.as_mut().unwrap();
        values.push_unset(None);
        Ok(self)
    }

    /// Set the value to be null in the query frame.
    fn null_value(mut self) -> Result<Self, Self::Error> {
        // apply value
        if self.values.is_none() {
            self.values = Some(Values::default());
        }
        let values = self.values.as_mut().unwrap();
        values.push_null(None);
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::Uncompressed;
    use std::time::{
        SystemTime,
        UNIX_EPOCH,
    };
    #[test]
    fn simple_query_builder_test() {
        let _payload = QueryFrameBuilder::default()
            .statement("INSERT_TX_QUERY".to_owned())
            .consistency(Consistency::One)
            .value(&"HASH_VALUE")
            .unwrap()
            .value(&"PAYLOAD_VALUE")
            .unwrap()
            .value(&"ADDRESS_VALUE")
            .unwrap()
            .value(&0_i64)
            .unwrap() // tx-value as i64
            .value(&"OBSOLETE_TAG_VALUE")
            .unwrap()
            .value(&SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs())
            .unwrap() // junk timestamp
            .value(&0)
            .unwrap() // current-index
            .unset_value()
            .unwrap() // not-set value for milestone
            .build()
            .unwrap()
            .encode::<Uncompressed>()
            .unwrap();
    }
}
