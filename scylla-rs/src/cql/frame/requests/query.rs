// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the query frame.

use super::*;

#[derive(Clone, Debug, Builder)]
#[builder(derive(Clone, Debug))]
#[builder(pattern = "owned", setter(strip_option))]
pub struct QueryFrame {
    pub(crate) statement: String,
    pub(crate) consistency: Consistency,
    #[builder(default)]
    pub(crate) flags: QueryFlags,
    #[builder(private, default)]
    pub(crate) values: Values,
    #[builder(default)]
    pub(crate) page_size: Option<i32>,
    #[builder(default)]
    pub(crate) paging_state: Option<Vec<u8>>,
    #[builder(default)]
    pub(crate) serial_consistency: Option<Consistency>,
    #[builder(default)]
    pub(crate) timestamp: Option<i64>,
}

impl QueryFrame {
    pub fn statement(&self) -> &String {
        &self.statement
    }

    pub fn consistency(&self) -> Consistency {
        self.consistency
    }

    pub fn flags(&self) -> QueryFlags {
        self.flags
    }

    pub fn values(&self) -> &Values {
        &self.values
    }

    pub fn page_size(&self) -> Option<i32> {
        self.page_size
    }

    pub fn paging_state(&self) -> &Option<Vec<u8>> {
        &self.paging_state
    }

    pub fn serial_consistency(&self) -> Option<Consistency> {
        self.serial_consistency
    }

    pub fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }

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
        let value_count = read_short(start, payload)?;
        let mut values = Values::default();
        for _ in 0..value_count {
            if flags.named_values() {
                values.push(Some(read_str(start, payload)?), read_bytes(start, payload)?);
            } else {
                values.push(None, read_bytes(start, payload)?);
            }
        }
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
        write_short(self.consistency as i16, payload);
        write_byte(self.flags.0, payload);
        if self.flags.values() {
            write_short(self.values.len() as i16, payload);
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
                write_short(serial_consistency as i16, payload);
            }
        }
        if let Some(timestamp) = self.timestamp {
            if self.flags.default_timestamp() {
                write_long(timestamp, payload);
            }
        }
    }
}

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
    use std::time::{
        SystemTime,
        UNIX_EPOCH,
    };
    #[test]
    // note: junk data
    fn simple_query_builder_test() {
        let frame = RequestFrame::from(
            QueryFrameBuilder::default()
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
                .unwrap(),
        );
        let mut payload = Vec::new();
        frame.to_payload(&mut payload);
    }
}
