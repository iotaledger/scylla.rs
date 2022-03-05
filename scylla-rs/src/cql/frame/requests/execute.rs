// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the execute frame.

use super::*;

#[derive(Clone, Debug, Builder)]
#[builder(derive(Clone, Debug))]
#[builder(pattern = "owned", setter(strip_option))]
pub struct ExecuteFrame {
    pub(crate) id: [u8; 16],
    pub(crate) consistency: Consistency,
    #[builder(default)]
    pub(crate) flags: QueryFlags,
    #[builder(private, default)]
    pub(crate) values: Vec<(Option<String>, Value)>,
    #[builder(default)]
    pub(crate) page_size: Option<i32>,
    #[builder(default)]
    pub(crate) paging_state: Option<Vec<u8>>,
    #[builder(default)]
    pub(crate) serial_consistency: Option<Consistency>,
    #[builder(default)]
    pub(crate) timestamp: Option<i64>,
}

impl ExecuteFrame {
    pub fn id(&self) -> &[u8; 16] {
        &self.id
    }

    pub fn consistency(&self) -> Consistency {
        self.consistency
    }

    pub fn flags(&self) -> QueryFlags {
        self.flags
    }

    pub fn values(&self) -> &Vec<(Option<String>, Value)> {
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
}

impl FromPayload for ExecuteFrame {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        let id = read_prepared_id(start, payload)?;
        let consistency = Consistency::try_from(read_short(start, payload)?)?;
        let flags = QueryFlags(read_byte(start, payload)?);
        let value_count = read_short(start, payload)?;
        let mut values = Vec::new();
        for _ in 0..value_count {
            if flags.named_values() {
                values.push((Some(read_string(start, payload)?), Value::from_payload(start, payload)?));
            } else {
                values.push((None, Value::from_payload(start, payload)?));
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
            id,
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

impl ToPayload for ExecuteFrame {
    fn to_payload(self, payload: &mut Vec<u8>) {
        payload.reserve(self.values.len() + self.paging_state.as_ref().map(|s| s.len()).unwrap_or_default() + 41);
        write_prepared_id(self.id, payload);
        write_short(self.consistency as i16, payload);
        write_byte(self.flags.0, payload);
        if self.flags.values() {
            write_short(self.values.len() as i16, payload);
            for (n, v) in self.values {
                if let Some(name) = n {
                    if self.flags.named_values() {
                        write_string(&name, payload);
                    }
                }
                v.to_payload(payload);
            }
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

impl From<QueryFrame> for ExecuteFrame {
    fn from(qf: QueryFrame) -> Self {
        Self {
            id: md5::compute(qf.statement().as_bytes()).into(),
            consistency: qf.consistency,
            flags: qf.flags,
            values: qf.values,
            page_size: qf.page_size,
            paging_state: qf.paging_state,
            serial_consistency: qf.serial_consistency,
            timestamp: qf.timestamp,
        }
    }
}

#[derive(Debug, Error)]
pub enum ExecuteBindError {
    #[error("Execute encode error: {0}")]
    EncodeError(#[from] anyhow::Error),
}

impl Binder for ExecuteFrameBuilder {
    type Error = ExecuteBindError;
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
        let mut value_buf = Vec::new();
        value.encode(&mut value_buf);
        if self.values.is_none() {
            self.values = Some(Vec::new());
        }
        let values = self.values.as_mut().unwrap();
        values.push((None, Value::Set(value_buf)));
        Ok(self)
    }
    /// Set the value to be unset in the query frame.
    fn unset_value(mut self) -> Result<Self, Self::Error> {
        // apply value
        if self.values.is_none() {
            self.values = Some(Vec::new());
        }
        let values = self.values.as_mut().unwrap();
        values.push((None, Value::Unset));
        Ok(self)
    }

    /// Set the value to be null in the query frame.
    fn null_value(mut self) -> Result<Self, Self::Error> {
        // apply value
        if self.values.is_none() {
            self.values = Some(Vec::new());
        }
        let values = self.values.as_mut().unwrap();
        values.push((None, Value::Null));
        Ok(self)
    }
}
