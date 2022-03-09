// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the STARTUP frame.

use super::*;
use std::collections::HashMap;

/**
   Initialize the connection. The server will respond by either a [`ReadyFrame`]
   (in which case the connection is ready for queries) or an [`AuthenticateFrame`]
   (in which case credentials will need to be provided using [`AuthResponseFrame`]).

   This must be the first message of the connection, except for [`OptionsFrame`] that can
   be sent before to find out the options supported by the server. Once the
   connection has been initialized, a client should not send any more STARTUP
   messages.

   The body is a `[string map]` of options. Possible options are:
   - `CQL_VERSION`: the version of CQL to use. This option is mandatory and
       currently the only version supported is "3.0.0". Note that this is
       different from the protocol version.
   - `COMPRESSION`: the compression algorithm to use for frames.
       This is optional; if not specified, no compression will be used.
   - `NO_COMPACT`: whether or not connection has to be established in compatibility
       mode. This mode will make all Thrift and Compact Tables to be exposed as if
       they were CQL Tables. This is optional; if not specified, the option will
       not be used.
   - `THROW_ON_OVERLOAD`: In case of server overloaded with too many requests, by default the server puts
        back pressure on the client connection. Instead, the server can send an OverloadedException error message back to
        the client if this option is set to true.
*/
#[derive(Clone, Debug, Builder)]
#[builder(derive(Clone, Debug))]
#[builder(pattern = "owned")]
pub struct StartupFrame {
    /// The requested options
    pub(crate) options: HashMap<String, String>,
}

impl FromPayload for StartupFrame {
    fn from_payload(start: &mut usize, payload: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            options: read_string_map(start, payload)?,
        })
    }
}

impl ToPayload for StartupFrame {
    fn to_payload(self, payload: &mut Vec<u8>) {
        write_string_map(&self.options, payload);
    }
}

impl StartupFrame {
    /// Create a new startup frame with an options map.
    pub fn new(options: HashMap<String, String>) -> Self {
        Self { options }
    }

    /// Get the options map.
    pub fn options(&self) -> &HashMap<String, String> {
        &self.options
    }
}

impl StartupFrameBuilder {
    /// Add an option to the options map.
    pub fn with_option(mut self, key: String, value: String) -> Self {
        match self.options {
            Some(ref mut options) => {
                options.insert(key, value);
            }
            None => {
                let mut options = HashMap::new();
                options.insert(key, value);
                self.options = Some(options);
            }
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::Uncompressed;

    #[test]
    fn simple_startup_builder_test() {
        let _payload = StartupFrameBuilder::default()
            .with_option("CQL_VERSION".to_string(), "3.0.0".to_string())
            .build()
            .unwrap()
            .encode::<Uncompressed>();
    }
}
