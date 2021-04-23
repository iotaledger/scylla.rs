// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::{
    cql::{
        frame::decoder::{ColumnDecoder, Frame},
        Decoder, Metadata, Rows,
    },
    rows,
};
use std::convert::TryInto;

rows!(
    rows: Info,
    row: Row {
        data_center: String,
        tokens: Vec<String>,
    },
    row_into: Row
);
