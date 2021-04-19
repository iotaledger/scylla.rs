// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::{Rows, *};

rows!(
    rows: Info,
    row: Row {
        data_center: String,
        tokens: Vec<String>,
    },
    row_into: Row
);
