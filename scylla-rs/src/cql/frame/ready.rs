// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the ready frame.

use super::decoder::{Decoder, Frame};

pub struct Ready;

impl Ready {
    pub fn new() -> Self {
        Self
    }
}
