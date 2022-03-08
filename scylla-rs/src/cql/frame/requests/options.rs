// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module implements the OPTIONS frame.

/**
    Asks the server to return which STARTUP options are supported. The body of an
    OPTIONS message should be empty and the server will respond with a SUPPORTED
    message.
*/
#[derive(Copy, Clone, Debug)]
pub struct OptionsFrame;
