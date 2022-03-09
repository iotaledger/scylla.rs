// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! This module defines the READY frame.

/**
   Indicates that the server is ready to process queries. This message will be
   sent by the server either after a [`StartupFrame`](super::StartupFrame) if no authentication is
   required (if authentication is required, the server indicates readiness by
   sending a [`AuthResponseFrame`](super::AuthResponseFrame)).

   The body of a READY message is empty.
*/
#[derive(Copy, Clone, Debug)]
pub struct ReadyFrame;
