// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#[tokio::test]
async fn establish_connection_with_regular_cql_port() {
    use crate::cql::{
        compression::Uncompressed,
        Cql,
    };
    let cql = Cql::<Uncompressed>::new()
        .address(([172, 17, 0, 2], 9042).into())
        .shard_id(0)
        .tokens()
        .build()
        .await;
    assert!(cql.is_ok());
}

#[tokio::test]
async fn establish_connection_with_shard_aware_port() {
    use crate::cql::{
        compression::Uncompressed,
        Cql,
    };
    let cql = Cql::<Uncompressed>::new()
        .address(([172, 17, 0, 2], 19042).into())
        .shard_id(0)
        .tokens()
        .build()
        .await;
    assert!(cql.is_ok());
}
