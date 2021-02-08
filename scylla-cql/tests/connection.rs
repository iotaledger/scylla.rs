// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use scylla_cql::connection::Cql;

#[tokio::test]
async fn establish_connection() {
    let cql = Cql::new()
        .address("172.17.0.2:19042".parse().unwrap())
        .shard_id(0)
        .tokens()
        .build()
        .await;
    assert!(cql.is_ok());
}
