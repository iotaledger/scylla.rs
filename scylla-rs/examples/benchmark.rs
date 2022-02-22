// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use log::*;
use scylla_rs::prelude::*;
use std::{
    net::SocketAddr,
    time::SystemTime,
};
use tokio::sync::mpsc::unbounded_channel;

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
    let node: SocketAddr = std::env::var("SCYLLA_NODE").map_or_else(
        |_| ([127, 0, 0, 1], 9042).into(),
        |n| {
            n.parse()
                .expect("Invalid SCYLLA_NODE env, use this format '127.0.0.1:19042' ")
        },
    );
    let combinations = vec![10i32, 100, 1000, 10000]
        .into_iter()
        .map(|n| std::iter::repeat(n).take(4))
        .flatten()
        .zip(std::iter::repeat(vec![2u8, 4, 8, 16].into_iter()).flatten());

    let mut timings = combinations.map(|(n, r)| (n, r, 0u128)).collect::<Vec<_>>();

    for (n, r, t) in timings.iter_mut() {
        let mut scylla = Scylla::new("datacenter1", *r, Default::default(), Some(CompressionType::Snappy));
        scylla.insert_node(node);
        scylla.insert_keyspace(KeyspaceConfig {
            name: "scylla_example".into(),
            data_centers: maplit::hashmap! {
                "datacenter1".to_string() => DatacenterConfig {replication_factor: 1}
            },
        });
        let runtime = Runtime::new(None, scylla).await.expect("Runtime failed to start!");
        match run_benchmark(*n).await {
            Ok(time) => {
                *t = time;
                info!("Successfully ran benchmark")
            }
            Err(e) => error!("{}", e),
        }
        runtime.handle().shutdown().await;
        runtime
            .block_on()
            .await
            .expect("Runtime failed to shutdown gracefully!");
    }
    drop_keyspace(node).await.expect("Failed to drop keyspace");
    info!("Timings:");
    info!("{:8}{:8}{:8}", "N", "R", "Time (ms)");
    for (n, r, t) in timings.iter() {
        info!("{:<8}{:<8}{:<8.2}", n * 2, r, *t as f32 / 1000000.);
    }
}

async fn run_benchmark(n: i32) -> anyhow::Result<u128> {
    warn!("Initializing database");

    let keyspace = MyKeyspace::new();

    parse_statement!(
        "CREATE KEYSPACE IF NOT EXISTS #ks
        WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}
        AND durable_writes = true",
        ks = keyspace.name()
    )
    .execute()
    .consistency(Consistency::All)
    .build()?
    .get_local()
    .await
    .map_err(|e| anyhow::anyhow!("Could not verify if keyspace was created: {}", e))?;

    parse_statement!("DROP TABLE IF EXISTS #.test", keyspace.name())
        .execute()
        .consistency(Consistency::All)
        .build()?
        .get_local()
        .await
        .map_err(|e| anyhow::anyhow!("Could not verify if table was dropped: {}", e))?;

    parse_statement!(
        "CREATE TABLE IF NOT EXISTS #.test (
            key text PRIMARY KEY,
            data blob
        )",
        keyspace.name()
    )
    .execute()
    .consistency(Consistency::All)
    .build()?
    .get_local()
    .await
    .map_err(|e| anyhow::anyhow!("Could not verify if table was created: {}", e))?;

    let insert = keyspace.insert_statement::<TestTable, TestTable>();
    let select = keyspace.select_statement::<TestTable, String, i32>();
    insert.prepare().get_local().await?;
    select.prepare().get_local().await?;

    let start = SystemTime::now();
    for i in 0..n {
        let key = format!("Key {}", i);
        insert
            .query_prepared()
            .bind_token(&key)?
            .bind(&i)?
            .build()?
            .send_local()
            .map_err(|e| {
                error!("{}", e);
                anyhow::anyhow!(e.to_string())
            })?;
    }

    let (sender, mut inbox) = unbounded_channel::<Result<Option<_>, _>>();
    for i in 0..n {
        let key = format!("Key {}", i);
        select
            .query_prepared::<i32>()
            .bind_token(&key)?
            .build()?
            .worker()
            .with_handle(sender.clone())
            .send_local()?;
    }
    drop(sender);
    while let Some(res) = inbox.recv().await {
        match res {
            Ok(_) => (),
            Err(e) => error!("Select error: {}", e),
        }
    }
    let time = start.elapsed().unwrap().as_nanos();
    info!("Finished benchmark. Total time: {} ms", time / 1000000);
    Ok(time)
}

async fn drop_keyspace(node: SocketAddr) -> anyhow::Result<()> {
    let mut scylla = Scylla::default();
    scylla.insert_node(node);
    let runtime = Runtime::new(None, scylla).await.expect("Runtime failed to start!");
    parse_statement!("DROP KEYSPACE scylla_example")
        .execute()
        .consistency(Consistency::All)
        .build()?
        .get_local()
        .await
        .map_err(|e| anyhow::anyhow!("Could not verify if keyspace was dropped: {}", e))?;
    runtime.handle().shutdown().await;
    runtime.block_on().await?;
    Ok(())
}

#[derive(Default, Clone, Debug)]
pub struct MyKeyspace {
    pub name: String,
}

impl MyKeyspace {
    pub fn new() -> Self {
        Self {
            name: "scylla_example".into(),
        }
    }
}

impl Keyspace for MyKeyspace {
    fn opts(&self) -> KeyspaceOpts {
        KeyspaceOptsBuilder::default()
            .replication(Replication::network_topology(
                btreemap! {"datacenter1".to_string() => 1},
            ))
            .durable_writes(true)
            .build()
            .unwrap()
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }
}

#[derive(Debug)]
pub struct TestTable {
    pub key: String,
    pub data: i32,
}

impl TestTable {
    pub fn new(key: String, data: i32) -> Self {
        Self { key, data }
    }
}

impl TableMetadata for TestTable {
    const NAME: &'static str = "test";
    const COLS: &'static [(&'static str, NativeType)] = &[("key", NativeType::Text), ("data", NativeType::Int)];
    const PARTITION_KEY: &'static [&'static str] = &["key"];
    const CLUSTERING_COLS: &'static [(&'static str, Order)] = &[];

    type PartitionKey = String;
    type PrimaryKey = String;

    fn partition_key(&self) -> &Self::PartitionKey {
        &self.key
    }

    fn primary_key(&self) -> &Self::PrimaryKey {
        &self.key
    }
}
impl Table for TestTable {}

impl TokenEncoder for TestTable {
    type Error = <<Self as TableMetadata>::PartitionKey as TokenEncoder>::Error;

    fn encode_token(&self) -> Result<TokenEncodeChain, Self::Error> {
        self.key.encode_token()
    }
}

impl Row for TestTable {
    fn try_decode_row<R: Rows + ColumnValue>(rows: &mut R) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            key: rows.column_value()?,
            data: rows.column_value()?,
        })
    }
}

impl Bindable for TestTable {
    fn bind<B: Binder>(&self, binder: B) -> Result<B, B::Error> {
        binder.bind(&self.key)?.bind(&self.data)
    }
}

impl Select<String, i32> for TestTable {
    fn statement(keyspace: &dyn Keyspace) -> SelectStatement {
        parse_statement!("SELECT data FROM #.test WHERE key = ?", keyspace.name())
    }
}
