// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use log::*;
use scylla::{
    query::Query,
    transport::Compression,
    *,
};
use scylla_rs::prelude::*;
use std::{
    convert::TryInto,
    net::SocketAddr,
    sync::Arc,
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

    let mut timings = vec![10i32, 100, 1000, 10000, 100000]
        .into_iter()
        .map(|n| (n, 0u128, 0u128))
        .collect::<Vec<_>>();
    let mut scylla = Scylla::new("datacenter1", 8, Default::default(), Some(CompressionType::Lz4));
    scylla.insert_node(node);
    scylla.insert_keyspace(KeyspaceConfig {
        name: "scylla_example".into(),
        data_centers: maplit::hashmap! {
            "datacenter1".to_string() => DatacenterConfig {replication_factor: 1}
        },
    });
    let runtime = Runtime::new(None, scylla).await.expect("Runtime failed to start!");
    for (n, _, t) in timings.iter_mut() {
        match run_benchmark_scylla_rs(*n).await {
            Ok(time) => {
                *t = time;
                info!("Successfully ran scylla-rs benchmark for {} queries", *n * 2);
            }
            Err(e) => error!("{}", e),
        }
    }
    runtime.handle().shutdown().await;
    runtime
        .block_on()
        .await
        .expect("Runtime failed to shutdown gracefully!");
    let session = Arc::new(
        SessionBuilder::new()
            .known_node_addr(node)
            .compression(Some(Compression::Lz4))
            .build()
            .await
            .unwrap(),
    );

    for (n, t, _) in timings.iter_mut() {
        match run_benchmark_scylla(&session, *n).await {
            Ok(time) => {
                *t = time;
                info!("Successfully ran scylla-rust-driver benchmark for {} queries", *n * 2);
            }
            Err(e) => error!("{}", e),
        }
    }
    info!("Timings:");
    info!("{:8} | {:^25} | {:^25} |", "", "scylla-rust-driver", "scylla-rs");
    info!("{:8} | {:-<25} | {:-<25} |", "", "", "");
    info!(
        "{:8} | {:12} {:12} | {:12} {:12} | {:>10}",
        "queries", "total (ms)", "average (ns)", "total (ms)", "average (ns)", "disparity"
    );
    info!(
        "{:-<8} | {:-<12} {:-<12} | {:-<12} {:-<12} | {:-<8}",
        "", "", "", "", "", ""
    );
    for (n, t1, t2) in timings.iter() {
        let n = n * 2;
        let (t1, t2) = (*t1 as i128, *t2 as i128);
        let diff = 100. * (t1 - t2) as f32 / f32::min(t1 as f32, t2 as f32);
        let (avg1, avg2) = (t1 / n as i128, t2 / n as i128);
        info!(
            "{:<8} | {:<12} {:<12.4} | {:<12} {:<12.4} | {:>10}",
            n,
            t1 / 1000000,
            avg1,
            t2 / 1000000,
            avg2,
            format!("{:+.1}%", diff)
        );
    }
}

async fn run_benchmark_scylla_rs(n: i32) -> anyhow::Result<u128> {
    warn!("Initializing database");

    let keyspace = MyKeyspace::new();
    keyspace
        .drop()
        .execute()
        .consistency(Consistency::All)
        .build()?
        .get_local()
        .await
        .map_err(|e| anyhow::anyhow!("Could not verify if keyspace was dropped: {}", e))?;
    keyspace
        .create()
        .execute()
        .consistency(Consistency::All)
        .build()?
        .get_local()
        .await
        .map_err(|e| anyhow::anyhow!("Could not verify if keyspace was created: {}", e))?;

    TestTable::drop(&keyspace)
        .execute()
        .consistency(Consistency::All)
        .build()?
        .get_local()
        .await
        .map_err(|e| anyhow::anyhow!("Could not verify if table was dropped: {}", e))?;

    TestTable::create(&keyspace)
        .execute()
        .consistency(Consistency::All)
        .build()?
        .get_local()
        .await
        .map_err(|e| anyhow::anyhow!("Could not verify if table was created: {}", e))?;

    let insert = Arc::new(keyspace.insert_statement::<TestTable, TestTable>());
    let select = Arc::new(keyspace.select_statement::<TestTable, String, i32>());
    insert.prepare().get_local().await?;
    select.prepare().get_local().await?;

    let start = SystemTime::now();
    let (sender, mut inbox) = unbounded_channel();
    for i in 0..n {
        let handle = sender.clone();
        let insert = insert.clone();
        tokio::task::spawn(async move {
            let key = format!("Key {}", i);
            handle.send(
                insert
                    .query_prepared()
                    .bind_token(&key)?
                    .bind(&i)?
                    .build()?
                    .get_local()
                    .await,
            )?;
            Result::<_, anyhow::Error>::Ok(())
        });
    }
    drop(sender);
    let mut count = 0;
    while let Some(res) = inbox.recv().await {
        count += 1;
        if let Err(e) = res {
            error!("Insert error: {}", e);
        }
    }
    if count != n {
        anyhow::bail!("Did not receive all insert confirmations!");
    }

    let (sender, mut inbox) = unbounded_channel::<(_, Result<Option<_>, _>)>();
    for i in 0..n {
        let handle = sender.clone();
        let select = select.clone();
        tokio::task::spawn(async move {
            let key = format!("Key {}", i);
            handle.send((
                i,
                select
                    .query_prepared::<i32>()
                    .bind_token(&key)?
                    .build()?
                    .get_local()
                    .await,
            ))?;
            Result::<_, anyhow::Error>::Ok(())
        });
    }
    drop(sender);
    let mut count = 0;
    while let Some((i, res)) = inbox.recv().await {
        count += 1;
        match res {
            Ok(o) => {
                if let Some(v) = o {
                    if v != i {
                        anyhow::bail!("Got wrong value for key {}: {}", i, v);
                    }
                } else {
                    error!("No rows found for i = {}!", i)
                }
            }
            Err(e) => error!("Select error: {}", e),
        }
    }
    if count != n {
        anyhow::bail!("Did not receive all values!");
    }
    let time = start.elapsed().unwrap().as_nanos();
    keyspace
        .drop()
        .execute()
        .consistency(Consistency::All)
        .build()?
        .get_local()
        .await
        .map_err(|e| anyhow::anyhow!("Could not verify if keyspace was dropped: {}", e))?;
    info!("Finished benchmark. Total time: {} ms", time / 1000000);
    Ok(time)
}

async fn run_benchmark_scylla(session: &Arc<Session>, n: i32) -> anyhow::Result<u128> {
    warn!("Initializing database");

    let mut query = Query::new("DROP KEYSPACE IF EXISTS scylla_example".to_string());
    query.set_consistency(scylla::frame::types::Consistency::All);
    session
        .query(query, ())
        .await
        .map_err(|e| anyhow::anyhow!("Could not verify if keyspace was dropped: {}", e))?;

    let mut query = Query::new(
        "CREATE KEYSPACE IF NOT EXISTS scylla_example
        WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}
        AND durable_writes = true"
            .to_string(),
    );
    query.set_consistency(scylla::frame::types::Consistency::All);
    session
        .query(query, ())
        .await
        .map_err(|e| anyhow::anyhow!("Could not verify if keyspace was created: {}", e))?;

    let mut query = Query::new("DROP TABLE IF EXISTS scylla_example.test".to_string());
    query.set_consistency(scylla::frame::types::Consistency::All);
    session
        .query(query, ())
        .await
        .map_err(|e| anyhow::anyhow!("Could not verify if table was dropped: {}", e))?;

    let mut query = Query::new(
        "CREATE TABLE IF NOT EXISTS scylla_example.test (
            key text PRIMARY KEY,
            data blob,
        )"
        .to_string(),
    );
    query.set_consistency(scylla::frame::types::Consistency::All);
    session
        .query(query, ())
        .await
        .map_err(|e| anyhow::anyhow!("Could not verify if table was created: {}", e))?;

    let prepared_insert = Arc::new(
        session
            .prepare("INSERT INTO scylla_example.test (key, data) VALUES (?, ?)")
            .await?,
    );

    let mut query = Query::new("SELECT data FROM scylla_example.test WHERE key = ?".to_string());
    query.set_consistency(scylla::frame::types::Consistency::One);
    let prepared_select = Arc::new(session.prepare(query).await?);

    let start = SystemTime::now();

    let (sender, mut inbox) = unbounded_channel();
    for i in 0..n {
        let handle = sender.clone();
        let session = session.clone();
        let prepared_insert = prepared_insert.clone();
        tokio::task::spawn(async move {
            handle.send(session.execute(&prepared_insert, (&format!("Key {}", i), &i)).await)?;
            Result::<_, anyhow::Error>::Ok(())
        });
    }
    drop(sender);
    let mut count = 0;
    while let Some(res) = inbox.recv().await {
        count += 1;
        if let Err(e) = res {
            error!("Insert error: {}", e);
        }
    }
    if count != n {
        anyhow::bail!("Did not receive all insert confirmations!");
    }

    let (sender, mut inbox) = tokio::sync::mpsc::unbounded_channel();
    for i in 0..n {
        let handle = sender.clone();
        let session = session.clone();
        let prepared_select = prepared_select.clone();
        tokio::task::spawn(async move {
            handle.send((i, session.execute(&prepared_select, (&format!("Key {}", i),)).await))
        });
    }
    drop(sender);
    let mut count = 0;
    while let Some((i, res)) = inbox.recv().await {
        count += 1;
        match res {
            Ok(r) => {
                if let Some(v) = r.rows.and_then(|r| r.into_iter().next()) {
                    let v = i32::from_be_bytes(
                        v.into_typed::<(Vec<u8>,)>()?
                            .0
                            .try_into()
                            .map_err(|_| anyhow::anyhow!("Could not decode blob!"))?,
                    );
                    if v != i {
                        anyhow::bail!("Got wrong value for key {}: {}", i, v);
                    }
                } else {
                    error!("No rows found for i = {}!", i)
                }
            }
            Err(e) => {
                error!("{}", e);
            }
        }
    }
    if count != n {
        anyhow::bail!("Did not receive all values!");
    }

    let time = start.elapsed().unwrap().as_nanos();
    let mut query = Query::new("DROP KEYSPACE IF EXISTS scylla_example".to_string());
    query.set_consistency(scylla::frame::types::Consistency::All);
    session
        .query(query, ())
        .await
        .map_err(|e| anyhow::anyhow!("Could not verify if keyspace was dropped: {}", e))?;
    info!("Finished benchmark. Total time: {} ms", time / 1000000);
    Ok(time)
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
    fn encode_token(&self) -> TokenEncodeChain {
        self.key.encode_token()
    }
}

impl RowDecoder for TestTable {
    fn try_decode_row(mut row: ResultRow) -> anyhow::Result<Self> {
        Ok(Self {
            key: row.decode_column()?,
            data: row.decode_column()?,
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
