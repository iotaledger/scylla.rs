// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use log::*;
use scylla::{
    query::Query,
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
    let mut scylla = Scylla::new("datacenter1", 8, Default::default());
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
    drop_keyspace(None).await.expect("Failed to drop keyspace");
    runtime.handle().shutdown().await;
    runtime
        .block_on()
        .await
        .expect("Runtime failed to shutdown gracefully!");
    let session = Arc::new(SessionBuilder::new().known_node_addr(node).build().await.unwrap());

    for (n, t, _) in timings.iter_mut() {
        match run_benchmark_scylla(&session, *n).await {
            Ok(time) => {
                *t = time;
                info!("Successfully ran scylla-rust-driver benchmark for {} queries", *n * 2);
            }
            Err(e) => error!("{}", e),
        }
    }
    drop_keyspace(Some(node)).await.expect("Failed to drop keyspace");
    info!("Timings:");
    info!("{:8} | {:^25} | {:^25} |", "", "scylla", "scylla-rs");
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
    format!(
        "CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{'class': 'NetworkTopologyStrategy', 'datacenter1': 1}}
        AND durable_writes = true",
        keyspace.name()
    )
    .parse::<CreateKeyspaceStatement>()
    .unwrap()
    .as_execute_query(&[])
    .consistency(Consistency::All)
    .build()?
    .get_local()
    .await
    .map_err(|e| anyhow::anyhow!("Could not verify if keyspace was created: {}", e))?;

    parse_statement!("DROP TABLE IF EXISTS scylla_example.test")
        .as_execute_query(&[])
        .consistency(Consistency::All)
        .build()?
        .get_local()
        .await
        .map_err(|e| anyhow::anyhow!("Could not verify if table was dropped: {}", e))?;

    parse_statement!(
        "CREATE TABLE IF NOT EXISTS scylla_example.test (
            key text PRIMARY KEY,
            data blob
        )"
    )
    .as_execute_query(&[])
    .consistency(Consistency::All)
    .build()?
    .get_local()
    .await
    .map_err(|e| anyhow::anyhow!("Could not verify if table was created: {}", e))?;

    keyspace.prepare_insert::<String, i32>().get_local().await?;
    keyspace.prepare_select::<String, (), i32>().get_local().await?;

    let start = SystemTime::now();
    let (sender, mut inbox) = unbounded_channel();
    for i in 0..n {
        let handle = sender.clone();
        let keyspace = keyspace.clone();
        tokio::task::spawn(async move {
            handle.send(
                keyspace
                    .insert_prepared(&format!("Key {}", i), &i)
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
        let keyspace = keyspace.clone();
        tokio::task::spawn(async move {
            handle.send((
                i,
                keyspace
                    .select::<i32>(&format!("Key {}", i), &())
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
    info!("Finished benchmark. Total time: {} ms", time / 1000000);
    Ok(time)
}

async fn run_benchmark_scylla(session: &Arc<Session>, n: i32) -> anyhow::Result<u128> {
    warn!("Initializing database");

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

    let prepared_insert = session
        .prepare("INSERT INTO scylla_example.test (key, data) VALUES (?, ?)")
        .await?;

    let mut query = Query::new("SELECT data FROM scylla_example.test WHERE key = ?".to_string());
    query.set_consistency(scylla::frame::types::Consistency::One);
    let prepared_select = session.prepare(query).await?;

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
    info!("Finished benchmark. Total time: {} ms", time / 1000000);
    Ok(time)
}

async fn drop_keyspace(node: Option<SocketAddr>) -> anyhow::Result<()> {
    if let Some(node) = node {
        let mut scylla = Scylla::default();
        scylla.insert_node(node);
        let runtime = Runtime::new(None, scylla).await.expect("Runtime failed to start!");
        parse_statement!("DROP KEYSPACE scylla_example")
            .as_execute_query(&[])
            .consistency(Consistency::All)
            .build()?
            .get_local()
            .await
            .map_err(|e| anyhow::anyhow!("Could not verify if keyspace was dropped: {}", e))?;
        runtime.handle().shutdown().await;
        runtime.block_on().await?;
    } else {
        parse_statement!("DROP KEYSPACE scylla_example")
            .as_execute_query(&[])
            .consistency(Consistency::All)
            .build()?
            .get_local()
            .await
            .map_err(|e| anyhow::anyhow!("Could not verify if keyspace was dropped: {}", e))?;
    }
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

impl ToString for MyKeyspace {
    fn to_string(&self) -> String {
        self.name.to_string()
    }
}

impl Insert<String, i32> for MyKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!("INSERT INTO test (key, data) VALUES (?, ?)").with_keyspace(self.name())
    }

    fn bind_values<T: Binder>(builder: T, key: &String, value: &i32) -> T {
        builder.value(key).value(value)
    }
}

impl Select<String, (), i32> for MyKeyspace {
    type QueryOrPrepared = PreparedStatement;

    fn statement(&self) -> SelectStatement {
        parse_statement!("SELECT data FROM test WHERE key = ?").with_keyspace(self.name())
    }

    fn bind_values<T: Binder>(builder: T, key: &String, _variables: &()) -> T {
        builder.value(key)
    }
}
