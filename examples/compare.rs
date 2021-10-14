// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use log::*;
use scylla::{
    query::Query,
    *,
};
use scylla_rs::prelude::*;
use std::{
    borrow::Cow,
    net::SocketAddr,
    sync::Arc,
    time::SystemTime,
};
use tokio::sync::{
    mpsc::unbounded_channel,
    Mutex,
};

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

    let timings = vec![10i32, 100, 1000, 10000]
        .into_iter()
        .map(|n| (n, Arc::new(Mutex::new(0u128)), Arc::new(Mutex::new(0u128))))
        .collect::<Vec<_>>();

    for (n, _, t) in timings.iter().cloned() {
        let runtime = Runtime::new(None, Scylla::new("datacenter1", num_cpus::get(), 8, Default::default()))
            .await
            .expect("runtime to run");
        let cluster_handle = runtime
            .handle()
            .cluster_handle()
            .await
            .expect("running scylla application");
        cluster_handle.add_node(node).await.expect("to add node");
        cluster_handle.build_ring(1).await.expect("to build ring");
        let handle = runtime.handle().clone();
        backstage::spawn_task("adding node task", async move {
            match run_benchmark_scylla_rs(n, t).await {
                Ok(_) => info!("Successfully ran scylla-rs benchmark for {} queries", n),
                Err(e) => error!("{}", e),
            }
            handle.shutdown().await;
        });
        runtime.block_on().await.expect("runtime to gracefully shutdown")
    }

    let session = Arc::new(
        SessionBuilder::new()
            .known_node_addr(([127, 0, 0, 1], 9042).into())
            .build()
            .await
            .unwrap(),
    );

    for (n, t, _) in timings.iter().cloned() {
        match run_benchmark_scylla(&session, n, t).await {
            Ok(_) => info!("Successfully ran scylla-rust-driver benchmark for {} queries", n),
            Err(e) => error!("{}", e),
        }
    }
    info!("Timings:");
    info!("{:8}{:8}{:10}{:8}", "queries", "scylla", "scylla-rs", "disparity");
    for (n, t1, t2) in timings.iter() {
        let (t1, t2) = (*t1.lock().await as i128, *t2.lock().await as i128);
        let mut diff = 100. * (t1 - t2) as f32 / f32::min(t1 as f32, t2 as f32);
        if t1 < t2 {
            diff = -diff;
        }
        info!("{:<8}{:<8}{:<10}{:<8}", n, t1, t2, format!("{:+.1}%", diff));
    }
}

async fn run_benchmark_scylla_rs(n: i32, t: Arc<Mutex<u128>>) -> anyhow::Result<()> {
    warn!("Initializing database");

    let keyspace = MyKeyspace::new();
    keyspace
        .execute_query(
            "CREATE KEYSPACE IF NOT EXISTS {{keyspace}}
            WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}
            AND durable_writes = true",
            &[],
        )
        .consistency(Consistency::All)
        .build()?
        .get_local()
        .await
        .map_err(|e| anyhow::anyhow!("Could not verify if keyspace was created: {}", e))?;

    keyspace
        .execute_query("DROP TABLE IF EXISTS {{keyspace}}.test", &[])
        .consistency(Consistency::All)
        .build()?
        .get_local()
        .await
        .map_err(|e| anyhow::anyhow!("Could not verify if table was dropped: {}", e))?;

    keyspace
        .execute_query(
            "CREATE TABLE IF NOT EXISTS {{keyspace}}.test (
                key text PRIMARY KEY,
                data blob,
            )",
            &[],
        )
        .consistency(Consistency::All)
        .build()?
        .get_local()
        .await
        .map_err(|e| anyhow::anyhow!("Could not verify if table was created: {}", e))?;

    keyspace.prepare_insert::<String, i32>().get_local().await?;
    keyspace.prepare_select::<String, i32>().get_local().await?;

    let start = SystemTime::now();
    for i in 0..n {
        keyspace
            .insert(&format!("Key {}", i), &i)
            .build()?
            .send_local()
            .map_err(|e| {
                error!("{}", e);
                anyhow::anyhow!(e.to_string())
            })?;
    }

    let (sender, mut inbox) = unbounded_channel::<Result<Option<_>, _>>();
    for i in 0..n {
        let handle = sender.clone();
        let keyspace = keyspace.clone();
        tokio::task::spawn(async move {
            handle.send(keyspace.select::<i32>(&format!("Key {}", i)).build()?.get_local().await)?;
            Result::<_, anyhow::Error>::Ok(())
        });
    }
    drop(sender);
    while let Some(res) = inbox.recv().await {
        match res {
            Ok(_) => (),
            Err(e) => error!("Select error: {}", e),
        }
    }
    let time = start.elapsed().unwrap().as_millis();
    info!(
        "Finished benchmark. Total time: {} ms",
        start.elapsed().unwrap().as_millis()
    );
    *t.lock().await = time;
    Ok(())
}

async fn run_benchmark_scylla(session: &Arc<Session>, n: i32, t: Arc<Mutex<u128>>) -> anyhow::Result<()> {
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
    for i in 0..n {
        session.execute(&prepared_insert, (&format!("Key {}", i), &i)).await?;
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
    while let Some((i, res)) = inbox.recv().await {
        match res {
            Ok(r) => {
                if r.rows.is_none() {
                    error!("No rows found for i = {}!", i)
                }
            }
            Err(e) => {
                error!("{}", e);
            }
        }
    }

    let time = start.elapsed().unwrap().as_millis();
    info!(
        "Finished benchmark. Total time: {} ms",
        start.elapsed().unwrap().as_millis()
    );
    *t.lock().await = time;
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
    fn statement(&self) -> Cow<'static, str> {
        format!("INSERT INTO {}.test (key, data) VALUES (?, ?)", self.name()).into()
    }

    fn bind_values<T: Values>(builder: T, key: &String, value: &i32) -> T::Return {
        builder.value(key).value(value)
    }
}

impl Select<String, i32> for MyKeyspace {
    type QueryOrPrepared = PreparedStatement;

    fn statement(&self) -> Cow<'static, str> {
        format!("SELECT data FROM {}.test WHERE key = ?", self.name()).into()
    }

    fn bind_values<T: Values>(builder: T, key: &String) -> T::Return {
        builder.value(key)
    }
}
