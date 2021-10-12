// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use anyhow::bail;
use backstage::prefab::websocket::*;
use futures::{
    SinkExt,
    StreamExt,
    TryStreamExt,
};
use log::*;
use scylla_rs::{
    app::cluster::Topology,
    prelude::*,
};
use std::{
    borrow::Cow,
    net::SocketAddr,
    sync::Arc,
    time::SystemTime,
};
use tokio::sync::{
    mpsc::{
        unbounded_channel,
        UnboundedSender,
    },
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
    let combinations = vec![10i32, 100, 1000, 10000]
        .into_iter()
        .map(|n| std::iter::repeat(n).take(4))
        .flatten()
        .zip(std::iter::repeat(vec![2u8, 4, 8, 16].into_iter()).flatten());

    let timings = combinations
        .map(|(n, r)| (n, r, Arc::new(Mutex::new(0u128))))
        .collect::<Vec<_>>();

    for (n, r, t) in timings.iter().cloned() {
        let runtime = Runtime::new(None, Scylla::new("datacenter1", num_cpus::get(), r, Default::default()))
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
            match run_benchmark_on_single_node(n, t, node).await {
                Ok(_) => info!("Successfully ran benchmark"),
                Err(e) => error!("{}", e),
            }
            handle.shutdown().await;
        });
        runtime.block_on().await.expect("runtime to gracefully shutdown")
    }
    info!("Timings:");
    info!("N\tR\tTime");
    for (n, r, t) in timings.iter() {
        info!("{}\t{}\t{}", n, r, *t.lock().await);
    }
}

async fn run_benchmark_on_single_node(n: i32, t: Arc<Mutex<u128>>, node: SocketAddr) -> anyhow::Result<()> {
    match init_database(n).await {
        Ok(time) => {
            *t.lock().await = time;
        }
        Err(e) => {
            error!("{}", e);
        }
    }
    Ok(())
}

async fn init_database(n: i32) -> anyhow::Result<u128> {
    warn!("Initializing database");
    let (sender, mut inbox) = unbounded_channel::<Result<(), WorkerError>>();
    let worker = BatchWorker::boxed(sender.clone());
    let token = 1;
    let keyspace = "scylla_example".to_string();
    let keyspace_statement = Query::new()
        .statement(&format!(
            "CREATE KEYSPACE IF NOT EXISTS {}
            WITH replication = {{'class': 'NetworkTopologyStrategy', 'datacenter1': 1}}
            AND durable_writes = true;",
            keyspace
        ))
        .consistency(Consistency::One)
        .build()?;
    send_local(token, keyspace_statement.0, worker).map_err(|e| {
        error!("{}", e);
        anyhow::anyhow!(e.to_string())
    })?;
    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(_) => (),
            Err(e) => bail!(e),
        }
    } else {
        bail!("Could not verify if keyspace was created!")
    }

    let worker = BatchWorker::boxed(sender.clone());
    let drop_statement = Query::new()
        .statement(&format!("DROP TABLE IF EXISTS {}.test;", keyspace))
        .consistency(Consistency::One)
        .build()?;
    send_local(token, drop_statement.0, worker).map_err(|e| {
        error!("{}", e);
        anyhow::anyhow!(e.to_string())
    })?;
    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(_) => (),
            Err(e) => bail!(e),
        }
    } else {
        bail!("Could not verify if table was dropped!")
    }

    let table_query = format!(
        "CREATE TABLE IF NOT EXISTS {}.test (
            key text PRIMARY KEY,
            data blob,
        );",
        keyspace
    );
    let worker = BatchWorker::boxed(sender.clone());
    let statement = Query::new()
        .statement(table_query.as_str())
        .consistency(Consistency::One)
        .build()?;
    send_local(token, statement.0, worker).map_err(|e| {
        error!("{}", e);
        anyhow::anyhow!(e.to_string())
    })?;
    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(_) => (),
            Err(e) => bail!(e),
        }
    } else {
        bail!("Could not verify if table was created!")
    }

    let keyspace = MyKeyspace::new();
    let worker = PrepareWorker::boxed(
        <MyKeyspace as Insert<String, i32>>::id(&keyspace),
        <MyKeyspace as Insert<String, i32>>::statement(&keyspace),
    );
    let Prepare(payload) = Prepare::new()
        .statement(&<MyKeyspace as Insert<String, i32>>::statement(&keyspace))
        .build()?;
    send_local(token, payload, worker).map_err(|e| {
        error!("{}", e);
        anyhow::anyhow!(e.to_string())
    })?;

    let worker = PrepareWorker::boxed(
        <MyKeyspace as Select<String, i32>>::id(&keyspace),
        <MyKeyspace as Select<String, i32>>::statement(&keyspace),
    );
    let Prepare(payload) = Prepare::new()
        .statement(&<MyKeyspace as Select<String, i32>>::statement(&keyspace))
        .build()?;
    send_local(token, payload, worker).map_err(|e| {
        error!("{}", e);
        anyhow::anyhow!(e.to_string())
    })?;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let start = SystemTime::now();
    for i in 0..n {
        keyspace
            .insert(&format!("Key {}", i), &i)
            .consistency(Consistency::One)
            .build()?
            .send_local()
            .map_err(|e| {
                error!("{}", e);
                anyhow::anyhow!(e.to_string())
            })?;
    }

    let (sender, mut inbox) = unbounded_channel::<Result<Option<_>, _>>();
    for i in 0..n {
        keyspace
            .select::<i32>(&format!("Key {}", i))
            .consistency(Consistency::One)
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
    let t = start.elapsed().unwrap().as_millis();
    info!(
        "Finished benchmark. Total time: {} ms",
        start.elapsed().unwrap().as_millis()
    );
    Ok(t)
}

#[derive(Debug)]
struct BatchWorker {
    sender: UnboundedSender<Result<(), WorkerError>>,
}

impl BatchWorker {
    pub fn boxed(sender: UnboundedSender<Result<(), WorkerError>>) -> Box<Self> {
        Box::new(Self { sender: sender.into() })
    }
}

impl Worker for BatchWorker {
    fn handle_response(self: Box<Self>, giveload: Vec<u8>) -> anyhow::Result<()> {
        self.sender.send(Ok(()))?;
        Ok(())
    }

    fn handle_error(self: Box<Self>, error: WorkerError, reporter: &ReporterHandle) -> anyhow::Result<()> {
        self.sender.send(Err(error))?;
        Ok(())
    }
}

#[derive(Default, Clone, Debug)]
pub struct MyKeyspace {
    pub name: Cow<'static, str>,
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
