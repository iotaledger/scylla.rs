// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use anyhow::bail;
use backstage::prelude::*;
use futures::FutureExt;
use log::*;
use scylla_rs::prelude::{stage::Reporter, *};
use std::{borrow::Cow, marker::PhantomData, sync::Arc, time::SystemTime};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedSender},
    Mutex,
};

#[tokio::main]
async fn main() {
    std::panic::set_hook(Box::new(|info| {
        log::error!("{}", info);
    }));
    std::env::set_var("RUST_LOG", "debug");
    // start the logger
    env_logger::init();

    let combinations = vec![10i32, 100, 1000, 10000]
        .into_iter()
        .map(|n| std::iter::repeat(n).take(4))
        .flatten()
        .zip(std::iter::repeat(vec![2u8, 4, 8, 16].into_iter()).flatten());

    let timings = combinations
        .map(|(n, r)| (n, r, Arc::new(Mutex::new(0u128))))
        .collect::<Vec<_>>();

    RuntimeScope::<ActorRegistry>::launch(move |scope| {
        async move {
            let scylla_builder = ScyllaBuilder::new()
                .listen_address(([127, 0, 0, 1], 8080).into())
                .thread_count(num_cpus::get())
                .local_dc("datacenter1".to_owned());
            for (n, r, t) in timings.iter().cloned() {
                let scylla = scylla_builder.clone().reporter_count(r).build();
                scope
                    .scope(|scope| {
                        async move {
                            warn!("Spawning scylla with ({}, {})", n, r);
                            let (_, mut handle) = scope.spawn_actor_unsupervised(scylla).await;
                            let ws = format!("ws://{}/", "127.0.0.1:8080");
                            let nodes = vec![([127, 0, 0, 1], 9042).into()];
                            match add_nodes(&ws, nodes, 1).await {
                                Ok(_) => match init_database(n).await {
                                    Ok(time) => {
                                        *t.lock().await = time;
                                    }
                                    Err(e) => {
                                        error!("{}", e);
                                        //scope.print_root().await;
                                    }
                                },
                                Err(e) => {
                                    error!("{}", e);
                                    //scope.print_root().await;
                                }
                            }
                            handle.send(ScyllaEvent::Shutdown).await.ok();
                            handle.into_inner().into_inner().closed().await;
                        }
                        .boxed()
                    })
                    .await
                    .ok();
                log::debug!("\n{}", scope.service_tree().await);
            }
            info!("Timings:");
            info!("N\tR\tTime");
            for (n, r, t) in timings.iter() {
                info!("{}\t{}\t{}", n, r, *t.lock().await);
            }
        }
        .boxed()
    })
    .await
    .unwrap();
}

async fn ctrl_c(mut sender: TokioSender<ScyllaEvent>) {
    tokio::signal::ctrl_c().await.unwrap();
    sender.send(ScyllaEvent::Shutdown).await.ok();
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
    send_local(token, keyspace_statement.0, worker, keyspace.clone());
    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(_) => (),
            Err(e) => bail!(e),
        }
    } else {
        bail!("Could not verify if keyspace was created!")
    }
    warn!("Dropping table");
    let worker = BatchWorker::boxed(sender.clone());
    let drop_statement = Query::new()
        .statement(&format!("DROP TABLE IF EXISTS {}.test;", keyspace))
        .consistency(Consistency::One)
        .build()?;
    send_local(token, drop_statement.0, worker, keyspace.clone());
    if let Some(msg) = inbox.recv().await {
        match msg {
            Ok(_) => (),
            Err(e) => bail!(e),
        }
    } else {
        bail!("Could not verify if table was dropped!")
    }

    warn!("Creating table");

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
    send_local(token, statement.0, worker, keyspace.clone());
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
    send_local(token, payload, worker, keyspace.name().to_string());

    let worker = PrepareWorker::boxed(
        <MyKeyspace as Select<String, i32>>::id(&keyspace),
        <MyKeyspace as Select<String, i32>>::statement(&keyspace),
    );
    let Prepare(payload) = Prepare::new()
        .statement(&<MyKeyspace as Select<String, i32>>::statement(&keyspace))
        .build()?;
    send_local(token, payload, worker, keyspace.name().to_string());

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let start = SystemTime::now();
    for i in 0..n {
        let worker = InsertWorker::boxed(keyspace.clone(), format!("Key {}", i), i, 3);
        let request = keyspace
            .insert(&format!("Key {}", i), &i)
            .consistency(Consistency::One)
            .build()?;
        request.send_local(worker);
    }

    let (sender, mut inbox) = unbounded_channel();
    for i in 0..n {
        let worker = ValueWorker::boxed(
            sender.clone(),
            keyspace.clone(),
            format!("Key {}", i),
            2,
            PhantomData::<i32>,
        );
        let request = keyspace
            .select(&format!("Key {}", i))
            .consistency(Consistency::One)
            .build()?;
        request.send_local(worker);
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

struct BatchWorker {
    sender: UnboundedSender<Result<(), WorkerError>>,
}

impl BatchWorker {
    pub fn boxed(sender: UnboundedSender<Result<(), WorkerError>>) -> Box<Self> {
        Box::new(Self { sender: sender.into() })
    }
}

impl Worker for BatchWorker {
    fn handle_response(self: Box<Self>, _giveload: Vec<u8>) -> anyhow::Result<()> {
        self.sender.send(Ok(()))?;
        Ok(())
    }

    fn handle_error(
        self: Box<Self>,
        error: WorkerError,
        _reporter: Option<&mut UnboundedSender<<Reporter as Actor>::Event>>,
    ) -> anyhow::Result<()> {
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

impl Keyspace for MyKeyspace {
    fn name(&self) -> &Cow<'static, str> {
        &self.name
    }
}

impl VoidDecoder for MyKeyspace {}

impl RowsDecoder<String, i32> for MyKeyspace {
    type Row = i32;
    fn try_decode(decoder: Decoder) -> anyhow::Result<Option<i32>> {
        anyhow::ensure!(decoder.is_rows()?, "Decoded response is not rows!");
        Self::Row::rows_iter(decoder)?
            .next()
            .map(|row| Some(row))
            .ok_or_else(|| anyhow::anyhow!("Row not found!"))
    }
}

impl<T> ComputeToken<T> for MyKeyspace {
    fn token(_key: &T) -> i64 {
        rand::random()
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
