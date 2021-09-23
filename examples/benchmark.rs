// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use anyhow::bail;
use log::*;
use scylla_rs::prelude::*;
use std::{borrow::Cow, marker::PhantomData, time::SystemTime};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

launcher!(builder: AppsBuilder {[] -> Scylla<Sender>: ScyllaBuilder<Sender>}, state: Apps {reporter_count: u8});

impl Builder for AppsBuilder {
    type State = Apps;
    fn build(self) -> Self::State {
        // create Scylla app
        let scylla_builder = ScyllaBuilder::new()
            .listen_address("127.0.0.1:8080".to_owned())
            .thread_count(num_cpus::get())
            .reporter_count(2)
            .local_dc("datacenter1".to_owned());
        // add it to launcher
        self.Scylla(scylla_builder).to_apps()
    }
}

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "debug");
    // start the logger
    env_logger::init();
    // create apps_builder and build apps

    let combinations = vec![10i32, 100, 1000, 10000]
        .into_iter()
        .map(|n| std::iter::repeat(n).take(4))
        .flatten()
        .zip(std::iter::repeat(vec![2u8, 4, 8, 16].into_iter()).flatten());

    let timings = combinations
        .map(|(n, r)| (n, r, std::sync::Arc::new(tokio::sync::Mutex::new(0u128))))
        .collect::<Vec<_>>();

    for (n, r, t) in timings.iter() {
        let apps = AppsBuilder::new().reporter_count(*r).build();
        // start launcher and Scylla :)
        apps.Scylla()
            .await
            .future(|apps| async {
                let ws = format!("ws://{}/", "127.0.0.1:8080");
                let nodes = vec![([127, 0, 0, 1], 9042).into()];
                match add_nodes(&ws, nodes, 1).await {
                    Ok(_) => match init_database(*n).await {
                        Ok(time) => {
                            *t.lock().await = time;
                        }
                        Err(e) => {
                            error!("{}", e);
                        }
                    },
                    Err(e) => {
                        error!("{}", e);
                    }
                }
                apps
            })
            .await
            .start(None)
            .await;
    }

    info!("Timings:");
    info!("N\tR\tTime");
    for (n, r, t) in timings.iter() {
        info!("{}\t{}\t{}", n, r, *t.lock().await);
    }
}

async fn init_database(n: i32) -> anyhow::Result<u128> {
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
    let (mut ws_stream, _) =
        tokio_tungstenite::connect_async(url::Url::parse(&format!("ws://{}/", "127.0.0.1:8080"))?).await?;
    let msg = SocketMsg::Scylla(ScyllaThrough::Shutdown);
    let j = serde_json::to_string(&msg).map_err(|_| anyhow::anyhow!("Invalid AddNode event"))?;
    let m = tokio_tungstenite::tungstenite::Message::text(j);
    futures::SinkExt::send(&mut ws_stream, m).await?;
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

    fn handle_error(self: Box<Self>, error: WorkerError, _reporter: &Option<ReporterHandle>) -> anyhow::Result<()> {
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
