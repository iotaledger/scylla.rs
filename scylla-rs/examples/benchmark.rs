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
        let mut scylla = Scylla::new("datacenter1", *r, Default::default());
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
    keyspace.prepare_select::<String, (), i32>().get_local().await?;

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
        keyspace
            .select::<i32>(&format!("Key {}", i), &())
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
    MyKeyspace::new()
        .execute_query("DROP KEYSPACE {{keyspace}}", &[])
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

impl ToString for MyKeyspace {
    fn to_string(&self) -> String {
        self.name.to_string()
    }
}

impl Insert<String, i32> for MyKeyspace {
    type QueryOrPrepared = PreparedStatement;
    fn statement(&self) -> InsertStatement {
        parse_statement!("INSERT INTO scylla_example.test (key, data) VALUES (?, ?)")
    }

    fn bind_values<T: Binder>(builder: T, key: &String, value: &i32) -> T {
        builder.value(key).value(value)
    }
}

impl Select<String, (), i32> for MyKeyspace {
    type QueryOrPrepared = PreparedStatement;

    fn statement(&self) -> SelectStatement {
        parse_statement!("SELECT data FROM scylla_example.test WHERE key = ?")
    }

    fn bind_values<T: Binder>(builder: T, key: &String, _variables: &()) -> T {
        builder.value(key)
    }
}
