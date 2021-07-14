// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use anyhow::bail;
use backstage::prelude::*;
use futures::FutureExt;
use log::*;
use scylla_rs::prelude::{stage::Reporter, *};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

#[tokio::main]
async fn main() {
    std::panic::set_hook(Box::new(|info| {
        log::error!("{}", info);
    }));
    std::env::set_var("RUST_LOG", "debug");
    // start the logger
    env_logger::init();
    let scylla_builder = ScyllaBuilder::new()
        .listen_address(([127, 0, 0, 1], 8080).into())
        .thread_count(num_cpus::get())
        .reporter_count(2)
        .local_dc("datacenter1".to_owned());
    // create apps_builder and build apps
    RuntimeScope::<ActorRegistry>::launch(|scope| {
        async move {
            let (_, shutdown_handle, _) = scope.spawn_actor_unsupervised(scylla_builder.build()).await?;
            tokio::task::spawn(ctrl_c(shutdown_handle));
            let ws = format!("ws://{}/", "127.0.0.1:8080");
            let nodes = vec![([127, 0, 0, 1], 9042).into()];
            match add_nodes(&ws, nodes, 1).await {
                Ok(_) => match init_database().await {
                    Ok(_) => log::debug!("{}", scope.service_tree().await),
                    Err(e) => {
                        error!("{}", e);
                        //scope.print_root().await;
                        log::debug!("{}", scope.service_tree().await);
                    }
                },
                Err(e) => {
                    error!("{}", e);
                    //scope.print_root().await;
                    log::debug!("{}", scope.service_tree().await);
                }
            }
            Ok(())
        }
        .boxed()
    })
    .await
    .unwrap();
}

async fn ctrl_c(shutdown_handle: ShutdownHandle) {
    tokio::signal::ctrl_c().await.unwrap();
    shutdown_handle.shutdown();
}

async fn init_database() -> anyhow::Result<()> {
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
    let table_queries = format!(
        "CREATE TABLE IF NOT EXISTS {0}.messages (
            message_id text PRIMARY KEY,
            message blob,
            metadata blob,
        );

        CREATE TABLE IF NOT EXISTS {0}.addresses  (
            address text,
            partition_id smallint,
            milestone_index int,
            output_type tinyint,
            transaction_id text,
            idx smallint,
            amount bigint,
            address_type tinyint,
            inclusion_state blob,
            PRIMARY KEY ((address, partition_id), milestone_index, output_type, transaction_id, idx)
        ) WITH CLUSTERING ORDER BY (milestone_index DESC, output_type DESC, transaction_id DESC, idx DESC);

        CREATE TABLE IF NOT EXISTS {0}.indexes  (
            indexation text,
            partition_id smallint,
            milestone_index int,
            message_id text,
            inclusion_state blob,
            PRIMARY KEY ((indexation, partition_id), milestone_index, message_id)
        ) WITH CLUSTERING ORDER BY (milestone_index DESC);

        CREATE TABLE IF NOT EXISTS {0}.parents  (
            parent_id text,
            partition_id smallint,
            milestone_index int,
            message_id text,
            inclusion_state blob,
            PRIMARY KEY ((parent_id, partition_id), milestone_index, message_id)
        ) WITH CLUSTERING ORDER BY (milestone_index DESC);

        CREATE TABLE IF NOT EXISTS {0}.transactions  (
            transaction_id text,
            idx smallint,
            variant text,
            message_id text,
            data blob,
            inclusion_state blob,
            milestone_index int,
            PRIMARY KEY (transaction_id, idx, variant, message_id, data)
        );

        CREATE TABLE IF NOT EXISTS {0}.milestones  (
            milestone_index int,
            message_id text,
            timestamp bigint,
            payload blob,
            PRIMARY KEY (milestone_index, message_id)
        );

        CREATE TABLE IF NOT EXISTS {0}.hints  (
            hint text,
            variant text,
            partition_id smallint,
            milestone_index int,
            PRIMARY KEY (hint, variant, partition_id)
        ) WITH CLUSTERING ORDER BY (variant DESC, partition_id DESC);

        CREATE TABLE IF NOT EXISTS {0}.sync  (
            key text,
            milestone_index int,
            synced_by tinyint,
            logged_by tinyint,
            PRIMARY KEY (key, milestone_index)
        ) WITH CLUSTERING ORDER BY (milestone_index DESC);
        
        CREATE TABLE IF NOT EXISTS {0}.analytics (
            key text,
            milestone_index int,
            message_count int,
            transaction_count int,
            transferred_tokens bigint,
            PRIMARY KEY (key, milestone_index)
        ) WITH CLUSTERING ORDER BY (milestone_index DESC);",
        keyspace
    );
    for query in table_queries.split(";").map(str::trim).filter(|s| !s.is_empty()) {
        let worker = BatchWorker::boxed(sender.clone());
        let statement = Query::new().statement(query).consistency(Consistency::One).build()?;
        send_local(token, statement.0, worker, keyspace.clone());
        if let Some(msg) = inbox.recv().await {
            match msg {
                Ok(_) => (),
                Err(e) => bail!(e),
            }
        } else {
            bail!("Could not verify if table was created!")
        }
    }

    Ok(())
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
