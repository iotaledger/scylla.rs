// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use scylla::application::*;
// launcher
launcher!(builder: AppsBuilder {[] -> Scylla<Sender>: ScyllaBuilder<Sender>}, state: Apps {});

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
    // start the logger
    env_logger::init();
    // create apps_builder and build apps
    let apps = AppsBuilder::new().build();
    // start launcher and Scylla :)
    apps.Scylla().await.start(None).await;
}
