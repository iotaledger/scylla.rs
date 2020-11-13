use scylla::application::*;
// launcher
launcher!(builder: AppsBuilder {[] -> Scylla: ScyllaBuilder<Sender>}, state: Apps {});

impl Builder for AppsBuilder {
    type State = Apps;
    fn build(self) -> Self::State {
        // create Scylla app
        let scylla_builder = ScyllaBuilder::new(); // TODO add args
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
