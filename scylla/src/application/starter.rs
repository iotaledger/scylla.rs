use super::*;
use futures::future::AbortHandle;
use tokio::net::TcpListener;

#[async_trait::async_trait]
impl<H: ScyllaScope> Starter<H> for ScyllaBuilder<H> {
    type Ok = ScyllaHandle<H>;
    type Error = String;
    type Input = Scylla<H>;
    async fn starter(mut self, handle: H, _input: Option<Self::Input>) -> Result<Self::Ok, Self::Error> {
        // create the listener
        let tcp_listener = TcpListener::bind(self.listen_address.clone().expect("Expected dashboard listen address"))
            .await
            .map_err(|_| "Unable to bind to dashboard listen address")?;
        let listener = ListenerBuilder::new().tcp_listener(tcp_listener).build();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let listener_handle = ListenerHandle::new(abort_handle);
        // add listener handle to the scylla application and build its state
        // TODO create cluster
        // build application
        let scylla = self.listener_handle(listener_handle).build();

        // get handle(supervisor) of the application to start the Children
        let supervisor = scylla.handle.clone().unwrap();
        // start listener in abortable mode
        tokio::spawn(listener.start_abortable(abort_registration, Some(supervisor.clone())));
        // TODO start cluster

        // start scylla application
        tokio::spawn(scylla.start(Some(handle)));
        Ok(supervisor)
    }
}
