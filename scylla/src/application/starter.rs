// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

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
        let tcp_listener = TcpListener::bind(
            self.listen_address
                .clone()
                .ok_or("Expected dashboard listen address but none was provided!")?,
        )
        .await
        .map_err(|e| format!("Unable to bind to dashboard listen address: {}", e))?;
        let listener = ListenerBuilder::new().tcp_listener(tcp_listener).build();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let listener_handle = ListenerHandle::new(abort_handle);
        // add listener handle to the scylla application and build its state
        // create cluster
        let cluster = ClusterBuilder::new()
            .reporter_count(self.reporter_count.clone().unwrap())
            .thread_count(self.thread_count.clone().unwrap())
            .data_centers(vec![self.local_dc.clone().unwrap()])
            .buffer_size(self.buffer_size.clone().unwrap_or(1024000))
            .recv_buffer_size(self.recv_buffer_size.clone())
            .send_buffer_size(self.send_buffer_size.clone())
            .authenticator(self.authenticator.clone().unwrap_or(PasswordAuth::default()))
            .build();
        // clone cluster handle
        let cluster_handle = cluster.clone_handle();
        // check if reporter_count is not zero
        if self.reporter_count.as_ref().unwrap().eq(&0) {
            return Err("reporter_count must be greater than zero, ensure your config is correct".to_owned());
        }
        if self.local_dc.as_ref().unwrap().eq(&"") {
            return Err("local_datacenter must be non-empty string, ensure your config is correct".to_owned());
        }
        // build application
        let scylla = self
            .listener_handle(listener_handle)
            .cluster_handle(cluster_handle)
            .build();
        // get handle(supervisor) of the application to start the Children
        let mut supervisor = scylla.handle.clone().unwrap();
        // start listener in abortable mode
        tokio::spawn(listener.start_abortable(abort_registration, Some(supervisor.clone())));
        // start cluster
        tokio::spawn(cluster.start(Some(supervisor.clone())));
        // start scylla application
        tokio::spawn(scylla.start(Some(handle)));
        // adjust the supervisor handle to identify it's returned to launcher
        supervisor.caller = Caller::Launcher;
        Ok(supervisor)
    }
}
