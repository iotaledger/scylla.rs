// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0
use super::{cluster::Cluster, websocket::Websocket, *};
pub(crate) use crate::cql::{CqlBuilder, PasswordAuth};
use std::net::SocketAddr;

/// Application state
pub struct Scylla {
    listen_address: SocketAddr,
    reporter_count: u8,
    thread_count: usize,
    local_dc: String,
    buffer_size: Option<usize>,
    recv_buffer_size: Option<u32>,
    send_buffer_size: Option<u32>,
    authenticator: Option<PasswordAuth>,
}

#[build]
#[derive(Debug, Clone)]
pub fn build_scylla(
    listen_address: SocketAddr,
    reporter_count: u8,
    thread_count: usize,
    local_dc: String,
    buffer_size: Option<usize>,
    recv_buffer_size: Option<u32>,
    send_buffer_size: Option<u32>,
    authenticator: Option<PasswordAuth>,
) -> Scylla {
    if reporter_count == 0 {
        panic!("reporter_count must be greater than zero, ensure your config is correct");
    }
    if local_dc == "" {
        panic!("local_datacenter must be non-empty string, ensure your config is correct");
    }

    Scylla {
        listen_address,
        reporter_count,
        thread_count,
        local_dc,
        buffer_size,
        recv_buffer_size,
        send_buffer_size,
        authenticator,
    }
}

#[async_trait]
impl Actor for Scylla {
    type Dependencies = ();
    type Event = ScyllaEvent;
    type Channel = TokioChannel<Self::Event>;

    async fn run<'a, Reg: RegistryAccess + Send + Sync>(
        &mut self,
        rt: &mut ActorScopedRuntime<'a, Self, Reg>,
        _deps: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        rt.update_status(ServiceStatus::Initializing).await;
        let my_handle = rt.my_handle().await;
        let websocket = Websocket {
            listen_address: self.listen_address,
        };
        rt.spawn_actor(websocket, my_handle.clone()).await;

        let cluster_builder = {
            cluster::ClusterBuilder::new()
                .reporter_count(self.reporter_count)
                .thread_count(self.thread_count)
                .data_centers(vec![self.local_dc.clone()])
                .recv_buffer_size(self.recv_buffer_size)
                .send_buffer_size(self.send_buffer_size)
                .buffer_size(self.buffer_size.unwrap_or(1024000))
                .authenticator(self.authenticator.clone().unwrap_or(PasswordAuth::default()))
        };
        rt.spawn_actor(cluster_builder.build(), my_handle.clone()).await;

        rt.update_status(ServiceStatus::Running).await;
        while let Some(event) = rt.next_event().await {
            match event {
                ScyllaEvent::Status(_) => {
                    let service_tree = rt.service_tree().await;
                    if service_tree.children.iter().any(|s| s.service.is_initializing()) {
                        rt.update_status(ServiceStatus::Initializing).await;
                    } else if service_tree.children.iter().any(|s| s.service.is_maintenance()) {
                        rt.update_status(ServiceStatus::Maintenance).await;
                    } else if service_tree.children.iter().any(|s| s.service.is_degraded()) {
                        rt.update_status(ServiceStatus::Degraded).await;
                    } else if service_tree.children.iter().all(|s| s.service.is_running()) {
                        rt.update_status(ServiceStatus::Running).await;
                    }
                }
                ScyllaEvent::Report(res) => match res {
                    Ok(s) => break,
                    Err(e) => match e.error.request() {
                        ActorRequest::Restart => match e.state {
                            ScyllaChild::Cluster(c) => {
                                rt.spawn_actor(c, my_handle.clone()).await;
                            }
                            ScyllaChild::Websocket(w) => {
                                rt.spawn_actor(w, my_handle.clone()).await;
                            }
                        },
                        ActorRequest::Reschedule(dur) => {
                            let mut handle_clone = my_handle.clone();
                            let evt = ScyllaEvent::report_err(ErrorReport::new(
                                e.state,
                                e.service,
                                ActorError::RuntimeError(ActorRequest::Restart),
                            ))
                            .unwrap();
                            let dur = *dur;
                            tokio::spawn(async move {
                                tokio::time::sleep(dur).await;
                                handle_clone.send(evt).await;
                            });
                        }
                        ActorRequest::Finish => log::error!("{}", e.error),
                        ActorRequest::Panic => panic!("{}", e.error),
                    },
                },
                ScyllaEvent::Shutdown => break,
            }
        }
        rt.update_status(ServiceStatus::Stopped).await;
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum ScyllaError {
    #[error("No cluster available!")]
    NoCluster,
    #[error(transparent)]
    Other {
        #[from]
        source: anyhow::Error,
    },
}

impl Into<ActorError> for ScyllaError {
    fn into(self) -> ActorError {
        match self {
            ScyllaError::NoCluster => ActorError::InvalidData(self.to_string()),
            ScyllaError::Other { source } => ActorError::Other {
                source,
                request: ActorRequest::Finish,
            },
        }
    }
}

/// SubEvent type, indicated the children
pub enum ScyllaChild {
    Cluster(Cluster),
    Websocket(Websocket),
}

/// Event type of the Scylla Application
pub enum ScyllaEvent {
    Status(Service),
    Report(Result<SuccessReport<ScyllaChild>, ErrorReport<ScyllaChild>>),
    Shutdown,
}

impl From<Cluster> for ScyllaChild {
    fn from(cluster: Cluster) -> Self {
        Self::Cluster(cluster)
    }
}

impl From<Websocket> for ScyllaChild {
    fn from(websocket: Websocket) -> Self {
        Self::Websocket(websocket)
    }
}

impl<T: Into<ScyllaChild>> SupervisorEvent<T> for ScyllaEvent {
    fn report(res: Result<SuccessReport<T>, ErrorReport<T>>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self::Report(
            res.map(|s| SuccessReport::new(s.state.into(), s.service))
                .map_err(|e| ErrorReport::new(e.state.into(), e.service, e.error)),
        ))
    }

    fn status(service: Service) -> Self {
        Self::Status(service)
    }
}

impl From<()> for ScyllaEvent {
    fn from(_: ()) -> Self {
        panic!()
    }
}
