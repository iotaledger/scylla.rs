// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    node::{Node, NodeEvent},
    *,
};
pub use reporter::{Reporter, ReporterEvent};
use std::{cell::UnsafeCell, collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::{net::TcpStream, sync::RwLock};

mod receiver;
mod reporter;
mod sender;

/// The thread-safe reusable payloads.
pub type Payloads = Arc<Vec<Reusable>>;

/// Stage state
pub struct Stage {
    address: SocketAddr,
    authenticator: PasswordAuth,
    appends_num: i16,
    reporter_count: u8,
    session_id: usize,
    shard_id: u16,
    payloads: Payloads,
    buffer_size: usize,
    recv_buffer_size: Option<u32>,
    send_buffer_size: Option<u32>,
}

#[build]
#[derive(Debug, Clone)]
pub fn build_stage(
    address: SocketAddr,
    authenticator: PasswordAuth,
    reporter_count: u8,
    shard_id: u16,
    buffer_size: usize,
    recv_buffer_size: Option<u32>,
    send_buffer_size: Option<u32>,
) -> Stage {
    // create reusable payloads as giveload
    let vector: Vec<Reusable> = Vec::new();
    let payloads: Payloads = Arc::new(vector);
    Stage {
        address,
        authenticator,
        appends_num: 32767 / (reporter_count as i16),
        reporter_count,
        session_id: 0,
        shard_id,
        payloads,
        buffer_size,
        recv_buffer_size,
        send_buffer_size,
    }
}

#[async_trait]
impl Actor for Stage {
    type Dependencies = Act<Node>;
    type Event = StageEvent;
    type Channel = TokioChannel<Self::Event>;

    async fn run<'a, Reg: RegistryAccess + Send + Sync>(
        &mut self,
        rt: &mut ActorScopedRuntime<'a, Self, Reg>,
        mut node: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        rt.update_status(ServiceStatus::Initializing).await;
        let mut my_handle = rt.my_handle().await;
        let mut reporter_handles = HashMap::with_capacity(self.reporter_count as usize);
        // init Reusable payloads holder to enable reporter/sender/receiver
        // to reuse the payload whenever is possible.
        let last_range = self.appends_num * (self.reporter_count as i16);
        {
            if let Some(payloads) = Arc::get_mut(&mut self.payloads) {
                for _ in 0..last_range {
                    payloads.push(Reusable::default())
                }
            } else {
                error!("Cannot acquire access to reusable payloads!");
                return Err(StageError::NoReusablePayloads.into());
            }
        }
        let streams: Vec<i16> = (0..last_range).collect();
        let mut streams_iter = streams.chunks_exact(self.appends_num as usize);

        for reporter_id in 0..self.reporter_count {
            if let Some(streams) = streams_iter.next() {
                // build reporter
                let reporter = reporter::ReporterBuilder::new()
                    .session_id(self.session_id)
                    .reporter_id(reporter_id)
                    .shard_id(self.shard_id)
                    .address(self.address.clone())
                    .payloads(self.payloads.clone())
                    .streams(streams.to_owned().into_iter().collect())
                    .build();

                let (_, reporter_handle) = rt.spawn_into_pool(reporter, my_handle.clone()).await;
                reporter_handles.insert(reporter_id, reporter_handle);
            } else {
                error!("Failed to create streams!");
                return Err(StageError::CannotCreateStreams.into());
            }
        }

        rt.add_resource(Arc::new(RwLock::new(reporter_handles.clone()))).await;

        info!("Sending register reporters event to node!");
        let event = NodeEvent::RegisterReporters(self.shard_id, reporter_handles);
        node.send(event).await.ok();

        rt.update_status(ServiceStatus::Running).await;
        while let Some(event) = rt.next_event().await {
            match event {
                StageEvent::Connect => {
                    // cql connect
                    let cql_builder = CqlBuilder::new()
                        .authenticator(self.authenticator.clone())
                        .address(self.address)
                        .shard_id(self.shard_id)
                        .recv_buffer_size(self.recv_buffer_size)
                        .send_buffer_size(self.send_buffer_size)
                        .build();
                    match cql_builder.await {
                        Ok(cql_conn) => {
                            self.session_id += 1;
                            // Split the stream
                            let stream: TcpStream = cql_conn.into();
                            let (socket_rx, socket_tx) = stream.into_split();
                            // spawn sender
                            let sender = sender::SenderBuilder::new()
                                .socket(socket_tx)
                                .appends_num(self.appends_num)
                                .payloads(self.payloads.clone())
                                .build();
                            rt.spawn_actor(sender, my_handle.clone()).await;
                            // spawn receiver
                            let receiver = receiver::ReceiverBuilder::new()
                                .socket(socket_rx)
                                .appends_num(self.appends_num)
                                .payloads(self.payloads.clone())
                                .session_id(self.session_id)
                                .buffer_size(self.buffer_size)
                                .build();
                            rt.spawn_actor(receiver, my_handle.clone()).await;
                        }
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(5000)).await;
                            // try to reconnect
                            my_handle.send(StageEvent::Connect).await.ok();
                        }
                    }
                }
                StageEvent::Report(res) => match res {
                    Ok(s) => break,
                    Err(e) => match e.error.request() {
                        ActorRequest::Restart => match e.state {
                            StageChild::Reporter(r) => {
                                todo!("Unregister/reregister the reporter?");
                                rt.spawn_into_pool(r, my_handle.clone()).await;
                            }
                            StageChild::Receiver(r) => {
                                rt.spawn_actor(r, my_handle.clone()).await;
                            }
                            StageChild::Sender(s) => {
                                rt.spawn_actor(s, my_handle.clone()).await;
                            }
                        },
                        ActorRequest::Reschedule(dur) => {
                            let mut handle_clone = my_handle.clone();
                            let evt = StageEvent::report_err(ErrorReport::new(
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
                StageEvent::Status(_) => todo!(),
            }
        }
        rt.update_status(ServiceStatus::Stopped).await;
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum StageError {
    #[error("Cannot acquire access to reusable payloads!")]
    NoReusablePayloads,
    #[error("Failed to create streams!")]
    CannotCreateStreams,
}

impl Into<ActorError> for StageError {
    fn into(self) -> ActorError {
        match self {
            StageError::NoReusablePayloads => ActorError::RuntimeError(ActorRequest::Panic),
            StageError::CannotCreateStreams => ActorError::RuntimeError(ActorRequest::Panic),
        }
    }
}

/// Stage event enum.
pub enum StageEvent {
    /// Establish connection to scylla shard.
    Connect,
    Report(Result<SuccessReport<StageChild>, ErrorReport<StageChild>>),
    Status(Service),
}

pub enum StageChild {
    Reporter(Reporter),
    Receiver(receiver::Receiver),
    Sender(sender::Sender),
}

impl From<Reporter> for StageChild {
    fn from(r: Reporter) -> Self {
        Self::Reporter(r)
    }
}

impl From<receiver::Receiver> for StageChild {
    fn from(r: receiver::Receiver) -> Self {
        Self::Receiver(r)
    }
}

impl From<sender::Sender> for StageChild {
    fn from(s: sender::Sender) -> Self {
        Self::Sender(s)
    }
}

impl<T: Into<StageChild>> SupervisorEvent<T> for StageEvent {
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

#[derive(Default)]
/// The reusable sender payload.
pub struct Reusable {
    value: UnsafeCell<Option<Vec<u8>>>,
}
impl Reusable {
    #[allow(clippy::mut_from_ref)]
    /// Return as mutable sender payload value.
    pub fn as_mut(&self) -> &mut Option<Vec<u8>> {
        unsafe { self.value.get().as_mut().unwrap() }
    }
    /// Return as reference sender payload.
    pub fn as_ref_payload(&self) -> Option<&Vec<u8>> {
        unsafe { self.value.get().as_ref().unwrap().as_ref() }
    }
    /// Return as mutable sender payload.
    pub fn as_mut_payload(&self) -> Option<&mut Vec<u8>> {
        self.as_mut().as_mut()
    }
}
unsafe impl Sync for Reusable {}
