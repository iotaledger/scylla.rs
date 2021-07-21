// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{reporter::*, *};
use anyhow::anyhow;
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf};

/// Sender state
pub struct Sender {
    socket: OwnedWriteHalf,
    payloads: Payloads,
    appends_num: i16,
}

#[build]
pub fn build_sender(socket: OwnedWriteHalf, payloads: Payloads, appends_num: i16) -> Sender {
    Sender {
        payloads,
        socket,
        appends_num,
    }
}

/// Sender event type.
type SenderEvent = i16;

#[async_trait]
impl Actor for Sender {
    type Dependencies = Pool<MapPool<Reporter, ReporterId>>;
    type Event = SenderEvent;
    type Channel = UnboundedTokioChannel<Self::Event>;

    async fn init<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        _rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
    ) -> Result<(), ActorError> {
        Ok(())
    }

    async fn run<Reg: RegistryAccess + Send + Sync, Sup: EventDriven>(
        &mut self,
        rt: &mut ActorScopedRuntime<Self, Reg, Sup>,
        reporter_pool: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
        Sup::Event: SupervisorEvent,
        <Sup::Event as SupervisorEvent>::Children: From<PhantomData<Self>>,
    {
        rt.update_status(ServiceStatus::Running).await.ok();
        while let Some(stream_id) = rt.next_event().await {
            // write the payload to the socket, make sure the result is valid
            if let Some(payload) = self.payloads[stream_id as usize].as_ref_payload() {
                if let Err(io_error) = self.socket.write_all(payload).await {
                    // send to reporter ReporterEvent::Err(io_error, stream_id)
                    if reporter_pool
                        .send(
                            &compute_reporter_num(stream_id, self.appends_num),
                            ReporterEvent::Err(anyhow!(io_error), stream_id),
                        )
                        .await
                        .is_err()
                    {
                        error!("No reporter found for stream {}!", stream_id);
                    }
                }
            } else {
                error!("No payload found for stream {}!", stream_id);
            }
        }
        rt.update_status(ServiceStatus::Stopped).await.ok();
        Ok(())
    }
}
