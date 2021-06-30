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
    type Dependencies = Pool<Reporter, ReporterId>;
    type Event = SenderEvent;
    type Channel = TokioChannel<Self::Event>;

    async fn run<'a, Reg: RegistryAccess + Send + Sync>(
        &mut self,
        rt: &mut ActorScopedRuntime<'a, Self, Reg>,
        reporter_pool: Self::Dependencies,
    ) -> Result<(), ActorError>
    where
        Self: Sized,
    {
        rt.update_status(ServiceStatus::Running).await;
        while let Some(stream_id) = rt.next_event().await {
            // write the payload to the socket, make sure the result is valid
            if let Some(payload) = self.payloads[stream_id as usize].as_ref_payload() {
                if let Err(io_error) = self.socket.write_all(payload).await {
                    // send to reporter ReporterEvent::Err(io_error, stream_id)
                    let mut handles = reporter_pool.write().await;
                    if let Some(reporter_handle) =
                        handles.get_by_metric(&compute_reporter_num(stream_id, self.appends_num))
                    {
                        backstage::actor::Sender::send(
                            reporter_handle,
                            ReporterEvent::Err(anyhow!(io_error), stream_id),
                        )
                        .await
                        .unwrap_or_else(|e| error!("{}", e))
                    } else {
                        error!("No reporter found for stream {}!", stream_id);
                    }
                }
            } else {
                error!("No payload found for stream {}!", stream_id);
            }
        }
        rt.update_status(ServiceStatus::Stopped).await;
        Ok(())
    }
}
