use super::*;

#[async_trait::async_trait]
impl<T: Appsthrough<ScyllaThrough>> Terminating<ScyllaHandle<T>> for Websocket {
    async fn terminating(&mut self, _status: Result<(), Need>, _supervisor: &mut Option<ScyllaHandle<T>>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Stopping);
        let event = ScyllaEvent::Children(ScyllaChild::Websocket(self.service.clone(), None));
        if let Some(s) = _supervisor {
            let _ = s.send(event);
        }
        _status
    }
}
