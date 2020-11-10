use super::*;

#[async_trait::async_trait]
impl<T: Appsthrough<ScyllaThrough>> Terminating<ScyllaHandle<T>> for Listener {
    async fn terminating(&mut self, _status: Result<(), Need>, _supervisor: &mut Option<ScyllaHandle<T>>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Stopping);
        let event = ScyllaEvent::Children(ScyllaChild::Listener(self.service.clone(), None));
        let _ = _supervisor.as_mut().unwrap().send(event);
        _status
    }
}
