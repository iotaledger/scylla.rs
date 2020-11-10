use super::*;

#[async_trait::async_trait]
impl<T: Appsthrough<ScyllaThrough>> Init<ScyllaHandle<T>> for Listener {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<ScyllaHandle<T>>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        let event = ScyllaEvent::Children(ScyllaChild::Listener(self.service.clone(), None));
        let _ = supervisor.as_mut().unwrap().send(event);
        status
    }
}
