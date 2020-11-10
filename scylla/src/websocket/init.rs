use super::*;

#[async_trait::async_trait]
impl<T: Appsthrough<ScyllaThrough>> Init<ScyllaHandle<T>> for Websocket {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<ScyllaHandle<T>>) -> Result<(), Need> {
        // todo authenticator using static secert key and noise protocol
        if true {
            self.service.update_status(ServiceStatus::Initializing);
            let event = ScyllaEvent::Children(ScyllaChild::Websocket(self.service.clone(), None));
            let _ = supervisor.as_mut().unwrap().send(event);
            status
        } else {
            // drop supervisor handle if failed to authenticate,as it's unnecessary to aknowledge_shutdown
            supervisor.take().unwrap();
            return Err(Need::Abort);
        }
    }
}
