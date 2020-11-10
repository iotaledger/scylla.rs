use super::*;

#[async_trait::async_trait]
impl<T: Appsthrough<ScyllaThrough>> Terminating<ScyllaHandle<T>> for Listener {
    async fn terminating(&mut self, _status: Result<(), Need>, _supervisor: &mut Option<ScyllaHandle<T>>) -> Result<(), Need> {
        todo!()
    }
}
