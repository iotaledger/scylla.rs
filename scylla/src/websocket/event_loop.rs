use super::*;

#[async_trait::async_trait]
impl<T: Appsthrough<ScyllaThrough>> EventLoop<ScyllaHandle<T>> for Websocket {
    async fn event_loop(&mut self, status: Result<(), Need>, supervisor: &mut Option<ScyllaHandle<T>>) -> Result<(), Need> {
        todo!()
    }
}
