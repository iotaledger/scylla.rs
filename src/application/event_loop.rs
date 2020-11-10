use super::*;

#[async_trait::async_trait]
impl<H: ScyllaScope> EventLoop<H> for Scylla {
    async fn event_loop(&mut self, status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need> {
        todo!()
    }
}
