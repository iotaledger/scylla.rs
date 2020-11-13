use super::*;

#[async_trait::async_trait]
impl<H: ScyllaScope> Init<H> for Scylla<H> {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need> {
        todo!()
    }
}
