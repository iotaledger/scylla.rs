use super::*;

#[async_trait::async_trait]
impl<H: ScyllaScope> Terminating<H> for Scylla {
    async fn terminating(&mut self, status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need> {
        todo!()
    }
}
