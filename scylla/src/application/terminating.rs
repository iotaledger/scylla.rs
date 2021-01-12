use super::*;

#[async_trait::async_trait]
impl<H: ScyllaScope> Terminating<H> for Scylla<H> {
    async fn terminating(&mut self, status: Result<(), Need>, _supervisor: &mut Option<H>) -> Result<(), Need> {
        status
    }
}
