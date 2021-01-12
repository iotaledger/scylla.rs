use super::*;

#[async_trait::async_trait]
impl<H: ScyllaScope> Init<H> for Scylla<H> {
    async fn init(&mut self, status: Result<(), Need>, supervisor: &mut Option<H>) -> Result<(), Need> {
        self.service.update_status(ServiceStatus::Initializing);
        let _ = supervisor.as_mut().unwrap().status_change(self.service.clone());
        status
    }
}
