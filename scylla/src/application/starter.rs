use super::*;

#[async_trait::async_trait]
impl<H: ScyllaScope> Starter<H> for ScyllaBuilder<H> {
    type Ok = ScyllaHandle<H>;
    type Error = String;
    type Input = Scylla<H>;
    async fn starter(self, _handle: H, _input: Option<Self::Input>) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}
