use super::*;

#[async_trait::async_trait]
impl<H: ScyllaScope> Starter<H> for ScyllaBuilder {
    type Ok = ScyllaHandle<H::AppsEvents>;
    type Error = String;
    type Input = Scylla;
    async fn starter(self, _handle: H, _input: Option<Self::Input>) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}
