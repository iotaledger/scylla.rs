// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub struct Request<S, K, V> {
    _marker: PhantomData<(S, K, V)>,
}

pub trait GetRequest<S, K> {
    fn to_get<V>(&self) -> Request<S, K, V>;
}

impl<S: Keyspace, K> GetRequest<S, K> for S {
    fn to_get<V>(&self) -> Request<S, K, V> {
        Request { _marker: PhantomData }
    }
}

pub struct SelectRequest<S, K, V> {
    token: i64,
    inner: Query,
    _marker: PhantomData<(S, K, V)>,
}

impl<S: Select<K, V>, K, V> Request<S, K, V> {
    fn select(&self, key: &K) -> SelectRequest<S, K, V> {
        S::select(key)
    }
}

impl<S: Select<K, V>, K, V> SelectRequest<S, K, V> {
    pub fn new(query: Query, token: i64) -> Self {
        Self {
            token,
            inner: query,
            _marker: PhantomData,
        }
    }

    pub fn send_local(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeRows<S, K, V>> {
        S::send_local(self.token, self.inner.0, worker);
        DecodeResult {
            inner: DecodeRows::<S, K, V> { _marker: PhantomData },
            request_type: RequestType::Select,
        }
    }

    pub fn send_global(self, worker: Box<dyn Worker>) -> DecodeResult<DecodeRows<S, K, V>> {
        S::send_global(self.token, self.inner.0, worker);
        DecodeResult {
            inner: DecodeRows::<S, K, V> { _marker: PhantomData },
            request_type: RequestType::Select,
        }
    }
}

impl<S: Select<K, V>, K, V> DecodeRows<S, K, V> {
    pub fn decode(&self, bytes: Vec<u8>) -> Result<Option<V>, CqlError> {
        S::try_decode(bytes.into())
    }
}

pub trait Select<K, V>: Keyspace + RowsDecoder<K, V> {
    fn select(key: &K) -> SelectRequest<Self, K, V>;
}

mod tests {

    use super::*;
    use std::borrow::Cow;

    struct Mainnet;

    impl Keyspace for Mainnet {
        type Error = Cow<'static, str>;

        const NAME: &'static str = "Mainnet";

        fn send_local(token: i64, payload: Vec<u8>, worker: Box<dyn Worker>) {
            todo!()
        }

        fn send_global(token: i64, payload: Vec<u8>, worker: Box<dyn Worker>) {
            todo!()
        }
    }

    impl Select<u32, f32> for Mainnet {
        fn select(key: &u32) -> SelectRequest<Self, u32, f32> {
            todo!()
        }
    }

    impl RowsDecoder<u32, f32> for Mainnet {
        fn try_decode(decoder: Decoder) -> Result<Option<f32>, CqlError> {
            todo!()
        }
    }

    #[derive(Debug)]
    struct TestWorker;

    impl Worker for TestWorker {
        fn handle_response(self: Box<Self>, giveload: Vec<u8>) {
            todo!()
        }

        fn handle_error(
            self: Box<Self>,
            error: crate::worker::WorkerError,
            reporter: &Option<crate::stage::ReporterHandle>,
        ) {
            todo!()
        }
    }

    #[test]
    fn test() {
        let worker = TestWorker;
        let res = Mainnet.to_get::<f32>().select(&8).send_local(Box::new(worker));
    }
}
