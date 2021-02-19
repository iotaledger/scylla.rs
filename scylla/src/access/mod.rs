// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

pub mod delete;
pub mod insert;
pub mod keyspace;
pub mod select;
pub mod update;

use super::Worker;
use keyspace::Keyspace;
use scylla_cql::{CqlError, Query, RowsDecoder, VoidDecoder};
use std::{marker::PhantomData, ops::Deref};

#[repr(u8)]
#[derive(Clone)]
enum RequestType {
    Insert = 0,
    Update = 1,
    Delete = 2,
    Select = 3,
}

#[derive(Clone, Copy, Default)]
pub struct DecodeRows<S, K, V> {
    _marker: PhantomData<(S, K, V)>,
}

impl<'a, S: RowsDecoder<K, V>, K, V> DecodeRows<S, K, V> {
    pub fn decode(&self, bytes: Vec<u8>) -> Result<Option<V>, CqlError> {
        S::try_decode(bytes.into())
    }
}

#[derive(Copy, Clone, Default)]
pub struct DecodeVoid<S> {
    _marker: PhantomData<S>,
}

impl<S: VoidDecoder> DecodeVoid<S> {
    pub fn decode(&self, bytes: Vec<u8>) -> Result<(), CqlError> {
        S::try_decode(bytes.into())
    }
}

#[derive(Clone)]
pub struct DecodeResult<T> {
    inner: T,
    request_type: RequestType,
}

impl<T> Deref for DecodeResult<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

mod tests {

    use crate::Worker;

    use super::{
        delete::{Delete, GetDeleteRequest},
        insert::{GetInsertRequest, Insert},
        keyspace::Keyspace,
        select::{GetSelectRequest, Select, SelectRequest},
        update::{GetUpdateRequest, Update},
    };
    use scylla_cql::{CqlError, Decoder, Query, RowsDecoder, VoidDecoder};

    #[derive(Default)]
    struct Mainnet;

    impl Keyspace for Mainnet {
        const NAME: &'static str = "Mainnet";

        fn send_local(&self, token: i64, payload: Vec<u8>, worker: Box<dyn Worker>) {
            todo!()
        }

        fn send_global(&self, token: i64, payload: Vec<u8>, worker: Box<dyn Worker>) {
            todo!()
        }
    }

    impl<'a> Select<'a, u32, f32> for Mainnet {
        fn get_request(&'a self, key: &u32) -> SelectRequest<'a, Self, u32, f32> {
            todo!()
        }
    }

    impl<'a> Select<'a, u32, i32> for Mainnet {
        fn get_request(&'a self, key: &u32) -> SelectRequest<'a, Self, u32, i32> {
            todo!()
        }
    }

    impl<'a> Insert<'a, u32, f32> for Mainnet {
        fn get_request(&'a self, key: &u32, value: &f32) -> super::insert::InsertRequest<'a, Self, u32, f32> {
            todo!()
        }
    }

    impl<'a> Update<'a, u32, f32> for Mainnet {
        fn get_request(&'a self, key: &u32, value: &f32) -> super::update::UpdateRequest<'a, Self, u32, f32> {
            todo!()
        }
    }

    impl<'a> Delete<'a, u32, f32> for Mainnet {
        fn get_request(&'a self, key: &u32) -> super::delete::DeleteRequest<'a, Self, u32, f32> {
            todo!()
        }
    }

    impl<'a> Delete<'a, u32, i32> for Mainnet {
        fn get_request(&'a self, key: &u32) -> super::delete::DeleteRequest<'a, Self, u32, i32> {
            let query = Query::new()
                .statement(&format!("DELETE FROM {}.table WHERE key = ?", Mainnet::name()))
                .consistency(scylla_cql::Consistency::One)
                .value(key.to_string())
                .build();

            let token = rand::random::<i64>();

            super::delete::DeleteRequest::new(query, token, self)
        }
    }

    impl RowsDecoder<u32, f32> for Mainnet {
        fn try_decode(decoder: Decoder) -> Result<Option<f32>, CqlError> {
            todo!()
        }
    }

    impl RowsDecoder<u32, i32> for Mainnet {
        fn try_decode(decoder: Decoder) -> Result<Option<i32>, CqlError> {
            todo!()
        }
    }

    impl VoidDecoder for Mainnet {}

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
    fn test_select() {
        let worker = TestWorker;
        let res = Mainnet.select::<f32>(&3).send_local(Box::new(worker));
    }

    #[test]
    fn test_insert() {
        let worker = TestWorker;
        let res = Mainnet.insert(&3, &8.0).send_local(Box::new(worker));
    }

    #[test]
    fn test_update() {
        let worker = TestWorker;
        let res = Mainnet.update(&3, &8.0).send_local(Box::new(worker));
    }

    #[test]
    fn test_delete() {
        let worker = TestWorker;
        let res = Mainnet.delete::<f32>(&3).send_local(Box::new(worker));
    }
}
