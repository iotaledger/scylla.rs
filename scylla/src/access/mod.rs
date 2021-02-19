// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

pub mod delete;
pub mod insert;
pub mod keyspace;
pub mod select;
pub mod update;

use super::Worker;
use keyspace::Keyspace;
use scylla_cql::{CqlError, Decoder, Frame, Query, RowsDecoder, VoidDecoder};
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

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

#[derive(Copy, Clone, Default)]
pub struct DecodeVoid<S> {
    _marker: PhantomData<S>,
}

#[derive(Clone)]
pub struct DecodeResult<T> {
    inner: T,
    request_type: RequestType,
}

mod tests {

    use crate::Worker;

    use super::{
        delete::Delete,
        insert::Insert,
        keyspace::Keyspace,
        select::{GetRequest, Select, SelectRequest},
        update::Update,
    };
    use scylla_cql::{CqlError, Decoder, RowsDecoder, VoidDecoder};
    use std::borrow::Cow;

    #[derive(Default)]
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

    impl<'a> Select<'a, u32, f32> for Mainnet {
        fn select(&'a self, key: &u32) -> SelectRequest<'a, Self, u32, f32> {
            todo!()
        }
    }

    impl<'a> Insert<'a, u32, f32> for Mainnet {
        fn insert(&'a self, key: &u32, value: &f32) -> super::insert::InsertRequest<'a, Self, u32, f32> {
            todo!()
        }
    }

    impl<'a> Update<'a, u32, f32> for Mainnet {
        fn update(&'a self, key: &u32, value: &f32) -> super::update::UpdateRequest<'a, Self, u32, f32> {
            todo!()
        }
    }

    impl<'a> Delete<'a, u32> for Mainnet {
        fn delete(&'a self, key: &u32) -> super::delete::DeleteRequest<'a, Self, u32> {
            todo!()
        }
    }

    impl RowsDecoder<u32, f32> for Mainnet {
        fn try_decode(decoder: Decoder) -> Result<Option<f32>, CqlError> {
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
        let res = Mainnet.to_get::<f32>().select(&8).send_local(Box::new(worker));
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
        let res = Mainnet.delete(&3).send_local(Box::new(worker));
    }
}
