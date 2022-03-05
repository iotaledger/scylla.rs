// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::prelude::ColumnEncoder;
use std::fmt::Debug;

/// Defines how values are bound to the frame
pub trait Binder {
    type Error: Debug;
    /// Add a single value
    fn value<V: ColumnEncoder>(self, value: &V) -> Result<Self, Self::Error>
    where
        Self: Sized;
    /// Add a slice of values
    fn bind<V: Bindable>(self, values: &V) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        values.bind(self)
    }
    /// Unset value
    fn unset_value(self) -> Result<Self, Self::Error>
    where
        Self: Sized;
    /// Set Null value, note: for write queries this will create tombstone for V;
    fn null_value(self) -> Result<Self, Self::Error>
    where
        Self: Sized;
}

/// Defines a query bindable value
pub trait Bindable {
    /// Bind the value using the provided binder
    fn bind<B: Binder>(&self, binder: B) -> Result<B, B::Error>;
}

impl<T: ColumnEncoder> Bindable for T {
    fn bind<B: Binder>(&self, binder: B) -> Result<B, B::Error> {
        binder.value(self)
    }
}

impl Bindable for () {
    fn bind<B: Binder>(&self, binder: B) -> Result<B, B::Error> {
        Ok(binder)
    }
}

impl<T: Bindable> Bindable for [T] {
    fn bind<B: Binder>(&self, mut binder: B) -> Result<B, B::Error> {
        for v in self.iter() {
            binder = v.bind(binder)?;
        }
        Ok(binder)
    }
}
