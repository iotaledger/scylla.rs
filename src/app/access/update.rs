// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    insert::UpsertBuilder,
    *,
};
use crate::cql::query::StatementType;

/// Update query trait which creates an `UpdateRequest`
/// that can be sent to the `Ring`.
///
/// ## Examples
/// ```
/// use scylla_rs::{
///     app::access::{
///         ComputeToken,
///         GetUpdateRequest,
///         Keyspace,
///         Update,
///     },
///     cql::{
///         Batch,
///         Consistency,
///         PreparedStatement,
///         Values,
///         VoidDecoder,
///     },
/// };
/// use std::borrow::Cow;
/// # #[derive(Default, Clone, Debug)]
/// # struct MyKeyspace {
/// #     pub name: Cow<'static, str>,
/// # }
/// #
/// # impl MyKeyspace {
/// #     pub fn new() -> Self {
/// #         Self {
/// #             name: "my_keyspace".into(),
/// #         }
/// #     }
/// # }
///
/// # impl Keyspace for MyKeyspace {
/// #     fn name(&self) -> &Cow<'static, str> {
/// #         &self.name
/// #     }
/// # }
/// # impl ComputeToken<i32> for MyKeyspace {
/// #     fn token(_key: &i32) -> i64 {
/// #         rand::random()
/// #     }
/// # }
/// # type MyKeyType = i32;
/// # type MyValueType = f32;
/// impl Update<MyKeyType, MyValueType> for MyKeyspace {
///     type QueryOrPrepared = PreparedStatement;
///     fn statement(&self) -> Cow<'static, str> {
///         format!("UPDATE {}.table SET val1 = ?, val2 = ? WHERE key = ?", self.name()).into()
///     }
///
///     fn bind_values<T: Values>(builder: T, key: &MyKeyType, value: &MyValueType) -> T::Return {
///         builder.value(key).value(value).value(value)
///     }
/// }
///
/// # let keyspace = MyKeyspace::new();
/// # let (my_key, my_val) = (1, 1.0);
/// let request = keyspace // A Scylla keyspace
///     .update(&my_key, &my_val) // Get the Update Request
///     .consistency(Consistency::One)
///     .build()?;
/// # Ok::<(), anyhow::Error>(())
/// ```
pub trait Update<K, V>: Keyspace + VoidDecoder + ComputeToken<K> {
    /// Set the query type; `QueryStatement` or `PreparedStatement`
    type QueryOrPrepared: QueryOrPrepared;

    /// Create your update statement here.
    fn statement(&self) -> Cow<'static, str>;
    /// Get the MD5 hash of this implementation's statement
    /// for use when generating queries that should use
    /// the prepared statement.
    fn id(&self) -> [u8; 16] {
        md5::compute(self.update_statement().as_bytes()).into()
    }
    /// Bind the cql values to the builder
    fn bind_values<T: Values>(builder: T, key: &K, value: &V) -> T::Return;
}

/// Wrapper for the `Update` trait which provides the `update` function
pub trait GetUpdateRequest<K, V>: Keyspace {
    /// Calls the appropriate `Update` implementation for this Key/Value pair
    fn update<'a>(&'a self, key: &'a K, value: &'a V) -> UpsertBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Update<K, V>,
    {
        UpsertBuilder {
            keyspace: self,
            statement: self.statement(),
            key,
            value,
            builder: Self::QueryOrPrepared::encode_statement(Query::new(), &self.statement()),
            _marker: PhantomData,
        }
    }
    /// Calls the `Update` implementation for this Key/Value pair using a query statement
    fn update_query<'a>(
        &'a self,
        key: &'a K,
        value: &'a V,
    ) -> UpsertBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Update<K, V>,
    {
        UpsertBuilder {
            keyspace: self,
            statement: self.statement(),
            key,
            value,
            builder: QueryStatement::encode_statement(Query::new(), &self.statement()),
            _marker: PhantomData,
        }
    }
    /// Calls the `Update` implementation for this Key/Value pair using a prepared statement id
    fn update_prepared<'a>(
        &'a self,
        key: &'a K,
        value: &'a V,
    ) -> UpsertBuilder<'a, Self, K, V, QueryConsistency, StaticRequest>
    where
        Self: Update<K, V>,
    {
        UpsertBuilder {
            keyspace: self,
            statement: self.statement(),
            key,
            value,
            builder: PreparedStatement::encode_statement(Query::new(), &self.statement()),
            _marker: PhantomData,
        }
    }

    /// Specifies the returned Value type for an upcoming select request
    fn update_with<'a>(
        &'a self,
        statement: &str,
        key: &'a K,
        value: &'a V,
        statement_type: StatementType,
    ) -> UpsertBuilder<'a, Self, K, V, QueryConsistency, DynamicRequest> {
        match statement_type {
            StatementType::Query => self.update_query_with(statement, key, value),
            StatementType::Prepared => self.update_prepared_with(statement, key, value),
        }
    }
    /// Specifies the returned Value type for an upcoming select request using a query statement
    fn update_query_with<'a>(
        &'a self,
        statement: &str,
        key: &'a K,
        value: &'a V,
    ) -> UpsertBuilder<'a, Self, K, V, QueryConsistency, DynamicRequest> {
        UpsertBuilder {
            keyspace: self,
            statement: statement.to_owned().into(),
            key,
            value,
            builder: QueryStatement::encode_statement(Query::new(), statement),
            _marker: PhantomData,
        }
    }
    /// Specifies the returned Value type for an upcoming select request using a prepared statement id
    fn update_prepared_with<'a>(
        &'a self,
        statement: &str,
        key: &'a K,
        value: &'a V,
    ) -> UpsertBuilder<'a, Self, K, V, QueryConsistency, DynamicRequest> {
        UpsertBuilder {
            keyspace: self,
            statement: statement.to_owned().into(),
            key,
            value,
            builder: PreparedStatement::encode_statement(Query::new(), statement),
            _marker: PhantomData,
        }
    }
}

impl<S: Keyspace, K, V> GetUpdateRequest<K, V> for S {}
