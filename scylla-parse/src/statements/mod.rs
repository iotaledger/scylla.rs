use crate::{
    keywords::*,
    Brackets,
    Caching,
    ColumnOrder,
    CompactionType,
    Compression,
    CqlType,
    If,
    KeyspaceQualifiedName,
    List,
    MapLiteral,
    Name,
    Nothing,
    Parens,
    Parse,
    Peek,
    Relation,
    Replication,
    SetLiteral,
    SpeculativeRetry,
    StatementOpt,
    StatementOptValue,
    StatementStream,
    TableOpts,
    Term,
    Token,
};
use derive_builder::Builder;
use derive_more::{
    From,
    TryInto,
};
use scylla_parse_macros::ParseFromStr;
use std::{
    collections::HashMap,
    convert::TryFrom,
    fmt::{
        Display,
        Formatter,
    },
    str::FromStr,
};

mod ddl;
pub use ddl::*;

mod dml;
pub use dml::*;

mod index;
pub use index::*;

mod views;
pub use views::*;

mod function;
pub use function::*;

mod security;
pub use security::*;

mod trigger;
pub use trigger::*;

#[derive(ParseFromStr, Clone, Debug, TryInto, From)]
pub enum Statement {
    DataDefinition(DataDefinitionStatement),
    DataManipulation(DataManipulationStatement),
    SecondaryIndex(SecondaryIndexStatement),
    MaterializedView(MaterializedViewStatement),
    Role(RoleStatement),
    Permission(PermissionStatement),
    User(UserStatement),
    UserDefinedFunction(UserDefinedFunctionStatement),
    UserDefinedType(UserDefinedTypeStatement),
    Trigger(TriggerStatement),
}

macro_rules! impl_try_into_statements {
    ($($via:ty => {$($stmt:ty),*}),*) => {
        $($(
            impl std::convert::TryInto<$stmt> for Statement {
                type Error = anyhow::Error;

                fn try_into(self) -> Result<$stmt, Self::Error> {
                    match <$via>::try_from(self) {
                        Ok(v) => v.try_into().map_err(|e: &str| anyhow::anyhow!(e)),
                        Err(err) => Err(anyhow::anyhow!(
                            "Could not convert Statement to {}: {}",
                            std::any::type_name::<$stmt>(),
                            err
                        )),
                    }
                }
            }
        )*)*
    };
}

impl_try_into_statements!(
    DataDefinitionStatement => {UseStatement, CreateKeyspaceStatement, AlterKeyspaceStatement, DropKeyspaceStatement, CreateTableStatement, AlterTableStatement, DropTableStatement, TruncateStatement},
    DataManipulationStatement => {InsertStatement, UpdateStatement, DeleteStatement, SelectStatement, BatchStatement},
    SecondaryIndexStatement => {CreateIndexStatement, DropIndexStatement},
    MaterializedViewStatement => {CreateMaterializedViewStatement, AlterMaterializedViewStatement, DropMaterializedViewStatement},
    RoleStatement => {CreateRoleStatement, AlterRoleStatement, DropRoleStatement, GrantRoleStatement, RevokeRoleStatement, ListRolesStatement},
    PermissionStatement => {GrantPermissionStatement, RevokePermissionStatement, ListPermissionsStatement},
    UserStatement => {CreateUserStatement, AlterUserStatement, DropUserStatement, ListUsersStatement},
    UserDefinedFunctionStatement => {CreateFunctionStatement, DropFunctionStatement, CreateAggregateFunctionStatement, DropAggregateFunctionStatement},
    UserDefinedTypeStatement => {CreateUserDefinedTypeStatement, AlterUserDefinedTypeStatement, DropUserDefinedTypeStatement},
    TriggerStatement => {CreateTriggerStatement, DropTriggerStatement}
);

impl Parse for Statement {
    type Output = Statement;

    fn parse(s: &mut crate::StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(stmt) = s.parse_if() {
            Self::DataDefinition(stmt?)
        } else if let Some(stmt) = s.parse_if() {
            Self::DataManipulation(stmt?)
        } else if let Some(stmt) = s.parse_if() {
            Self::SecondaryIndex(stmt?)
        } else if let Some(stmt) = s.parse_if() {
            Self::MaterializedView(stmt?)
        } else if let Some(stmt) = s.parse_if() {
            Self::Role(stmt?)
        } else if let Some(stmt) = s.parse_if() {
            Self::Permission(stmt?)
        } else if let Some(stmt) = s.parse_if() {
            Self::User(stmt?)
        } else if let Some(stmt) = s.parse_if() {
            Self::UserDefinedFunction(stmt?)
        } else if let Some(stmt) = s.parse_if() {
            Self::UserDefinedType(stmt?)
        } else if let Some(stmt) = s.parse_if() {
            Self::Trigger(stmt?)
        } else {
            anyhow::bail!("Invalid statement: {}", s.parse_from::<Token>()?)
        })
    }
}

// TODO: Add more usability APIs
pub trait KeyspaceExt {
    fn get_keyspace(&self) -> Option<String>;

    fn set_keyspace(&mut self, keyspace: &str);
}

pub trait WhereExt {
    fn iter_where(&self) -> Option<std::slice::Iter<Relation>>;
}

pub trait KeyspaceOptionsExt {
    fn set_replication(&mut self, replication: Replication);
    fn set_durable_writes(&mut self, durable_writes: bool);

    fn with_replication(mut self, replication: Replication) -> Self
    where
        Self: Sized,
    {
        self.set_replication(replication);
        self
    }
    fn with_durable_writes(mut self, durable_writes: bool) -> Self
    where
        Self: Sized,
    {
        self.set_durable_writes(durable_writes);
        self
    }
}

pub trait TableOptionsExt {
    fn table_opts(&self) -> &Option<TableOpts>;
    fn table_opts_mut(&mut self) -> &mut Option<TableOpts>;
    fn statement_opts(&self) -> Option<&HashMap<Name, StatementOptValue>> {
        self.table_opts().as_ref().and_then(|o| o.options.as_ref())
    }
    fn statement_opts_mut(&mut self) -> Option<&mut HashMap<Name, StatementOptValue>> {
        self.table_opts_mut().as_mut().and_then(|o| o.options.as_mut())
    }
    fn add_option(&mut self, option: StatementOpt) {
        self.table_opts_mut()
            .get_or_insert_with(Default::default)
            .options
            .get_or_insert_with(Default::default)
            .insert(option.name, option.value);
    }
    fn set_compact_storage(&mut self, compact_storage: bool) {
        self.table_opts_mut()
            .get_or_insert_with(Default::default)
            .compact_storage = compact_storage;
    }
    fn set_clustering_order(&mut self, clustering_order: Vec<ColumnOrder>) {
        self.table_opts_mut()
            .get_or_insert_with(Default::default)
            .clustering_order
            .replace(clustering_order);
    }
    fn set_comment(&mut self, comment: &str) {
        self.add_option(
            StatementStream::new(&format!("comment = '{}'", comment))
                .parse()
                .unwrap(),
        )
    }

    fn set_speculative_retry(&mut self, speculative_retry: SpeculativeRetry) {
        self.add_option(
            StatementStream::new(&format!("speculative_retry = {}", speculative_retry))
                .parse()
                .unwrap(),
        )
    }

    fn set_change_data_capture(&mut self, cdc: bool) {
        self.add_option(StatementStream::new(&format!("cdc = {}", cdc)).parse().unwrap())
    }

    fn set_gc_grace_seconds(&mut self, gc_grace_seconds: i32) {
        self.add_option(
            StatementStream::new(&format!("gc_grace_seconds = {}", gc_grace_seconds))
                .parse()
                .unwrap(),
        )
    }

    fn set_bloom_filter_fp_chance(&mut self, bloom_filter_fp_chance: f32) {
        self.add_option(
            StatementStream::new(&format!("bloom_filter_fp_chance = {}", bloom_filter_fp_chance))
                .parse()
                .unwrap(),
        )
    }

    fn set_default_time_to_live(&mut self, default_time_to_live: i32) {
        self.add_option(
            StatementStream::new(&format!("default_time_to_live = {}", default_time_to_live))
                .parse()
                .unwrap(),
        )
    }

    fn set_compaction(&mut self, compaction: impl CompactionType) {
        println!("compaction: {}", compaction);
        self.add_option(
            StatementStream::new(&format!("compaction = {}", compaction))
                .parse()
                .unwrap(),
        )
    }

    fn set_compression(&mut self, compression: Compression) {
        self.add_option(
            StatementStream::new(&format!("compression = {}", compression))
                .parse()
                .unwrap(),
        )
    }

    fn set_caching(&mut self, caching: Caching) {
        self.add_option(StatementStream::new(&format!("caching = {}", caching)).parse().unwrap())
    }

    fn set_memtable_flush_period_in_ms(&mut self, memtable_flush_period_in_ms: i32) {
        self.add_option(
            StatementStream::new(&format!(
                "memtable_flush_period_in_ms = {}",
                memtable_flush_period_in_ms
            ))
            .parse()
            .unwrap(),
        )
    }

    fn set_read_repair(&mut self, read_repair: bool) {
        self.add_option(
            StatementStream::new(&format!(
                "read_repair = '{}'",
                if read_repair { "BLOCKING" } else { "NONE" }
            ))
            .parse()
            .unwrap(),
        )
    }
    fn set_additional_write_policy(&mut self, speculative_retry: SpeculativeRetry) {
        self.set_speculative_retry(speculative_retry)
    }

    fn with_compact_storage(mut self, compact_storage: bool) -> Self
    where
        Self: Sized,
    {
        self.set_compact_storage(compact_storage);
        self
    }
    fn with_clustering_order(mut self, clustering_order: Vec<ColumnOrder>) -> Self
    where
        Self: Sized,
    {
        self.set_clustering_order(clustering_order);
        self
    }
    fn with_comment(mut self, comment: &str) -> Self
    where
        Self: Sized,
    {
        self.set_comment(comment);
        self
    }
    fn with_speculative_retry(mut self, speculative_retry: SpeculativeRetry) -> Self
    where
        Self: Sized,
    {
        self.set_speculative_retry(speculative_retry);
        self
    }
    fn with_change_data_capture(mut self, cdc: bool) -> Self
    where
        Self: Sized,
    {
        self.set_change_data_capture(cdc);
        self
    }
    fn with_additional_write_policy(mut self, speculative_retry: SpeculativeRetry) -> Self
    where
        Self: Sized,
    {
        self.set_additional_write_policy(speculative_retry);
        self
    }
    fn with_gc_grace_seconds(mut self, gc_grace_seconds: i32) -> Self
    where
        Self: Sized,
    {
        self.set_gc_grace_seconds(gc_grace_seconds);
        self
    }
    fn with_bloom_filter_fp_chance(mut self, bloom_filter_fp_chance: f32) -> Self
    where
        Self: Sized,
    {
        self.set_bloom_filter_fp_chance(bloom_filter_fp_chance);
        self
    }
    fn with_default_time_to_live(mut self, default_time_to_live: i32) -> Self
    where
        Self: Sized,
    {
        self.set_default_time_to_live(default_time_to_live);
        self
    }
    fn with_compaction(mut self, compaction: impl CompactionType) -> Self
    where
        Self: Sized,
    {
        self.set_compaction(compaction);
        self
    }
    fn with_compression(mut self, compression: Compression) -> Self
    where
        Self: Sized,
    {
        self.set_compression(compression);
        self
    }
    fn with_caching(mut self, caching: Caching) -> Self
    where
        Self: Sized,
    {
        self.set_caching(caching);
        self
    }
    fn with_memtable_flush_period_in_ms(mut self, memtable_flush_period_in_ms: i32) -> Self
    where
        Self: Sized,
    {
        self.set_memtable_flush_period_in_ms(memtable_flush_period_in_ms);
        self
    }
    fn with_read_repair(mut self, read_repair: bool) -> Self
    where
        Self: Sized,
    {
        self.set_read_repair(read_repair);
        self
    }
}
