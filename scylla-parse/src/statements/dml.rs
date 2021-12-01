// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    ArithmeticOp,
    BindMarker,
    DurationLiteral,
    ListLiteral,
    Operator,
    ReservedKeyword,
    TupleLiteral,
};

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq, Eq)]
pub enum DataManipulationStatement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Batch(BatchStatement),
}

impl Parse for DataManipulationStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(keyword) = s.find::<ReservedKeyword>() {
            match keyword {
                ReservedKeyword::SELECT => Self::Select(s.parse()?),
                ReservedKeyword::INSERT => Self::Insert(s.parse()?),
                ReservedKeyword::UPDATE => Self::Update(s.parse()?),
                ReservedKeyword::DELETE => Self::Delete(s.parse()?),
                ReservedKeyword::BATCH => Self::Batch(s.parse()?),
                _ => anyhow::bail!("Expected a data manipulation statement, found {}", s.info()),
            }
        } else {
            anyhow::bail!("Expected a data manipulation statement, found {}", s.info())
        })
    }
}

impl Peek for DataManipulationStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<InsertStatement>()
            || s.check::<UpdateStatement>()
            || s.check::<DeleteStatement>()
            || s.check::<SelectStatement>()
            || s.check::<BatchStatement>()
    }
}

impl Display for DataManipulationStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Select(s) => s.fmt(f),
            Self::Insert(s) => s.fmt(f),
            Self::Update(s) => s.fmt(f),
            Self::Delete(s) => s.fmt(f),
            Self::Batch(s) => s.fmt(f),
        }
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option), build_fn(validate = "Self::validate"))]
pub struct SelectStatement {
    #[builder(setter(name = "set_distinct"), default)]
    pub distinct: bool,
    #[builder(setter(into))]
    pub select_clause: SelectClause,
    #[builder(setter(into))]
    pub from: KeyspaceQualifiedName,
    #[builder(setter(into), default)]
    pub where_clause: Option<WhereClause>,
    #[builder(setter(into), default)]
    pub group_by_clause: Option<GroupByClause>,
    #[builder(setter(into), default)]
    pub order_by_clause: Option<OrderByClause>,
    #[builder(setter(into), default)]
    pub per_partition_limit: Option<Limit>,
    #[builder(setter(into), default)]
    pub limit: Option<Limit>,
    #[builder(setter(name = "set_allow_filtering"), default)]
    pub allow_filtering: bool,
    #[builder(setter(name = "set_bypass_cache"), default)]
    pub bypass_cache: bool,
    #[builder(setter(into), default)]
    pub timeout: Option<DurationLiteral>,
}

impl SelectStatementBuilder {
    /// Set DISTINCT on the statement
    /// To undo this, use `set_distinct(false)`
    pub fn distinct(&mut self) -> &mut Self {
        self.distinct.replace(true);
        self
    }

    /// Set ALLOW FILTERING on the statement
    /// To undo this, use `set_allow_filtering(false)`
    pub fn allow_filtering(&mut self) -> &mut Self {
        self.allow_filtering.replace(true);
        self
    }

    /// Set BYPASS CACHE on the statement
    /// To undo this, use `set_bypass_cache(false)`
    pub fn bypass_cache(&mut self) -> &mut Self {
        self.bypass_cache.replace(true);
        self
    }

    fn validate(&self) -> Result<(), String> {
        if self
            .select_clause
            .as_ref()
            .map(|s| match s {
                SelectClause::Selectors(s) => s.is_empty(),
                _ => false,
            })
            .unwrap_or(false)
        {
            return Err("SELECT clause selectors cannot be empty".to_string());
        }
        Ok(())
    }
}

impl Parse for SelectStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        s.parse::<SELECT>()?;
        let mut res = SelectStatementBuilder::default();
        res.set_distinct(s.parse::<Option<DISTINCT>>()?.is_some())
            .select_clause(s.parse::<SelectClause>()?)
            .from(s.parse::<(FROM, KeyspaceQualifiedName)>()?.1);
        loop {
            if s.remaining() == 0 || s.parse::<Option<Semicolon>>()?.is_some() {
                break;
            }
            if let Some(where_clause) = s.parse::<Option<WhereClause>>()? {
                if res.where_clause.is_some() {
                    anyhow::bail!("Duplicate WHERE clause!");
                }
                res.where_clause(where_clause);
            } else if let Some(group_by_clause) = s.parse::<Option<GroupByClause>>()? {
                if res.group_by_clause.is_some() {
                    anyhow::bail!("Duplicate GROUP BY clause!");
                }
                res.group_by_clause(group_by_clause);
            } else if let Some(order_by_clause) = s.parse::<Option<OrderByClause>>()? {
                if res.order_by_clause.is_some() {
                    anyhow::bail!("Duplicate ORDER BY clause!");
                }
                res.order_by_clause(order_by_clause);
            } else if s.parse::<Option<(PER, PARTITION, LIMIT)>>()?.is_some() {
                if res.per_partition_limit.is_some() {
                    anyhow::bail!("Duplicate PER PARTITION LIMIT clause!");
                }
                res.per_partition_limit(s.parse::<Limit>()?);
            } else if s.parse::<Option<LIMIT>>()?.is_some() {
                if res.limit.is_some() {
                    anyhow::bail!("Duplicate LIMIT clause!");
                }
                res.limit(s.parse::<Limit>()?);
            } else if s.parse::<Option<(ALLOW, FILTERING)>>()?.is_some() {
                if res.allow_filtering.is_some() {
                    anyhow::bail!("Duplicate ALLOW FILTERING clause!");
                }
                res.allow_filtering();
            } else if s.parse::<Option<(BYPASS, CACHE)>>()?.is_some() {
                if res.bypass_cache.is_some() {
                    anyhow::bail!("Duplicate BYPASS CACHE clause!");
                }
                res.bypass_cache();
            } else if let Some(t) = s.parse_from::<If<(USING, TIMEOUT), DurationLiteral>>()? {
                if res.timeout.is_some() {
                    anyhow::bail!("Duplicate USING TIMEOUT clause!");
                }
                res.timeout(t);
            } else {
                return Ok(res
                    .build()
                    .map_err(|_| anyhow::anyhow!("Invalid tokens in SELECT statement: {}", s.info()))?);
            }
        }
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid SELECT statement: {}", e))?)
    }
}

impl Peek for SelectStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<SELECT>()
    }
}

impl Display for SelectStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SELECT {}{} FROM {}",
            if self.distinct { "DISTINCT " } else { "" },
            self.select_clause,
            self.from
        )?;
        if let Some(where_clause) = &self.where_clause {
            write!(f, " {}", where_clause)?;
        }
        if let Some(group_by_clause) = &self.group_by_clause {
            write!(f, " {}", group_by_clause)?;
        }
        if let Some(order_by_clause) = &self.order_by_clause {
            write!(f, " {}", order_by_clause)?;
        }
        if let Some(per_partition_limit) = &self.per_partition_limit {
            write!(f, " PER PARTITION LIMIT {}", per_partition_limit)?;
        }
        if let Some(limit) = &self.limit {
            write!(f, " LIMIT {}", limit)?;
        }
        if self.allow_filtering {
            write!(f, " ALLOW FILTERING")?;
        }
        if self.bypass_cache {
            write!(f, " BYPASS CACHE")?;
        }
        if let Some(timeout) = &self.timeout {
            write!(f, " USING TIMEOUT {}", timeout)?;
        }
        Ok(())
    }
}

impl KeyspaceExt for SelectStatement {
    fn get_keyspace(&self) -> Option<String> {
        self.from.keyspace.as_ref().map(|n| n.to_string())
    }

    fn set_keyspace(&mut self, keyspace: impl Into<Name>) {
        self.from.keyspace.replace(keyspace.into());
    }
}

impl WhereExt for SelectStatement {
    fn iter_where(&self) -> Option<std::slice::Iter<Relation>> {
        self.where_clause.as_ref().map(|w| w.relations.iter())
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
pub enum SelectClause {
    All,
    Selectors(Vec<Selector>),
}

impl Parse for SelectClause {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        Ok(if s.parse::<Option<Star>>()?.is_some() {
            SelectClause::All
        } else {
            SelectClause::Selectors(s.parse_from::<List<Selector, Comma>>()?)
        })
    }
}

impl Display for SelectClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectClause::All => write!(f, "*"),
            SelectClause::Selectors(selectors) => {
                for (i, selector) in selectors.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    selector.fmt(f)?;
                }
                Ok(())
            }
        }
    }
}

impl From<Vec<Selector>> for SelectClause {
    fn from(selectors: Vec<Selector>) -> Self {
        SelectClause::Selectors(selectors)
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
pub struct Selector {
    #[builder(setter(into))]
    pub kind: SelectorKind,
    #[builder(setter(strip_option), default)]
    pub as_id: Option<Name>,
}

impl Selector {
    pub fn column(name: impl Into<Name>) -> Self {
        Selector {
            kind: SelectorKind::Column(name.into()),
            as_id: Default::default(),
        }
    }

    pub fn term(term: impl Into<Term>) -> Self {
        Selector {
            kind: SelectorKind::Term(term.into()),
            as_id: Default::default(),
        }
    }

    pub fn cast(self, ty: impl Into<CqlType>) -> Self {
        Selector {
            kind: SelectorKind::Cast(Box::new(self), ty.into()),
            as_id: Default::default(),
        }
    }

    pub fn function(function: SelectorFunction) -> Self {
        Selector {
            kind: SelectorKind::Function(function),
            as_id: Default::default(),
        }
    }

    pub fn count() -> Self {
        Selector {
            kind: SelectorKind::Count,
            as_id: Default::default(),
        }
    }

    pub fn as_id(self, name: impl Into<Name>) -> Self {
        Self {
            kind: self.kind,
            as_id: Some(name.into()),
        }
    }
}

impl Parse for Selector {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        let (kind, as_id) = s.parse::<(SelectorKind, Option<(AS, Name)>)>()?;
        Ok(Self {
            kind,
            as_id: as_id.map(|(_, id)| id),
        })
    }
}

impl Display for Selector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.kind.fmt(f)?;
        if let Some(id) = &self.as_id {
            write!(f, " AS {}", id)?;
        }
        Ok(())
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
pub struct SelectorFunction {
    pub function: Name,
    pub args: Vec<Selector>,
}

impl SelectorFunction {
    pub fn new(function: Name) -> Self {
        SelectorFunction {
            function,
            args: Vec::new(),
        }
    }

    pub fn arg(mut self, arg: Selector) -> Self {
        self.args.push(arg);
        self
    }
}

impl Parse for SelectorFunction {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        let (function, args) = s.parse_from::<(Name, Parens<List<Selector, Comma>>)>()?;
        Ok(SelectorFunction { function, args })
    }
}

impl Peek for SelectorFunction {
    fn peek(mut s: StatementStream<'_>) -> bool {
        if s.parse::<Option<Name>>().transpose().is_some() {
            s.check::<LeftParen>()
        } else {
            false
        }
    }
}

impl Display for SelectorFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}({})",
            self.function,
            self.args.iter().map(|s| s.to_string()).collect::<Vec<_>>().join(", ")
        )
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
pub enum SelectorKind {
    Column(Name),
    Term(Term),
    Cast(Box<Selector>, CqlType),
    Function(SelectorFunction),
    Count,
}

impl Parse for SelectorKind {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        Ok(if s.parse::<Option<CAST>>()?.is_some() {
            let (selector, _, cql_type) = s.parse_from::<Parens<(Selector, AS, CqlType)>>()?;
            Self::Cast(Box::new(selector), cql_type)
        } else if s.parse::<Option<COUNT>>()?.is_some() {
            // TODO: Double check that this is ok
            s.parse_from::<Parens<char>>()?;
            Self::Count
        } else if let Some(f) = s.parse()? {
            Self::Function(f)
        } else if let Some(id) = s.parse()? {
            Self::Column(id)
        } else if let Some(term) = s.parse()? {
            Self::Term(term)
        } else {
            anyhow::bail!("Invalid selector: {}", s.info())
        })
    }
}

impl Display for SelectorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectorKind::Column(id) => id.fmt(f),
            SelectorKind::Term(term) => term.fmt(f),
            SelectorKind::Cast(selector, cql_type) => write!(f, "CAST({} AS {})", selector, cql_type),
            SelectorKind::Function(func) => func.fmt(f),
            SelectorKind::Count => write!(f, "COUNT(*)"),
        }
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option))]
pub struct InsertStatement {
    #[builder(setter(into))]
    pub table: KeyspaceQualifiedName,
    #[builder(setter(into))]
    pub kind: InsertKind,
    #[builder(setter(name = "set_if_not_exists"), default)]
    pub if_not_exists: bool,
    #[builder(default)]
    pub using: Option<Vec<UpdateParameter>>,
}

impl InsertStatementBuilder {
    /// Set IF NOT EXISTS on the statement.
    /// To undo this, use `set_if_not_exists(false)`.
    pub fn if_not_exists(&mut self) -> &mut Self {
        self.if_not_exists.replace(true);
        self
    }
}

impl Parse for InsertStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<(INSERT, INTO)>()?;
        let mut res = InsertStatementBuilder::default();
        res.table(s.parse::<KeyspaceQualifiedName>()?)
            .kind(s.parse::<InsertKind>()?);
        loop {
            if s.remaining() == 0 || s.parse::<Option<Semicolon>>()?.is_some() {
                break;
            }
            if s.parse::<Option<(IF, NOT, EXISTS)>>()?.is_some() {
                if res.if_not_exists.is_some() {
                    anyhow::bail!("Duplicate IF NOT EXISTS clause!");
                }
                res.if_not_exists();
            } else if s.parse::<Option<USING>>()?.is_some() {
                if res.using.is_some() {
                    anyhow::bail!("Duplicate USING clause!");
                }
                res.using(s.parse_from::<List<UpdateParameter, AND>>()?);
            } else {
                return Ok(res
                    .build()
                    .map_err(|_| anyhow::anyhow!("Invalid tokens in INSERT statement: {}", s.info()))?);
            }
        }
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid INSERT statement: {}", e))?)
    }
}

impl Peek for InsertStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<INSERT>()
    }
}

impl Display for InsertStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "INSERT INTO {} {}", self.table, self.kind)?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }
        if let Some(using) = &self.using {
            if !using.is_empty() {
                write!(
                    f,
                    " USING {}",
                    using.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(" AND ")
                )?;
            }
        }
        Ok(())
    }
}

impl KeyspaceExt for InsertStatement {
    fn get_keyspace(&self) -> Option<String> {
        self.table.keyspace.as_ref().map(|n| n.to_string())
    }

    fn set_keyspace(&mut self, keyspace: impl Into<Name>) {
        self.table.keyspace.replace(keyspace.into());
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
pub enum InsertKind {
    NameValue {
        names: Vec<Name>,
        values: TupleLiteral,
    },
    Json {
        json: LitStr,
        default: Option<ColumnDefault>,
    },
}

impl InsertKind {
    pub fn name_value(names: Vec<Name>, values: Vec<Term>) -> anyhow::Result<Self> {
        if names.is_empty() {
            anyhow::bail!("No column names specified!");
        }
        if values.is_empty() {
            anyhow::bail!("No values specified!");
        }
        if names.len() != values.len() {
            anyhow::bail!("Number of column names and values do not match!");
        }
        Ok(Self::NameValue {
            names,
            values: values.into(),
        })
    }

    pub fn json<S: Into<LitStr>, O: Into<Option<ColumnDefault>>>(json: S, default: O) -> Self {
        Self::Json {
            json: json.into(),
            default: default.into(),
        }
    }
}

impl Parse for InsertKind {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        if s.parse::<Option<JSON>>()?.is_some() {
            let (json, default) = s.parse_from::<(LitStr, Option<(DEFAULT, ColumnDefault)>)>()?;
            Ok(Self::Json {
                json,
                default: default.map(|(_, d)| d),
            })
        } else {
            let (names, _, values) = s.parse_from::<(Parens<List<Name, Comma>>, VALUES, TupleLiteral)>()?;
            Ok(Self::NameValue { names, values })
        }
    }
}

impl Display for InsertKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            InsertKind::NameValue { names, values } => write!(
                f,
                "({}) VALUES {}",
                names.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", "),
                values
            ),
            InsertKind::Json { json, default } => {
                write!(f, "JSON {}", json)?;
                if let Some(default) = default {
                    write!(f, " DEFAULT {}", default)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
pub enum UpdateParameter {
    TTL(Limit),
    Timestamp(Limit),
    Timeout(DurationLiteral),
}

impl UpdateParameter {
    pub fn ttl(limit: impl Into<Limit>) -> Self {
        Self::TTL(limit.into())
    }

    pub fn timestamp(limit: impl Into<Limit>) -> Self {
        Self::Timestamp(limit.into())
    }

    pub fn timeout(duration: impl Into<DurationLiteral>) -> Self {
        Self::Timeout(duration.into())
    }
}

impl Parse for UpdateParameter {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        if s.parse::<Option<TTL>>()?.is_some() {
            Ok(UpdateParameter::TTL(s.parse()?))
        } else if s.parse::<Option<TIMESTAMP>>()?.is_some() {
            Ok(UpdateParameter::Timestamp(s.parse()?))
        } else if s.parse::<Option<TIMEOUT>>()?.is_some() {
            Ok(UpdateParameter::Timeout(s.parse()?))
        } else {
            anyhow::bail!("Invalid update parameter: {}", s.info())
        }
    }
}

impl Peek for UpdateParameter {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(TTL, Limit)>() || s.check::<(TIMESTAMP, Limit)>() || s.check::<(TIMEOUT, DurationLiteral)>()
    }
}

impl Display for UpdateParameter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateParameter::TTL(limit) => write!(f, "TTL {}", limit),
            UpdateParameter::Timestamp(limit) => write!(f, "TIMESTAMP {}", limit),
            UpdateParameter::Timeout(duration) => write!(f, "TIMEOUT {}", duration),
        }
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option), build_fn(validate = "Self::validate"))]
pub struct UpdateStatement {
    #[builder(setter(into))]
    pub table: KeyspaceQualifiedName,
    #[builder(default)]
    pub using: Option<Vec<UpdateParameter>>,
    pub set_clause: Vec<Assignment>,
    #[builder(setter(into))]
    pub where_clause: WhereClause,
    #[builder(setter(into), default)]
    pub if_clause: Option<IfClause>,
}

impl UpdateStatementBuilder {
    /// Set IF EXISTS on the statement.
    pub fn if_exists(&mut self) -> &mut Self {
        self.if_clause.replace(Some(IfClause::Exists));
        self
    }

    fn validate(&self) -> Result<(), String> {
        if self.set_clause.as_ref().map(|s| s.is_empty()).unwrap_or(false) {
            return Err("SET clause assignments cannot be empty".to_string());
        }
        if self
            .where_clause
            .as_ref()
            .map(|s| s.relations.is_empty())
            .unwrap_or(false)
        {
            return Err("WHERE clause cannot be empty".to_string());
        }
        Ok(())
    }
}

impl Parse for UpdateStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<UPDATE>()?;
        let mut res = UpdateStatementBuilder::default();
        res.table(s.parse::<KeyspaceQualifiedName>()?);
        if let Some(u) = s.parse_from::<If<USING, List<UpdateParameter, AND>>>()? {
            res.using(u);
        }
        res.set_clause(s.parse_from::<(SET, List<Assignment, Comma>)>()?.1)
            .where_clause(s.parse::<WhereClause>()?);
        if let Some(i) = s.parse::<Option<IfClause>>()? {
            res.if_clause(i);
        }
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid UPDATE statement: {}", e))?)
    }
}

impl Peek for UpdateStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<UPDATE>()
    }
}

impl Display for UpdateStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UPDATE {}", self.table)?;
        if let Some(using) = &self.using {
            if !using.is_empty() {
                write!(
                    f,
                    " USING {}",
                    using.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(" AND ")
                )?;
            }
        }
        write!(
            f,
            " SET {} {}",
            self.set_clause
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            self.where_clause
        )?;
        if let Some(if_clause) = &self.if_clause {
            write!(f, " {}", if_clause)?;
        }
        Ok(())
    }
}

impl KeyspaceExt for UpdateStatement {
    fn get_keyspace(&self) -> Option<String> {
        self.table.keyspace.as_ref().map(|n| n.to_string())
    }

    fn set_keyspace(&mut self, keyspace: impl Into<Name>) {
        self.table.keyspace.replace(keyspace.into());
    }
}

impl WhereExt for UpdateStatement {
    fn iter_where(&self) -> Option<std::slice::Iter<Relation>> {
        Some(self.where_clause.relations.iter())
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
pub enum Assignment {
    Simple {
        selection: SimpleSelection,
        term: Term,
    },
    Arithmetic {
        assignee: Name,
        lhs: Name,
        op: ArithmeticOp,
        rhs: Term,
    },
    Append {
        assignee: Name,
        list: ListLiteral,
        item: Name,
    },
}

impl Assignment {
    pub fn simple(selection: impl Into<SimpleSelection>, term: impl Into<Term>) -> Self {
        Self::Simple {
            selection: selection.into(),
            term: term.into(),
        }
    }

    pub fn arithmetic(assignee: impl Into<Name>, lhs: impl Into<Name>, op: ArithmeticOp, rhs: impl Into<Term>) -> Self {
        Self::Arithmetic {
            assignee: assignee.into(),
            lhs: lhs.into(),
            op,
            rhs: rhs.into(),
        }
    }

    pub fn append(assignee: impl Into<Name>, list: Vec<impl Into<Term>>, item: impl Into<Name>) -> Self {
        Self::Append {
            assignee: assignee.into(),
            list: list.into_iter().map(Into::into).collect::<Vec<_>>().into(),
            item: item.into(),
        }
    }
}

impl Parse for Assignment {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(
            if let Some((assignee, _, list, _, item)) = s.parse::<Option<(_, Equals, _, Plus, _)>>()? {
                Self::Append { assignee, list, item }
            } else if let Some((assignee, _, lhs, op, rhs)) = s.parse::<Option<(_, Equals, _, _, _)>>()? {
                Self::Arithmetic { assignee, lhs, op, rhs }
            } else {
                let (selection, _, term) = s.parse::<(_, Equals, _)>()?;
                Self::Simple { selection, term }
            },
        )
    }
}

impl Display for Assignment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Assignment::Simple { selection, term } => write!(f, "{} = {}", selection, term),
            Assignment::Arithmetic { assignee, lhs, op, rhs } => write!(f, "{} = {} {} {}", assignee, lhs, op, rhs),
            Assignment::Append { assignee, list, item } => {
                write!(f, "{} = {} + {}", assignee, list, item)
            }
        }
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
pub enum SimpleSelection {
    Column(Name),
    Term(Name, Term),
    Field(Name, Name),
}

impl SimpleSelection {
    pub fn column<T: Into<Name>>(name: T) -> Self {
        Self::Column(name.into())
    }

    pub fn term<N: Into<Name>, T: Into<Term>>(name: N, term: T) -> Self {
        Self::Term(name.into(), term.into())
    }

    pub fn field<T: Into<Name>>(name: T, field: T) -> Self {
        Self::Field(name.into(), field.into())
    }
}

impl Parse for SimpleSelection {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some((column, _, field)) = s.parse::<Option<(_, Dot, _)>>()? {
            Self::Field(column, field)
        } else if let Some((column, term)) = s.parse_from::<Option<(Name, Brackets<Term>)>>()? {
            Self::Term(column, term)
        } else {
            Self::Column(s.parse()?)
        })
    }
}

impl Display for SimpleSelection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Column(name) => name.fmt(f),
            Self::Term(name, term) => write!(f, "{}[{}]", name, term),
            Self::Field(column, field) => write!(f, "{}.{}", column, field),
        }
    }
}

impl<N: Into<Name>> From<N> for SimpleSelection {
    fn from(name: N) -> Self {
        Self::Column(name.into())
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
pub struct Condition {
    pub lhs: SimpleSelection,
    pub op: Operator,
    pub rhs: Term,
}

impl Condition {
    pub fn new(lhs: impl Into<SimpleSelection>, op: impl Into<Operator>, rhs: impl Into<Term>) -> Self {
        Self {
            lhs: lhs.into(),
            op: op.into(),
            rhs: rhs.into(),
        }
    }
}

impl Parse for Condition {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        let (lhs, op, rhs) = s.parse()?;
        Ok(Condition { lhs, op, rhs })
    }
}

impl Display for Condition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.lhs, self.op, self.rhs)
    }
}

#[derive(ParseFromStr, Clone, Debug, ToTokens, PartialEq, Eq)]
pub enum IfClause {
    Exists,
    Conditions(Vec<Condition>),
}

impl IfClause {
    pub fn exists() -> Self {
        Self::Exists
    }

    pub fn conditions<T: Into<Condition>>(conditions: Vec<T>) -> Self {
        Self::Conditions(conditions.into_iter().map(Into::into).collect())
    }
}

impl Parse for IfClause {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<IF>()?;
        Ok(if s.parse::<Option<EXISTS>>()?.is_some() {
            IfClause::Exists
        } else {
            IfClause::Conditions(s.parse_from::<List<Condition, AND>>()?)
        })
    }
}
impl Peek for IfClause {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<IF>()
    }
}

impl Display for IfClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exists => write!(f, "IF EXISTS"),
            Self::Conditions(conditions) => {
                if conditions.is_empty() {
                    return Ok(());
                }
                write!(
                    f,
                    "IF {}",
                    conditions
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>()
                        .join(" AND ")
                )
            }
        }
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(setter(strip_option), build_fn(validate = "Self::validate"))]
pub struct DeleteStatement {
    #[builder(default)]
    pub selections: Option<Vec<SimpleSelection>>,
    #[builder(setter(into))]
    pub from: KeyspaceQualifiedName,
    #[builder(default)]
    pub using: Option<Vec<UpdateParameter>>,
    #[builder(setter(into))]
    pub where_clause: WhereClause,
    #[builder(default)]
    pub if_clause: Option<IfClause>,
}

impl DeleteStatementBuilder {
    /// Set IF EXISTS on the statement.
    pub fn if_exists(&mut self) -> &mut Self {
        self.if_clause.replace(Some(IfClause::Exists));
        self
    }

    fn validate(&self) -> Result<(), String> {
        if self
            .where_clause
            .as_ref()
            .map(|s| s.relations.is_empty())
            .unwrap_or(false)
        {
            return Err("WHERE clause cannot be empty".to_string());
        }
        Ok(())
    }
}

impl Parse for DeleteStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<DELETE>()?;
        let mut res = DeleteStatementBuilder::default();
        if let Some(s) = s.parse_from::<Option<List<SimpleSelection, Comma>>>()? {
            res.selections(s);
        }
        res.from(s.parse::<(FROM, KeyspaceQualifiedName)>()?.1);
        if let Some(u) = s.parse_from::<If<USING, List<UpdateParameter, AND>>>()? {
            res.using(u);
        }
        res.where_clause(s.parse::<WhereClause>()?);
        if let Some(i) = s.parse::<Option<IfClause>>()? {
            res.if_clause(i);
        }
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid DELETE statement: {}", e))?)
    }
}

impl Peek for DeleteStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<DELETE>()
    }
}

impl Display for DeleteStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DELETE")?;
        if let Some(selections) = &self.selections {
            if !selections.is_empty() {
                write!(
                    f,
                    " {}",
                    selections.iter().map(|s| s.to_string()).collect::<Vec<_>>().join(", ")
                )?;
            }
        }
        write!(f, " FROM {}", self.from)?;
        if let Some(using) = &self.using {
            if !using.is_empty() {
                write!(
                    f,
                    " USING {}",
                    using.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", ")
                )?;
            }
        }
        write!(f, " {}", self.where_clause)?;
        if let Some(if_clause) = &self.if_clause {
            write!(f, " {}", if_clause)?;
        }
        Ok(())
    }
}

impl KeyspaceExt for DeleteStatement {
    fn get_keyspace(&self) -> Option<String> {
        self.from.keyspace.as_ref().map(|n| n.to_string())
    }

    fn set_keyspace(&mut self, keyspace: impl Into<Name>) {
        self.from.keyspace.replace(keyspace.into());
    }
}

impl WhereExt for DeleteStatement {
    fn iter_where(&self) -> Option<std::slice::Iter<Relation>> {
        Some(self.where_clause.relations.iter())
    }
}

#[derive(ParseFromStr, Builder, Clone, Debug, ToTokens, PartialEq, Eq)]
#[builder(build_fn(validate = "Self::validate"))]
pub struct BatchStatement {
    #[builder(default)]
    pub kind: BatchKind,
    #[builder(setter(strip_option), default)]
    pub using: Option<Vec<UpdateParameter>>,
    pub statements: Vec<ModificationStatement>,
}

impl BatchStatement {
    pub fn add_parse_statement(&mut self, statement: &str) -> anyhow::Result<()> {
        self.statements.push(statement.parse()?);
        Ok(())
    }

    pub fn add_statement(&mut self, statement: ModificationStatement) {
        self.statements.push(statement);
    }

    pub fn parse_statement(mut self, statement: &str) -> anyhow::Result<Self> {
        self.add_parse_statement(statement)?;
        Ok(self)
    }

    pub fn statement(mut self, statement: ModificationStatement) -> Self {
        self.add_statement(statement);
        self
    }

    pub fn insert(mut self, statement: InsertStatement) -> Self {
        self.statements.push(statement.into());
        self
    }

    pub fn update(mut self, statement: UpdateStatement) -> Self {
        self.statements.push(statement.into());
        self
    }

    pub fn delete(mut self, statement: DeleteStatement) -> Self {
        self.statements.push(statement.into());
        self
    }
}

impl BatchStatementBuilder {
    pub fn parse_statement(&mut self, statement: &str) -> anyhow::Result<&mut Self> {
        self.statements
            .get_or_insert_with(Default::default)
            .push(statement.parse()?);
        Ok(self)
    }

    pub fn statement(&mut self, statement: ModificationStatement) -> &mut Self {
        self.statements.get_or_insert_with(Default::default).push(statement);
        self
    }

    pub fn insert(&mut self, statement: InsertStatement) -> &mut Self {
        self.statements
            .get_or_insert_with(Default::default)
            .push(statement.into());
        self
    }

    pub fn update(&mut self, statement: UpdateStatement) -> &mut Self {
        self.statements
            .get_or_insert_with(Default::default)
            .push(statement.into());
        self
    }

    pub fn delete(&mut self, statement: DeleteStatement) -> &mut Self {
        self.statements
            .get_or_insert_with(Default::default)
            .push(statement.into());
        self
    }

    fn validate(&self) -> Result<(), String> {
        if self.statements.as_ref().map(|s| s.is_empty()).unwrap_or(false) {
            return Err("Batch cannot contain zero statements".to_string());
        }
        Ok(())
    }
}

impl Parse for BatchStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        s.parse::<BEGIN>()?;
        let mut res = BatchStatementBuilder::default();
        res.kind(s.parse()?);
        s.parse::<BATCH>()?;
        if let Some(u) = s.parse_from::<If<USING, List<UpdateParameter, AND>>>()? {
            res.using(u);
        }
        let mut statements = Vec::new();
        while let Some(res) = s.parse::<Option<ModificationStatement>>()? {
            statements.push(res);
        }
        res.statements(statements);
        s.parse::<(APPLY, BATCH)>()?;
        s.parse::<Option<Semicolon>>()?;
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid BATCH statement: {}", e))?)
    }
}

impl Peek for BatchStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<BEGIN>()
    }
}

impl Display for BatchStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BEGIN")?;
        match self.kind {
            BatchKind::Logged => (),
            BatchKind::Unlogged => write!(f, " UNLOGGED")?,
            BatchKind::Counter => write!(f, " COUNTER")?,
        };
        write!(f, " BATCH")?;
        if let Some(using) = &self.using {
            if !using.is_empty() {
                write!(
                    f,
                    " USING {}",
                    using.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(" AND ")
                )?;
            }
        }
        write!(
            f,
            " {}",
            self.statements
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join("; ")
        )?;
        write!(f, " APPLY BATCH")?;
        Ok(())
    }
}

#[derive(ParseFromStr, Clone, Debug, TryInto, From, ToTokens, PartialEq, Eq)]
pub enum ModificationStatement {
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
}

impl Parse for ModificationStatement {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if let Some(keyword) = s.find::<ReservedKeyword>() {
            match keyword {
                ReservedKeyword::INSERT => Self::Insert(s.parse()?),
                ReservedKeyword::UPDATE => Self::Update(s.parse()?),
                ReservedKeyword::DELETE => Self::Delete(s.parse()?),
                _ => anyhow::bail!(
                    "Expected a data modification statement (INSERT / UPDATE / DELETE)! Found {}",
                    keyword
                ),
            }
        } else {
            anyhow::bail!(
                "Expected a data modification statement (INSERT / UPDATE / DELETE), found {}",
                s.info()
            )
        })
    }
}
impl Peek for ModificationStatement {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<InsertStatement>() || s.check::<UpdateStatement>() || s.check::<DeleteStatement>()
    }
}

impl Display for ModificationStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Insert(s) => s.fmt(f),
            Self::Update(s) => s.fmt(f),
            Self::Delete(s) => s.fmt(f),
        }
    }
}

#[derive(Copy, Clone, Debug, ToTokens, PartialEq, Eq)]
pub enum BatchKind {
    Logged,
    Unlogged,
    Counter,
}

impl Parse for BatchKind {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        Ok(if s.parse::<Option<UNLOGGED>>()?.is_some() {
            BatchKind::Unlogged
        } else if s.parse::<Option<COUNTER>>()?.is_some() {
            BatchKind::Counter
        } else {
            BatchKind::Logged
        })
    }
}

impl Default for BatchKind {
    fn default() -> Self {
        BatchKind::Logged
    }
}

#[derive(Clone, Debug, ToTokens, PartialEq, Eq)]
pub struct WhereClause {
    pub relations: Vec<Relation>,
}

impl Parse for WhereClause {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        let (_, relations) = s.parse_from::<(WHERE, List<Relation, AND>)>()?;
        Ok(WhereClause { relations })
    }
}

impl Peek for WhereClause {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<WHERE>()
    }
}

impl Display for WhereClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.relations.is_empty() {
            return Ok(());
        }
        write!(
            f,
            "WHERE {}",
            self.relations
                .iter()
                .map(|r| r.to_string())
                .collect::<Vec<_>>()
                .join(" AND ")
        )
    }
}

impl From<Vec<Relation>> for WhereClause {
    fn from(relations: Vec<Relation>) -> Self {
        WhereClause { relations }
    }
}

#[derive(Clone, Debug, ToTokens, PartialEq, Eq)]
pub struct GroupByClause {
    pub columns: Vec<Name>,
}

impl Parse for GroupByClause {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        let (_, _, columns) = s.parse_from::<(GROUP, BY, List<Name, Comma>)>()?;
        Ok(GroupByClause { columns })
    }
}

impl Peek for GroupByClause {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(GROUP, BY)>()
    }
}

impl Display for GroupByClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.columns.is_empty() {
            return Ok(());
        }
        write!(
            f,
            "GROUP BY {}",
            self.columns
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl<T: Into<Name>> From<Vec<T>> for GroupByClause {
    fn from(columns: Vec<T>) -> Self {
        GroupByClause {
            columns: columns.into_iter().map(|c| c.into()).collect(),
        }
    }
}

#[derive(Clone, Debug, ToTokens, PartialEq, Eq)]
pub struct OrderByClause {
    pub columns: Vec<ColumnOrder>,
}

impl Parse for OrderByClause {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self> {
        let (_, _, columns) = s.parse_from::<(ORDER, BY, List<ColumnOrder, Comma>)>()?;
        Ok(OrderByClause { columns })
    }
}

impl Peek for OrderByClause {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<(ORDER, BY)>()
    }
}

impl Display for OrderByClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.columns.is_empty() {
            return Ok(());
        }
        write!(
            f,
            "ORDER BY {}",
            self.columns
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl<T: Into<ColumnOrder>> From<Vec<T>> for OrderByClause {
    fn from(columns: Vec<T>) -> Self {
        OrderByClause {
            columns: columns.into_iter().map(|c| c.into()).collect(),
        }
    }
}

#[derive(Clone, Debug, From, ToTokens, PartialEq, Eq)]
pub enum Limit {
    Literal(i32),
    #[from(ignore)]
    BindMarker(BindMarker),
}

impl Parse for Limit {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        if let Some(bind) = s.parse::<Option<BindMarker>>()? {
            Ok(Limit::BindMarker(bind))
        } else {
            Ok(Limit::Literal(s.parse::<i32>()?))
        }
    }
}

impl Peek for Limit {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<i32>() || s.check::<BindMarker>()
    }
}

impl Display for Limit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Limit::Literal(i) => i.fmt(f),
            Limit::BindMarker(b) => b.fmt(f),
        }
    }
}

impl<T: Into<BindMarker>> From<T> for Limit {
    fn from(bind: T) -> Self {
        Limit::BindMarker(bind.into())
    }
}

#[derive(Copy, Clone, Debug, ToTokens, PartialEq, Eq)]
pub enum ColumnDefault {
    Null,
    Unset,
}

impl Parse for ColumnDefault {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
        if s.parse::<Option<NULL>>()?.is_some() {
            Ok(ColumnDefault::Null)
        } else if s.parse::<Option<UNSET>>()?.is_some() {
            Ok(ColumnDefault::Unset)
        } else {
            anyhow::bail!("Invalid column default: {}", s.info())
        }
    }
}

impl Peek for ColumnDefault {
    fn peek(s: StatementStream<'_>) -> bool {
        s.check::<NULL>() || s.check::<UNSET>()
    }
}

impl Display for ColumnDefault {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnDefault::Null => write!(f, "NULL"),
            ColumnDefault::Unset => write!(f, "UNSET"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        KeyspaceQualifyExt,
        Order,
    };

    #[test]
    fn test_parse_select() {
        let mut builder = SelectStatementBuilder::default();
        builder.select_clause(vec![
            Selector::column("movie"),
            Selector::column("director").as_id("Movie Director"),
        ]);
        assert!(builder.build().is_err());
        builder.from("movies".dot("NerdMovies"));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.where_clause(vec![
            Relation::normal("year", Operator::Equal, 2012_i32),
            Relation::tuple(
                vec!["main_actor"],
                Operator::In,
                vec![LitStr::from("Nathan Fillion"), LitStr::from("John O'Goodman")],
            ),
            Relation::token(
                vec!["director"],
                Operator::GreaterThan,
                FunctionCall::new("token", vec![LitStr::from("movie")]),
            ),
        ]);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.distinct();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.select_clause(SelectClause::All);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.group_by_clause(vec!["director", "main_actor", "year", "movie"]);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.order_by_clause(vec![("director", Order::Ascending), ("year", Order::Descending)]);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.per_partition_limit(10);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.limit(BindMarker::Anonymous);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.limit("bind_marker");
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.allow_filtering().bypass_cache();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.timeout(std::time::Duration::from_secs(10));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_insert() {
        let mut builder = InsertStatementBuilder::default();
        builder.table("test");
        assert!(builder.build().is_err());
        builder.kind(
            InsertKind::name_value(
                vec!["movie".into(), "director".into(), "main_actor".into(), "year".into()],
                vec![
                    LitStr::from("Serenity").into(),
                    LitStr::from("Joss Whedon").into(),
                    LitStr::from("Nathan Fillion").into(),
                    2005_i32.into(),
                ],
            )
            .unwrap(),
        );
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_not_exists();
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.using(vec![
            UpdateParameter::ttl(86400),
            UpdateParameter::timestamp(1000),
            UpdateParameter::timeout(std::time::Duration::from_secs(60)),
        ]);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.kind(InsertKind::json(
            r#"{
                "movie": "Serenity", 
                "director": "Joss Whedon", 
                "main_actor": "Nathan Fillion", 
                "year": 2005
            }"#,
            ColumnDefault::Null,
        ));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_update() {
        let mut builder = UpdateStatementBuilder::default();
        builder.table("test");
        assert!(builder.build().is_err());
        builder.set_clause(vec![
            Assignment::simple("director", LitStr::from("Joss Whedon")),
            Assignment::simple("main_actor", LitStr::from("Nathan Fillion")),
            Assignment::arithmetic("year", "year", ArithmeticOp::Add, 10_i32),
            Assignment::append("my_list", vec![LitStr::from("foo"), LitStr::from("bar")], "my_list"),
        ]);
        assert!(builder.build().is_err());
        builder.where_clause(vec![Relation::normal(
            "movie",
            Operator::Equal,
            LitStr::from("Serenity"),
        )]);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_clause(IfClause::Exists);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_clause(IfClause::conditions(vec![
            Condition::new("director", Operator::Equal, LitStr::from("Joss Whedon")),
            Condition::new(
                SimpleSelection::field("my_type", "my_field"),
                Operator::LessThan,
                100_i32,
            ),
            Condition::new(
                SimpleSelection::term("my_list", 0_i32),
                Operator::Like,
                LitStr::from("foo%"),
            ),
        ]));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.using(vec![
            UpdateParameter::ttl(86400),
            UpdateParameter::timestamp(1000),
            UpdateParameter::timeout(std::time::Duration::from_secs(60)),
        ]);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_delete() {
        let mut builder = DeleteStatementBuilder::default();
        builder.from("test");
        assert!(builder.build().is_err());
        builder.where_clause(vec![Relation::normal(
            "movie",
            Operator::Equal,
            LitStr::from("Serenity"),
        )]);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_clause(IfClause::Exists);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.if_clause(IfClause::conditions(vec![
            Condition::new("director", Operator::Equal, LitStr::from("Joss Whedon")),
            Condition::new(
                SimpleSelection::field("my_type", "my_field"),
                Operator::LessThan,
                100_i32,
            ),
            Condition::new(
                SimpleSelection::term("my_list", 0_i32),
                Operator::Like,
                LitStr::from("foo%"),
            ),
        ]));
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }

    #[test]
    fn test_parse_batch() {
        let mut builder = BatchStatementBuilder::default();
        builder.using(vec![
            UpdateParameter::ttl(86400),
            UpdateParameter::timestamp(1000),
            UpdateParameter::timeout(std::time::Duration::from_secs(60)),
        ]);
        assert!(builder.build().is_err());
        builder.insert(
            InsertStatementBuilder::default()
                .table("NerdMovies")
                .kind(
                    InsertKind::name_value(
                        vec!["movie".into(), "director".into(), "main_actor".into(), "year".into()],
                        vec![
                            LitStr::from("Serenity").into(),
                            LitStr::from("Joss Whedon").into(),
                            LitStr::from("Nathan Fillion").into(),
                            2005_i32.into(),
                        ],
                    )
                    .unwrap(),
                )
                .if_not_exists()
                .using(vec![UpdateParameter::ttl(86400)])
                .build()
                .unwrap(),
        );
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.kind(BatchKind::Unlogged);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.kind(BatchKind::Logged);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder.kind(BatchKind::Counter);
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
        builder
            .update(
                UpdateStatementBuilder::default()
                    .table("NerdMovies")
                    .set_clause(vec![
                        Assignment::simple("director", LitStr::from("Joss Whedon")),
                        Assignment::simple("main_actor", LitStr::from("Nathan Fillion")),
                    ])
                    .where_clause(vec![Relation::normal(
                        "movie",
                        Operator::Equal,
                        LitStr::from("Serenity"),
                    )])
                    .if_clause(IfClause::Exists)
                    .build()
                    .unwrap(),
            )
            .delete(
                DeleteStatementBuilder::default()
                    .from("NerdMovies")
                    .where_clause(vec![Relation::normal(
                        "movie",
                        Operator::Equal,
                        LitStr::from("Serenity"),
                    )])
                    .if_clause(IfClause::Exists)
                    .build()
                    .unwrap(),
            );
        let statement = builder.build().unwrap().to_string();
        assert_eq!(builder.build().unwrap(), statement.parse().unwrap());
    }
}
