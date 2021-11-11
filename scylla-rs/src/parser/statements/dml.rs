use crate::parser::{
    keywords::*, ArithmeticOp, ColumnDefault, CqlType, DurationLiteral, FromClause, GroupByClause, Identifier, Limit,
    List, ListLiteral, Operator, OrderingClause, Parens, Parse, Peek, StatementStream, TableName, Term, TupleLiteral,
    WhereClause,
};
use derive_builder::Builder;

pub enum DataManipulationStatement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Batch(BatchStatement),
}

#[derive(Builder, Clone, Debug)]
pub struct SelectStatement {
    #[builder(default = "false")]
    pub distinct: bool,
    pub select_clause: SelectClauseKind,
    pub from: FromClause,
    #[builder(default = "None")]
    pub where_clause: Option<WhereClause>,
    #[builder(default = "None")]
    pub group_by_clause: Option<GroupByClause>,
    #[builder(default = "None")]
    pub order_by_clause: Option<OrderingClause>,
    #[builder(default = "None")]
    pub per_partition_limit: Option<Limit>,
    #[builder(default = "None")]
    pub limit: Option<Limit>,
    #[builder(default = "false")]
    pub allow_filtering: bool,
    #[builder(default = "false")]
    pub bypass_cache: bool,
    #[builder(default = "None")]
    pub timeout: Option<DurationLiteral>,
}

impl Parse for SelectStatement {
    type Output = SelectStatement;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        s.parse::<SELECT>()?;
        let mut res = SelectStatementBuilder::default();
        res.distinct(s.parse::<Option<DISTINCT>>()?.is_some())
            .select_clause(s.parse()?)
            .from(s.parse()?)
            .where_clause(s.parse()?)
            .group_by_clause(s.parse()?)
            .order_by_clause(s.parse()?);
        if s.parse_if::<(PER, PARTITION, LIMIT)>().is_some() {
            res.per_partition_limit(Some(s.parse::<Limit>()?));
        }
        if s.parse_if::<LIMIT>().is_some() {
            res.limit(Some(s.parse::<Limit>()?));
        }
        if s.parse_if::<(ALLOW, FILTERING)>().is_some() {
            res.allow_filtering(true);
        }
        if s.parse_if::<(BYPASS, CACHE)>().is_some() {
            res.bypass_cache(true);
        }
        if s.parse_if::<(USING, TIMEOUT)>().is_some() {
            res.timeout(Some(s.parse::<DurationLiteral>()?));
        }
        Ok(res
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid SELECT statement: {}", e))?)
    }
}

#[derive(Clone, Debug)]
pub enum SelectClauseKind {
    All,
    Selectors(Vec<Selector>),
}

impl Parse for SelectClauseKind {
    type Output = SelectClauseKind;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        Ok(if s.parse_if::<Star>().is_some() {
            SelectClauseKind::All
        } else {
            SelectClauseKind::Selectors(s.parse_from::<List<Selector, Comma>>()?)
        })
    }
}

#[derive(Clone, Debug)]
pub struct Selector {
    pub kind: SelectorKind,
    pub as_id: Option<Identifier>,
}

impl Parse for Selector {
    type Output = Selector;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        let (kind, as_id) = s.parse::<(SelectorKind, Option<(AS, Identifier)>)>()?;
        Ok(Self {
            kind,
            as_id: as_id.map(|(_, id)| id),
        })
    }
}

#[derive(Clone, Debug)]
pub struct SelectorFunction {
    pub function: Identifier,
    pub args: Vec<Selector>,
}

impl Parse for SelectorFunction {
    type Output = SelectorFunction;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        let (function, args) = s.parse_from::<(Identifier, Parens<List<Selector, Comma>>)>()?;
        Ok(SelectorFunction { function, args })
    }
}

impl Peek for SelectorFunction {
    fn peek(mut s: StatementStream<'_>) -> bool {
        if s.parse_if::<Identifier>().is_some() {
            s.check::<LeftParen>()
        } else {
            false
        }
    }
}

#[derive(Clone, Debug)]
pub enum SelectorKind {
    Column(Identifier),
    Term(Term),
    Cast(Box<Selector>, CqlType),
    Function(SelectorFunction),
    Count,
}

impl Parse for SelectorKind {
    type Output = SelectorKind;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output>
    where
        Self: Sized,
    {
        Ok(if s.parse_if::<CAST>().is_some() {
            let (selector, _, cql_type) = s.parse_from::<Parens<(Selector, AS, CqlType)>>()?;
            Self::Cast(Box::new(selector), cql_type)
        } else if s.parse_if::<COUNT>().is_some() {
            // TODO: Double check that this is ok
            s.parse_from::<Parens<char>>()?;
            Self::Count
        } else if let Some(f) = s.parse_if() {
            Self::Function(f?)
        } else if let Some(id) = s.parse_if() {
            Self::Column(id?)
        } else if let Some(term) = s.parse_if() {
            Self::Term(term?)
        } else {
            anyhow::bail!("Invalid selector!")
        })
    }
}

#[derive(Copy, Clone, Debug)]
pub enum SelectKind {
    Json,
    Distinct,
}

pub struct InsertStatement {
    pub table: TableName,
    pub kind: InsertKind,
    pub if_not_exists: bool,
    pub using: Vec<UpdateParameter>,
}

pub enum InsertKind {
    NameValue {
        names: Vec<Identifier>,
        values: TupleLiteral,
    },
    Json {
        json: String,
        default: Option<ColumnDefault>,
    },
}

pub enum UpdateParameter {
    TTL(Limit),
    Timestamp(Limit),
}

pub struct UpdateStatement {
    pub table: TableName,
    pub using: Vec<UpdateParameter>,
    pub set_clause: Vec<Assignment>,
    pub where_clause: WhereClause,
    pub if_clause: Option<IfClause>,
}

pub enum Assignment {
    Simple {
        selection: SimpleSelection,
        term: Term,
    },
    Arithmetic {
        assignee: Identifier,
        lhs: Identifier,
        op: ArithmeticOp,
        rhs: Term,
    },
    Append {
        assignee: Identifier,
        list: ListLiteral,
        item: Identifier,
    },
}

pub enum SimpleSelection {
    Column(Identifier),
    Term(Identifier, Term),
    Field(Identifier, Identifier),
}

pub struct Condition {
    pub lhs: SimpleSelection,
    pub op: Operator,
    pub rhs: Term,
}

pub enum IfClause {
    Exists,
    Conditions(Vec<Condition>),
}

pub struct DeleteStatement {
    pub selections: Vec<SimpleSelection>,
    pub from: TableName,
    pub using: Vec<UpdateParameter>,
    pub where_clause: WhereClause,
    pub if_clause: Option<IfClause>,
}

pub struct BatchStatement {
    pub kind: BatchKind,
    pub using: Vec<UpdateParameter>,
    pub statements: Vec<DataManipulationStatement>,
}

pub enum BatchKind {
    Logged,
    Unlogged,
    Counter,
}
