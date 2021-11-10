use crate::parser::{
    keywords::*,
    ArithmeticOp,
    ColumnDefault,
    CqlType,
    GroupByClause,
    Identifier,
    ListLiteral,
    MaybeBound,
    Operator,
    OrderingClause,
    Parse,
    Peek,
    StatementStream,
    TableName,
    Term,
    TupleLiteral,
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
    pub distinct: bool,
    pub select_clause: SelectClauseKind,
    pub from: TableName,
    pub where_clause: Option<WhereClause>,
    pub group_by_clause: Option<GroupByClause>,
    pub order_by_clause: Option<OrderingClause>,
    pub per_partition_limit: Option<MaybeBound>,
    pub limit: Option<MaybeBound>,
    pub allow_filtering: bool,
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
            res.per_partition_limit(s.parse()?);
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
            SelectClauseKind::Selectors(s.parse()?)
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
        Ok(Self {
            kind: s.parse()?,
            as_id: s.parse()?,
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
        let function = s.parse()?;
        s.parse::<LeftParen>()?;
        let args = s.parse()?;
        s.parse::<RightParen>()?;
        Ok(SelectorFunction { function, args })
    }
}

impl Peek for SelectorFunction {
    fn peek(s: StatementStream<'_>) -> bool {
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
            let (_, selector, _, cql_type, _) = s.parse::<(LeftParen, _, AS, _, RightParen)>()?;
            Self::Cast(Box::new(selector), cql_type)
        } else if s.parse_if::<COUNT>().is_some() {
            // TODO: Double check that this is ok
            s.parse::<(LeftParen, char, RightParen)>()?;
            Self::Count
        } else if let Some(f) = s.parse_if() {
            Self::Function(f?)
        } else if let Some(term) = s.parse_if() {
            Self::Term(term?)
        } else if let Some(id) = s.parse_if() {
            Self::Column(id?)
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
    TTL(MaybeBound),
    Timestamp(MaybeBound),
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
