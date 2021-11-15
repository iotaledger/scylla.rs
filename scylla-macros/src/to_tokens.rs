use std::collections::HashMap;

use quote::{
    quote,
    ToTokens,
};
use scylla_parse::*;

pub struct Tokenable<T>(pub T);

impl ToTokens for Tokenable<&DataManipulationStatement> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        match &self.0 {
            DataManipulationStatement::Select(s) => {
                let s = Tokenable(s);
                tokens.extend(quote! {DataManipulationStatement::Select(#s)});
            }
            DataManipulationStatement::Insert(s) => {
                let s = Tokenable(s);
                tokens.extend(quote! {DataManipulationStatement::Insert(#s)});
            }
            DataManipulationStatement::Update(s) => {
                let s = Tokenable(s);
                tokens.extend(quote! {DataManipulationStatement::Update(#s)});
            }
            DataManipulationStatement::Delete(s) => {
                let s = Tokenable(s);
                tokens.extend(quote! {DataManipulationStatement::Delete(#s)});
            }
            DataManipulationStatement::Batch(s) => {
                let s = Tokenable(s);
                tokens.extend(quote! {DataManipulationStatement::Batch(#s)});
            }
        }
    }
}
impl ToTokens for Tokenable<&SelectStatement> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let SelectStatement {
            distinct,
            select_clause,
            from,
            where_clause,
            group_by_clause,
            order_by_clause,
            per_partition_limit,
            limit,
            allow_filtering,
            bypass_cache,
            timeout,
        } = self.0;
        let select_clause = Tokenable(select_clause);
        let from = Tokenable(from);
        let where_clause = Tokenable(where_clause);
        let group_by_clause = Tokenable(group_by_clause);
        let order_by_clause = Tokenable(order_by_clause);
        let per_partition_limit = Tokenable(per_partition_limit);
        let limit = Tokenable(limit);
        let timeout = Tokenable(timeout);
        tokens.extend(quote! {
            SelectStatement {
                distinct: #distinct,
                select_clause: #select_clause,
                from: #from,
                where_clause: #where_clause,
                group_by_clause: #group_by_clause,
                order_by_clause: #order_by_clause,
                per_partition_limit: #per_partition_limit,
                limit: #limit,
                allow_filtering: #allow_filtering,
                bypass_cache: #bypass_cache,
                timeout: #timeout,
            }
        });
    }
}

impl ToTokens for Tokenable<&InsertStatement> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let InsertStatement {
            table,
            kind,
            if_not_exists,
            using,
        } = self.0;
        let table = Tokenable(table);
        let kind = Tokenable(kind);
        let using = Tokenable(using);
        tokens.extend(quote! {
            InsertStatement {
                table: #table,
                kind: #kind,
                if_not_exists: #if_not_exists,
                using: #using,
            }
        });
    }
}

impl ToTokens for Tokenable<&UpdateStatement> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let UpdateStatement {
            table,
            using,
            set_clause,
            where_clause,
            if_clause,
        } = self.0;
        let table = Tokenable(table);
        let using = Tokenable(using);
        let set_clause = Tokenable(set_clause);
        let where_clause = Tokenable(where_clause);
        let if_clause = Tokenable(if_clause);
        tokens.extend(quote! {
            UpdateStatement {
                table: #table,
                using: #using,
                set_clause: #set_clause,
                where_clause: #where_clause,
                if_clause: #if_clause,
            }
        });
    }
}

impl ToTokens for Tokenable<&DeleteStatement> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let DeleteStatement {
            selections,
            from,
            using,
            where_clause,
            if_clause,
        } = self.0;
        let selections = Tokenable(selections);
        let from = Tokenable(from);
        let using = Tokenable(using);
        let where_clause = Tokenable(where_clause);
        let if_clause = Tokenable(if_clause);
        tokens.extend(quote! {
            DeleteStatement {
                selections: #selections,
                from: #from,
                using: #using,
                where_clause: #where_clause,
                if_clause: #if_clause,
            }
        });
    }
}

impl ToTokens for Tokenable<&BatchStatement> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let BatchStatement {
            kind,
            using,
            statements,
        } = self.0;
        let kind = Tokenable(kind);
        let using = Tokenable(using);
        let statements = Tokenable(statements);
        tokens.extend(quote! {
            BatchStatement {
                kind: #kind,
                using: #using,
                statements: #statements,
            }
        });
    }
}

impl<'a, T> ToTokens for Tokenable<&'a Option<T>>
where
    Tokenable<&'a T>: ToTokens,
{
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            Some(t) => {
                let t = Tokenable(t);
                quote! {Some(#t)}
            }
            None => quote! {None},
        });
    }
}

impl<'a, T> ToTokens for Tokenable<&'a Box<T>>
where
    Tokenable<&'a T>: ToTokens,
{
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let i = Tokenable(self.0.as_ref());
        tokens.extend(quote! {Box::new(#i)});
    }
}

impl<'a, T> ToTokens for Tokenable<&'a Vec<T>>
where
    Tokenable<&'a T>: ToTokens,
{
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let t = self.0.iter().map(|i| Tokenable(i));
        tokens.extend(quote! { vec![#(#t),*]});
    }
}

impl<'a, K: 'static, V: 'static> ToTokens for Tokenable<&'a HashMap<K, V>>
where
    Tokenable<&'a K>: ToTokens,
    Tokenable<&'a V>: ToTokens,
{
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let t = self.0.iter().map(|(k, v)| {
            let (k, v) = (Tokenable(k), Tokenable(v));
            quote! {#k => #v}
        });
        tokens.extend(quote! { hashmap![#(#t),*]});
    }
}

impl ToTokens for Tokenable<&SelectClauseKind> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        match self.0 {
            SelectClauseKind::All => *tokens = quote! {#tokens SelectClauseKind::All},
            SelectClauseKind::Selectors(s) => {
                let s = Tokenable(s);
                tokens.extend(quote! {SelectClauseKind::Selectors(#s)})
            }
        }
    }
}

impl ToTokens for Tokenable<&FromClause> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let name = Tokenable(&self.0.table);
        tokens.extend(quote! {FromClause {table: #name}})
    }
}

impl ToTokens for Tokenable<&WhereClause> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let r = Tokenable(&self.0.relations);
        tokens.extend(quote! {WhereClause {relations: #r}});
    }
}

impl ToTokens for Tokenable<&GroupByClause> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let c = Tokenable(&self.0.columns);
        tokens.extend(quote! {GroupByClause {columns: #c}});
    }
}

impl ToTokens for Tokenable<&OrderingClause> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let c = Tokenable(&self.0.columns);
        tokens.extend(quote! {OrderingClause {columns: #c}});
    }
}

impl ToTokens for Tokenable<&Limit> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        match self.0 {
            Limit::Literal(l) => {
                tokens.extend(quote! {Limit::Literal(#l)});
            }
            Limit::BindMarker(b) => {
                let b = Tokenable(b);
                tokens.extend(quote! {Limit::BindMarker(#b)});
            }
        }
    }
}

impl ToTokens for Tokenable<&DurationLiteral> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let (months, days, nanos) = (self.0.months, self.0.days, self.0.nanos);
        tokens.extend(quote! {
            DurationLiteral {
                months: #months,
                days: #days,
                nanos: #nanos,
            }
        });
    }
}

impl ToTokens for Tokenable<&Selector> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let (kind, as_id) = (Tokenable(&self.0.kind), Tokenable(&self.0.as_id));
        tokens.extend(quote! {
            Selector {
                kind: #kind,
                as_id: #as_id,
            }
        });
    }
}

impl ToTokens for Tokenable<&TableName> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let (keyspace, name) = (Tokenable(&self.0.keyspace), Tokenable(&self.0.name));
        tokens.extend(quote! {TableName {keyspace: #keyspace, name: #name}});
    }
}

impl ToTokens for Tokenable<&Relation> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        match self.0 {
            Relation::Normal { column, operator, term } => {
                let (column, operator, term) = (Tokenable(column), Tokenable(operator), Tokenable(term));
                tokens.extend(quote! {
                    Relation::Normal {
                        column: #column,
                        operator: #operator,
                        term: #term,
                    }
                });
            }
            Relation::Tuple {
                columns,
                operator,
                tuple_literal,
            } => {
                let (columns, operator, tuple_literal) =
                    (Tokenable(columns), Tokenable(operator), Tokenable(tuple_literal));
                tokens.extend(quote! {
                    Relation::Tuple {
                        columns: #columns,
                        operator: #operator,
                        tuple_literal: #tuple_literal,
                    }
                });
            }
            Relation::Token {
                columns,
                operator,
                term,
            } => {
                let (columns, operator, term) = (Tokenable(columns), Tokenable(operator), Tokenable(term));
                tokens.extend(quote! {
                    Relation::Token {
                        columns: #columns,
                        operator: #operator,
                        term: #term,
                    }
                });
            }
        }
    }
}

impl ToTokens for Tokenable<&Name> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            Name::Quoted(q) => quote! {Name::Quoted(#q.to_string())},
            Name::Unquoted(u) => quote! {Name::Unquoted(#u.to_string())},
        });
    }
}

impl ToTokens for Tokenable<&ColumnOrder> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let (column, order) = (Tokenable(&self.0.column), Tokenable(&self.0.order));
        tokens.extend(quote! {ColumnOrder {column: #column, order: #order}});
    }
}

impl ToTokens for Tokenable<&BindMarker> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            BindMarker::Anonymous => quote! {BindMarker::Anonymous},
            BindMarker::Named(n) => {
                let n = Tokenable(n);
                quote! {BindMarker::Named(#n)}
            }
        })
    }
}

impl ToTokens for Tokenable<&SelectorKind> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            SelectorKind::Column(c) => {
                let c = Tokenable(c);
                quote! {SelectorKind::Column(#c)}
            }
            SelectorKind::Term(t) => {
                let t = Tokenable(t);
                quote! {SelectorKind::Term(#t)}
            }
            SelectorKind::Cast(s, t) => {
                let (s, t) = (Tokenable(s), Tokenable(t));
                quote! {SelectorKind::Cast(#s, #t)}
            }
            SelectorKind::Function(f) => {
                let f = Tokenable(f);
                quote! {SelectorKind::Function(#f)}
            }
            SelectorKind::Count => quote! {SelectorKind::Count},
        });
    }
}

impl ToTokens for Tokenable<&Operator> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            Operator::Equal => quote! {Operator::Equal},
            Operator::NotEqual => quote! {Operator::NotEqual},
            Operator::GreaterThan => quote! {Operator::GreaterThan},
            Operator::GreaterThanOrEqual => quote! {Operator::GreaterThanOrEqual},
            Operator::LessThan => quote! {Operator::LessThan},
            Operator::LessThanOrEqual => quote! {Operator::LessThanOrEqual},
            Operator::In => quote! {Operator::In},
            Operator::Contains => quote! {Operator::Contains},
            Operator::ContainsKey => quote! {Operator::ContainsKey},
            Operator::Like => quote! {Operator::Like},
        });
    }
}

impl ToTokens for Tokenable<&Term> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            Term::Constant(c) => {
                let c = Tokenable(c);
                quote! {Term::Constant(#c)}
            }
            Term::Literal(l) => {
                let l = Tokenable(l);
                quote! {Term::Literal(#l)}
            }
            Term::FunctionCall(f) => {
                let f = Tokenable(f);
                quote! {Term::FunctionCall(#f)}
            }
            Term::ArithmeticOp { lhs, op, rhs } => {
                let (lhs, op, rhs) = (Tokenable(lhs), Tokenable(op), Tokenable(rhs));
                quote! {Term::ArithmeticOp {lhs: #lhs, op: #op, rhs: #rhs}}
            }
            Term::TypeHint { hint, ident } => {
                let (hint, ident) = (Tokenable(hint), Tokenable(ident));
                quote! {Term::TypeHint {hint: #hint, ident: #ident}}
            }
            Term::BindMarker(b) => {
                let b = Tokenable(b);
                quote! {Term::BindMarker(#b)}
            }
        });
    }
}

impl ToTokens for Tokenable<&(Term, Term)> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let (t1, t2) = (Tokenable(&self.0 .0), Tokenable(&self.0 .1));
        tokens.extend(quote! {(#t1, #t2)});
    }
}

impl ToTokens for Tokenable<&TupleLiteral> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let i = Tokenable(&self.0.elements);
        tokens.extend(quote! {TupleLiteral {elements: #i}});
    }
}

impl ToTokens for Tokenable<&Order> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            Order::Ascending => quote! {Order::Ascending},
            Order::Descending => quote! {Order::Descending},
        });
    }
}

impl ToTokens for Tokenable<&CqlType> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            CqlType::Native(n) => {
                let n = Tokenable(n);
                quote! {CqlType::Native(#n)}
            }
            CqlType::Collection(c) => {
                let c = Tokenable(c);
                quote! {CqlType::Collection(#c)}
            }
            CqlType::UserDefined(u) => {
                let u = Tokenable(u);
                quote! {CqlType::UserDefined(#u)}
            }
            CqlType::Tuple(t) => {
                let t = Tokenable(t);
                quote! {CqlType::Tuple(#t)}
            }
            CqlType::Custom(c) => {
                quote! {CqlType::Custom(#c.to_string())}
            }
        });
    }
}

impl ToTokens for Tokenable<&SelectorFunction> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let (function, args) = (Tokenable(&self.0.function), Tokenable(&self.0.args));
        tokens.extend(quote! {SelectorFunction {function: #function, args: #args}});
    }
}

impl ToTokens for Tokenable<&Constant> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            Constant::Null => quote! {Constant::Null},
            Constant::String(s) => quote! {Constant::String(#s.to_string())},
            Constant::Integer(i) => quote! {Constant::Integer(#i.to_string())},
            Constant::Float(f) => quote! {Constant::Float(#f.to_string())},
            Constant::Boolean(b) => quote! {Constant::Boolean(#b)},
            Constant::Uuid(u) => {
                let u = u.to_string();
                quote! {Constant::Uuid(Uuid::parse_str(#u).unwrap())}
            }
            Constant::Hex(h) => {
                quote! {Constant::Hex(vec![#(#h),*])}
            }
            Constant::Blob(b) => {
                quote! {Constant::Blob(vec![#(#b),*])}
            }
        })
    }
}

impl ToTokens for Tokenable<&Literal> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            Literal::Collection(c) => {
                let c = Tokenable(c);
                quote! {Literal::Collection(#c)}
            }
            Literal::UserDefined(u) => {
                let u = Tokenable(u);
                quote! {Literal::UserDefined(#u)}
            }
            Literal::Tuple(t) => {
                let t = Tokenable(t);
                quote! {Literal::Tuple(#t)}
            }
        });
    }
}

impl ToTokens for Tokenable<&FunctionCall> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let (name, args) = (Tokenable(&self.0.name), Tokenable(&self.0.args));
        tokens.extend(quote! {FunctionCall {name: #name, args: #args}});
    }
}

impl ToTokens for Tokenable<&FunctionName> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let (keyspace, name) = (Tokenable(&self.0.keyspace), Tokenable(&self.0.name));
        tokens.extend(quote! {FunctionName {keyspace: #keyspace, name: #name}});
    }
}

impl ToTokens for Tokenable<&ArithmeticOp> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            ArithmeticOp::Add => quote! {ArithmeticOp::Add},
            ArithmeticOp::Sub => quote! {ArithmeticOp::Sub},
            ArithmeticOp::Mul => quote! {ArithmeticOp::Mul},
            ArithmeticOp::Div => quote! {ArithmeticOp::Div},
            ArithmeticOp::Mod => quote! {ArithmeticOp::Mod},
        })
    }
}

impl ToTokens for Tokenable<&NativeType> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            NativeType::Ascii => quote! {NativeType::Ascii},
            NativeType::Bigint => quote! {NativeType::Bigint},
            NativeType::Blob => quote! {NativeType::Blob},
            NativeType::Boolean => quote! {NativeType::Boolean},
            NativeType::Counter => quote! {NativeType::Counter},
            NativeType::Decimal => quote! {NativeType::Decimal},
            NativeType::Double => quote! {NativeType::Double},
            NativeType::Float => quote! {NativeType::Float},
            NativeType::Int => quote! {NativeType::Int},
            NativeType::Text => quote! {NativeType::Text},
            NativeType::Timestamp => quote! {NativeType::Timestamp},
            NativeType::Uuid => quote! {NativeType::Uuid},
            NativeType::Varchar => quote! {NativeType::Varchar},
            NativeType::Varint => quote! {NativeType::Varint},
            NativeType::Timeuuid => quote! {NativeType::Timeuuid},
            NativeType::Inet => quote! {NativeType::Inet},
            NativeType::Date => quote! {NativeType::Date},
            NativeType::Duration => quote! {NativeType::Duration},
            NativeType::Smallint => quote! {NativeType::Smallint},
            NativeType::Time => quote! {NativeType::Time},
            NativeType::Tinyint => quote! {NativeType::Tinyint},
        })
    }
}

impl ToTokens for Tokenable<&CollectionType> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            CollectionType::List(e) => {
                let e = Tokenable(e);
                quote! {CollectionType::List(#e)}
            }
            CollectionType::Set(e) => {
                let e = Tokenable(e);
                quote! {CollectionType::Set(#e)}
            }
            CollectionType::Map(k, v) => {
                let (k, v) = (Tokenable(k), Tokenable(v));
                quote! {CollectionType::Map(#k, #v)}
            }
        })
    }
}

impl ToTokens for Tokenable<&UserDefinedType> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let (keyspace, ident) = (Tokenable(&self.0.keyspace), Tokenable(&self.0.ident));
        tokens.extend(quote! {UserDefinedType {keyspace: #keyspace, ident: #ident}});
    }
}

impl ToTokens for Tokenable<&CollectionTypeLiteral> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            CollectionTypeLiteral::List(l) => {
                let l = Tokenable(l);
                quote! {CollectionTypeLiteral::List(#l)}
            }
            CollectionTypeLiteral::Set(s) => {
                let s = Tokenable(s);
                quote! {CollectionTypeLiteral::Set(#s)}
            }
            CollectionTypeLiteral::Map(m) => {
                let m = Tokenable(m);
                quote! {CollectionTypeLiteral::Map(#m)}
            }
        })
    }
}

impl ToTokens for Tokenable<&UserDefinedTypeLiteral> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let fields = Tokenable(&self.0.fields);
        tokens.extend(quote! {UserDefinedTypeLiteral {fields: #fields}});
    }
}

impl ToTokens for Tokenable<&ListLiteral> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let elements = Tokenable(&self.0.elements);
        tokens.extend(quote! {ListLiteral {elements: #elements}});
    }
}

impl ToTokens for Tokenable<&SetLiteral> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let elements = Tokenable(&self.0.elements);
        tokens.extend(quote! {SetLiteral {elements: #elements}});
    }
}

impl ToTokens for Tokenable<&MapLiteral> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let elements = Tokenable(&self.0.elements);
        tokens.extend(quote! {MapLiteral {elements: #elements}});
    }
}

impl ToTokens for Tokenable<&InsertKind> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            InsertKind::NameValue { names, values } => {
                let (names, values) = (Tokenable(names), Tokenable(values));
                quote! {InsertKind::NameValue {names: #names, values: #values}}
            }
            InsertKind::Json { json, default } => {
                let default = Tokenable(default);
                quote! {InsertKind::Json {json: #json.to_string(), default: #default}}
            }
        })
    }
}

impl ToTokens for Tokenable<&ColumnDefault> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            ColumnDefault::Null => quote! {ColumnDefault::Null},
            ColumnDefault::Unset => quote! {ColumnDefault::Unset},
        })
    }
}

impl ToTokens for Tokenable<&UpdateParameter> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            UpdateParameter::TTL(t) => {
                let t = Tokenable(t);
                quote! {UpdateParameter::TTL(#t)}
            }
            UpdateParameter::Timestamp(t) => {
                let t = Tokenable(t);
                quote! {UpdateParameter::Timestamp(#t)}
            }
            UpdateParameter::Timeout(t) => {
                let t = Tokenable(t);
                quote! {UpdateParameter::Timeout(#t)}
            }
        })
    }
}

impl ToTokens for Tokenable<&Assignment> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            Assignment::Simple { selection, term } => {
                let (selection, term) = (Tokenable(selection), Tokenable(term));
                quote! {Assignment::Simple {selection: #selection, term: #term}}
            }
            Assignment::Arithmetic { assignee, lhs, op, rhs } => {
                let (assignee, lhs, op, rhs) = (Tokenable(assignee), Tokenable(lhs), Tokenable(op), Tokenable(rhs));
                quote! {Assignment::Arithmetic {assignee: #assignee, lhs: #lhs, op: #op, rhs: #rhs}}
            }
            Assignment::Append { assignee, list, item } => {
                let (assignee, list, item) = (Tokenable(assignee), Tokenable(list), Tokenable(item));
                quote! {Assignment::Append {assignee: #assignee, list: #list, item: #item}}
            }
        })
    }
}

impl ToTokens for Tokenable<&SimpleSelection> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            SimpleSelection::Column(c) => {
                let c = Tokenable(c);
                quote! {SimpleSelection::Column(#c)}
            }
            SimpleSelection::Term(n, t) => {
                let (n, t) = (Tokenable(n), Tokenable(t));
                quote! {SimpleSelection::Term(#n, #t)}
            }
            SimpleSelection::Field(n, f) => {
                let (n, f) = (Tokenable(n), Tokenable(f));
                quote! {SimpleSelection::Field(#n, #f)}
            }
        })
    }
}

impl ToTokens for Tokenable<&IfClause> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            IfClause::Exists => quote! {IfClause::Exists},
            IfClause::Conditions(c) => {
                let c = Tokenable(c);
                quote! {IfClause::Conditions(#c)}
            }
        })
    }
}

impl ToTokens for Tokenable<&Condition> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        let (lhs, op, rhs) = (Tokenable(&self.0.lhs), Tokenable(&self.0.op), Tokenable(&self.0.rhs));
        tokens.extend(quote! {Condition {lhs: #lhs, op: #op, rhs: #rhs}});
    }
}

impl ToTokens for Tokenable<&BatchKind> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            BatchKind::Logged => quote! {BatchKind::Logged},
            BatchKind::Unlogged => quote! {BatchKind::Unlogged},
            BatchKind::Counter => quote! {BatchKind::Counter},
        })
    }
}

impl ToTokens for Tokenable<&ModificationStatement> {
    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
        tokens.extend(match self.0 {
            ModificationStatement::Insert(i) => {
                let i = Tokenable(i);
                quote! {ModificationStatement::Insert(#i)}
            }
            ModificationStatement::Update(u) => {
                let u = Tokenable(u);
                quote! {ModificationStatement::Update(#u)}
            }
            ModificationStatement::Delete(d) => {
                let d = Tokenable(d);
                quote! {ModificationStatement::Delete(#d)}
            }
        })
    }
}
