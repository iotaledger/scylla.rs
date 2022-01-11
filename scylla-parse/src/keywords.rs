// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    Alphanumeric,
    CustomToTokens,
    Parse,
    StatementStream,
};
use scylla_parse_macros::{
    ParseFromStr,
    ToTokens,
};
use std::{
    fmt::Display,
    str::FromStr,
};

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, ToTokens)]
pub enum ReservedKeyword {
    ADD,
    ALLOW,
    ALTER,
    AND,
    APPLY,
    ASC,
    AUTHORIZE,
    BATCH,
    BEGIN,
    BY,
    COLUMNFAMILY,
    CREATE,
    DELETE,
    DESC,
    DESCRIBE,
    DROP,
    ENTRIES,
    EXECUTE,
    FROM,
    FULL,
    GRANT,
    IF,
    IN,
    INDEX,
    INFINITY,
    INSERT,
    INTO,
    KEYSPACE,
    LIMIT,
    MODIFY,
    NAN,
    NORECURSIVE,
    NOT,
    NULL,
    OF,
    ON,
    OR,
    ORDER,
    PRIMARY,
    RENAME,
    REPLACE,
    REVOKE,
    SCHEMA,
    SELECT,
    SET,
    TABLE,
    TO,
    TRUNCATE,
    UNLOGGED,
    UPDATE,
    USE,
    USING,
    VIEW,
    WHERE,
    WITH,
}

macro_rules! keyword {
    ($t:ident) => {
        #[derive(ParseFromStr, Copy, Clone, Debug)]
        pub struct $t;
        impl Parse for $t {
            type Output = Self;
            fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
                let this = stringify!($t);
                if let Some(token) = s.nextn(this.len()) {
                    if token.to_uppercase().as_str() != this {
                        anyhow::bail!("Expected keyword '{}', found {}", this, s.info_with_token(token))
                    }
                    Ok($t)
                } else {
                    anyhow::bail!("Expected keyword '{}', found end of stream", this)
                }
            }
        }
    };
}

keyword!(ACCESS);
keyword!(ADD);
keyword!(AGGREGATE);
keyword!(ALL);
keyword!(ALLOW);
keyword!(ALTER);
keyword!(AND);
keyword!(APPLY);
keyword!(AS);
keyword!(ASC);
keyword!(ASCII);
keyword!(AUTHORIZE);
keyword!(BATCH);
keyword!(BEGIN);
keyword!(BIGINT);
keyword!(BLOB);
keyword!(BOOLEAN);
keyword!(BY);
keyword!(BYPASS);
keyword!(CALLED);
keyword!(CACHE);
keyword!(CAST);
keyword!(CLUSTERING);
keyword!(COLUMNFAMILY);
keyword!(COMPACT);
keyword!(CONTAINS);
keyword!(COUNT);
keyword!(COUNTER);
keyword!(CREATE);
keyword!(CUSTOM);
keyword!(DATACENTERS);
keyword!(DATE);
keyword!(DECIMAL);
keyword!(DEFAULT);
keyword!(DELETE);
keyword!(DESC);
keyword!(DESCRIBE);
keyword!(DISTINCT);
keyword!(DOUBLE);
keyword!(DROP);
keyword!(ENTRIES);
keyword!(EXECUTE);
keyword!(EXISTS);
keyword!(FALSE);
keyword!(FILTERING);
keyword!(FINALFUNC);
keyword!(FLOAT);
keyword!(FROM);
keyword!(FROZEN);
keyword!(FULL);
keyword!(FUNCTION);
keyword!(FUNCTIONS);
keyword!(GRANT);
keyword!(GROUP);
keyword!(IF);
keyword!(IN);
keyword!(INDEX);
keyword!(INET);
keyword!(INFINITY);
keyword!(INITCOND);
keyword!(INPUT);
keyword!(INSERT);
keyword!(INT);
keyword!(INTO);
keyword!(IS);
keyword!(JSON);
keyword!(KEY);
keyword!(KEYS);
keyword!(KEYSPACE);
keyword!(KEYSPACES);
keyword!(LANGUAGE);
keyword!(LIKE);
keyword!(LIMIT);
keyword!(LIST);
keyword!(LOGIN);
keyword!(MAP);
keyword!(MATERIALIZED);
keyword!(MBEAN);
keyword!(MBEANS);
keyword!(MODIFY);
keyword!(NAN);
keyword!(NOLOGIN);
keyword!(NORECURSIVE);
keyword!(NOSUPERUSER);
keyword!(NOT);
keyword!(NULL);
keyword!(OF);
keyword!(ON);
keyword!(OPTIONS);
keyword!(OR);
keyword!(ORDER);
keyword!(PARTITION);
keyword!(PASSWORD);
keyword!(PER);
keyword!(PERCENTILE);
keyword!(PERMISSION);
keyword!(PERMISSIONS);
keyword!(PRIMARY);
keyword!(RENAME);
keyword!(REPLACE);
keyword!(RETURNS);
keyword!(REVOKE);
keyword!(ROLE);
keyword!(ROLES);
keyword!(SCHEMA);
keyword!(SELECT);
keyword!(SET);
keyword!(SFUNC);
keyword!(SMALLINT);
keyword!(STATIC);
keyword!(STORAGE);
keyword!(STYPE);
keyword!(SUPERUSER);
keyword!(TABLE);
keyword!(TEXT);
keyword!(TIME);
keyword!(TIMEOUT);
keyword!(TIMESTAMP);
keyword!(TIMEUUID);
keyword!(TINYINT);
keyword!(TO);
keyword!(TOKEN);
keyword!(TRIGGER);
keyword!(TRUNCATE);
keyword!(TTL);
keyword!(TUPLE);
keyword!(TRUE);
keyword!(TYPE);
keyword!(UNLOGGED);
keyword!(UNSET);
keyword!(UPDATE);
keyword!(USE);
keyword!(USER);
keyword!(USERS);
keyword!(USING);
keyword!(UUID);
keyword!(VALUES);
keyword!(VARCHAR);
keyword!(VARINT);
keyword!(VIEW);
keyword!(WHERE);
keyword!(WITH);
keyword!(WRITETIME);

impl Display for ReservedKeyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ReservedKeyword::ADD => "ADD",
                ReservedKeyword::ALLOW => "ALLOW",
                ReservedKeyword::ALTER => "ALTER",
                ReservedKeyword::AND => "AND",
                ReservedKeyword::APPLY => "APPLY",
                ReservedKeyword::ASC => "ASC",
                ReservedKeyword::AUTHORIZE => "AUTHORIZE",
                ReservedKeyword::BATCH => "BATCH",
                ReservedKeyword::BEGIN => "BEGIN",
                ReservedKeyword::BY => "BY",
                ReservedKeyword::COLUMNFAMILY => "COLUMNFAMILY",
                ReservedKeyword::CREATE => "CREATE",
                ReservedKeyword::DELETE => "DELETE",
                ReservedKeyword::DESC => "DESC",
                ReservedKeyword::DESCRIBE => "DESCRIBE",
                ReservedKeyword::DROP => "DROP",
                ReservedKeyword::ENTRIES => "ENTRIES",
                ReservedKeyword::EXECUTE => "EXECUTE",
                ReservedKeyword::FROM => "FROM",
                ReservedKeyword::FULL => "FULL",
                ReservedKeyword::GRANT => "GRANT",
                ReservedKeyword::IF => "IF",
                ReservedKeyword::IN => "IN",
                ReservedKeyword::INDEX => "INDEX",
                ReservedKeyword::INFINITY => "INFINITY",
                ReservedKeyword::INSERT => "INSERT",
                ReservedKeyword::INTO => "INTO",
                ReservedKeyword::KEYSPACE => "KEYSPACE",
                ReservedKeyword::LIMIT => "LIMIT",
                ReservedKeyword::MODIFY => "MODIFY",
                ReservedKeyword::NAN => "NAN",
                ReservedKeyword::NORECURSIVE => "NORECURSIVE",
                ReservedKeyword::NOT => "NOT",
                ReservedKeyword::NULL => "NULL",
                ReservedKeyword::OF => "OF",
                ReservedKeyword::ON => "ON",
                ReservedKeyword::OR => "OR",
                ReservedKeyword::ORDER => "ORDER",
                ReservedKeyword::PRIMARY => "PRIMARY",
                ReservedKeyword::RENAME => "RENAME",
                ReservedKeyword::REPLACE => "REPLACE",
                ReservedKeyword::REVOKE => "REVOKE",
                ReservedKeyword::SCHEMA => "SCHEMA",
                ReservedKeyword::SELECT => "SELECT",
                ReservedKeyword::SET => "SET",
                ReservedKeyword::TABLE => "TABLE",
                ReservedKeyword::TO => "TO",
                ReservedKeyword::TRUNCATE => "TRUNCATE",
                ReservedKeyword::UNLOGGED => "UNLOGGED",
                ReservedKeyword::UPDATE => "UPDATE",
                ReservedKeyword::USE => "USE",
                ReservedKeyword::USING => "USING",
                ReservedKeyword::VIEW => "VIEW",
                ReservedKeyword::WHERE => "WHERE",
                ReservedKeyword::WITH => "WITH",
            }
        )
    }
}

impl FromStr for ReservedKeyword {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        Ok(match s.to_uppercase().as_str() {
            "ADD" => ReservedKeyword::ADD,
            "ALLOW" => ReservedKeyword::ALLOW,
            "ALTER" => ReservedKeyword::ALTER,
            "AND" => ReservedKeyword::AND,
            "APPLY" => ReservedKeyword::APPLY,
            "ASC" => ReservedKeyword::ASC,
            "AUTHORIZE" => ReservedKeyword::AUTHORIZE,
            "BATCH" => ReservedKeyword::BATCH,
            "BEGIN" => ReservedKeyword::BEGIN,
            "BY" => ReservedKeyword::BY,
            "COLUMNFAMILY" => ReservedKeyword::COLUMNFAMILY,
            "CREATE" => ReservedKeyword::CREATE,
            "DELETE" => ReservedKeyword::DELETE,
            "DESC" => ReservedKeyword::DESC,
            "DESCRIBE" => ReservedKeyword::DESCRIBE,
            "DROP" => ReservedKeyword::DROP,
            "ENTRIES" => ReservedKeyword::ENTRIES,
            "EXECUTE" => ReservedKeyword::EXECUTE,
            "FROM" => ReservedKeyword::FROM,
            "FULL" => ReservedKeyword::FULL,
            "GRANT" => ReservedKeyword::GRANT,
            "IF" => ReservedKeyword::IF,
            "IN" => ReservedKeyword::IN,
            "INDEX" => ReservedKeyword::INDEX,
            "INFINITY" => ReservedKeyword::INFINITY,
            "INSERT" => ReservedKeyword::INSERT,
            "INTO" => ReservedKeyword::INTO,
            "KEYSPACE" => ReservedKeyword::KEYSPACE,
            "LIMIT" => ReservedKeyword::LIMIT,
            "MODIFY" => ReservedKeyword::MODIFY,
            "NAN" => ReservedKeyword::NAN,
            "NORECURSIVE" => ReservedKeyword::NORECURSIVE,
            "NOT" => ReservedKeyword::NOT,
            "NULL" => ReservedKeyword::NULL,
            "OF" => ReservedKeyword::OF,
            "ON" => ReservedKeyword::ON,
            "OR" => ReservedKeyword::OR,
            "ORDER" => ReservedKeyword::ORDER,
            "PRIMARY" => ReservedKeyword::PRIMARY,
            "RENAME" => ReservedKeyword::RENAME,
            "REPLACE" => ReservedKeyword::REPLACE,
            "REVOKE" => ReservedKeyword::REVOKE,
            "SCHEMA" => ReservedKeyword::SCHEMA,
            "SELECT" => ReservedKeyword::SELECT,
            "SET" => ReservedKeyword::SET,
            "TABLE" => ReservedKeyword::TABLE,
            "TO" => ReservedKeyword::TO,
            "TRUNCATE" => ReservedKeyword::TRUNCATE,
            "UNLOGGED" => ReservedKeyword::UNLOGGED,
            "UPDATE" => ReservedKeyword::UPDATE,
            "USE" => ReservedKeyword::USE,
            "USING" => ReservedKeyword::USING,
            "VIEW" => ReservedKeyword::VIEW,
            "WHERE" => ReservedKeyword::WHERE,
            "WITH" => ReservedKeyword::WITH,
            _ => anyhow::bail!("Invalid keyword: {}", s),
        })
    }
}

impl Parse for ReservedKeyword {
    type Output = Self;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        ReservedKeyword::from_str(&s.parse_from::<Alphanumeric>()?)
    }
}

macro_rules! punctuation {
    ($t:ident, $c:literal) => {
        #[derive(ParseFromStr, Copy, Clone, Debug)]
        pub struct $t;
        impl Parse for $t {
            type Output = Self;
            fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
                let c = s.parse::<char>()?;
                if c != $c {
                    anyhow::bail!("Expected '{}', found {}", $c, s.info_with_token(c.to_string()));
                }
                Ok($t)
            }
        }
    };
}

punctuation!(Comma, ',');
punctuation!(Semicolon, ';');
punctuation!(LeftParen, '(');
punctuation!(RightParen, ')');
punctuation!(LeftBrace, '{');
punctuation!(RightBrace, '}');
punctuation!(LeftBracket, '[');
punctuation!(RightBracket, ']');
punctuation!(LeftAngle, '<');
punctuation!(RightAngle, '>');
punctuation!(Equals, '=');
punctuation!(Plus, '+');
punctuation!(Minus, '-');
punctuation!(Dash, '-');
punctuation!(Star, '*');
punctuation!(Slash, '/');
punctuation!(Percent, '%');
punctuation!(Tilde, '~');
punctuation!(Exclamation, '!');
punctuation!(Question, '?');
punctuation!(Colon, ':');
punctuation!(Dot, '.');
punctuation!(SingleQuote, '\'');
punctuation!(DoubleQuote, '\"');
