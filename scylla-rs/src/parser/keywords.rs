use std::{
    fmt::Display,
    str::FromStr,
};

use super::{
    Alphanumeric,
    Parse,
    Peek,
    StatementStream,
    Token,
};

#[derive(Copy, Clone, Debug)]
pub enum Keyword {
    ADD,
    AGGREGATE,
    ALL,
    ALLOW,
    ALTER,
    AND,
    APPLY,
    AS,
    ASC,
    ASCII,
    AUTHORIZE,
    BATCH,
    BEGIN,
    BIGINT,
    BLOB,
    BOOLEAN,
    BY,
    CALLED,
    CLUSTERING,
    COLUMNFAMILY,
    COMPACT,
    CONTAINS,
    COUNT,
    COUNTER,
    CREATE,
    CUSTOM,
    DATE,
    DECIMAL,
    DELETE,
    DESC,
    DESCRIBE,
    DISTINCT,
    DOUBLE,
    DROP,
    ENTRIES,
    EXECUTE,
    EXISTS,
    FILTERING,
    FINALFUNC,
    FLOAT,
    FROM,
    FROZEN,
    FULL,
    FUNCTION,
    FUNCTIONS,
    GRANT,
    GROUP,
    IF,
    IN,
    INDEX,
    INET,
    INFINITY,
    INITCOND,
    INPUT,
    INSERT,
    INT,
    INTO,
    JSON,
    KEY,
    KEYS,
    KEYSPACE,
    KEYSPACES,
    LANGUAGE,
    LIMIT,
    LIST,
    LOGIN,
    MAP,
    MODIFY,
    NAN,
    NOLOGIN,
    NORECURSIVE,
    NOSUPERUSER,
    NOT,
    NULL,
    OF,
    ON,
    OPTIONS,
    OR,
    ORDER,
    PARTITION,
    PASSWORD,
    PERMISSION,
    PRIMARY,
    RENAME,
    REPLACE,
    RETURNS,
    REVOKE,
    ROLE,
    ROLES,
    SCHEMA,
    SELECT,
    SET,
    SFUNC,
    SMALLINT,
    STATIC,
    STORAGE,
    STYPE,
    SUPERUSER,
    TABLE,
    TEXT,
    TIME,
    TIMESTAMP,
    TIMEUUID,
    TINYINT,
    TO,
    TOKEN,
    TRIGGER,
    TRUNCATE,
    TTL,
    TUPLE,
    TYPE,
    UNLOGGED,
    UPDATE,
    USE,
    USER,
    USERS,
    USING,
    UUID,
    VALUES,
    VARCHAR,
    VARINT,
    WHERE,
    WITH,
    WRITETIME,
}

macro_rules! keyword {
    ($t:ident) => {
        pub struct $t;
        impl Parse for $t {
            type Output = $t;
            fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
                let this = stringify!($t);
                if let Ok(token) = s.parse_from::<Token>() {
                    if token != this {
                        anyhow::bail!("Expected keyword {}, found {}", this, token)
                    }
                }
                Ok($t)
            }
        }
        impl Peek for $t {
            fn peek(mut s: StatementStream<'_>) -> bool {
                let this = stringify!($t);
                s.parse_from::<Token>().ok().map(|s| s.as_str()) == Some(this)
            }
        }
    };
}

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
keyword!(CALLED);
keyword!(CAST);
keyword!(CLUSTERING);
keyword!(COLUMNFAMILY);
keyword!(COMPACT);
keyword!(CONTAINS);
keyword!(COUNT);
keyword!(COUNTER);
keyword!(CREATE);
keyword!(CUSTOM);
keyword!(DATE);
keyword!(DECIMAL);
keyword!(DELETE);
keyword!(DESC);
keyword!(DESCRIBE);
keyword!(DISTINCT);
keyword!(DOUBLE);
keyword!(DROP);
keyword!(ENTRIES);
keyword!(EXECUTE);
keyword!(EXISTS);
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
keyword!(JSON);
keyword!(KEY);
keyword!(KEYS);
keyword!(KEYSPACE);
keyword!(KEYSPACES);
keyword!(LANGUAGE);
keyword!(LIMIT);
keyword!(LIST);
keyword!(LOGIN);
keyword!(MAP);
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
keyword!(TIMESTAMP);
keyword!(TIMEUUID);
keyword!(TINYINT);
keyword!(TO);
keyword!(TOKEN);
keyword!(TRIGGER);
keyword!(TRUNCATE);
keyword!(TTL);
keyword!(TUPLE);
keyword!(TYPE);
keyword!(UNLOGGED);
keyword!(UPDATE);
keyword!(USE);
keyword!(USER);
keyword!(USERS);
keyword!(USING);
keyword!(UUID);
keyword!(VALUES);
keyword!(VARCHAR);
keyword!(VARINT);
keyword!(WHERE);
keyword!(WITH);
keyword!(WRITETIME);

impl Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Keyword::ADD => "ADD",
                Keyword::AGGREGATE => "AGGREGATE",
                Keyword::ALL => "ALL",
                Keyword::ALLOW => "ALLOW",
                Keyword::ALTER => "ALTER",
                Keyword::AND => "AND",
                Keyword::APPLY => "APPLY",
                Keyword::AS => "AS",
                Keyword::ASC => "ASC",
                Keyword::ASCII => "ASCII",
                Keyword::AUTHORIZE => "AUTHORIZE",
                Keyword::BATCH => "BATCH",
                Keyword::BEGIN => "BEGIN",
                Keyword::BIGINT => "BIGINT",
                Keyword::BLOB => "BLOB",
                Keyword::BOOLEAN => "BOOLEAN",
                Keyword::BY => "BY",
                Keyword::CALLED => "CALLED",
                Keyword::CLUSTERING => "CLUSTERING",
                Keyword::COLUMNFAMILY => "COLUMNFAMILY",
                Keyword::COMPACT => "COMPACT",
                Keyword::CONTAINS => "CONTAINS",
                Keyword::COUNT => "COUNT",
                Keyword::COUNTER => "COUNTER",
                Keyword::CREATE => "CREATE",
                Keyword::CUSTOM => "CUSTOM",
                Keyword::DATE => "DATE",
                Keyword::DECIMAL => "DECIMAL",
                Keyword::DELETE => "DELETE",
                Keyword::DESC => "DESC",
                Keyword::DESCRIBE => "DESCRIBE",
                Keyword::DISTINCT => "DISTINCT",
                Keyword::DOUBLE => "DOUBLE",
                Keyword::DROP => "DROP",
                Keyword::ENTRIES => "ENTRIES",
                Keyword::EXECUTE => "EXECUTE",
                Keyword::EXISTS => "EXISTS",
                Keyword::FILTERING => "FILTERING",
                Keyword::FINALFUNC => "FINALFUNC",
                Keyword::FLOAT => "FLOAT",
                Keyword::FROM => "FROM",
                Keyword::FROZEN => "FROZEN",
                Keyword::FULL => "FULL",
                Keyword::FUNCTION => "FUNCTION",
                Keyword::FUNCTIONS => "FUNCTIONS",
                Keyword::GRANT => "GRANT",
                Keyword::GROUP => "GROUP",
                Keyword::IF => "IF",
                Keyword::IN => "IN",
                Keyword::INDEX => "INDEX",
                Keyword::INET => "INET",
                Keyword::INFINITY => "INFINITY",
                Keyword::INITCOND => "INITCOND",
                Keyword::INPUT => "INPUT",
                Keyword::INSERT => "INSERT",
                Keyword::INT => "INT",
                Keyword::INTO => "INTO",
                Keyword::JSON => "JSON",
                Keyword::KEY => "KEY",
                Keyword::KEYS => "KEYS",
                Keyword::KEYSPACE => "KEYSPACE",
                Keyword::KEYSPACES => "KEYSPACES",
                Keyword::LANGUAGE => "LANGUAGE",
                Keyword::LIMIT => "LIMIT",
                Keyword::LIST => "LIST",
                Keyword::LOGIN => "LOGIN",
                Keyword::MAP => "MAP",
                Keyword::MODIFY => "MODIFY",
                Keyword::NAN => "NAN",
                Keyword::NOLOGIN => "NOLOGIN",
                Keyword::NORECURSIVE => "NORECURSIVE",
                Keyword::NOSUPERUSER => "NOSUPERUSER",
                Keyword::NOT => "NOT",
                Keyword::NULL => "NULL",
                Keyword::OF => "OF",
                Keyword::ON => "ON",
                Keyword::OPTIONS => "OPTIONS",
                Keyword::OR => "OR",
                Keyword::ORDER => "ORDER",
                Keyword::PARTITION => "PARTITION",
                Keyword::PASSWORD => "PASSWORD",
                Keyword::PERMISSION => "PERMISSION",
                Keyword::PRIMARY => "PRIMARY",
                Keyword::RENAME => "RENAME",
                Keyword::REPLACE => "REPLACE",
                Keyword::RETURNS => "RETURNS",
                Keyword::REVOKE => "REVOKE",
                Keyword::ROLE => "ROLE",
                Keyword::ROLES => "ROLES",
                Keyword::SCHEMA => "SCHEMA",
                Keyword::SELECT => "SELECT",
                Keyword::SET => "SET",
                Keyword::SFUNC => "SFUNC",
                Keyword::SMALLINT => "SMALLINT",
                Keyword::STATIC => "STATIC",
                Keyword::STORAGE => "STORAGE",
                Keyword::STYPE => "STYPE",
                Keyword::SUPERUSER => "SUPERUSER",
                Keyword::TABLE => "TABLE",
                Keyword::TEXT => "TEXT",
                Keyword::TIME => "TIME",
                Keyword::TIMESTAMP => "TIMESTAMP",
                Keyword::TIMEUUID => "TIMEUUID",
                Keyword::TINYINT => "TINYINT",
                Keyword::TO => "TO",
                Keyword::TOKEN => "TOKEN",
                Keyword::TRIGGER => "TRIGGER",
                Keyword::TRUNCATE => "TRUNCATE",
                Keyword::TTL => "TTL",
                Keyword::TUPLE => "TUPLE",
                Keyword::TYPE => "TYPE",
                Keyword::UNLOGGED => "UNLOGGED",
                Keyword::UPDATE => "UPDATE",
                Keyword::USE => "USE",
                Keyword::USER => "USER",
                Keyword::USERS => "USERS",
                Keyword::USING => "USING",
                Keyword::UUID => "UUID",
                Keyword::VALUES => "VALUES",
                Keyword::VARCHAR => "VARCHAR",
                Keyword::VARINT => "VARINT",
                Keyword::WHERE => "WHERE",
                Keyword::WITH => "WITH",
                Keyword::WRITETIME => "WRITETIME",
            }
        )
    }
}

impl FromStr for Keyword {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        Ok(match s.to_uppercase().as_str() {
            "ADD" => Keyword::ADD,
            "AGGREGATE" => Keyword::AGGREGATE,
            "ALL" => Keyword::ALL,
            "ALLOW" => Keyword::ALLOW,
            "ALTER" => Keyword::ALTER,
            "AND" => Keyword::AND,
            "APPLY" => Keyword::APPLY,
            "AS" => Keyword::AS,
            "ASC" => Keyword::ASC,
            "ASCII" => Keyword::ASCII,
            "AUTHORIZE" => Keyword::AUTHORIZE,
            "BATCH" => Keyword::BATCH,
            "BEGIN" => Keyword::BEGIN,
            "BIGINT" => Keyword::BIGINT,
            "BLOB" => Keyword::BLOB,
            "BOOLEAN" => Keyword::BOOLEAN,
            "BY" => Keyword::BY,
            "CALLED" => Keyword::CALLED,
            "CLUSTERING" => Keyword::CLUSTERING,
            "COLUMNFAMILY" => Keyword::COLUMNFAMILY,
            "COMPACT" => Keyword::COMPACT,
            "CONTAINS" => Keyword::CONTAINS,
            "COUNT" => Keyword::COUNT,
            "COUNTER" => Keyword::COUNTER,
            "CREATE" => Keyword::CREATE,
            "CUSTOM" => Keyword::CUSTOM,
            "DATE" => Keyword::DATE,
            "DECIMAL" => Keyword::DECIMAL,
            "DELETE" => Keyword::DELETE,
            "DESC" => Keyword::DESC,
            "DESCRIBE" => Keyword::DESCRIBE,
            "DISTINCT" => Keyword::DISTINCT,
            "DOUBLE" => Keyword::DOUBLE,
            "DROP" => Keyword::DROP,
            "ENTRIES" => Keyword::ENTRIES,
            "EXECUTE" => Keyword::EXECUTE,
            "EXISTS" => Keyword::EXISTS,
            "FILTERING" => Keyword::FILTERING,
            "FINALFUNC" => Keyword::FINALFUNC,
            "FLOAT" => Keyword::FLOAT,
            "FROM" => Keyword::FROM,
            "FROZEN" => Keyword::FROZEN,
            "FULL" => Keyword::FULL,
            "FUNCTION" => Keyword::FUNCTION,
            "FUNCTIONS" => Keyword::FUNCTIONS,
            "GRANT" => Keyword::GRANT,
            "GROUP" => Keyword::GROUP,
            "IF" => Keyword::IF,
            "IN" => Keyword::IN,
            "INDEX" => Keyword::INDEX,
            "INET" => Keyword::INET,
            "INFINITY" => Keyword::INFINITY,
            "INITCOND" => Keyword::INITCOND,
            "INPUT" => Keyword::INPUT,
            "INSERT" => Keyword::INSERT,
            "INT" => Keyword::INT,
            "INTO" => Keyword::INTO,
            "JSON" => Keyword::JSON,
            "KEY" => Keyword::KEY,
            "KEYS" => Keyword::KEYS,
            "KEYSPACE" => Keyword::KEYSPACE,
            "KEYSPACES" => Keyword::KEYSPACES,
            "LANGUAGE" => Keyword::LANGUAGE,
            "LIMIT" => Keyword::LIMIT,
            "LIST" => Keyword::LIST,
            "LOGIN" => Keyword::LOGIN,
            "MAP" => Keyword::MAP,
            "MODIFY" => Keyword::MODIFY,
            "NAN" => Keyword::NAN,
            "NOLOGIN" => Keyword::NOLOGIN,
            "NORECURSIVE" => Keyword::NORECURSIVE,
            "NOSUPERUSER" => Keyword::NOSUPERUSER,
            "NOT" => Keyword::NOT,
            "NULL" => Keyword::NULL,
            "OF" => Keyword::OF,
            "ON" => Keyword::ON,
            "OPTIONS" => Keyword::OPTIONS,
            "OR" => Keyword::OR,
            "ORDER" => Keyword::ORDER,
            "PASSWORD" => Keyword::PASSWORD,
            "PERMISSION" => Keyword::PERMISSION,
            "PRIMARY" => Keyword::PRIMARY,
            "RENAME" => Keyword::RENAME,
            "REPLACE" => Keyword::REPLACE,
            "RETURNS" => Keyword::RETURNS,
            "REVOKE" => Keyword::REVOKE,
            "ROLE" => Keyword::ROLE,
            "ROLES" => Keyword::ROLES,
            "SCHEMA" => Keyword::SCHEMA,
            "SELECT" => Keyword::SELECT,
            "SET" => Keyword::SET,
            "SFUNC" => Keyword::SFUNC,
            "SMALLINT" => Keyword::SMALLINT,
            "STATIC" => Keyword::STATIC,
            "STORAGE" => Keyword::STORAGE,
            "STYPE" => Keyword::STYPE,
            "SUPERUSER" => Keyword::SUPERUSER,
            "TABLE" => Keyword::TABLE,
            "TEXT" => Keyword::TEXT,
            "TIME" => Keyword::TIME,
            "TIMESTAMP" => Keyword::TIMESTAMP,
            "TIMEUUID" => Keyword::TIMEUUID,
            "TINYINT" => Keyword::TINYINT,
            "TO" => Keyword::TO,
            "TOKEN" => Keyword::TOKEN,
            "TRIGGER" => Keyword::TRIGGER,
            "TRUNCATE" => Keyword::TRUNCATE,
            "TTL" => Keyword::TTL,
            "TUPLE" => Keyword::TUPLE,
            "TYPE" => Keyword::TYPE,
            "UNLOGGED" => Keyword::UNLOGGED,
            "UPDATE" => Keyword::UPDATE,
            "USE" => Keyword::USE,
            "USER" => Keyword::USER,
            "USERS" => Keyword::USERS,
            "USING" => Keyword::USING,
            "UUID" => Keyword::UUID,
            "VALUES" => Keyword::VALUES,
            "VARCHAR" => Keyword::VARCHAR,
            "VARINT" => Keyword::VARINT,
            "WHERE" => Keyword::WHERE,
            "WITH" => Keyword::WITH,
            "WRITETIME" => Keyword::WRITETIME,
            _ => anyhow::bail!("Invalid keyword: {}", s),
        })
    }
}

impl Parse for Keyword {
    type Output = Keyword;
    fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Keyword::from_str(&s.parse_from::<Alphanumeric>()?)
    }
}

impl Peek for Keyword {
    fn peek(s: StatementStream<'_>) -> bool {
        if let Ok(token) = s.parse_from::<Alphanumeric>() {
            Keyword::from_str(&token).is_ok()
        } else {
            false
        }
    }
}

macro_rules! punctuation {
    ($t:ident, $c:literal) => {
        pub struct $t;
        impl Parse for $t {
            type Output = $t;
            fn parse(s: &mut StatementStream<'_>) -> anyhow::Result<Self::Output> {
                let c = s.parse::<char>()?;
                if c != $c {
                    anyhow::bail!("Expected {}, found {}", $c, c);
                }
                Ok($t)
            }
        }
        impl Peek for $t {
            fn peek(s: StatementStream<'_>) -> bool {
                s.peek() == Some($c)
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
punctuation!(Star, '*');
punctuation!(Slash, '/');
punctuation!(Percent, '%');
punctuation!(Tilde, '~');
punctuation!(Exclamation, '!');
punctuation!(Question, '?');
punctuation!(Colon, ':');
punctuation!(Dot, '.');
punctuation!(Quote, '"');
