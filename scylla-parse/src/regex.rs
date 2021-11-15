use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
    pub static ref IDENT_REGEX: Regex = Regex::new(r#"[a-z][link:[a-z0-9]]*|"[^"]*""#).unwrap();
    pub static ref UNQUOTED_REGEX: Regex = Regex::new(r"[a-z][link:[a-z0-9]]*").unwrap();
    pub static ref QUOTED_REGEX: Regex = Regex::new(r#""[^"]*""#).unwrap();
    pub static ref INTEGER_REGEX: Regex = Regex::new(r"-?[0-9]+").unwrap();
    pub static ref FLOAT_REGEX: Regex = Regex::new(r"-?[0-9]+(.[0-9]*)?([eE][+-]?[0-9+])?").unwrap();
    pub static ref HEX_REGEX: Regex = Regex::new(r"[0-9a-f]").unwrap();
    pub static ref BLOB_REGEX: Regex = Regex::new(r"0x[0-9a-f]").unwrap();
    pub static ref UUID_REGEX: Regex =
        Regex::new(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}").unwrap();
    pub static ref BOOLEAN_REGEX: Regex = Regex::new(r"true|false").unwrap();
    pub static ref STRING_REGEX: Regex = Regex::new(r#"'([^']*)'|\$\$((?:(?!\$\$).)*)\$\$"#).unwrap();
    pub static ref INDEX_REGEX: Regex = Regex::new(r"[a-zA-Z_0-9]+").unwrap();
    pub static ref VIEW_REGEX: Regex = Regex::new(r"[a-zA-Z_0-9]+").unwrap();
}
