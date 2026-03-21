// =============================================================================
// sql/lexer.rs — SQL Lexer
// =============================================================================
//
// 編譯器前端通常分兩步：
// 1. Lexer（詞法分析）
//    - 把原始字串切成 token
//    - 例如 "SELECT * FROM users" 會切成 Keyword(Select), Star, Keyword(From), Ident("users")
//
// 2. Parser（語法分析）
//    - 再把 token 串成 AST
//
// 這個檔案只處理第 1 步。

use crate::error::{FerrisDbError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    Keyword(Keyword),
    Ident(String),
    IntLit(i64),
    StringLit(String),
    Star,
    Comma,
    LParen,
    RParen,
    Eq,
    Dot,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    Semicolon,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Keyword {
    Explain,
    Analyze,
    Alter,
    Select,
    From,
    Where,
    With,
    Recursive,
    Case,
    When,
    Then,
    Else,
    End,
    Count,
    Sum,
    Min,
    Max,
    Group,
    Having,
    Order,
    By,
    Asc,
    As,
    All,
    Desc,
    Distinct,
    Limit,
    Insert,
    Into,
    Values,
    Create,
    View,
    Index,
    Table,
    Add,
    Column,
    Drop,
    If,
    Exists,
    Not,
    Is,
    In,
    Between,
    Like,
    Update,
    Set,
    Delete,
    Using,
    Over,
    Partition,
    RowNumber,
    Rank,
    Join,
    On,
    Union,
    Inner,
    Left,
    And,
    Or,
    Int,
    Text,
    Bool,
    Null,
    True,
    False,
}

pub struct Lexer<'a> {
    input: &'a str,
    chars: Vec<char>,
    pos: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Lexer<'a> {
        Lexer {
            input,
            chars: input.chars().collect(),
            pos: 0,
        }
    }

    pub fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();

        while let Some(ch) = self.peek() {
            if ch.is_whitespace() {
                self.bump();
                continue;
            }

            let token = match ch {
                '*' => {
                    self.bump();
                    Token::Star
                }
                ',' => {
                    self.bump();
                    Token::Comma
                }
                '(' => {
                    self.bump();
                    Token::LParen
                }
                ')' => {
                    self.bump();
                    Token::RParen
                }
                ';' => {
                    self.bump();
                    Token::Semicolon
                }
                '=' => {
                    self.bump();
                    Token::Eq
                }
                '.' => {
                    self.bump();
                    Token::Dot
                }
                '!' => {
                    self.bump();
                    if self.peek() == Some('=') {
                        self.bump();
                        Token::Ne
                    } else {
                        return Err(FerrisDbError::InvalidCommand(
                            "unexpected '!' in SQL".to_string(),
                        ));
                    }
                }
                '<' => {
                    self.bump();
                    match self.peek() {
                        Some('=') => {
                            self.bump();
                            Token::Le
                        }
                        Some('>') => {
                            self.bump();
                            Token::Ne
                        }
                        _ => Token::Lt,
                    }
                }
                '>' => {
                    self.bump();
                    if self.peek() == Some('=') {
                        self.bump();
                        Token::Ge
                    } else {
                        Token::Gt
                    }
                }
                '\'' => self.lex_string()?,
                '-' | '0'..='9' => self.lex_number()?,
                _ if is_ident_start(ch) => self.lex_ident_or_keyword(),
                _ => {
                    return Err(FerrisDbError::InvalidCommand(format!(
                        "unexpected character '{}' in SQL",
                        ch
                    )))
                }
            };

            tokens.push(token);
        }

        Ok(tokens)
    }

    fn lex_string(&mut self) -> Result<Token> {
        self.expect_char('\'')?;
        let mut value = String::new();

        while let Some(ch) = self.peek() {
            self.bump();
            if ch == '\'' {
                return Ok(Token::StringLit(value));
            }
            value.push(ch);
        }

        Err(FerrisDbError::InvalidCommand(
            "unterminated SQL string literal".to_string(),
        ))
    }

    fn lex_number(&mut self) -> Result<Token> {
        let mut buf = String::new();
        if self.peek() == Some('-') {
            buf.push('-');
            self.bump();
        }

        let mut has_digit = false;
        while let Some(ch) = self.peek() {
            if ch.is_ascii_digit() {
                has_digit = true;
                buf.push(ch);
                self.bump();
            } else {
                break;
            }
        }

        if !has_digit {
            return Err(FerrisDbError::InvalidCommand(
                "invalid integer literal in SQL".to_string(),
            ));
        }

        let value = buf.parse::<i64>().map_err(|_| {
            FerrisDbError::InvalidCommand(format!("invalid integer literal '{}'", buf))
        })?;
        Ok(Token::IntLit(value))
    }

    fn lex_ident_or_keyword(&mut self) -> Token {
        let mut buf = String::new();
        while let Some(ch) = self.peek() {
            if is_ident_continue(ch) {
                buf.push(ch);
                self.bump();
            } else {
                break;
            }
        }

        match keyword_from_ident(&buf) {
            Some(keyword) => Token::Keyword(keyword),
            None => Token::Ident(buf),
        }
    }

    fn expect_char(&mut self, expected: char) -> Result<()> {
        match self.peek() {
            Some(ch) if ch == expected => {
                self.bump();
                Ok(())
            }
            _ => Err(FerrisDbError::InvalidCommand(format!(
                "expected '{}' in SQL input: {}",
                expected, self.input
            ))),
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn bump(&mut self) {
        self.pos += 1;
    }
}

fn is_ident_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_ident_continue(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn keyword_from_ident(ident: &str) -> Option<Keyword> {
    match ident.to_ascii_uppercase().as_str() {
        "SELECT" => Some(Keyword::Select),
        "EXPLAIN" => Some(Keyword::Explain),
        "ANALYZE" => Some(Keyword::Analyze),
        "ALTER" => Some(Keyword::Alter),
        "FROM" => Some(Keyword::From),
        "WHERE" => Some(Keyword::Where),
        "WITH" => Some(Keyword::With),
        "RECURSIVE" => Some(Keyword::Recursive),
        "CASE" => Some(Keyword::Case),
        "WHEN" => Some(Keyword::When),
        "THEN" => Some(Keyword::Then),
        "ELSE" => Some(Keyword::Else),
        "END" => Some(Keyword::End),
        "COUNT" => Some(Keyword::Count),
        "SUM" => Some(Keyword::Sum),
        "MIN" => Some(Keyword::Min),
        "MAX" => Some(Keyword::Max),
        "GROUP" => Some(Keyword::Group),
        "HAVING" => Some(Keyword::Having),
        "ORDER" => Some(Keyword::Order),
        "BY" => Some(Keyword::By),
        "ASC" => Some(Keyword::Asc),
        "AS" => Some(Keyword::As),
        "ALL" => Some(Keyword::All),
        "DESC" => Some(Keyword::Desc),
        "DISTINCT" => Some(Keyword::Distinct),
        "LIMIT" => Some(Keyword::Limit),
        "INSERT" => Some(Keyword::Insert),
        "INTO" => Some(Keyword::Into),
        "VALUES" => Some(Keyword::Values),
        "CREATE" => Some(Keyword::Create),
        "VIEW" => Some(Keyword::View),
        "INDEX" => Some(Keyword::Index),
        "TABLE" => Some(Keyword::Table),
        "ADD" => Some(Keyword::Add),
        "COLUMN" => Some(Keyword::Column),
        "DROP" => Some(Keyword::Drop),
        "IF" => Some(Keyword::If),
        "EXISTS" => Some(Keyword::Exists),
        "NOT" => Some(Keyword::Not),
        "IS" => Some(Keyword::Is),
        "IN" => Some(Keyword::In),
        "BETWEEN" => Some(Keyword::Between),
        "LIKE" => Some(Keyword::Like),
        "UPDATE" => Some(Keyword::Update),
        "SET" => Some(Keyword::Set),
        "DELETE" => Some(Keyword::Delete),
        "USING" => Some(Keyword::Using),
        "OVER" => Some(Keyword::Over),
        "PARTITION" => Some(Keyword::Partition),
        "ROW_NUMBER" => Some(Keyword::RowNumber),
        "RANK" => Some(Keyword::Rank),
        "JOIN" => Some(Keyword::Join),
        "ON" => Some(Keyword::On),
        "UNION" => Some(Keyword::Union),
        "INNER" => Some(Keyword::Inner),
        "LEFT" => Some(Keyword::Left),
        "AND" => Some(Keyword::And),
        "OR" => Some(Keyword::Or),
        "INT" => Some(Keyword::Int),
        "TEXT" => Some(Keyword::Text),
        "BOOL" => Some(Keyword::Bool),
        "NULL" => Some(Keyword::Null),
        "TRUE" => Some(Keyword::True),
        "FALSE" => Some(Keyword::False),
        _ => None,
    }
}
