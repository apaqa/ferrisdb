// =============================================================================
// sql/mod.rs — SQL 模組入口
// =============================================================================
//
// SQL 支援通常可拆成兩個階段：
// 1. Parsing：把 SQL 字串轉成 AST
// 2. Execution：把 AST 真的執行到資料庫上
//
// 這一階段只做第 1 步，因此這個模組提供：
// - ast：抽象語法樹
// - lexer：字元流 -> token 流
// - parser：token 流 -> AST

pub mod ast;
pub mod catalog;
pub mod executor;
pub mod index;
pub mod lexer;
pub mod optimizer;
pub mod parser;
pub mod plan_cache;
pub mod row;
pub mod statistics;
