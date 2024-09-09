


use utils::delete_query::{DeleteParser, DeleteQuery};
use utils::insert_query::{InsertParser, InsertQuery};
use utils::select_query::{SelectParser, SelectQuery};
use utils::update_query::{UpdateParser, UpdateQuery};

use crate::error::{self, ErrorType};
use crate::utils::delete_query::delete;
use crate::utils::insert_query::insert;
use crate::utils::select_query::select;
use crate::utils::update_query::update;
use crate::utils;

#[derive(Debug)]
pub enum Query {
    Select(SelectQuery),
    Insert(InsertQuery),
    Delete(DeleteQuery),
    Update(UpdateQuery),
}

#[derive(Debug)]
pub enum CommandType {
    Select,
    Insert,
    Delete,
    Update,
}

pub trait CommandParser {
    fn validate_syntax(&self, parsed_query: &[String]) -> Result<(), ErrorType>;
    fn parse(&self, parsed_query: Vec<String>) -> Result<Query, ErrorType>;
}

pub fn parse_query(path: &str, query: &str) -> Result<(), ErrorType> {
    let parsed_query: Vec<String> = query
        .split_whitespace()
        .map(|s| s.to_string().to_lowercase())
        .collect();

    if parsed_query.len() < 4 {
        let error = ErrorType::InvalidSyntax;
        error::print_error(error, "Sintaxis inválida");
        return Err(ErrorType::InvalidSyntax);
    }
    let command: Box<dyn CommandParser> = match parsed_query[0].as_str() {
        "select" => Box::new(SelectParser),
        "insert" => Box::new(InsertParser),
        "delete" => Box::new(DeleteParser),
        "update" => Box::new(UpdateParser),
        _ => {
            let error = ErrorType::InvalidSyntax;
            error::print_error(error, "Comando no válido");
            return Err(ErrorType::InvalidSyntax);
        }
    };
    command.validate_syntax(&parsed_query)?;
    match command.parse(parsed_query) {
        Ok(query) => execute(path, query),        
        Err(error) => return Err(error),
    };
    Ok(())
}


pub fn execute(path:&str, query: Query) {
    match query {
        Query::Select(select_query) => {
            let _ = select(path, select_query);
        }
        Query::Insert(insert_query) => {
            let _ = insert(path, insert_query);
        }
        Query::Delete(delete_query) => {
            let _ = delete(path, delete_query);
        }
        Query::Update(update_query) => {
            let _ = update(path, update_query);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select_query() {
        let query = "select * from table_name";
        let result = parse_query("fake_path.csv", query);
        assert!(result.is_ok(), "Failed to parse valid SELECT query");
    }

    #[test]
    fn test_parse_insert_query() {
        let query = "INSERT INTO ordenes (id, id_cliente, producto, cantidad) VALUES (111, 6, 'Laptop', 3);";
        let result = parse_query("fake_path.csv",query);
        assert!(result.is_ok(), "Failed to parse valid INSERT query");
    }

    #[test]
    fn test_parse_update_query() {
        let query = "update table_name set column1 = 'value1' where column2 = 'value2'";
        let result = parse_query("fake_path.csv",query);
        assert!(result.is_ok(), "Failed to parse valid UPDATE query");
    }

    #[test]
    fn test_parse_delete_query() {
        let query = "delete from table_name where column1 = 'value1'";
        let result = parse_query("fake_path.csv",query);
        assert!(result.is_ok(), "Failed to parse valid DELETE query");
    }

    #[test]
    fn test_parse_invalid_command() {
        let query = "not_a_command table table_name";
        let result = parse_query("fake_path.csv",query);
        assert!(result.is_err(), "Error for invalid command");
        if let Err(error) = result {
            assert_eq!(error, ErrorType::InvalidSyntax);
        }
    }

    #[test]
    fn test_parse_short_query() {
        let query = "insert into";
        let result = parse_query("fake_path.csv",query);
        assert!(result.is_err(), "Error for too short query");
        if let Err(error) = result {
            assert_eq!(error, ErrorType::InvalidSyntax);
        }
    }
}
