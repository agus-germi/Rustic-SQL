pub mod delete_query;
pub mod insert_query;
pub mod select_query;
pub mod update_query;

use delete_query::{DeleteParser, DeleteQuery};
use insert_query::{InsertParser, InsertQuery};
use select_query::{SelectParser, SelectQuery};
use update_query::{UpdateParser, UpdateQuery};

use crate::error::{self, ErrorType};
use crate::execute;

#[derive(Debug)]
pub enum Query {
    Select(SelectQuery),
    Insert(InsertQuery),
    Delete(DeleteQuery),
    Update(UpdateQuery),
}

trait CommandParser {
    fn parse(&self, parsed_query: Vec<String>) -> Result<Query, ErrorType>;
}

pub fn parse_query(query: &str) -> Result<(), ErrorType> {
    let parsed_query: Vec<String> = query
        .split_whitespace()
        .map(|s| s.to_string().to_lowercase())
        .collect();
    if parsed_query.len() < 4 {
        //Entiendo que no puede haber una consulta con menos de 4 palabras
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
    let _query = match command.parse(parsed_query) {
        Ok(query) => execute(query),
        Err(error) => return Err(error),
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select_query() {
        let query = "select * from table_name";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse valid SELECT query");
    }

    #[test]
    fn test_parse_insert_query() {
        let query = "insert into table_name values ('value1', 'value2')";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse valid INSERT query");
    }

    #[test]
    fn test_parse_update_query() {
        let query = "update table_name set column1 = 'value1' where column2 = 'value2'";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse valid UPDATE query");
    }

    #[test]
    fn test_parse_delete_query() {
        let query = "delete from table_name where column1 = 'value1'";
        let result = parse_query(query);
        assert!(result.is_ok(), "Failed to parse valid DELETE query");
    }

    #[test]
    fn test_parse_invalid_command() {
        let query = "not_a_command table table_name";
        let result = parse_query(query);
        assert!(result.is_err(), "Error for invalid command");
        if let Err(error) = result {
            assert_eq!(error, ErrorType::InvalidSyntax);
        }
    }

    #[test]
    fn test_parse_short_query() {
        let query = "insert into";
        let result = parse_query(query);
        assert!(result.is_err(), "Error for too short query");
        if let Err(error) = result {
            assert_eq!(error, ErrorType::InvalidSyntax);
        }
    }
}
