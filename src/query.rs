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
    println!("comando: {:?}", parsed_query[0]);
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
