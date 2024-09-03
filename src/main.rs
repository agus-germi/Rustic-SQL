mod error;


use std::env; //to get the arguments from the command line
use error::{ErrorType, print_error};
#[derive(Debug)]
pub enum CommandType {
    Select,
    Insert,
    Delete,

}
#[derive(Debug)]
pub enum Query {
    Select(SelectQuery),
    //Insert(InsertQuery),
    //Delete(DeleteQuery),
    //Update(UpdateQuery),
}

#[derive(Debug)]
pub struct SelectQuery {
    pub table_name: String,
    pub columns: Vec<String>,
    pub condition: Vec<String>,
}


trait CommandParser {
    fn parse(&self,  parsed_query: Vec<String>) -> Result<Query, ErrorType> ;
}

pub struct SelectParser;

impl CommandParser for SelectParser {
    fn parse(&self, parsed_query: Vec<String>) -> Result<Query, ErrorType> {
        let table_name: String;
        //TODO: get rid of duplicated code
        let table_name_index = parsed_query.iter().position(|x| x == "from");
        if let Some(index) = table_name_index{
            table_name = parsed_query[index + 1].to_string();
        }else {
            error::print_error(ErrorType::InvalidSyntax, "Sintaxis inválida, falta 'from'");
            return Err(ErrorType::InvalidSyntax);
        }
        let columns = get_columns(&parsed_query);
        let condition = get_condition_columns(&parsed_query);


        Ok(Query::Select(SelectQuery {
            table_name,
            columns,
            condition,


        }))
    }
}

pub fn parse_query(query: &str) -> Result<(), ErrorType> {
    let parsed_query: Vec<String> = query.split_whitespace().map(|s| s.to_string().to_lowercase()).collect();
    if parsed_query.len() < 4 {
        //Entiendo que no puede haber una consulta con menos de 4 palabras
        let error = ErrorType::InvalidSyntax;
        error::print_error(error, "Sintaxis inválida");
        return Err(ErrorType::InvalidSyntax);
    }
    println!("comando: {:?}", parsed_query[0]);
    let command: Box<dyn CommandParser> = match parsed_query[0].as_str() {
        "select" => Box::new(SelectParser),
        _ => {
            let error = ErrorType::InvalidSyntax;
            error::print_error(error, "Comando no válido");
            return Err(ErrorType::InvalidSyntax);
        }
    };
    let query = match command.parse(parsed_query) {
        Ok(query) => execute(query),
        Err(error) => return Err(error),
    };
    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect(); 

    if args.len() != 3{
        let error_description = "Uso: cargo run -- ruta/a/tablas \"<consulta>\"";
        let error = ErrorType::InvalidSyntax;
        print_error(error, error_description);
        return ();
    }
    let query = &args[2];
    
    if let Err(error) = parse_query(query) {
        return;
    }

    println!("Hola");
}

fn select(query: SelectQuery) {
    println!("SELECT");
}
fn execute(query: Query) {
    match query {
        Query::Select(select_query) => {
            select(select_query);
        }
        _ => {
            println!("No implementado");
        }
    }
}

//------ EXTRA FUNCTIONS ------
fn get_columns(parsed_query: &Vec<String>) -> Vec<String> {
    let mut columns = Vec::new();
    let mut index = 1;
    while parsed_query[index] != "from" {
        columns.push(parsed_query[index].to_string());
        index += 1;
    }
    println!("COLUMNS: {:?}", columns);
    columns

}

fn get_condition_columns(parsed_query: &Vec<String>) -> Vec<String> {
    let mut condition_columns = Vec::new();
    let mut index = parsed_query.iter().position(|x| x == "where");
    if let Some(mut index) = index {
        index += 1;
        while index < parsed_query.len() {
            condition_columns.push(parsed_query[index].to_string());
            index += 1;
        }
    }
    println!("COND: {:?}", condition_columns);
    condition_columns
    
}