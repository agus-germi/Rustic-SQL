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
    // Fields specific to Select query
}


trait CommandParser {
    fn parse(&self,  parsed_query: Vec<String>) -> Result<Query, ErrorType> ;
}

pub struct SelectParser;

impl CommandParser for SelectParser {
    fn parse(&self, parsed_query: Vec<String>) -> Result<Query, ErrorType> {
        let mut table_name = String::new();
        //TODO: get rid of duplicated code
        let table_name_index = parsed_query.iter().position(|x| x == "from");
        if let Some(index) = table_name_index{
            table_name = parsed_query[index + 1].to_string();
        }else {
            error::print_error(ErrorType::InvalidSyntax, "Sintaxis inválida, falta 'from'");
            return Err(ErrorType::InvalidSyntax);
        }
        // Parse Select query
        Ok(Query::Select(SelectQuery {
            // Initialize fields
        }))
    }
}

pub fn parse_query(query: &str){
    let parsed_query: Vec<String> = query.split_whitespace().map(|s| s.to_string().to_lowercase()).collect();

    let command: Box<dyn CommandParser> = match parsed_query[0].as_str() {
        "select" => Box::new(SelectParser),
        _ => {
            let error = ErrorType::InvalidSyntax;
            error::print_error(error, "Comando no válido");
            return();
        }
    };
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
    parse_query(query);
    println!("Hola");
}
