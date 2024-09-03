mod error;
mod query_parser;
mod extras;


use std::env; //to get the arguments from the command line
use error::{ErrorType, print_error};
use query_parser::{parse_query, Query, SelectParser, SelectQuery};

#[derive(Debug)]
pub enum CommandType {
    Select,
    Insert,
    Delete,

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

// -- EXECUTE FUNCTION --

pub fn select(query: SelectQuery) {
    println!("SELECT");
}
pub fn execute(query: Query) {
    match query {
        Query::Select(select_query) => {
            select(select_query);
        }
        _ => {
            println!("No implementado");
        }
    }
}

