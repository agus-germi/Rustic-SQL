mod error;
mod query_parser;
mod extras;


use std::{env, fs::File, io::{self, BufRead}}; //to get the arguments from the command line
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
    println!("{}", query.table_name.trim());
    let path = format!("{}.csv", query.table_name);
    println!("{}", path);
    if let Ok(file) = File::open(&path) {
        let reader = io::BufReader::new(file);
        for line in reader.lines() {
            match line {
                Ok(line_content) => {
                    // Process each line here
                    println!("{}", line_content);
                }
                Err(e) => {
                    let error = ErrorType::InvalidTable;
                    print_error(error, "Error al leer el archivo");
                    return;
                }
            }
        }
    } else {
        let error = ErrorType::InvalidTable;
        print_error(error, "Error al leer el archivo");
        return;
    }
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

