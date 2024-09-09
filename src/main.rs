use error::{print_error, ErrorType};
use sql::query::parse_query;

use std::env;

pub mod error;
pub mod extras;






fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        let error_description = "Uso: cargo run -- ruta/a/tablas \"<consulta>\"";
        let error = ErrorType::InvalidSyntax;
        print_error(error, error_description);
        return;
    }
    let query = &args[2];
    let path = &args[1];

    if let Err(_error) = parse_query(path, query) {}
}

