mod error;


use std::env; //to get the arguments from the command line
use error::{ErrorType, print_error};


fn main() {
    let args: Vec<String> = env::args().collect(); 

    if args.len() < 4{
        let error_description = "Uso: cargo run -- ruta/a/tablas \"<consulta>\"";
        let error = ErrorType::InvalidSyntax;
        print_error(error, error_description);
        return ();
    }
    println!("Hola");
}
