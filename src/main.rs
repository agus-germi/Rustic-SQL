mod error;
mod query_parser;
mod extras;


use std::{env, f32::consts::E, fs::File, io::{self, BufRead}, result}; //to get the arguments from the command line
use error::{ErrorType, print_error};
use extras::{cast_to_value, get_column_index, get_int_value, get_str_value, Value};
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
// -- MINI FILTER FUNCTION --
trait Operations {
    fn apply_operation(&self, value1: Value, value2: Value) -> bool;  
 }
 struct EqualOperator;
 struct GreaterThanOperator;
 struct LessThanOperator;
 
 
 impl Operations for EqualOperator {
   fn apply_operation(&self, value1: Value, value2: Value) -> bool {
     let int_value1 = get_int_value(&value1);
     let int_value2 = get_int_value(&value2);
     let str_value1 = get_str_value(&value1);
     let str_value2 = get_str_value(&value2);
     match (int_value1, int_value2, str_value1, str_value2) {
       (Some(i1), Some(i2), _, _) => i1 == i2,
       (_, _, Some(s1), Some(s2)) => s1 == s2,
       _ => false,
     }
 }}
 
 impl Operations for GreaterThanOperator {
   fn apply_operation(&self, value1: Value, value2: Value) -> bool {
     let int_value1 = get_int_value(&value1);
     let int_value2 = get_int_value(&value2);
     let str_value1 = get_str_value(&value1);
     let str_value2 = get_str_value(&value2);
     match (int_value1, int_value2, str_value1, str_value2) {
       (Some(i1), Some(i2), _, _) => i1 > i2,
       (_, _, Some(s1), Some(s2)) => false, //FIXME: turn it into a syntax error
       _ => false,
     }
   }
 }
 
 impl Operations for LessThanOperator {
   fn apply_operation(&self, value1: Value, value2: Value) -> bool {
     let int_value1 = get_int_value(&value1);
     let int_value2 = get_int_value(&value2);
     let str_value1 = get_str_value(&value1);
     let str_value2 = get_str_value(&value2);
     match (int_value1, int_value2, str_value1, str_value2) {
       (Some(i1), Some(i2), _, _) => i1 < i2,
       (_, _, Some(s1), Some(s2)) => false, //FIXME: turn it into a syntax error
       _ => false,
     }
   }
 } 
pub fn filter(value1: Value, value2: Value, operator: &str) -> bool {
    let operator: Box<dyn Operations> = match operator {
        "=" => Box::new(EqualOperator),
        ">" => Box::new(GreaterThanOperator),
        "<" => Box::new(LessThanOperator),
        _ => return false,
    };
    operator.apply_operation(value1, value2)
}
//  FILTER FUNCTION --

pub fn filter_row(row: Vec<String>, query: &SelectQuery, headers: &Vec<&str>) -> bool{
    if query.condition.is_empty() {
        return true;
    }
    let column_condition_index = get_column_index(headers, query.condition[0].as_str());
    let column_condition_value = cast_to_value(query.condition[2].as_str());
    let operator = query.condition[1].as_str();
    let value = cast_to_value(&row[column_condition_index]);
    println!("Value: {:?}, Condition: {:?}, Operator: {:?}", value, column_condition_value, operator);

    filter(value, column_condition_value, operator)
}

// -- EXECUTE FUNCTION --

pub fn select(query: SelectQuery) -> Result<(), ErrorType>{
    let relative_path = format!("{}.csv", query.table_name);


    if let Ok(file) = File::open(&relative_path) {
        let mut reader: io::BufReader<File> = io::BufReader::new(file);
        let mut header: String = String::new();

        //Obtengo los headers
        reader.read_line(&mut header);
        let header = header.trim();
        let headers: Vec<&str> = header.split(',').collect();

        let mut lines = reader.lines();
        let mut result_table: Vec<String> = Vec::new();
        for line in lines{
            if let Ok(line) = line {
                let values: Vec<String> = line.split(",").map(|s| s.to_string()).collect();
                if filter_row(values, &query, &headers){
                    
                    result_table.push(line);
                };
            } else {
                // TODO: handle error
            }
        }
        print_selected_rows(result_table, &query, &headers)
    } else {
        print_error(ErrorType::InvalidTable, "No se pudo abrir el archivo");
        return Err(ErrorType::InvalidTable);    }
    Ok(())
}

pub fn print_selected_rows(result_table: Vec<String>, query: &SelectQuery, headers: &Vec<&str>) {
    if query.columns[0] == "*" {
        println!("{}", headers.join(","));
        for row in result_table {
            println!("{}", row);
        }
    } else {
        //code
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

fn get_headers(reader : &mut io::BufReader<File>) -> Vec<String> {
    let mut header = String::new();
    reader.read_line(&mut header);
    let header = header.trim();
    let headers: Vec<String> = header.split(',').map(|s| s.to_string()).collect();
    headers
}
