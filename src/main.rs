mod error;
mod query_parser;
mod extras;
mod select_query;

use std::{env, f32::consts::E, fs::{File, OpenOptions}, io::{self, BufRead, Write}, result}; //to get the arguments from the command line
use error::{ErrorType, print_error};
use extras::{cast_to_value, get_column_index, get_int_value, get_str_value, Value};
use query_parser::{parse_query, InsertQuery, Query, SelectParser, SelectQuery};
use select_query::select;

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

// -- INSERT FUNCTION --
pub fn insert(query: InsertQuery) -> Result<(), ErrorType>{
    let relative_path = format!("{}.csv", query.table_name);
    if let Ok(file) = File::open(&relative_path) {
        let mut reader: io::BufReader<File> = io::BufReader::new(file);
        let mut header: String = String::new();

        //Obtengo los headers
        reader.read_line(&mut header);
        let header = header.trim();
        let headers: Vec<&str> = header.split(',').collect();
        let row_to_insert = generate_row_to_insert(&headers, query);
        
        write_csv(&relative_path, Some(row_to_insert));

    } else {
        print_error(ErrorType::InvalidTable, "No se pudo abrir el archivo");
        return Err(ErrorType::InvalidTable);    }
    Ok(())
  


}

pub fn generate_row_to_insert(headers: &Vec<&str>,query: InsertQuery ) -> Vec<String> {
    let mut row_to_insert: Vec<String> = vec![String::new(); headers.len()];        
    for i in headers{
        for j in &query.columns{
            if j == i {
                println!("vector {:?}", row_to_insert);
                let index = get_column_index(&headers, &j);
                let index = index as usize;

                row_to_insert[index].push_str(&query.values[index-1]);
            }
            else {
                let index = get_column_index(&headers, &i);
                let index = index as usize;
                row_to_insert[index].push_str("");
            }
        }
    }
    row_to_insert
}
pub fn write_csv(path: &str, values: Option<Vec<String>>) {
    println!("Write_csv function\n");
  
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)
        .map_err(|e| e.to_string());
  
    //TODO: get rid of this duplucated code in the open_file function
    let mut file = match file {
        Ok(f) => f,
        Err(e) => {
  
            println!("Failed to open the file: {}", e);
            return;
        }
    };
  
    // 1st) creo la linea
    let mut line = String::new();
    if let Some(values) = values {
        for (i, value) in values.iter().enumerate() { 
            if i > 0 {
                line.push(','); 
            }
            line.push_str(value); 
        }
        line.push('\n'); 
  
          // 2nd) escribo la linea
        if let Err(e) = file.write_all(line.as_bytes()) {
          let error = ErrorType::InvalidTable;
          print_error(error, "No se pudo escribir en el archivo");
        } 
    } 
  }
pub fn execute(query: Query) {
    match query {
        Query::Select(select_query) => {
            select(select_query);
        }
        Query::Insert(insert_query ) => {
            insert(insert_query);
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
