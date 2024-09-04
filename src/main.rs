mod error;
mod query_parser;
mod extras;
mod select_query;
mod insert_query;
mod delete_query;
use crate::delete_query::delete;

use std::{env, fs::{self, File, OpenOptions}, io::{self, BufRead, BufReader, BufWriter, Write}};
use error::{ErrorType, print_error};
use crate::insert_query::insert;
use extras::{  get_column_index, get_int_value, get_str_value,  Value};
use query_parser::{parse_query, InsertQuery, Query, UpdateQuery};
use select_query::{filter_row, select};
use std::io::Cursor;


#[derive(Debug)]
pub enum CommandType {
    Select,
    Insert,
    Delete,
    Update
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

//  Update FUNCTION --
pub fn update(query: UpdateQuery) -> Result<(), ErrorType> {
    let relative_path = format!("{}.csv", query.table_name);
    if let Ok(file) = File::open(&relative_path) {
        let mut reader: io::BufReader<File> = io::BufReader::new(file);

        let mut header: String = String::new();

        //Obtengo los headers
        reader.read_line(&mut header);
        let header = header.trim();
        let headers: Vec<&str> = header.split(',').collect();
        
        if query.condition.is_empty(){
            //let row_to_insert = generate_row_to_insert(&headers, &query.columns, &query.values);
            //write_csv(&relative_path, Some(row_to_insert));
        }
        let mut line_number = 0;
        let mut updated_line: Vec<String> = Vec::new();
        let mut i = 0;
        for line in reader.lines(){
            i += 1;
            if let Ok(line) = line {
                let mut values: Vec<String> = line.split(",").map(|s: &str| s.to_string()).collect();
                if filter_row(&values, &query.condition, &headers){
                    println!("{:?}", values);
                    updated_line = create_updated_line(&headers, &query.columns,&query.values, &values);
                    println!("{:?}", updated_line);

                    line_number = i;
                    println!("indice {:?}", line_number);


                };
            } else {
                // TODO: handle error
            }
            
        }
        println!(" LINEA: {:?}", updated_line);
        update_line(relative_path.as_str(), line_number, Some(&updated_line));

    } else {
        print_error(ErrorType::InvalidTable, "No se pudo abrir el archivo");
        return Err(ErrorType::InvalidTable);}
    Ok(())

}

pub fn create_updated_line(headers: &Vec<&str>, columns: &Vec<String>, values_to_update: &Vec<String>,values: &Vec<String>) -> Vec<String> {
    let mut row_to_insert: Vec<String> = vec![String::new(); headers.len()];        
   
    for i in headers{
       
        let n_column = get_column_index(&headers, &i) as usize;
        row_to_insert[n_column as usize].push_str(&values[n_column as usize]);
        
        for j in columns{
            if j == i {
                let n_column = get_column_index(&headers, &j) as usize;
                let n_value = get_column_index(&columns.iter().map(|s| s.as_str()).collect::<Vec<&str>>(), &i) as usize;
                row_to_insert[n_column] = values_to_update[n_value].to_string();}
        }        
    }
    row_to_insert
}

fn update_line(file_path: &str, line_index: usize, row: Option<&Vec<String>>) -> io::Result<()> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let lines = reader.lines();

    let temp_file_path = format!("{}.csv", file_path);
    let temp_file = File::create(&temp_file_path)?;
    let mut writer = BufWriter::new(temp_file);
    let mut updated_line = String::new();
    
    if let Some(row) = row {
        for (i, value) in row.iter().enumerate() { 
            if i > 0 {
                updated_line.push(','); 
            }
            updated_line.push_str(value); 
        }
    }
    
    for (index, line) in lines.enumerate() {
        if index != line_index {
            writeln!(writer, "{}", line?)?;
        } else {
            writeln!(writer, "{}", updated_line)?;
        }
    }
    // TODO: handle ?  -> do i have to ?
    writer.flush()?;
    drop(writer);

    fs::rename(temp_file_path, file_path)?;

    Ok(())
}


// --
pub fn execute(query: Query) {
    match query {
        Query::Select(select_query) => {
            let _ = select(select_query);
        }
        Query::Insert(insert_query ) => {
            let _ = insert(insert_query);
        }
        Query::Delete(delete_query ) => {
            let _ = delete(delete_query);
        }
        Query::Update(update_query ) => {
            println!("{:?}", update_query);
            let _ = update(update_query);
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
