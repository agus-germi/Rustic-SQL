use std::{fs::{File, OpenOptions}, io::{self, BufRead, Write}};

use super::{CommandParser, Query};
use crate::{error::{print_error, ErrorType}, extras::{cleaned_values, get_column_index}};


#[derive(Debug)]
pub struct InsertQuery {
    pub table_name: String,
    pub columns: Vec<String>,
    pub values: Vec<String>,
}

pub struct InsertParser;

impl CommandParser for InsertParser {
    fn parse(&self, parsed_query: Vec<String>) -> Result<Query, ErrorType> {        let mut table_index = 0;

        let _table_name_index = parsed_query.iter().position(|x| x == "insert").and_then(|index| {
            if index + 1 < parsed_query.len() && parsed_query[index + 1] == "into" {
                table_index = index + 2;
                Some(index)
            } else {
                None
            }
        });
        let table_name = parsed_query[table_index].to_string();
        let mut value_index = 0;
        if let Some(index) = parsed_query.iter().position(|x| x == "values") {
            value_index = index;
        }
        let values = cleaned_values(parsed_query[value_index + 1..].to_vec());
        let columns = cleaned_values(parsed_query[table_index + 1..value_index].to_vec());
        

        Ok(Query::Insert(InsertQuery {
            table_name,
            columns,
            values,
        }))
    }
}



// -- INSERT FUNCTION --

pub fn insert(query: InsertQuery) -> Result<(), ErrorType>{
    let relative_path = format!("{}.csv", query.table_name);
    if let Ok(file) = File::open(&relative_path) {
        let mut reader: io::BufReader<File> = io::BufReader::new(file);
        let mut header: String = String::new();

        //Obtengo los headers
        let _ = reader.read_line(&mut header);
        let header = header.trim();
        let headers: Vec<&str> = header.split(',').collect();
        let row_to_insert = generate_row_to_insert(&headers,&query.columns, &query.values);
        
        write_csv(&relative_path, Some(row_to_insert));

    } else {
        print_error(ErrorType::InvalidTable, "No se pudo abrir el archivo");
        return Err(ErrorType::InvalidTable);    }
    Ok(())
  


}

pub fn generate_row_to_insert(headers: &Vec<&str>, columns: &Vec<String>, values: &Vec<String>  ) -> Vec<String> {
    let mut row_to_insert: Vec<String> = vec![String::new(); headers.len()];        
    for i in headers{
        for j in columns{
            if j == i {

                let n_column = get_column_index(&headers, &j) as usize;
                let n_value = get_column_index(&columns.iter().map(|s| s.as_str()).collect::<Vec<&str>>(), &i) as usize;
                row_to_insert[n_column] = values[n_value].to_string();
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
        if let Err(_e) = file.write_all(line.as_bytes()) {
          let error = ErrorType::InvalidTable;
          print_error(error, "No se pudo escribir en el archivo");
        } 
    } 
  }