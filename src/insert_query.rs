

use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, Write}; 
use crate::error::{print_error, ErrorType};
use crate::extras::get_column_index;
use crate::InsertQuery;


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
        if let Err(e) = file.write_all(line.as_bytes()) {
          let error = ErrorType::InvalidTable;
          print_error(error, "No se pudo escribir en el archivo");
        } 
    } 
  }