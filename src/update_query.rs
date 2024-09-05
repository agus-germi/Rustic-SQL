use crate::{error, extras, insert_query::{self}, query_parser::UpdateQuery, select_query};
use std::{fs::{self, File}, io::{self, BufRead, BufReader, BufWriter, Write}};
use error::{ErrorType, print_error};
use insert_query::generate_row_to_insert;
use extras::{get_column_index, write_csv};
use select_query::filter_row;


//  Update FUNCTION --
pub fn update(query: UpdateQuery) -> Result<(), ErrorType> {
    let relative_path = format!("{}.csv", query.table_name);
    if let Ok(file) = File::open(&relative_path) {
        let mut reader: io::BufReader<File> = io::BufReader::new(file);

        let mut header: String = String::new();

        //Obtengo los headers
        let _ = reader.read_line(&mut header);
        let header = header.trim();
        let headers: Vec<&str> = header.split(',').collect();
        
        if query.condition.is_empty(){
            let row_to_insert = generate_row_to_insert(&headers, &query.columns, &query.values);
            write_csv(&relative_path, Some(row_to_insert));
        }else{
            let mut line_number = 0;
            let mut updated_line: Vec<String> = Vec::new();
            let mut i = 0;
            for line in reader.lines(){
                i += 1;
                if let Ok(line) = line {
                    let values: Vec<String> = line.split(",").map(|s: &str| s.to_string()).collect();
                    if filter_row(&values, &query.condition, &headers){
                        updated_line = create_updated_line(&headers, &query.columns,&query.values, &values);
                        line_number = i;
                    };
                } else {
                    // TODO: handle error
                }
            
            }
            let _ = update_line(relative_path.as_str(), line_number, Some(&updated_line));
        }

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