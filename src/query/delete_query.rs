use std::{fs::{self, File}, io::{self, BufRead, BufReader, BufWriter, Write}};

use super::{select_query::filter_row, CommandParser, Query};
use crate::{error::{self, print_error, ErrorType}, extras::{cleaned_values, get_condition_columns}};




#[derive(Debug)]
pub struct DeleteQuery {
    pub table_name: String,
    pub condition: Vec<String>,
}

pub struct DeleteParser;
impl CommandParser for DeleteParser {
    fn parse(&self, parsed_query: Vec<String>) -> Result<Query, ErrorType> {
        let table_name: String;
        //TODO: get rid of duplicated code
        let table_name_index = parsed_query.iter().position(|x| x == "from");
        if let Some(index) = table_name_index{
            table_name = parsed_query[index + 1].to_string();
        }else {
            error::print_error(ErrorType::InvalidSyntax, "Sintaxis inválida, falta 'from'");
            return Err(ErrorType::InvalidSyntax);
        }
        let condition = cleaned_values(get_condition_columns(&parsed_query));

        Ok(Query::Delete(DeleteQuery {
            table_name,
            condition,
        }))
    }
    
}


pub fn delete(delete_query: DeleteQuery) -> Result<(), ErrorType>{
    let relative_path = format!("{}.csv", delete_query.table_name);
    let mut index: usize = 0;
    if let Ok(file) = File::open(&relative_path) {
        let mut reader: io::BufReader<File> = io::BufReader::new(file);
        let mut header: String = String::new();

        let _ = reader.read_line(&mut header);
        let header = header.trim();
        let headers: Vec<&str> = header.split(',').collect();


        let lines = reader.lines();
        for line in lines{
            index += 1;
            if let Ok(line) = line {
                let values: Vec<String> = line.split(",").map(|s| s.to_string()).collect();
                if filter_row(&values, &delete_query.condition, &headers){
                    let _ = delete_line(&relative_path, index);
                    index -= 1;
                };
            } else {
                // TODO: handle error
                println!("Error");
            }
        }
    } else {
        print_error(ErrorType::InvalidTable, "No se pudo abrir el archivo");
        return Err(ErrorType::InvalidTable);    }
    Ok(())


}
fn delete_line(file_path: &str, line_to_delete: usize) -> io::Result<()> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let lines = reader.lines();

    let temp_file_path = format!("{}.csv", file_path);
    let temp_file = File::create(&temp_file_path)?;
    let mut writer = BufWriter::new(temp_file);

    for (index, line) in lines.enumerate() {
        if index != line_to_delete {
            writeln!(writer, "{}", line?)?;
        }
    }
    // TODO: handle ?  -> do i have to ?
    writer.flush()?;
    drop(writer);

    fs::rename(temp_file_path, file_path)?;

    Ok(())
}