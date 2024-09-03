use std::{fs::File, io::{self, BufRead}};
use crate::{error::{print_error, ErrorType}, extras::{cast_to_value, get_column_index}, filter, query_parser};
use query_parser::{parse_query, Query, SelectParser, SelectQuery};



pub fn filter_row(row: Vec<String>, query: &SelectQuery, headers: &Vec<&str>) -> bool{
    if query.condition.is_empty() {
        return true;
    }
    let column_condition_index = get_column_index(headers, query.condition[0].as_str());
    let column_condition_value = cast_to_value(query.condition[2].as_str());
    let operator = query.condition[1].as_str();
    let value = cast_to_value(&row[column_condition_index]);
    //TODO: 1) AND/OR /ETC
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
        //TODO: 2) PRINT result_table with only the selected columns
    }
}