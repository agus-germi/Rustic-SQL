use std::{
    fs::{File, OpenOptions},
    io::{self, BufRead, Write},
};

use crate::query::{CommandParser, Query};
use crate::{
    error::{self, print_error, ErrorType},
    extras::{cleaned_values, get_column_index},
};

#[derive(Debug)]
pub struct InsertQuery {
    pub table_name: String,
    pub columns: Vec<String>,
    pub values: Vec<String>,
}

pub struct InsertParser;

impl CommandParser for InsertParser {
    fn validate_syntax(&self, parsed_query: &[String]) -> Result<(), ErrorType> {
        if parsed_query.len() < 4 || parsed_query[0] != "insert" || parsed_query[1] != "into" {
            error::print_error(
                ErrorType::InvalidSyntax,
                "Sintaxis inválida: falta 'INSERT INTO'",
            );
            return Err(ErrorType::InvalidSyntax);
        }
        let table_name_index = parsed_query
            .iter()
            .position(|x| x == "into")
            .map(|index| index + 1);
        let values_start = parsed_query.iter().position(|x| x == "values");
        let table_index = table_name_index.ok_or(ErrorType::InvalidSyntax)?;
        let value_index = values_start.ok_or(ErrorType::InvalidSyntax)?;
        let number_col = parsed_query[table_index + 1..value_index].len();
        let number_val = parsed_query[value_index + 1..].len();
        if number_col != number_val {
            error::print_error(
                ErrorType::InvalidSyntax,
                "Cantidad de columnas no coincide con cantidad de valores",
            );
            return Err(ErrorType::InvalidSyntax);
        }

        Ok(())
    }
    fn parse(&self, parsed_query: Vec<String>) -> Result<Query, ErrorType> {
        let mut table_index = 0;
        let _table_name_index = parsed_query
            .iter()
            .position(|x| x == "insert")
            .and_then(|index| {
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

pub fn insert(path: &str, query: InsertQuery) -> Result<(), ErrorType> {
    if let Ok(file) = File::open(path) {
        let mut reader: io::BufReader<File> = io::BufReader::new(file);
        let mut header: String = String::new();
        let _ = reader.read_line(&mut header);
        let header = header.trim();
        let headers: Vec<String> = header.split(',').map(|s| s.to_string()).collect();
        let row_to_insert = generate_row_to_insert(&headers, &query.columns, &query.values);

        write_csv(path, Some(row_to_insert));
    } else {
        print_error(ErrorType::InvalidTable, "No se pudo abrir el archivo");
        return Err(ErrorType::InvalidTable);
    }
    Ok(())
}

pub fn generate_row_to_insert(
    headers: &[String],
    columns: &Vec<String>,
    values: &[String],
) -> Vec<String> {
    let mut row_to_insert: Vec<String> = vec![String::new(); headers.len()];
    for i in headers {
        for j in columns {
            if j == i {
                let n_column = get_column_index(headers, j) as usize;
                let n_value = get_column_index(columns, i) as usize;
                row_to_insert[n_column] = values[n_value].to_string();
            } else {
                let index = get_column_index(headers, i);
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
        .open(path)
        .map_err(|e| e.to_string());

    let mut file = match file {
        Ok(f) => f,
        Err(e) => {
            println!("Failed to open the file: {}", e);
            return;
        }
    };
    let mut line = String::new();
    if let Some(values) = values {
        for (i, value) in values.iter().enumerate() {
            if i > 0 {
                line.push(',');
            }
            line.push_str(value);
        }
        line.push('\n');
        if let Err(_e) = file.write_all(line.as_bytes()) {
            let error = ErrorType::InvalidTable;
            print_error(error, "No se pudo escribir en el archivo");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_parser() {
        let parser = InsertParser;
        let input = vec![
            "insert".to_string(),
            "into".to_string(),
            "test_table".to_string(),
            "name".to_string(),
            "age".to_string(),
            "values".to_string(),
            "Alice".to_string(),
            "30".to_string(),
        ];

        let result = parser.parse(input);

        if let Ok(Query::Insert(insert_query)) = result {
            assert_eq!(insert_query.table_name, "test_table");
            assert_eq!(insert_query.columns, vec!["name", "age"]);
            assert_eq!(insert_query.values, vec!["Alice", "30"]);
        }
    }

    #[test]
    fn test_generate_row_to_insert() {
        let headers = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        let columns = vec!["name".to_string(), "age".to_string()];
        let values = vec!["Alice".to_string(), "30".to_string()];

        let result = generate_row_to_insert(&headers, &columns, &values);

        assert_eq!(result, vec!["", "Alice", "30"]);
    }

    #[test]
    fn test_write_csv() -> Result<(), Box<dyn std::error::Error>> {
        let test_file: &str = "test_write_csv.csv";
        let data = vec!["1".to_string(), "Alice".to_string(), "30".to_string()];

        let _ = std::fs::remove_file(test_file);

        write_csv(test_file, Some(data));

        let contents = std::fs::read_to_string(test_file)?;
        assert!(contents.contains("1,Alice,30"));

        std::fs::remove_file(test_file)?;

        Ok(())
    }
}
