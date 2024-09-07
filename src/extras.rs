use std::fs::OpenOptions;
use std::io::Write;

use crate::error::print_error;
use crate::error::ErrorType;

#[derive(Debug, PartialEq)]
pub enum Value {
    Int(i32),
    Str(String),
}

pub fn cast_to_value(s: &str) -> Value {
    if let Ok(int_value) = s.parse::<i32>() {
        Value::Int(int_value)
    } else {
        Value::Str(s.to_string())
    }
}

pub fn get_int_value(value: &Value) -> Option<i32> {
    match value {
        Value::Int(v) => Some(*v),
        Value::Str(_) => None,
    }
}

pub fn get_str_value(value: &Value) -> Option<String> {
    match value {
        Value::Int(_) => None,
        Value::Str(v) => Some(v.to_string().to_lowercase()),
    }
}

pub fn get_columns(parsed_query: &Vec<String>) -> Vec<String> {
    let mut columns = Vec::new();
    let mut index = 1;
    if parsed_query[0] == "update" {
        while parsed_query[index] != "set" {
            index += 1;
        }
        index += 1;
    }
    while index < parsed_query.len() && parsed_query[index] != "from" {
        columns.push(parsed_query[index].to_string());
        index += 1;
    }
    columns
}

pub fn get_condition_columns(parsed_query: &Vec<String>) -> Vec<String> {
    let mut condition_columns = Vec::new();
    let index = parsed_query.iter().position(|x| x == "where");
    if let Some(mut index) = index {
        index += 1;
        while index < parsed_query.len() {
            condition_columns.push(parsed_query[index].to_string());
            index += 1;
        }
    }
    condition_columns
}

pub fn get_column_index(headers: &Vec<&str>, column_name: &str) -> isize {
    let mut index = 0;
    for header in headers {
        if *header == column_name {
            return index;
        }
        index += 1;
    }
    -1
}

pub fn cleaned_values(columns: Vec<String>) -> Vec<String> {
    columns
        .iter()
        .map(|col| {
            col.trim_matches(|c| c == '(' || c == ')' || c == ',' || c == '\'' || c == ';')
                .trim()
                .to_string()
        })
        .collect()
}
//FIXME: there is an equal function
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
            return;
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cast_to_value_int() {
        assert_eq!(cast_to_value("42"), Value::Int(42));
    }

    #[test]
    fn test_cast_to_value_str() {
        assert_eq!(cast_to_value("hello"), Value::Str("hello".to_string()));
    }

    #[test]
    fn test_get_int_value_some() {
        let value = Value::Int(42);
        assert_eq!(get_int_value(&value), Some(42));
    }

    #[test]
    fn test_get_int_value_none() {
        let value = Value::Str("hello".to_string());
        assert_eq!(get_int_value(&value), None);
    }

    #[test]
    fn test_get_str_value_some() {
        let value = Value::Str("hello".to_string());
        assert_eq!(
            get_str_value(&value),
            Some("hello".to_string().to_lowercase())
        );
    }

    #[test]
    fn test_get_str_value_none() {
        let value = Value::Int(42);
        assert_eq!(get_str_value(&value), None);
    }

    #[test]
    fn test_get_columns() {
        let parsed_query = vec!["select", "column1", "column2", "from", "table"]
            .iter()
            .map(|&s| s.to_string())
            .collect::<Vec<String>>();
        assert_eq!(get_columns(&parsed_query), vec!["column1", "column2"]);
    }

    #[test]
    fn test_get_condition_columns_with_where() {
        let parsed_query = vec![
            "select", "column1", "from", "table", "where", "column1", "=", "42",
        ]
        .iter()
        .map(|&s| s.to_string())
        .collect::<Vec<String>>();
        let length = 3;
        assert_eq!(get_condition_columns(&parsed_query).len(), length);
    }

    #[test]
    fn test_get_condition_columns_without_where() {
        let parsed_query = vec!["select", "column1", "column2", "from", "table"]
            .iter()
            .map(|&s| s.to_string())
            .collect::<Vec<String>>();
        assert_eq!(get_condition_columns(&parsed_query), Vec::<String>::new());
    }

    #[test]
    fn test_get_column_index_found() {
        let headers = vec!["column1", "column2", "column3"];
        let column_name = "column2";
        assert_eq!(get_column_index(&headers, column_name), 1);
    }

    #[test]
    fn test_get_column_index_not_found() {
        let headers = vec!["column1", "column2", "column3"];
        let column_name = "column4";
        assert_eq!(get_column_index(&headers, column_name), -1);
    }
}
