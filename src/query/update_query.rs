use std::{
    fs::{self, File},
    io::{self, BufRead, BufReader, BufWriter, Write},
};

use super::{
    insert_query::{generate_row_to_insert, write_csv},
    select_query::filter_row,
    CommandParser, Query,
};
use crate::{
    error::{self, print_error, ErrorType},
    extras::{cleaned_values, get_column_index, get_condition_columns},
};

#[derive(Debug)]
pub struct UpdateQuery {
    pub table_name: String,
    pub columns: Vec<String>,
    pub values: Vec<String>,
    pub condition: Vec<String>,
}
//[ ]: reduce lines of code in parse function -> 36
pub struct UpdateParser;
impl CommandParser for UpdateParser {
    fn parse(&self, parsed_query: Vec<String>) -> Result<Query, ErrorType> {
        let table_name: String;
        //TODO: get rid of duplicated code
        let index_name;
        let table_name_index = parsed_query.iter().position(|x| x == "update");
        if let Some(index) = table_name_index {
            table_name = parsed_query[index + 1].to_string();
            index_name = index + 1;
        } else {
            error::print_error(
                ErrorType::InvalidSyntax,
                "Sintaxis inválida, falta 'update'",
            );
            return Err(ErrorType::InvalidSyntax);
        }
        let mut columns = Vec::new();
        let mut values = Vec::new();
        //TODO: find a way of getting it done better
        for i in (index_name + 1)..parsed_query.len() {
            if parsed_query[i] == "=" && i + 1 < parsed_query.len() {
                columns.push(parsed_query[i - 1].to_string());
                values.push(parsed_query[i + 1].to_string());
            } else if parsed_query[i] == "where" {
                break;
            }
        }
        values = cleaned_values(values);
        columns = cleaned_values(columns);
        let condition = cleaned_values(get_condition_columns(&parsed_query));
        Ok(Query::Update(UpdateQuery {
            table_name,
            columns,
            values,
            condition,
        }))
    }
}
// [ ]: reduce lines of code in update function -> 38
pub fn update(query: UpdateQuery) -> Result<(), ErrorType> {
    let relative_path = format!("{}.csv", query.table_name);
    if let Ok(file) = File::open(&relative_path) {
        let mut reader: io::BufReader<File> = io::BufReader::new(file);
        let mut header: String = String::new();
        let _ = reader.read_line(&mut header);
        let header = header.trim();
        let headers: Vec<&str> = header.split(',').collect();
        if query.condition.is_empty() {
            let row_to_insert = generate_row_to_insert(&headers, &query.columns, &query.values);
            write_csv(&relative_path, Some(row_to_insert));
        } else {
            let mut line_number;
            let mut updated_line;
            let mut i = 0;
            for line in reader.lines() {
                i += 1;
                if let Ok(line) = line {
                    let values: Vec<String> =
                        line.split(",").map(|s: &str| s.to_string()).collect();
                    if filter_row(&values, &query.condition, &headers) {
                        updated_line =
                            create_updated_line(&headers, &query.columns, &query.values, &values);
                        line_number = i;
                        let _ =
                            update_line(relative_path.as_str(), line_number, Some(&updated_line));
                    };
                } else {
                    print_error(ErrorType::InvalidTable, "No se pudo leer el archivo");
                    return Err(ErrorType::InvalidTable);
                }
            }
        }
    } else {
        print_error(ErrorType::InvalidTable, "No se pudo abrir el archivo");
        return Err(ErrorType::InvalidTable);
    }
    Ok(())
}

pub fn create_updated_line(
    headers: &Vec<&str>,
    columns: &Vec<String>,
    values_to_update: &Vec<String>,
    values: &Vec<String>,
) -> Vec<String> {
    let mut row_to_insert: Vec<String> = vec![String::new(); headers.len()];

    for i in headers {
        let n_column = get_column_index(&headers, &i) as usize;
        row_to_insert[n_column as usize].push_str(&values[n_column as usize]);

        for j in columns {
            if j == i {
                let n_column = get_column_index(&headers, &j) as usize;
                let n_value = get_column_index(
                    &columns.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
                    &i,
                ) as usize;
                row_to_insert[n_column] = values_to_update[n_value].to_string();
            }
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
    writer.flush()?;
    drop(writer);
    fs::rename(temp_file_path, file_path)?;
    Ok(())
}
