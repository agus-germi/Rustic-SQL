use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead},
};

use super::{CommandParser, Query};
use crate::{
    error::{self, print_error, ErrorType},
    extras::{
        cast_to_value, cleaned_values, get_column_index, get_columns, get_condition_columns,
        get_int_value, get_str_value,
    },
    filter,
};

#[derive(Debug)]
pub struct SelectQuery {
    pub table_name: String,
    pub columns: Vec<String>,
    pub condition: Vec<String>,
    pub order_by: Vec<String>,
}
pub struct SelectParser;
impl CommandParser for SelectParser {
    fn validate_syntax(&self, parsed_query: &[String]) -> Result<(), ErrorType> {
        if parsed_query.len() < 4 || parsed_query[0] != "select" || !parsed_query.contains(&"from".to_string()) {
            error::print_error(ErrorType::InvalidSyntax, "Sintaxis inválida: falta 'SELECT' o 'FROM'");
            return Err(ErrorType::InvalidSyntax);
        }

        let from_index = parsed_query.iter().position(|x| x == "from").ok_or(ErrorType::InvalidSyntax)?;
        if from_index <= 1 {
            error::print_error(ErrorType::InvalidSyntax, "Sintaxis inválida: falta la lista de columnas");
            return Err(ErrorType::InvalidSyntax);
        }

        Ok(())
    }

    //[ ]: reduce lines of code in parse function -> 35
    fn parse(&self, parsed_query: Vec<String>) -> Result<Query, ErrorType> {
        let table_name: String;
        //TODO: get rid of duplicated code
        let table_name_index = parsed_query.iter().position(|x| x == "from");
        if let Some(index) = table_name_index {
            table_name = parsed_query[index + 1].to_string();
        } else {
            error::print_error(ErrorType::InvalidSyntax, "Sintaxis inválida, falta 'from'");
            return Err(ErrorType::InvalidSyntax);
        }
        let columns = cleaned_values(get_columns(&parsed_query));
        let mut condition = cleaned_values(get_condition_columns(&parsed_query));
        let mut order_index = 0;
        let mut order_by: Vec<String> = Vec::new();
        let _order_index = condition
            .iter()
            .position(|x| x == "order")
            .and_then(|index| {
                if index + 1 < condition.len() && condition[index + 1] == "by" {
                    order_index = index + 2;
                    order_by = cleaned_values(condition[order_index..].to_vec());
                    condition = condition[..index].to_vec();
                    Some(index)
                } else {
                    None
                }
            });
        Ok(Query::Select(SelectQuery {
            table_name,
            columns,
            condition,
            order_by,
        }))
    }
}

pub fn filter_row(row: &Vec<String>, condition: &[String], headers: &[&str]) -> bool {
    if condition.is_empty() {
        return true;
    }
    let headers_vec: Vec<String> = headers.iter().map(|&s| s.to_string()).collect();
    let column_condition_index = get_column_index(&headers_vec, condition[0].as_str());
    let column_condition_value = cast_to_value(condition[2].as_str());
    let operator = condition[1].as_str();
    let value = cast_to_value(&row[column_condition_index as usize]);
    if condition.len() > 3 {
        let (results, ops) = extract_bools_and_operators(condition, row, headers);
        evaluate_logical_conditions(results, ops)
    } else {
        filter(value, column_condition_value, operator)
    }
}

pub fn select(query: SelectQuery) -> Result<(), ErrorType> {
    println!("Selecting from table: {}", query.table_name);
    let relative_path = format!("{}.csv", query.table_name);
    if let Ok(file) = File::open(&relative_path) {
        let mut reader: io::BufReader<File> = io::BufReader::new(file);
        let mut header: String = String::new();
        let _ = reader.read_line(&mut header);
        let header = header.trim();
        let headers: Vec<&str> = header.split(',').collect();
        let lines = reader.lines();
        let mut result_table: Vec<String> = Vec::new();
        for line in lines {
            if let Ok(line) = line {
                let values: Vec<String> = line.split(",").map(|s| s.to_string()).collect();
                if filter_row(&values, &query.condition, &headers) {
                    result_table.push(line);
                };
            } else {
                print_error(ErrorType::InvalidTable, "No se pudo leer el archivo");
                return Err(ErrorType::InvalidTable);
            }
        }
        let (order_map, insertion_order) = parse_order_by(&query.order_by, &headers);
        order_rows(&mut result_table, order_map, insertion_order);
        print_selected_rows(result_table, &query, &headers)
    } else {
        print_error(ErrorType::InvalidTable, "No se pudo abrir el archivo");
        return Err(ErrorType::InvalidTable);
    }
    Ok(())
}

pub fn print_selected_rows(mut result_table: Vec<String>, query: &SelectQuery, headers: &[&str]) {
    result_table.insert(0, headers.join(","));
    if query.columns[0] == "*" {
        for row in result_table {
            println!("{}", row);
        }
    } else {
        let mut selected_indices = Vec::new();
        for column in &query.columns {
            let column_index = get_column_index(
                &headers
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<String>>(),
                column,
            );
            selected_indices.push(column_index);
        }
        for row in result_table {
            let selected_row: Vec<&str> = selected_indices
                .iter()
                .map(|&i| row.split(',').collect::<Vec<&str>>()[i as usize])
                .collect();
            println!("{}", selected_row.join(","));
        }
    }
}

pub fn parse_order_by(
    order_by: &[String],
    headers: &[&str],
) -> (HashMap<usize, String>, Vec<usize>) {
    let mut order_map = HashMap::new();
    let mut insertion_order: Vec<usize> = Vec::new();
    let mut i = 0;
    let mut column_index;
    while i < order_by.len() {
        let column = &order_by[i];
        if i + 1 < order_by.len() {
            if &order_by[i + 1] == "asc" || &order_by[i + 1] == "desc" {
                column_index = get_column_index(
                    &headers
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<String>>(),
                    column,
                ) as usize;
                insertion_order.push(column_index);

                order_map.insert(column_index, order_by[i + 1].to_string());
                i += 2;
            }
        } else {
            column_index = get_column_index(
                &headers
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<String>>(),
                column,
            ) as usize;
            insertion_order.push(column_index);

            order_map.insert(column_index, "asc".to_string());
            i += 1;
        }
    }
    (order_map, insertion_order)
}
// [ ]: reduce lines of code in order_rows function -> 41
fn order_rows(
    result_table: &mut [String],
    order_map: HashMap<usize, String>,
    insertion_order: Vec<usize>,
) {
    result_table.sort_by(|a, b| {
        let columns_a: Vec<&str> = a.split(',').collect();
        let columns_b: Vec<&str> = b.split(',').collect();
        for &index in &insertion_order {
            if let Some(order) = order_map.get(&index) {
                let val_a = columns_a[index];
                let val_b = columns_b[index];
                let val_a = cast_to_value(val_a);
                let val_b = cast_to_value(val_b);

                let a_str = get_str_value(&val_a);
                let b_str = get_str_value(&val_b);
                let a_int = get_int_value(&val_a);
                let b_int = get_int_value(&val_b);

                let cmp = match (a_int, b_int, a_str, b_str) {
                    (Some(i1), Some(i2), _, _) => i1.cmp(&i2),
                    (_, _, Some(s1), Some(s2)) => s1.cmp(&s2),
                    _ => std::cmp::Ordering::Equal,
                };
                match order.as_str() {
                    "asc" => {
                        if cmp != std::cmp::Ordering::Equal {
                            return cmp;
                        }
                    }
                    "desc" => {
                        if cmp != std::cmp::Ordering::Equal {
                            return cmp.reverse();
                        }
                    }
                    _ => (),
                }
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn extract_bools_and_operators(
    condition: &[String],
    row: &Vec<String>,
    headers: &[&str],
) -> (Vec<bool>, Vec<String>) {
    let mut bools = Vec::new();
    let mut ops = Vec::new();
    let mut i = 0;

    while i < condition.len() {
        if condition[i] == "and" || condition[i] == "or" || condition[i] == "not" {
            ops.push(condition[i].to_string());
            i += 1;
        } else if i + 2 < condition.len() {
            let column = &condition[i];
            let operator = &condition[i + 1];
            let value = &condition[i + 2];
            let column_index = get_column_index(
                &headers
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<String>>(),
                column,
            ) as usize;
            let column_value = cast_to_value(&row[column_index]);
            let condition_value = cast_to_value(value);
            let result = filter(column_value, condition_value, operator);
            bools.push(result);

            i += 3;
        }
    }
    println!("{:?}", row);
    (bools, ops)
}

fn evaluate_logical_conditions(bools: Vec<bool>, ops: Vec<String>) -> bool {
    let mut result = bools[0];
    for (i, op) in ops.iter().enumerate() {
        match op.as_str() {
            "and" => result = result && bools[i + 1],
            "or" => result = result || bools[i + 1],
            "not" => result = !result,
            _ => {}
        }
    }
    result
}

// -- Testing --

#[cfg(test)]
#[test]
fn test_select_parser() {
    let parser = SelectParser;
    let input = vec![
        "select".to_string(),
        "*".to_string(),
        "from".to_string(),
        "test_table".to_string(),
        "where".to_string(),
        "age".to_string(),
        ">".to_string(),
        "25".to_string(),
        "order".to_string(),
        "by".to_string(),
        "name".to_string(),
        "asc".to_string(),
    ];

    let result = parser.parse(input);

    if let Ok(Query::Select(select_query)) = result {
        assert_eq!(select_query.table_name, "test_table");
        assert_eq!(select_query.columns, vec!["*"]);
        assert_eq!(select_query.condition, vec!["age", ">", "25"]);
        assert_eq!(select_query.order_by, vec!["name", "asc"]);
    }
}

#[test]
fn test_filter_row_match() {
    let headers = vec!["id", "name", "age"];
    let condition = vec!["age".to_string(), ">".to_string(), "25".to_string()];
    let row = vec!["1".to_string(), "Agus".to_string(), "30".to_string()];

    assert!(filter_row(&row, &condition, &headers));
}

#[test]
fn test_filter_row_no_match() {
    let headers = vec!["id", "name", "age"];
    let condition = vec!["age".to_string(), ">".to_string(), "30".to_string()];
    let row = vec!["1".to_string(), "Agus".to_string(), "25".to_string()];

    assert!(!filter_row(&row, &condition, &headers));
}

#[test]
fn test_parse_order_by_insertion_order() {
    let order_by = vec![
        "age".to_string(),
        "asc".to_string(),
        "name".to_string(),
        "desc".to_string(),
    ];
    let headers = vec!["id", "name", "age"];

    let (order_map, insertion_order) = parse_order_by(&order_by, &headers);

    let mut expected_order_map = HashMap::new();
    expected_order_map.insert(2, "asc".to_string());
    expected_order_map.insert(1, "desc".to_string());

    assert_eq!(order_map, expected_order_map);
    assert_eq!(insertion_order, vec![2, 1]);
}

#[test]
fn test_order_rows_with_one_condition() {
    let mut result_table = vec![
        "1,Agus,30".to_string(),
        "2,Bob,25".to_string(),
        "3,Gon,35".to_string(),
    ];

    let mut order_map = HashMap::new();
    order_map.insert(2, "asc".to_string());

    let insertion_order = vec![2];
    order_rows(&mut result_table, order_map, insertion_order);

    assert_eq!(
        result_table,
        vec![
            "2,Bob,25".to_string(),
            "1,Agus,30".to_string(),
            "3,Gon,35".to_string(),
        ]
    );
}

#[test]
fn test_order_rows_when_tie() {
    let mut result_table = vec![
        "1,Agus,30".to_string(),
        "2,Bob,25".to_string(),
        "3,Agus,35".to_string(),
        "4,Daniel,25".to_string(),
    ];

    let mut order_map = HashMap::new();

    order_map.insert(1, "asc".to_string());
    order_map.insert(2, "desc".to_string());

    let insertion_order = vec![1, 2];
    order_rows(&mut result_table, order_map, insertion_order);
    println!("{:?}", result_table);

    assert_eq!(
        result_table,
        vec![
            "3,Agus,35".to_string(),
            "1,Agus,30".to_string(),
            "2,Bob,25".to_string(),
            "4,Daniel,25".to_string(),
        ]
    );
}
