use std::{collections::HashMap, fs::File, io::{self, BufRead}};

use super::{CommandParser, Query};
use crate::{error::{self, print_error, ErrorType}, extras::{cast_to_value, cleaned_values, get_column_index, get_columns, get_condition_columns, get_int_value, get_str_value}, filter};

#[derive(Debug)]
pub struct SelectQuery {
    pub table_name: String,
    pub columns: Vec<String>,
    pub condition: Vec<String>,
    pub order_by: Vec<String>,
}
pub struct SelectParser;

impl CommandParser for SelectParser {
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
        let columns = cleaned_values(get_columns(&parsed_query));
        let mut condition = get_condition_columns(&parsed_query);

        let mut order_index = 0;
        let mut order_by: Vec<String> = Vec::new();
        let _order_index = condition.iter().position(|x| x == "order").and_then(|index| {
            if index + 1 < condition.len() && condition[index + 1] == "by" {
                order_index = index + 2;
                order_by = cleaned_values(condition[order_index ..].to_vec());
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

pub fn filter_row(row: &Vec<String>, condition: &Vec<String>, headers: &Vec<&str>) -> bool{
    if condition.is_empty(){ //Para update sin restriccion 
        return true;
    }
    let column_condition_index = get_column_index(headers, condition[0].as_str());
    let column_condition_value = cast_to_value(condition[2].as_str());
    let operator = condition[1].as_str();
    let value = cast_to_value(&row[column_condition_index as usize]);
    if condition.len() > 3 {
        //apply_logicOp(row, condition, headers);
        return false;
    }else{
        return filter(value, column_condition_value, operator);
    }
}

// -- EXECUTE FUNCTION --

pub fn select(query: SelectQuery) -> Result<(), ErrorType>{
    let relative_path = format!("{}.csv", query.table_name);
    if let Ok(file) = File::open(&relative_path) {
        let mut reader: io::BufReader<File> = io::BufReader::new(file);
        let mut header: String = String::new();
        reader.read_line(&mut header);
        let header = header.trim();
        let headers: Vec<&str> = header.split(',').collect();

        let mut lines = reader.lines();
        let mut result_table: Vec<String> = Vec::new();

        for line in lines{
            if let Ok(line) = line {
                let values: Vec<String> = line.split(",").map(|s| s.to_string()).collect();
                if filter_row(&values, &query.condition, &headers){
                    result_table.push(line);
                };
            } else {
                // TODO: handle error
            }
        }
        let (order_map, insertion_order) = parse_order_by(&query.order_by, &headers);
        order_rows(&mut result_table, order_map, insertion_order);
        print_selected_rows(result_table, &query, &headers)
    } else {
        print_error(ErrorType::InvalidTable, "No se pudo abrir el archivo");
        return Err(ErrorType::InvalidTable);    }
    Ok(())
}

pub fn print_selected_rows(mut result_table:  Vec<String>, query: &SelectQuery, headers: &Vec<&str>) {
    let mut table = result_table;
    table.insert(0, headers.join(","));
    if query.columns[0] == "*" {
        for row in table {
            println!("{}", row);
        }
    } else {
        let mut selected_indices = Vec::new();
        for column in &query.columns {
            let column_index = get_column_index(&headers, column);
            selected_indices.push(column_index);            
        }
        for row in table {
            let selected_row: Vec<&str> = selected_indices.iter().map(|&i| row.split(',').collect::<Vec<&str>>()[i as usize]).collect();
            println!("{}", selected_row.join(","));
        }
    }
}

pub fn apply_and(row: &Vec<String>, condition: &Vec<String>, headers: &Vec<&str>) {   
    let mut i = 3 as usize;
    let mut final_index = 7 as usize;
    let mut resultado = true;
    println!("FILA: {:?}", row);

    while final_index <= condition.len() && resultado != false {
        println!("1era cond: {:?}", condition[.. i].to_vec() );
        let first_condition = filter_row(row, &condition[.. i].to_vec(), headers);
        println!("resultado: {:?}", first_condition);
        println!("operador: {:?}", &condition[i]);
        let operator = &condition[i];
        println!("2da cond: {:?}", condition[i + 1 .. final_index].to_vec() );
        let second_condition = filter_row(row, &condition[i + 1 .. final_index].to_vec(), headers);
        println!("resultado: {:?}", second_condition);
        if operator == "and" {
            resultado = first_condition && second_condition;
            println!("RESULTADO FINAL: {:?}", first_condition && second_condition);

        } 
     
        println!("RESULTADO FINAL: {:?}", first_condition && second_condition);
        i += 4;
        final_index += 4;

    }
}
trait LogicOp {
    fn apply_operation(&self, first_cond: bool, second_cond: bool) -> bool;  
}
struct AndOperator;
struct OrOperator;

impl LogicOp for AndOperator {
    fn apply_operation(&self, first_cond: bool, second_cond: bool) -> bool {
        // Implementation for AndOperator
        true
    }
}

impl LogicOp for OrOperator {
    fn apply_operation(&self, first_cond: bool, second_cond: bool) -> bool {
        // Implementation for OrOperator
        true
    }
}

pub fn logic_op(first_cond: bool, second_cond: bool, operator: &str) -> bool {
    let operator: Box<dyn LogicOp> = match operator {
        "and" => Box::new(AndOperator),
        "or" => Box::new(OrOperator),
        _ => return false,
    };
    operator.apply_operation( first_cond, second_cond)
}

pub fn parse_order_by(order_by: &Vec<String>, headers: &Vec<&str>) -> (HashMap<usize,String> , Vec<usize>) {
    let mut order_map = HashMap::new();
    let mut insertion_order: Vec<usize> = Vec::new();
    let mut i = 0;
    let mut column_index = 0;
    while i < order_by.len() {
        let column = &order_by[i];
        if (i + 1 < order_by.len()){
            if &order_by[i + 1 ] == "asc" || &order_by[i + 1] == "desc" {
                column_index = get_column_index(headers, column) as usize;
                insertion_order.push(column_index);

                order_map.insert(column_index, order_by[i + 1].to_string());
                i += 2;}
        }else{
            column_index = get_column_index(headers, column) as usize;
            insertion_order.push(column_index);

            order_map.insert(column_index, "asc".to_string());
            i += 1;
        }
    }
    println!("{:?}", order_map);
    (order_map, insertion_order)
}


fn order_rows(result_table: &mut Vec<String>, order_map:HashMap<usize,String>, insertion_order: Vec<usize>) {
    println!("{:?}", insertion_order);
    result_table.sort_by(|a, b| 
    {
        let columns_a: Vec<&str> = a.split(',').collect();
        let columns_b: Vec<&str> = b.split(',').collect();
        let mut i = 0;
        for (&index, order) in &order_map {
            if (index == insertion_order[i]) {
                println!("index: {:?}", insertion_order[i]);
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