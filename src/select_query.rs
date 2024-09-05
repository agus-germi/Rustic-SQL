use std::{collections::HashMap, fs::File, io::{self, BufRead}};
use crate::{error::{print_error, ErrorType}, extras::{cast_to_value, get_column_index}, filter, query_parser::{self, Query}};
use query_parser::SelectQuery;

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

pub fn parse_order_by(order_by: &Vec<String>, headers: &Vec<&str>) -> HashMap<String,String> {
    let mut order_map = HashMap::new();
    let mut i = 0;
    while i < order_by.len() {
        let column = &order_by[i];
        if &order_by[i + 1 ] == "asc" || &order_by[i + 1] == "desc" {
            order_map.insert(column.to_string(), order_by[i + 1].to_string());
            i += 2;
        }else{
            order_map.insert(column.to_string(), "asc".to_string());
            i += 1;
        }
    }
    order_map
}


