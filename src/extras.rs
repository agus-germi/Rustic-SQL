#[derive(Debug)]
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
    while parsed_query[index] != "from" {
        columns.push(parsed_query[index].to_string());
        index += 1;
    }
    columns

}

pub fn get_condition_columns(parsed_query: &Vec<String>) -> Vec<String> {
    let mut condition_columns = Vec::new();
    let mut index = parsed_query.iter().position(|x| x == "where");
    if let Some(mut index) = index {
        index += 1;
        while index < parsed_query.len() {
            condition_columns.push(parsed_query[index].to_string());
            index += 1;
        }
    }
    println!("COND: {:?}", condition_columns);
    condition_columns
    
}

pub fn get_column_index(headers:&Vec<&str>, column_name: &str) -> usize {
    let mut index = 0;
    for header in headers {
        if *header == column_name {
            return index;
        }
        index += 1;
    }
    index
}