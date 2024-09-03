
pub fn get_columns(parsed_query: &Vec<String>) -> Vec<String> {
    let mut columns = Vec::new();
    let mut index = 1;
    while parsed_query[index] != "from" {
        columns.push(parsed_query[index].to_string());
        index += 1;
    }
    println!("COLUMNS: {:?}", columns);
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