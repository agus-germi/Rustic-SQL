use error::{print_error, ErrorType};
use extras::{get_int_value, get_str_value, Value};
use query::{
    delete_query::delete, insert_query::insert, parse_query, select_query::select,
    update_query::update, Query,
};
use std::env;

pub mod error;
pub mod extras;
pub mod query;

#[derive(Debug)]
pub enum CommandType {
    Select,
    Insert,
    Delete,
    Update,
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        let error_description = "Uso: cargo run -- ruta/a/tablas \"<consulta>\"";
        let error = ErrorType::InvalidSyntax;
        print_error(error, error_description);
        return;
    }
    let query = &args[2];

    if let Err(_error) = parse_query(query) {
        return;
    }
}
// -- MINI FILTER FUNCTION --
trait Operations {
    fn apply_operation(&self, value1: Value, value2: Value) -> bool;
}
struct EqualOperator;
struct GreaterThanOperator;
struct LessThanOperator;

impl Operations for EqualOperator {
    fn apply_operation(&self, value1: Value, value2: Value) -> bool {
        let int_value1 = get_int_value(&value1);
        let int_value2 = get_int_value(&value2);
        let str_value1 = get_str_value(&value1);
        let str_value2 = get_str_value(&value2);
        match (int_value1, int_value2, str_value1, str_value2) {
            (Some(i1), Some(i2), _, _) => i1 == i2,
            (_, _, Some(s1), Some(s2)) => s1 == s2,
            _ => false,
        }
    }
}

impl Operations for GreaterThanOperator {
    fn apply_operation(&self, value1: Value, value2: Value) -> bool {
        let int_value1 = get_int_value(&value1);
        let int_value2 = get_int_value(&value2);
        let str_value1 = get_str_value(&value1);
        let str_value2 = get_str_value(&value2);
        match (int_value1, int_value2, str_value1, str_value2) {
            (Some(i1), Some(i2), _, _) => i1 > i2,
            (_, _, Some(_s1), Some(_s2)) => false,
            _ => false,
        }
    }
}

impl Operations for LessThanOperator {
    fn apply_operation(&self, value1: Value, value2: Value) -> bool {
        let int_value1 = get_int_value(&value1);
        let int_value2 = get_int_value(&value2);
        let str_value1 = get_str_value(&value1);
        let str_value2 = get_str_value(&value2);
        match (int_value1, int_value2, str_value1, str_value2) {
            (Some(i1), Some(i2), _, _) => i1 < i2,
            (_, _, Some(_s1), Some(_s2)) => false,
            _ => false,
        }
    }
}

pub fn filter(value1: Value, value2: Value, operator: &str) -> bool {
    let operator: Box<dyn Operations> = match operator {
        "=" => Box::new(EqualOperator),
        ">" => Box::new(GreaterThanOperator),
        "<" => Box::new(LessThanOperator),
        _ => return false,
    };
    operator.apply_operation(value1, value2)
}

// --
pub fn execute(query: Query) {
    match query {
        Query::Select(select_query) => {
            let _ = select(select_query);
        }
        Query::Insert(insert_query) => {
            let _ = insert(insert_query);
        }
        Query::Delete(delete_query) => {
            let _ = delete(delete_query);
        }
        Query::Update(update_query) => {
            let _ = update(update_query);
        }
    }
}
