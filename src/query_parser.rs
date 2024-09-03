use crate::error::{self, ErrorType};
use crate::execute;
use crate::extras::{get_columns, get_condition_columns, cleaned_values};

#[derive(Debug)]
pub enum Query {
    Select(SelectQuery),
    Insert(InsertQuery),
    //Delete(DeleteQuery),
    //Update(UpdateQuery),
}

#[derive(Debug)]
pub struct SelectQuery {
    pub table_name: String,
    pub columns: Vec<String>,
    pub condition: Vec<String>,
}

#[derive(Debug)]
pub struct InsertQuery {
    pub table_name: String,
    pub columns: Vec<String>,
    pub values: Vec<String>,
}

trait CommandParser {
    fn parse(&self,  parsed_query: Vec<String>) -> Result<Query, ErrorType> ;
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
        let columns = get_columns(&parsed_query);
        let condition = get_condition_columns(&parsed_query);


        Ok(Query::Select(SelectQuery {
            table_name,
            columns,
            condition,

        }))
    }
}
pub struct InsertParser;

impl CommandParser for InsertParser {
    fn parse(&self, parsed_query: Vec<String>) -> Result<Query, ErrorType> {
        let table_name: String;
        let mut table_index = 0;

        let _table_name_index = parsed_query.iter().position(|x| x == "insert").and_then(|index| {
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
        let mut values = cleaned_values(parsed_query[value_index + 1..].to_vec());
        let columns = cleaned_values(parsed_query[table_index + 1..value_index].to_vec());
        

        Ok(Query::Insert(InsertQuery {
            table_name,
            columns,
            values,
        }))
    }
}

pub fn parse_query(query: &str) -> Result<(), ErrorType> {
    let parsed_query: Vec<String> = query.split_whitespace().map(|s| s.to_string().to_lowercase()).collect();
    if parsed_query.len() < 4 {
        //Entiendo que no puede haber una consulta con menos de 4 palabras
        let error = ErrorType::InvalidSyntax;
        error::print_error(error, "Sintaxis inválida");
        return Err(ErrorType::InvalidSyntax);
    }
    println!("comando: {:?}", parsed_query[0]);
    let command: Box<dyn CommandParser> = match parsed_query[0].as_str() {
        "select" => Box::new(SelectParser),
        "insert" => Box::new(InsertParser),
        _ => {
            let error = ErrorType::InvalidSyntax;
            error::print_error(error, "Comando no válido");
            return Err(ErrorType::InvalidSyntax);
        }
    };
    let query = match command.parse(parsed_query) {
        Ok(query) => execute(query),
        Err(error) => return Err(error),
    };
    Ok(())
}