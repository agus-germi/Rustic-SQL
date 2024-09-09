use utils::delete_query::{DeleteParser, DeleteQuery};
use utils::insert_query::{InsertParser, InsertQuery};
use utils::select_query::{SelectParser, SelectQuery};
use utils::update_query::{UpdateParser, UpdateQuery};

use crate::error::{self, ErrorType};
use crate::utils;
use crate::utils::delete_query::delete;
use crate::utils::insert_query::insert;
use crate::utils::select_query::select;
use crate::utils::update_query::update;

#[derive(Debug)]

/// Enum que representa los diferentes tipos de consultas posibles.
///
/// # Variantes
/// * `Select` - Consulta de selección.
/// * `Insert` - Consulta de inserción.
/// * `Delete` - Consulta de eliminación.
/// * `Update` - Consulta de actualización.
///
///
pub enum Query {
    Select(SelectQuery),
    Insert(InsertQuery),
    Delete(DeleteQuery),
    Update(UpdateQuery),
}

#[derive(Debug)]
/// Enum que representa los diferentes tipos de comandos SQL.
///
/// # Notas
/// Utilizado para determinar qué tipo de consulta se está realizando y asignar el parser correspondiente.
pub enum CommandType {
    Select,
    Insert,
    Delete,
    Update,
}

/// Trait para el análisis de comandos SQL.
///
/// Los implementadores de este trait deben proporcionar métodos para validar
/// la sintaxis de los comandos y para parsear los comandos en una estructura `Query`.
///
pub trait CommandParser {
    /// Valida la sintaxis del comando SQL.
    ///
    /// # Argumentos
    /// * `parsed_query` - Un vector de `String` que representa el comando SQL descompuesto en tokens.
    ///
    /// # Retorno
    /// Devuelve `Ok(())` si la sintaxis es válida, o un `ErrorType::InvalidSyntax` si la sintaxis es incorrecta.
    ///
    fn validate_syntax(&self, parsed_query: &[String]) -> Result<(), ErrorType>;

    /// Parsea el comando SQL en una estructura `Query`.
    ///
    /// # Argumentos
    /// * `parsed_query` - Un vector de `String` que representa el comando SQL descompuesto en tokens.
    ///
    /// # Retorno
    /// Devuelve un `Ok(Query)` si el parseo es exitoso, o un `ErrorType::InvalidSyntax` si ocurre un error durante el parseo.
    ///
    fn parse(&self, parsed_query: Vec<String>) -> Result<Query, ErrorType>;
}

/// Parsea y ejecuta una consulta SQL.
///
/// # Argumentos
/// * `path` - La ruta del archivo sobre el que se debe ejecutar la consulta.
/// * `query` - La consulta SQL en formato de cadena.
///
/// # Retorno
/// Devuelve `Ok(())` si la ejecución es exitosa, o un `ErrorType` si ocurre un error durante el parseo o ejecución de la consulta.
///
/// # Notas
/// Esta función es la principal para ejecutar consultas SQL.
/// Es la encargada de parsear la consulta y determinar qué tipo de consulta se está realizando, en base a eso, se ejecuta la consulta correspondiente.

pub fn parse_query(path: &str, query: &str) -> Result<(), ErrorType> {
    let parsed_query: Vec<String> = query
        .split_whitespace()
        .map(|s| s.to_string().to_lowercase())
        .collect();

    if parsed_query.len() < 4 {
        let error = ErrorType::InvalidSyntax;
        error::print_error(error, "Sintaxis inválida");
        return Err(ErrorType::InvalidSyntax);
    }
    let command: Box<dyn CommandParser> = match parsed_query[0].as_str() {
        "select" => Box::new(SelectParser),
        "insert" => Box::new(InsertParser),
        "delete" => Box::new(DeleteParser),
        "update" => Box::new(UpdateParser),
        _ => {
            let error = ErrorType::InvalidSyntax;
            error::print_error(error, "Comando no válido");
            return Err(ErrorType::InvalidSyntax);
        }
    };
    command.validate_syntax(&parsed_query)?;
    match command.parse(parsed_query) {
        Ok(query) => execute(path, query),
        Err(error) => return Err(error),
    };
    Ok(())
}

/// Ejecuta una consulta SQL en el archivo especificado.
///
/// # Argumentos
/// * `path` - La ruta del archivo sobre el que se debe ejecutar la consulta.
/// * `query` - La consulta SQL a ejecutar, encapsulada en una variante de `Query`.
///
/// # Notas
/// De acuerdo a la consulta SQL, se ejecuta la función correspondiente.
pub fn execute(path: &str, query: Query) {
    match query {
        Query::Select(select_query) => {
            let _ = select(path, select_query);
        }
        Query::Insert(insert_query) => {
            let _ = insert(path, insert_query);
        }
        Query::Delete(delete_query) => {
            let _ = delete(path, delete_query);
        }
        Query::Update(update_query) => {
            let _ = update(path, update_query);
        }
    }
}
#[cfg(test)]
mod tests_query {
    use super::*;

    #[test]
    fn test_parse_select_query() {
        let query = "select * from table_name";
        let result = parse_query("fake_path.csv", query);
        assert!(result.is_ok(), "No se pudo parsear la consulta SELECT");
    }

    #[test]
    fn test_parse_insert_query() {
        let query = "INSERT INTO ordenes (id, id_cliente, producto, cantidad) VALUES (111, 6, 'Laptop', 3);";
        let result = parse_query("fake_path.csv", query);
        assert!(result.is_ok(), "No se pudo parsear la consulta INSERT");
    }

    #[test]
    fn test_parse_update_query() {
        let query = "update table_name set column1 = 'value1' where column2 = 'value2'";
        let result = parse_query("fake_path.csv", query);
        assert!(result.is_ok(), "No se pudo parsear la consulta UPDATE");
    }

    #[test]
    fn test_parse_delete_query() {
        let query = "delete from table_name where column1 = 'value1'";
        let result = parse_query("fake_path.csv", query);
        assert!(result.is_ok(), "No se pudo parsear la consulta DELETE");
    }

    #[test]
    fn test_parse_invalid_command() {
        let query = "not_a_command table table_name";
        let result = parse_query("fake_path.csv", query);
        assert!(result.is_err(), "Comando no válido");
        if let Err(error) = result {
            assert_eq!(error, ErrorType::InvalidSyntax);
        }
    }

    #[test]
    fn test_parse_short_query() {
        let query = "insert into";
        let result = parse_query("fake_path.csv", query);
        assert!(result.is_err(), "Sintaxis inválida");
        if let Err(error) = result {
            assert_eq!(error, ErrorType::InvalidSyntax);
        }
    }
}
