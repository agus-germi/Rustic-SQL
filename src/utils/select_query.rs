use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead},
};

use crate::query::{CommandParser, Query};
use crate::{
    error::{self, print_error, ErrorType},
    extras::{
        cast_to_value, cleaned_values, get_column_index, get_columns, get_condition_columns,
        get_int_value, get_str_value,
    },
};

use crate::operations::filter;

#[derive(Debug)]

/// Representa una consulta `SELECT`, con los parámetros:
/// 
/// * `table_name` - Nombre de la tabla de la cual se seleccionarán los datos.
/// * `columns` - Columnas que se van a seleccionar.
/// * `condition` - Condiciones que deben cumplirse para seleccionar filas.
/// * `order_by` - Criterios de ordenamiento para los resultados.
/// 
pub struct SelectQuery {
    pub table_name: String,
    pub columns: Vec<String>,
    pub condition: Vec<String>,
    pub order_by: Vec<String>,
}
pub struct SelectParser;
impl CommandParser for SelectParser {
    /// Valida la sintaxis de la consulta `SELECT`.
    ///
    /// # Argumentos
    /// * `parsed_query` - Una referencia a un `Vec<String>` con los componentes de la consulta ya parseados.
    ///
    /// # Retorno
    /// Devuelve `Ok(())` si la sintaxis es válida, o `Err(ErrorType)` si es inválida.
    /// 
    fn validate_syntax(&self, parsed_query: &[String]) -> Result<(), ErrorType> {
        if parsed_query.len() < 4
            || parsed_query[0] != "select"
            || !parsed_query.contains(&"from".to_string())
        {
            error::print_error(
                ErrorType::InvalidSyntax,
                "Sintaxis inválida: falta 'SELECT' o 'FROM'",
            );
            return Err(ErrorType::InvalidSyntax);
        }

        let from_index = parsed_query
            .iter()
            .position(|x| x == "from")
            .ok_or(ErrorType::InvalidSyntax)?;
        if from_index <= 1 {
            error::print_error(
                ErrorType::InvalidSyntax,
                "Sintaxis inválida: falta la lista de columnas",
            );
            return Err(ErrorType::InvalidSyntax);
        }

        Ok(())
    }

    /// Parsea la consulta `SELECT` en un objeto `Query`.
    ///
    /// # Argumentos
    /// * `parsed_query` - Una `Vec<String>` que contiene los componentes de la consulta.
    ///
    /// # Retorno
    /// Devuelve un `Query::Select` que contiene los detalles de la consulta, o un `Err(ErrorType)` en caso de error.
    /// 
    fn parse(&self, parsed_query: Vec<String>) -> Result<Query, ErrorType> {
        let table_name = extract_table_name(&parsed_query)?;
        let columns = cleaned_values(get_columns(&parsed_query));
        let mut condition = cleaned_values(get_condition_columns(&parsed_query));
        let order_by = extract_order_by(&mut condition);

        Ok(Query::Select(SelectQuery {
            table_name,
            columns,
            condition,
            order_by,
        }))
    }
}

/// Extrae el nombre de la tabla de la consulta `SELECT`.
///
/// # Argumentos
/// * `parsed_query` - Una referencia a un `Vec<String>` con la consulta ya parseada.
///
/// # Retorno
/// Devuelve el nombre de la tabla o un `Err(ErrorType)` si no se encuentra.
/// 
fn extract_table_name(parsed_query: &[String]) -> Result<String, ErrorType> {
    parsed_query
        .iter()
        .position(|x| x == "from")
        .map(|index| parsed_query[index + 1].to_string())
        .ok_or_else(|| {
            error::print_error(ErrorType::InvalidSyntax, "Sintaxis inválida, falta 'from'");
            ErrorType::InvalidSyntax
        })
}

/// Extrae las columnas para la cláusula `ORDER BY` y las remueve de las condiciones.
///
/// # Argumentos
/// * `condition` - Una referencia mutable a un `Vec<String>` que contiene las condiciones de la consulta.
///
/// # Retorno
/// Devuelve un `Vec<String>` con las columnas de ordenamiento.
/// 
fn extract_order_by(condition: &mut Vec<String>) -> Vec<String> {
    if let Some(index) = condition.iter().position(|x| x == "order") {
        if index + 1 < condition.len() && condition[index + 1] == "by" {
            let order_by = cleaned_values(condition[index + 2..].to_vec());
            *condition = condition[..index].to_vec(); // Modify condition to exclude order clause
            return order_by;
        }
    }
    Vec::new()
}

/// Filtra una fila de acuerdo a las condiciones dadas.
///
/// # Argumentos
/// * `row` - Una referencia a un `Vec<String>` que representa los valores de la fila.
/// * `condition` - Una referencia a un `Vec<String>` que contiene las condiciones a evaluar.
/// * `headers` - Una referencia a un `Vec<&str>` que representa los encabezados de las columnas.
///
/// # Retorno
/// Devuelve `true` si la fila cumple las condiciones, o `false` en caso contrario.
/// 
/// # Notas
/// Las condiciones pueden ser simples o compuestas, y pueden incluir operadores lógicos como `AND`, `OR` y `NOT`.
/// Esta funcion tambien es utilizada en update y delete dado que tambien se necesita filtrar las filas.
/// 
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

/// Ejecuta la consulta `SELECT` sobre el archivo CSV especificado.
///
/// # Argumentos
/// * `path` - Ruta del archivo CSV.
/// * `query` - Un objeto `SelectQuery` con los detalles de la consulta.
///
/// # Retorno
/// Devuelve `Ok(())` si la operación fue exitosa, o `Err(ErrorType)` si hubo algún error.
/// 
/// # Notas
/// Esta función lee el archivo línea por línea, y filtra linea a linea (usando filter_row) quedandose con las que cumplen la condición.
/// 
pub fn select(path: &str, query: SelectQuery) -> Result<(), ErrorType> {
    if let Ok(file) = File::open(path) {
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

/// Imprime las filas seleccionadas por la consulta.
///
/// # Argumentos
/// * `result_table` - Un `Vec<String>` con las filas seleccionadas.
/// * `query` - Una referencia a la consulta `SelectQuery`.
/// * `headers` - Una referencia a un `Vec<&str>` con los nombres de las columnas.
/// 
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

/// Parsea las columnas y direcciones especificadas en la cláusula `ORDER BY`.
///
/// # Argumentos
/// * `order_by` - Una referencia a un `Vec<String>` con las columnas para ordenar.
/// * `headers` - Una referencia a un `Vec<&str>` con los nombres de las columnas.
///
/// # Retorno
/// Devuelve un `HashMap` que relaciona el índice de la columna con el ordenamiento (`asc` o `desc`)
/// y un `Vec<usize>` que representa el orden de inserción dentro de ese hashmap.
/// 
/// # Notas
/// Entiendo que si no se proporciona un ordenamiento, se asume que es ascendente.
/// 
pub fn parse_order_by(
    order_by: &[String],
    headers: &[&str],
) -> (HashMap<usize, String>, Vec<usize>) {
    let mut order_map = HashMap::new();
    let mut insertion_order = Vec::new();

    let headers_vec: Vec<String> = headers.iter().map(|s| s.to_string()).collect();

    let mut i = 0;
    while i < order_by.len() {
        let column = &order_by[i];
        let direction =
            if i + 1 < order_by.len() && (order_by[i + 1] == "asc" || order_by[i + 1] == "desc") {
                i += 2;
                &order_by[i - 1]
            } else {
                i += 1;
                "asc"
            };

        let column_index = get_column_index(&headers_vec, column) as usize;
        insertion_order.push(column_index);
        order_map.insert(column_index, direction.to_string());
    }

    (order_map, insertion_order)
}

/// Ordena las filas de acuerdo a las columnas especificadas en `ORDER BY`.
///
/// # Argumentos
/// * `result_table` - Una referencia mutable a un `Vec<String>` con las filas a ordenar, previamente seleccionadas del csv.
/// * `order_map` - Un `HashMap<usize, String>` que indica el índice de la columna y su dirección de orden.
/// * `insertion_order` - Un `Vec<usize>` que representa el orden de precedencia de las columnas para aplicar el ordenamiento.
/// 
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
                let cmp = compare_columns(columns_a[index], columns_b[index]);

                if cmp != std::cmp::Ordering::Equal {
                    return if order == "asc" { cmp } else { cmp.reverse() };
                }
            }
        }
        std::cmp::Ordering::Equal
    });
}

/// Compara dos valores de columna.
///
/// # Argumentos
/// * `val_a` - Valor de la primera columna como `&str`.
/// * `val_b` - Valor de la segunda columna como `&str`.
///
/// # Retorno
/// Devuelve un `Ordering` que indica si el valor es menor, igual o mayor.
/// 
fn compare_columns(val_a: &str, val_b: &str) -> std::cmp::Ordering {
    let val_a = cast_to_value(val_a);
    let val_b = cast_to_value(val_b);

    match (
        get_int_value(&val_a),
        get_int_value(&val_b),
        get_str_value(&val_a),
        get_str_value(&val_b),
    ) {
        (Some(i1), Some(i2), _, _) => i1.cmp(&i2),
        (_, _, Some(s1), Some(s2)) => s1.cmp(&s2),
        _ => std::cmp::Ordering::Equal,
    }
}

/// Extrae las evaluaciones booleanas y los operadores de las condiciones lógicas.
///
/// # Argumentos
/// * `condition` - Una referencia a un `Vec<String>` con las condiciones.
/// * `row` - Una referencia a un `Vec<String>` con los valores de la fila.
/// * `headers` - Una referencia a un `Vec<&str>` con los encabezados de las columnas.
///
/// # Retorno
/// Devuelve un `Vec<bool>` con los resultados de las evaluaciones y un `Vec<String>` con los operadores lógicos.
/// 
/// # Notas
/// Esta función es utilizada para evaluar condiciones compuestas con operadores lógicos.
/// Cada bool corresponde a una condicion simple.
/// 
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

/// Evalúa las condiciones lógicas utilizando los operadores especificados.
///
/// # Argumentos
/// * `bools` - Un `Vec<bool>` que contiene los resultados de las evaluaciones.
/// * `ops` - Un `Vec<String>` con los operadores lógicos.
///
/// # Retorno
/// Devuelve un `bool` que indica el resultado final de la evaluación.
/// 
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

#[cfg(test)]
mod tests_select_query {
    use std::collections::HashMap;

    use crate::{query::{CommandParser, Query}, utils::select_query::{filter_row, order_rows, parse_order_by}};

    use super::SelectParser;

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
    }}
