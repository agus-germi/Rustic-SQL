use std::{
    fs::{self, File},
    io::{self, BufRead, BufReader, BufWriter, Write},
};

use crate::utils::{
    insert_query::{generate_row_to_insert, write_csv},
    select_query::filter_row,
};

use crate::query::{CommandParser, Query};
use crate::{
    error::{self, print_error, ErrorType},
    extras::{cleaned_values, get_column_index, get_condition_columns},
};

#[derive(Debug)]

/// Representa una consulta `UPDATE`, con los parámetros:
///
/// * `table_name` - El nombre de la tabla a actualizar.
/// * `columns` - Las columnas que se actualizarán.
/// * `values` - Los nuevos valores para las columnas.
/// * `condition` - La condición para seleccionar las filas a actualizar.
///
pub struct UpdateQuery {
    pub table_name: String,
    pub columns: Vec<String>,
    pub values: Vec<String>,
    pub condition: Vec<String>,
}

pub struct UpdateParser;
impl CommandParser for UpdateParser {
    /// Valida la sintaxis de la consulta `UPDATE`.
    ///
    /// # Argumentos
    /// * `parsed_query` - Una referencia a un `Vec<String>` con los componentes de la consulta ya parseados.
    ///
    /// # Retorno
    /// Devuelve `Ok(())` si la sintaxis es válida, o `Err(ErrorType)` si es inválida.
    ///
    fn validate_syntax(&self, parsed_query: &[String]) -> Result<(), ErrorType> {
        if parsed_query.len() < 4
            || parsed_query[0] != "update"
            || !parsed_query.contains(&"set".to_string())
        {
            error::print_error(
                ErrorType::InvalidSyntax,
                "Sintaxis inválida: falta 'UPDATE' o 'SET'",
            );
            return Err(ErrorType::InvalidSyntax);
        }

        let set_index = parsed_query
            .iter()
            .position(|x| x == "set")
            .ok_or(ErrorType::InvalidSyntax)?;
        let mut set_found = false;

        for i in (set_index + 1)..parsed_query.len() {
            if parsed_query[i] == "=" && i + 1 < parsed_query.len() {
                set_found = true;
            } else if parsed_query[i] == "where" {
                break;
            }
        }

        if !set_found {
            error::print_error(
                ErrorType::InvalidSyntax,
                "Sintaxis inválida: no se encontraron asignaciones 'columna=valor'",
            );
            return Err(ErrorType::InvalidSyntax);
        }

        Ok(())
    }

    /// Parsea la consulta `UPDATE` en un objeto `Query`.
    ///
    /// # Argumentos
    /// * `parsed_query` - Una `Vec<String>` que contiene los componentes de la consulta.
    ///
    /// # Retorno
    /// Devuelve un `Query::Update` que contiene los detalles de la consulta, o un `Err(ErrorType)` en caso de error.
    ///
    fn parse(&self, parsed_query: Vec<String>) -> Result<Query, ErrorType> {
        let table_name = extract_table_name(&parsed_query)?;
        let set_index = parsed_query.iter().position(|x| x == "set").unwrap_or(0);

        let (columns, values) = extract_columns_and_values(&parsed_query, set_index + 1);

        let condition = cleaned_values(get_condition_columns(&parsed_query));

        Ok(Query::Update(UpdateQuery {
            table_name,
            columns,
            values,
            condition,
        }))
    }
}

/// Extrae el nombre de la tabla de la consulta de actualización.
///
/// # Argumentos
/// * `parsed_query` - Un vector de `String` que representa la consulta descompuesta en tokens.
///
/// # Retorno
/// Devuelve el nombre de la tabla como un `String` si se encuentra, o un `ErrorType::InvalidSyntax` si no se encuentra.
///
fn extract_table_name(parsed_query: &[String]) -> Result<String, ErrorType> {
    parsed_query
        .iter()
        .position(|x| x == "update")
        .map(|index| parsed_query[index + 1].to_string())
        .ok_or_else(|| {
            error::print_error(
                ErrorType::InvalidSyntax,
                "Sintaxis inválida, falta 'update'",
            );
            ErrorType::InvalidSyntax
        })
}

/// Extrae las columnas y los valores de la consulta de actualización.
///
/// # Argumentos
/// * `parsed_query` - Un vector de `String` que representa la consulta descompuesta en tokens.
/// * `start_index` - El índice de inicio para la extracción de columnas y valores.
///
/// # Retorno
/// Devuelve una tupla con dos vectores de `String`, el primero contiene las columnas y el segundo contiene los valores.
///
fn extract_columns_and_values(
    parsed_query: &[String],
    start_index: usize,
) -> (Vec<String>, Vec<String>) {
    let mut columns = Vec::new();
    let mut values = Vec::new();

    let mut i = start_index;
    while i < parsed_query.len() {
        if parsed_query[i] == "=" && i + 1 < parsed_query.len() {
            columns.push(parsed_query[i - 1].to_string());
            values.push(parsed_query[i + 1].to_string());
            i += 2;
        } else if parsed_query[i] == "where" {
            break;
        } else {
            i += 1;
        }
    }

    (cleaned_values(columns), cleaned_values(values))
}
/// Actualiza las filas del archivo según la consulta.
///
/// # Argumentos
/// * `path` - La ruta del archivo CSV.
/// * `query` - La consulta de actualización.
///
/// # Retorno
/// Devuelve `Ok(())` si la actualización es exitosa, o un `ErrorType` si ocurre un error durante la actualización.
///
pub fn update(path: &str, query: UpdateQuery) -> Result<(), ErrorType> {
    let file = File::open(path).map_err(|_| {
        print_error(ErrorType::InvalidTable, "No se pudo abrir el archivo");
        ErrorType::InvalidTable
    })?;

    let mut reader = io::BufReader::new(file);
    let mut header = String::new();
    reader.read_line(&mut header).map_err(|_| {
        print_error(ErrorType::InvalidTable, "No se pudo leer el archivo");
        ErrorType::InvalidTable
    })?;

    let headers: Vec<&str> = header.trim().split(',').collect();

    if query.condition.is_empty() {
        let row_to_insert = generate_row_to_insert(
            &headers
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>(),
            &query.columns,
            &query.values,
        );
        return {
            write_csv(path, Some(row_to_insert));
            Ok(())
        };
    }

    update_rows(path, reader, &headers, &query)?;
    Ok(())
}

/// Actualiza las filas en el archivo basándose en la condición de la consulta.
///
/// # Argumentos
/// * `path` - La ruta del archivo CSV.
/// * `reader` - Un `BufReader` para leer el archivo.
/// * `headers` - Los encabezados de las columnas.
/// * `query` - La consulta de actualización.
///
/// # Retorno
/// Devuelve `Ok(())` si la actualización es exitosa, o un `ErrorType` si ocurre un error durante la actualización.
///
/// # Notas
/// Se toma linea a linea y se filtra según la condición indicada en la consulta. Si la fila cumple con la condición, se actualiza.
///
fn update_rows(
    path: &str,
    reader: io::BufReader<File>,
    headers: &[&str],
    query: &UpdateQuery,
) -> Result<(), ErrorType> {
    for (i, line) in reader.lines().enumerate() {
        let line = line.map_err(|_| {
            print_error(ErrorType::InvalidTable, "No se pudo leer el archivo");
            ErrorType::InvalidTable
        })?;

        let values: Vec<String> = line.split(',').map(|s| s.to_string()).collect();

        if filter_row(&values, &query.condition, headers) {
            let updated_line = create_updated_line(headers, &query.columns, &query.values, &values);
            let _ = update_line(path, i + 1, Some(&updated_line));
        }
    }
    Ok(())
}

/// Crea una línea actualizada con los nuevos valores.
///
/// # Argumentos
/// * `headers` - Los encabezados de las columnas.
/// * `columns` - Las columnas que se actualizarán.
/// * `values_to_update` - Los nuevos valores para las columnas.
/// * `values` - Los valores actuales en la fila.
///
/// # Retorno
/// Devuelve un vector de `String` que representa la línea actualizada.
///
pub fn create_updated_line(
    headers: &[&str],
    columns: &Vec<String>,
    values_to_update: &[String],
    values: &[String],
) -> Vec<String> {
    let mut row_to_insert: Vec<String> = vec![String::new(); headers.len()];

    for i in headers {
        let n_column = get_column_index(
            &headers
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>(),
            i,
        ) as usize;
        row_to_insert[n_column as usize].push_str(&values[n_column as usize]);

        for j in columns {
            if j == i {
                let n_column = get_column_index(
                    &headers
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<String>>(),
                    j,
                ) as usize;
                let n_value = get_column_index(columns, i) as usize;
                row_to_insert[n_column] = values_to_update[n_value].to_string();
            }
        }
    }
    row_to_insert
}

/// Actualiza una línea específica en el archivo CSV.
///
/// # Argumentos
/// * `file_path` - La ruta del archivo CSV.
/// * `line_index` - El índice de la línea a actualizar.
/// * `row` - Una opción que contiene la nueva línea, o `None` para mantener la línea original.
///
/// # Retorno
/// Devuelve un `io::Result<()>` que indica el éxito o el fallo de la operación.
///
pub fn update_line(
    file_path: &str,
    line_index: usize,
    row: Option<&Vec<String>>,
) -> io::Result<()> {
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

// Testing -----

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_update_parser() {
        let parser = UpdateParser;
        let input = vec![
            "update".to_string(),
            "my_table".to_string(),
            "column1".to_string(),
            "=".to_string(),
            "value1".to_string(),
            "where".to_string(),
            "column2".to_string(),
            "=".to_string(),
            "value2".to_string(),
        ];

        let result = parser.parse(input);
        println!("{:?}", result);
        if let Ok(Query::Update(update_query)) = result {
            assert_eq!(update_query.table_name, "my_table");
            assert_eq!(update_query.columns, vec!["column1".to_string()]);
            assert_eq!(update_query.values, vec!["value1".to_string()]);
            assert_eq!(
                update_query.condition,
                vec!["column2".to_string(), "=".to_string(), "value2".to_string()]
            );
        }
    }

    #[test]
    fn test_create_updated_line() {
        let headers = vec!["column1", "column2", "column3"];
        let columns = vec!["column2".to_string()];
        let values_to_update = vec!["new_value2".to_string()];
        let values = vec![
            "value1".to_string(),
            "value2".to_string(),
            "value3".to_string(),
        ];

        let updated_line = create_updated_line(&headers, &columns, &values_to_update, &values);
        assert_eq!(
            updated_line,
            vec![
                "value1".to_string(),
                "new_value2".to_string(),
                "value3".to_string()
            ]
        );
    }

    #[test]
    fn test_update_line() -> Result<(), Box<dyn std::error::Error>> {
        let test_file = "test_update_line.csv";

        let mut file = File::create(test_file)?;
        writeln!(file, "id,id_cliente,producto,cantidad")?;
        writeln!(file, "1,1,manzana,5")?;
        writeln!(file, "2,8,pera,3")?;

        update_line(
            test_file,
            2,
            Some(&vec![
                "2".to_string(),
                "8".to_string(),
                "pera".to_string(),
                "10".to_string(),
            ]),
        )?;

        let contents = fs::read_to_string(test_file)?;
        assert!(contents.contains("2,8,pera,10"));

        fs::remove_file(test_file)?;

        Ok(())
    }
}
