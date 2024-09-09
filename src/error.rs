#[derive(Debug, PartialEq)]

/// Representa los diferentes tipos de errores que pueden ocurrir.
///
/// # Variantes
/// * `InvalidTable` - Error cuando la tabla no es válida.
/// * `InvalidColumn` - Error cuando una columna no es válida.
/// * `InvalidSyntax` - Error cuando hay un error de sintaxis en la consulta.
/// * `Error` - Maneja errores más generales que no encajan en las categorías anteriores.
pub enum ErrorType {
    InvalidTable,
    InvalidColumn,
    InvalidSyntax,
    Error, 
}

/// Imprime un mensaje de error en la salida estándar de error (`stderr`) basado en el tipo de error recibido.
///
/// # Argumentos
/// * `error_type` - El tipo de error (`ErrorType`) que ocurrió.
/// * `description` - Una descripción adicional sobre el error.
///
/// # Ejemplo
/// ```rust
/// use sql::error::{print_error, ErrorType};
/// // Error por tabla inválida
/// print_error(ErrorType::InvalidTable, "La tabla 'usuarios' no existe");
///
/// // Error por columna inválida
/// print_error(ErrorType::InvalidColumn, "La columna 'nombre' no existe en la tabla 'usuarios'");
///
/// // Error de sintaxis
/// print_error(ErrorType::InvalidSyntax, "Error de sintaxis en la cláusula WHERE");
///
/// // Error general
/// print_error(ErrorType::Error, "Error desconocido durante la ejecución");
/// ```
///
/// # Notas
/// Esta función usa `eprintln!` para imprimir los errores en `stderr`, lo cual es útil en 
/// aplicaciones de línea de comandos para diferenciar la salida normal de los mensajes de error.
/// 
pub fn print_error(error_type: ErrorType, description: &str) {
    match error_type {
        ErrorType::InvalidTable => eprintln!("[INVALID_TABLE]: [{}]", description),
        ErrorType::InvalidColumn => eprintln!("[INVALID_COLUMN]: [{}]", description),
        ErrorType::InvalidSyntax => eprintln!("[INVALID_SYNTAX]: [{}]", description),
        ErrorType::Error => eprintln!("[ERROR]: [{}]", description),
    }
}
