#[derive(Debug, PartialEq)]
pub enum ErrorType {
    InvalidTable,
    InvalidColumn,
    InvalidSyntax,
    Error, // to handle more general errors
}

pub fn print_error(error_type: ErrorType, description: &str) {
    match error_type {
        ErrorType::InvalidTable => eprintln!("[INVALID_TABLE]: [{}]", description),
        ErrorType::InvalidColumn => eprintln!("[INVALID_COLUMN]: [{}]", description),
        ErrorType::InvalidSyntax => eprintln!("[INVALID_SYNTAX]: [{}]", description),
        ErrorType::Error => eprintln!("[ERROR]: [{}]", description),
    }
}
