use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write}; 

use crate::query_parser::DeleteQuery;
use crate::error::{print_error, ErrorType};
use crate::select_query::filter_row; 

pub fn delete(delete_query: DeleteQuery) -> Result<(), ErrorType>{
    let relative_path = format!("{}.csv", delete_query.table_name);
    let mut index: usize = 0;
    if let Ok(file) = File::open(&relative_path) {
        let mut reader: io::BufReader<File> = io::BufReader::new(file);
        let mut header: String = String::new();

        let _ = reader.read_line(&mut header);
        let header = header.trim();
        let headers: Vec<&str> = header.split(',').collect();


        let lines = reader.lines();
        for line in lines{
            index += 1;
            if let Ok(line) = line {
                let values: Vec<String> = line.split(",").map(|s| s.to_string()).collect();
                if filter_row(values, &delete_query.condition, &headers){
                    delete_line(&relative_path, index);
                    index -= 1;
                };
            } else {
                // TODO: handle error
                println!("Error");
            }
        }
    } else {
        print_error(ErrorType::InvalidTable, "No se pudo abrir el archivo");
        return Err(ErrorType::InvalidTable);    }
    Ok(())


}
fn delete_line(file_path: &str, line_to_delete: usize) -> io::Result<()> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let lines = reader.lines();

    let temp_file_path = format!("{}.csv", file_path);
    let temp_file = File::create(&temp_file_path)?;
    let mut writer = BufWriter::new(temp_file);

    for (index, line) in lines.enumerate() {
        if index != line_to_delete {
            writeln!(writer, "{}", line?)?;
        }
    }
    // TODO: handle ?  -> do i have to ?
    writer.flush()?;
    drop(writer);

    fs::rename(temp_file_path, file_path)?;

    Ok(())
}