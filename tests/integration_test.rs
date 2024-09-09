use sql::utils::delete_query::{delete, DeleteQuery};
use sql::utils::insert_query::{insert, InsertQuery};
use sql::utils::update_query::update_line;
use std::fs::{self, File};
use std::io::Write;
use std::process::Command;

#[test]
fn test_select_every_row() {
    let output_file = "tests/output1.csv";

    // Borro el archivo si ya existe
    let _ = fs::remove_file(output_file);

    let output = Command::new("cargo")
        .arg("run")
        .arg("--")
        .arg("tests/ordenes.csv")
        .arg("SELECT * FROM ordenes;")
        .output();

    let output = match output {
        Ok(o) => o,
        Err(e) => {
            eprintln!("No se pudo ejecutar el comando: {}", e);
            return;
        }
    };

    fs::write(output_file, &output.stdout).unwrap_or(());

    assert!(fs::metadata(output_file).is_ok(), "Output no fue creado");

    //leo el output file
    let actual_output = match fs::read_to_string(output_file) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("No se pudo leer el output file: {}", e);
            return;
        }
    };
    let expected_output = "id,id_cliente,producto,cantidad\n101,1,Laptop,1\n103,1,Monitor,1\n102,2,Teléfono,2\n104,3,Teclado,1\n105,4,Mouse,2\n106,5,Impresora,1\n107,6,Altavoces,1\n108,4,Auriculares,1\n109,5,Laptop,1\n110,6,Teléfono,2\n";

    assert_eq!(
        actual_output, expected_output,
        "Output no coincide con el resultado esperado"
    );
    let _ = fs::remove_file(output_file); //borro el archivo después de la prueba
}

#[test]
fn test_select_with_where_clause() {
    let output_file = "tests/output2.csv";

    let _ = std::fs::remove_file(output_file);

    let output = std::process::Command::new("cargo")
        .arg("run")
        .arg("--")
        .arg("tests/ordenes.csv")
        .arg("SELECT id, producto, id_cliente FROM ordenes WHERE cantidad > 1;")
        .output();

    let output = match output {
        Ok(o) => o,
        Err(e) => {
            eprintln!("No se pudo ejecutar el comando: {}", e);
            return;
        }
    };

    std::fs::write(output_file, &output.stdout).unwrap_or(());

    assert!(
        std::fs::metadata(output_file).is_ok(),
        "Output no fue creado"
    );

    let actual_output = match std::fs::read_to_string(output_file) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("No se pudo leer el output file: {}", e);
            return;
        }
    };

    let expected_output = "id,producto,id_cliente\n102,Teléfono,2\n105,Mouse,4\n110,Teléfono,6\n";

    assert_eq!(
        actual_output, expected_output,
        "Output no coincide con el resultado esperado"
    );

    let _ = std::fs::remove_file(output_file);
}

#[test]
fn test_select_with_where_and_order_by_clause() {
    let output_file = "tests/output3.csv";

    let _ = std::fs::remove_file(output_file);

    let output = std::process::Command::new("cargo")
        .arg("run")
        .arg("--")
        .arg("tests/clientes.csv")
        .arg("SELECT id, nombre, email FROM clientes WHERE apellido = 'López' ORDER BY email DESC;")
        .output();

    let output = match output {
        Ok(o) => o,
        Err(e) => {
            eprintln!("No se pudo ejecutar el comando: {}", e);
            return;
        }
    };

    std::fs::write(output_file, &output.stdout).unwrap_or(());

    assert!(
        std::fs::metadata(output_file).is_ok(),
        "No se creó el archivo de salida"
    );

    let actual_output = match std::fs::read_to_string(output_file) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("No se pudo leer el archivo de salida: {}", e);
            return;
        }
    };

    let expected_output =
        "id,nombre,email\n5,José,jose.lopez@email.com\n2,Ana,ana.lopez@email.com\n";

    assert_eq!(
        actual_output, expected_output,
        "El resultado no coincide con el resultado esperado"
    );

    let _ = std::fs::remove_file(output_file);
}

#[test]
fn test_select_with_two_conditions() {
    let output_file = "tests/output4.csv";

    let _ = std::fs::remove_file(output_file);

    let output = std::process::Command::new("cargo")
        .arg("run")
        .arg("--")
        .arg("tests/ordenes.csv")
        .arg("SELECT id, id_cliente, producto, cantidad FROM ordenes WHERE cantidad > 1 AND producto = 'mouse';")
        .output();

    let output = match output {
        Ok(o) => o,
        Err(e) => {
            eprintln!("No se pudo ejecutar el comando: {}", e);
            return;
        }
    };

    std::fs::write(output_file, &output.stdout).unwrap_or(());

    assert!(
        std::fs::metadata(output_file).is_ok(),
        "No se creó el archivo de salida"
    );

    let actual_output = match std::fs::read_to_string(output_file) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("No se pudo leer el archivo de salida: {}", e);
            return;
        }
    };

    let expected_output = "id,id_cliente,producto,cantidad\n105,4,Mouse,2\n";

    assert_eq!(
        actual_output, expected_output,
        "El resultado no coincide con el resultado esperado"
    );

    let _ = std::fs::remove_file(output_file);
}

#[test]
fn test_select_with_three_conditions() {
    let output_file = "tests/output5.csv";

    let _ = std::fs::remove_file(output_file);

    let output = std::process::Command::new("cargo")
        .arg("run")
        .arg("--")
        .arg("tests/ordenes.csv")
        .arg("SELECT id, id_cliente, producto, cantidad FROM ordenes WHERE producto = 'mouse' or id_cliente = 6 and cantidad > 1;")
        .output();

    let output = match output {
        Ok(o) => o,
        Err(e) => {
            eprintln!("No se pudo ejecutar el comando: {}", e);
            return;
        }
    };

    std::fs::write(output_file, &output.stdout).unwrap_or(());

    assert!(
        std::fs::metadata(output_file).is_ok(),
        "No se creó el archivo de salida"
    );

    let actual_output = match std::fs::read_to_string(output_file) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("No se pudo leer el archivo de salida: {}", e);
            return;
        }
    };

    let expected_output = "id,id_cliente,producto,cantidad\n105,4,Mouse,2\n110,6,Teléfono,2\n";

    assert_eq!(
        actual_output, expected_output,
        "El resultado no coincide con el resultado esperado"
    );

    let _ = std::fs::remove_file(output_file);
}

#[test]
fn test_delete_function() -> Result<(), Box<dyn std::error::Error>> {
    let test_file = "test_delete_function.csv";

    let mut file = File::create(test_file)?;
    writeln!(file, "id,name")?;
    writeln!(file, "1,Agus")?;
    writeln!(file, "2,Tina")?;

    let delete_query = DeleteQuery {
        table_name: "test_delete_function".to_string(),
        condition: vec!["id".to_string(), "=".to_string(), "1".to_string()],
    };

    let _ = delete(test_file, delete_query);

    let contents = fs::read_to_string(test_file)?;
    let expected_result = "id,name\n2,Tina\n";
    assert_eq!(contents, expected_result);

    fs::remove_file(test_file)?; //elimino el archivo de prueba

    Ok(())
}

#[test]
fn test_insert() -> Result<(), Box<dyn std::error::Error>> {
    let test_file = "test_insert.csv";

    let mut file = File::create(test_file)?;
    writeln!(file, "id,name,age")?;

    let insert_query = InsertQuery {
        table_name: "test_insert".to_string(),
        columns: vec!["name".to_string(), "age".to_string()],
        values: vec!["Alice".to_string(), "30".to_string()],
    };

    let _ = insert(test_file, insert_query);

    let contents = fs::read_to_string(test_file)?;
    assert!(contents.contains(",Alice,30"));

    fs::remove_file(test_file)?;

    Ok(())
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
