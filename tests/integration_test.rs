use sql::utils::delete_query::{delete, DeleteQuery};
use sql::utils::insert_query::{insert, InsertQuery};
use sql::utils::update_query::update_line;
use std::fs::{self, File};
use std::io::Write;
use std::process::Command;

#[test]
#[ignore = "not yet implemented"]
fn test_query_output_to_csv() {
    let output_file = "tests/output.csv";

    let _ = fs::remove_file(output_file);

    let output = Command::new("cargo")
        .arg("run")
        .arg("--")
        .arg("tests/ordenes.csv")
        .arg("SELECT * FROM ordenes;")
        .output();

    match output {
        Ok(o) => {
            let _ = fs::write(output_file, &o.stdout);
            assert!(
                fs::metadata(output_file).is_ok(),
                "Output file was not created"
            );

            let actual_output = fs::read_to_string(output_file).expect("msg");

            let expected_output = "id,producto,id_cliente\n102,Teléfono,2\n105,Mouse,4\n109,Laptop,5\n110,Teléfono,6\n111,Laptop,6\n";

            assert_eq!(actual_output, expected_output);
        }
        Err(_) => {
            assert_eq!(true, false);
        }
    }
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
