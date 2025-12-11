// use normfiy::normalizer;
// mod normalizer;
// use normalizer::Normifier;
// mod database_builder;
// use database_builder::DataBase;
// mod dtype;
use normify::{DataBase, Normifier};
use serde_json::Value;
use std::fs;
use std::io::Read;


fn get_payload(filename: &str) -> Value {
    let path: String = format!("./tests/{}.json",filename);
    let mut f = fs::File::open(path).expect("Failed to open test file");
    let mut content: String = String::new();
    f.read_to_string(&mut content).expect("Failed to read test file to string");
    serde_json::from_str(&content).expect("Failed to parse content into Value type")
}

#[test]
fn main_test() {
    let mut file = fs::File::open("./tests/test_file.json").expect("couldn't open file");
    let mut json_content: String = String::new();
    file.read_to_string(&mut json_content)
        .expect("Unable to read json file");
    let payload: Value =
        serde_json::from_str(&json_content).expect("Could not parse json to Value variant"); // use serde-json to get the json_str as a Value variant
    let data: Normifier = normify::from_value(payload).expect("error parsing data from paylaod");
    for (name, data) in data.iter_tables() {
        println!("Table: {}\n", name);
        for (col_name, col) in data.iter_columns() {
            println!("\tColumn: {}", col_name);
            for item in col {
                println!("\t\t{}", item);
            }
        }
        println!();
    }
    let db: DataBase = DataBase::from_norm(data);
    println!("{}", db.tables.len());
    for table in db.tables {
        println!("{:?}", table);
    }
    assert_eq!(1, 1);
}

#[test]
fn regular_test() {
    let mut payload: Value = get_payload("test_file");
    let norm_result = Result<Normifier, serde_json::Error> = normify::from_value(payload);
    
}
