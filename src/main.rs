mod normalizer;
use normalizer::Normifier;
mod database_builder;
mod dtype;
use serde_json::Value;
use std::fs;
use std::io::Read;

fn main() {
    let mut file = fs::File::open("./tests/test_file.json").expect("couldn't open file");
    let mut json_content: String = String::new();
    file.read_to_string(&mut json_content)
        .expect("Unable to read json file");
    let payload: Value =
        serde_json::from_str(&json_content).expect("Could not parse json to Value variant"); // use serde-json to get the json_str as a Value variant
    let data: Normifier = Normifier::from_value(payload).expect("error parsing data from paylaod");
    for (name, table) in data.iter_tables() {
        println!("Table: {}\n\n", name);
        for (field, col) in table.iter_columns() {
            println!("\tColumn: {}\n", field);
            println!("\t\t{:?}\n", col);
        }
    }
}
