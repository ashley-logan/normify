use serde_json::Value;
use std::fs;
use std::io::Read;
mod normalizer;
use normalizer::Normalizer;
use std::collections::HashMap;

fn main() {
    let mut file = fs::File::open("./tests/test_file.json").expect("couldn't open file");
    let mut json_content: String = String::new();
    file.read_to_string(&mut json_content)
        .expect("Unable to read json file");
    let payload: Value =
        serde_json::from_str(&json_content).expect("Could not parse json to Value variant"); // use serde-json to get the json_str as a Value variant
    let mut context = Normalizer::new(); // initialize the context for normalization
    context.normify_json(payload).expect("error normify"); // normalize
    let tables: HashMap<String, HashMap<String, Vec<Value>>> = context.get_tables(); // get the tables as a hashmap of table names to tables
    // println!("{:?}", tables);
    // println!("{:?}", payload);
}
