// use serde_json::Value;
// use std::fs;
// use std::io::Read;

// fn get_payload(filename: &str) -> Value {
//     let path: String = format!("./tests/{}.json", filename);
//     let mut f = fs::File::open(path).expect("Failed to open test file");
//     let mut content: String = String::new();
//     f.read_to_string(&mut content)
//         .expect("Failed to read test file to string");
//     serde_json::from_str(&content).expect("Failed to parse content into Value type")
// }

// fn main_test() {
//     let mut file = fs::File::open("./tests/test_file.json").expect("couldn't open file");
//     let mut json_content: String = String::new();
//     file.read_to_string(&mut json_content)
//         .expect("Unable to read json file");
//     let payload: Value =
//         serde_json::from_str(&json_content).expect("Could not parse json to Value variant"); // use serde-json to get the json_str as a Value variant
//     let data: Normifier = normify::from_value(payload).expect("error parsing data from paylaod");
//     for (name, data) in data.iter_tables() {
//         println!("Table: {}\n", name);
//         for (col_name, col) in data.iter_columns() {
//             println!("\tColumn: {}", col_name);
//             for item in col {
//                 println!("\t\t{}", item);
//             }
//         }
//         println!();
//     }
//     let db: DataBase = DataBase::from_norm(data);
//     println!("{}", db.tables.len());
//     for table in db.tables {
//         println!("{:?}", table);
//     }
//     assert_eq!(1, 1);
// }

use normify::{Normifier, Result};
use std::fs;
use std::io;
use std::path;
use test_case::test_case;

fn iter_test_data() -> io::Result<impl Iterator<Item = path::PathBuf>> {
    let iter = fs::read_dir("tests/data")?.filter_map(|entry| entry.ok().map(|e| e.path()));

    Ok(iter)
}
#[test_case("large_test.json")]
#[test_case("results.json")]
#[test_case("nested_test.json")]
#[test_case("edge_case_test.json")]
#[test_case("test_file.json")]
#[test_case("sorted_data.json")]

fn test_column_size(test_file: &str) -> Result<()> {
    let f = fs::File::open(format!("tests/data/{}", test_file))?;
    let rdr = io::BufReader::new(f);
    let mut norm = Normifier::new();
    norm.normify_from_reader(rdr)?;
    for (tname, tbl) in norm.get_database().iter_tables() {
        println!("Testing table {}", tname);
        let num_rows = tbl.num_rows();
        let mut fails = 0_usize;
        for (name, col) in tbl.iter_data_cols() {
            if col.len() != num_rows {
                fails += 1;
                print!(
                    "Column {} failed\nExpected: {}\n Got: {}\n",
                    name,
                    num_rows,
                    col.len()
                );
            }
            println!("Column {} passed", name);
        }
        if fails > 0 {
            panic!("{} columns failed\n", fails);
        }
    }
    Ok(())
}
