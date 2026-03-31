use normify::Normifier;
use std::fs::File;
use std::io::BufReader;

fn main() {
    let f = File::open("examples/all_results.json").unwrap();
    let rdr = BufReader::new(f);

    let mut norm = Normifier::new();
    norm.normify_from_reader(rdr).unwrap();

    let db: &normify::Database = norm.get_database();

    db["Courses_table"].data_cols["LocationCodes"].print_col_fmt(None);
    // for (name, tbl) in db.iter_tables() {
    //     println!(
    //         "Table {} has {} columns, and {} rows",
    //         name,
    //         tbl.num_cols(),
    //         tbl.num_rows()
    //     );
    // }
}

// let mut file = File::open("foo.txt")?;
//     let mut contents = String::new();
//     file.read_to_string(&mut contents)?;
//     assert_eq!(contents, "Hello, world!");
//     Ok(())
