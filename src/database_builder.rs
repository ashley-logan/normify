pub struct DataBase {
    schema: HashMap<String, DataType>,
    tables: HashMap<String, DataFrame>,
}

impl DataBase {
    pub fn new() -> Self {
        DataBase {
            schema: HashMap::new(),
            tables: HashMap::new(),
        }
    }
    pub fn build_series(&mut self, col: HashMap<String, Vec<Value>) {
        todo!();
    }

    
pub fn build_series(
    schema: &mut HashMap<String, DataType>,
    col_name: String,
    col_data: Vec<Value>,
) -> Series {
    match col_data {
        v if v.iter().all(|i| i.is_i64()) => {
            if !schema.contains_key(col_name.as_str()) {
                schema.insert(col_name.clone(), DataType::Int64);
            }
            Series::new(
                col_name.as_str().into(),
                v.iter().map(|i| i.as_i64().unwrap()).collect::<Vec<i64>>(),
            )
        }
        v if v.iter().all(|f| f.is_f64()) => {
            if !schema.contains_key(col_name.as_str()) {
                schema.insert(col_name.clone(), DataType::Float64);
            }
            Series::new(
                col_name.as_str().into(),
                v.iter().map(|f| f.as_f64().unwrap()).collect::<Vec<f64>>(),
            )
        }
        v if v.iter().all(|s| s.is_string()) => {
            if !schema.contains_key(col_name.as_str()) {
                schema.insert(col_name.clone(), DataType::String);
            }
            Series::new(
                col_name.as_str().into(),
                v.iter().map(|s| s.to_string()).collect::<Vec<String>>(),
            )
        }
        v if v.iter().all(|b| b.is_boolean()) => {
            if !schema.contains_key(col_name.as_str()) {
                schema.insert(col_name.clone(), DataType::Boolean);
            }
            Series::new(
                col_name.as_str().into(),
                v.iter()
                    .map(|b| b.as_bool().unwrap())
                    .collect::<Vec<bool>>(),
            )
        }
        _ => {
            panic!("Could not determine type of column {}", col_name);
        }
    }
}

pub fn build_dataframe(
    table_name: String,
    table_data: Vec<Map<String, Value>>,
) -> Result<(HashMap<String, DataType>, DataFrame), PolarsError> {
    let mut schema: HashMap<String, DataType> = HashMap::new();
    let normal_table = normalize_table(table_data);
    let series_vec = normal_table
        .into_iter()
        .map(|(name, data)| build_series(&mut schema, name, data))
        .collect::<Vec<Series>>();
    let df: DataFrame = df!(table_name => series_vec)?;
    Ok((schema, df))
}
}