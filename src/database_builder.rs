use crate::{
    dtype::Dtype,
    normalizer::{Normifier, TableData},
};
use indexmap::IndexMap;
use polars::{
    frame::DataFrame,
    prelude::{DataType, NamedFrom, Series},
};

#[derive(Debug)]
pub struct DataBase {
    pub schemas: IndexMap<String, IndexMap<String, DataType>>,
    pub tables: IndexMap<String, DataFrame>,
}

impl DataBase {
    pub fn new() -> Self {
        DataBase {
            schemas: IndexMap::new(),
            tables: IndexMap::new(),
        }
    }

    pub fn build_database(&mut self, tbls: NormifyContext) {
        for (name, data) in tbls.tables {
            let (df, schema) = df_from_rows(name, data);
            self.tables.insert(name, df);
            self.schemas.insert(name, schema);
        }
    }
}

fn df_from_rows<T: From<Dtype>>(
    tbl_name: String,
    data: TableData,
) -> (DataFrame, IndexMap<String, DataType>) {
    let mut schema: IndexMap<String, DataType> = IndexMap::new();
    let mut cols: IndexMap<String, Vec<T>> = IndexMap::new();
    let series_vec: Vec<Series> = Vec::new();

    for row in data.records {
        for (field, ele) in row {
            cols.entry(field).or_insert_with(Vec::new).push(ele.into());
        }
    }
    for (name, data) in cols {
        let s: Series = Series::new(name.into(), data);
        schema.insert(name, s.dtype().clone());
        series_vec.push(s);
    }
    (df!(tbl_name => series_vec), schema)
}
