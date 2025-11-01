use crate::{
    dtype::Dtype,
    normalizer::{Normifier, TableData},
};
use derive_more::TryIntoError
use indexmap::IndexMap;
use polars::{
    frame::DataFrame,
    prelude::{DataType, Schema, Series},
};

#[derive(Debug)]
pub struct DataBase {
    pub schemas: IndexMap<String, Schema>,
    pub tables: IndexMap<String, DataFrame>,
}

impl DataBase {
    pub fn new() -> Self {
        DataBase {
            schemas: IndexMap::new(),
            tables: IndexMap::new(),
        }
    }

    fn stringify_column(col: Vec<Dtype>) -> Vec<Option<Vec<Option<String>>>> {
        let mut new_col: Vec<Option<Vec<Option<String>>>> = Vec::new();
        for arr in col {
            let new_arr: Option<Vec<Option<String>>>;
            new_arr = {
                if let Some(vec) = arr.into_vec() {
                    Some(vec.into_iter().map(|i| {
                                            if !i.is_null() {Some(i.to_string())}
                                            else {None}
                                        }).collect::<Vec<Option<String>>>())
                } else {
                    None
                }
            };
            new_col.push(new_arr);
        }
        new_col
    }

    fn array_is_normal(col: &mut Vec<Vec<Dtype>>) -> bool {
        use Dtype as D;
        if let Some(first_arr) = col.iter().find(|&v| {
            !v.is_empty() && v.iter().any(|i| !i.is_null())
        }) { 
            let first_ele = first_arr.iter().find(|&i| !i.is_null()).unwrap();
            return match first_ele {
                D::Bool(_) => col.iter().all(|v| D::array_is_type(v, D::is_bool)),
                D::String(_) => col.iter().all(|v| D::array_is_type(v, D::is_string)),
                D::UInt(_) => col.iter().all(|v| D::array_is_type(v, D::is_uint)),
                D::Int(_) => col.iter().all(|v| D::array_is_type(v, D::is_int)),
                D::Float(_) => col.iter().all(|v| D::array_is_type(v, D::is_float)),
                _ => false
            };

        } else {true} 
        
    }


    fn transform_column(col: Vec<Dtype>) {
        use Dtype as D;
        match col {
            c if D::array_is_type(c, D::is_bool) => { D::cast_array_as::<bool>(c);}
            c if D::array_is_type(c, D::is_uint) => { D::cast_array_as::<u64>(c);}
            c if D::array_is_type(c, D::is_int) => { D::cast_array_as::<i64>(c);}
            c if D::array_is_type(c, D::is_float) => { D::cast_array_as::<f64>(c);}
            c if D::array_is_type(c, D::is_string) => { D::cast_array_as::<String>(c);}
            c if D::array_is_type(c, D::is_array) => { 
                c.into_iter().map(Self::transform_column);
                }
                /* for array
                    Step 1: check that each array has a consistent internal type
                    Step 2: if false, convert each element in each array to a string representation,
                        otherwise, store that type for later comparison
                    Step 3: check that the stored types match 
                    Step 4: if false, convert each element in each array to a string representation
                */




        }

    }
    pub fn build_series(column: Vec<Dtype>) -> Series {
        let s: Series = column.into_iter().map
    }

    pub fn build_df(&mut self, tbl: &TableData) {
        todo!();
    }

    pub fn from_norm(norm: Normifier) -> Self {
        let mut this_db: DataBase = DataBase::new();
        for tbl in norm.iter_tables() {
            this_db.build_df(tbl);
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
