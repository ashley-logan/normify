use crate::{
    dtype::Dtype,
    normalizer::{Normifier, TableData},
};
use indexmap::IndexMap;
use polars::{frame::DataFrame, prelude::*, series::IntoSeries};

#[derive(Debug)]
pub struct DataBase {
    pub schemas: IndexMap<String, Schema>,
    pub tables: Vec<DataFrame>,
}

impl DataBase {
    pub fn new() -> Self {
        DataBase {
            schemas: IndexMap::new(),
            tables: Vec::new(),
        }
    }

    pub fn stringify_collection(collection: Vec<Dtype>) -> Series {
        Series::from_iter(collection.into_iter().map(|x| {
            if x.is_null() {
                None
            } else {
                Some(x.to_string())
            }
        }))
    }

    pub fn stringify_nested_collection(name: String, n_collection: Vec<Vec<Dtype>>) -> Series {
        // todo: max subarray size\
        let values_cap: usize = n_collection
            .iter()
            .map(|x| x.len())
            .max()
            .unwrap_or_default();
        let mut s_builder: ListStringChunkedBuilder =
            ListStringChunkedBuilder::new(name.into(), n_collection.len(), values_cap);
        for inner_array in n_collection {
            s_builder
                .append_series(&(Self::stringify_collection(inner_array)))
                .expect("failed appending series");
        }
        s_builder.finish().into_series()
    }
    pub fn is_normal_collection(determinant: &Dtype, collection: &[Dtype]) -> bool {
        use Dtype as DT;
        match determinant {
            DT::Bool(_) => collection.iter().all(|x| x.is_bool() || x.is_null()),
            DT::UInt(_) => collection.iter().all(|x| x.is_uint() || x.is_null()),
            DT::Int(_) => collection.iter().all(|x| x.is_int() || x.is_null()),
            DT::Float(_) => collection.iter().all(|x| x.is_float() || x.is_null()),
            DT::String(_) => collection.iter().all(|x| x.is_string() || x.is_null()),
            _ => panic!("Cannot call is_normal_collection on a nested structure"),
        }
    }

    pub fn collection_to_series(determinant: &Dtype, collection: Vec<Dtype>) -> Series {
        use Dtype as DT;
        match determinant {
            DT::Bool(_) => collection
                .into_iter()
                .map(|x| if let DT::Bool(b) = x { Some(b) } else { None })
                .collect(),
            DT::UInt(_) => collection
                .into_iter()
                .map(|x| if let DT::UInt(u) = x { Some(u) } else { None })
                .collect(),
            DT::Int(_) => collection
                .into_iter()
                .map(|x| if let DT::Int(i) = x { Some(i) } else { None })
                .collect(),
            DT::Float(_) => collection
                .into_iter()
                .map(|x| if let DT::Float(f) = x { Some(f) } else { None })
                .collect(),
            DT::String(_) => collection
                .into_iter()
                .map(|x| if let DT::String(s) = x { Some(s) } else { None })
                .collect(),
            DT::Array(_) => {
                panic!("cannot parse a nested collection")
            }
            DT::Null => {
                panic!("collection should not contain all null values")
            }
        }
    }

    fn build_list_chunked(
        data: Vec<Series>,
        builder: &mut Box<dyn ListBuilderTrait>,
    ) -> ListChunked {
        for s in data {
            builder.append_series(&s).expect("could not build column");
        }
        builder.finish()
    }

    fn get_list_builder(determinant: &Dtype, data: &[Vec<Dtype>]) -> Box<dyn ListBuilderTrait> {
        // returns a new list_builder according to columns capacity and the list's inner type and capacity
        let capacity = data.len();
        let values_capacity = data.iter().map(|x| x.len()).max().unwrap_or_default();
        match determinant {
            &Dtype::Array(_) => panic!("cannot call get_list_builder on array type"),
            &Dtype::Null => panic!("cannot call get_list_builder on null type"),
            &Dtype::String(_) => Box::new(ListStringChunkedBuilder::new(
                PlSmallStr::EMPTY,
                capacity,
                values_capacity,
            )),
            &Dtype::Bool(_) => Box::new(ListBooleanChunkedBuilder::new(
                PlSmallStr::EMPTY,
                capacity,
                values_capacity,
            )),
            &Dtype::UInt(_) => Box::new(ListPrimitiveChunkedBuilder::<UInt64Type>::new(
                PlSmallStr::EMPTY,
                capacity,
                values_capacity,
                DataType::UInt64,
            )),
            &Dtype::Int(_) => Box::new(ListPrimitiveChunkedBuilder::<Int64Type>::new(
                PlSmallStr::EMPTY,
                capacity,
                values_capacity,
                DataType::Int64,
            )),
            &Dtype::Float(_) => Box::new(ListPrimitiveChunkedBuilder::<Float64Type>::new(
                PlSmallStr::EMPTY,
                capacity,
                values_capacity,
                DataType::Float64,
            )),
        }
    }
    /* idea: instead of Dtype enums, create structs that have a shared trait with
        custom implemention to allow easy unwrapping, processing, and conversion
    */

    // unwrap a vector of Dtype::Array variants into vectors
    fn unwrap_nested(nested: Vec<Dtype>) -> Vec<Vec<Dtype>> {
        if !Dtype::array_is_type(&nested, Dtype::is_array) {
            // checks that every element is either an array variant or null variant
            panic!("cannot pass flat vector to unwrap_nested")
        }
        let mut unnested = vec![];
        for sub_array in nested {
            if let Dtype::Array(a) = sub_array {
                unnested.push(a);
            } else {
                unnested.push(vec![]);
            }
        }
        unnested
    }
    pub fn build_series(name: String, data: Vec<Dtype>) -> Series {
        // todo: handle case where some elements are Uints and other are Ints
        // let the first non-null value in the vector determine the target type for the series
        println!("parsing column {}", name);
        let determining_element: Dtype = data.iter().find(|&x| !x.is_null()).unwrap().clone();
        let normal: bool;
        if matches!(determining_element, Dtype::Array(_)) {
            // if data is an vector of array types, find the first non-null element within the flattened data
            let unnested_data: Vec<Vec<Dtype>> = Self::unwrap_nested(data);
            let array_determinant: Dtype = unnested_data
                .iter()
                .flatten()
                .find(|&x| !x.is_null())
                .unwrap()
                .clone();

            normal = unnested_data
                .iter()
                .all(|x| Self::is_normal_collection(&array_determinant, x));

            if normal {
                println!("column: {} is nested and already normal", name);
                let mut list_builder = Self::get_list_builder(&array_determinant, &unnested_data);
                let mut series_vec: Vec<Series> = vec![];
                for sub_array in unnested_data.into_iter() {
                    // cast each subarray to a series
                    series_vec.push(Self::collection_to_series(&array_determinant, sub_array));
                }
                Self::build_list_chunked(series_vec, &mut list_builder).into_series()
            } else {
                println!("column: {} is nested and is not normal", name);
                // if nested array column is not normal,
                // convert each element to a string
                Self::stringify_nested_collection(name, unnested_data)
            }
        } else {
            println!("column: {} is flat and already normal", name);
            // if the target type is a Dtype-primitive, check that all elements are of that type as well
            normal = Self::is_normal_collection(&determining_element, data.as_slice());
            if normal {
                // if data is already normal, cast to a series
                Self::collection_to_series(&determining_element, data)
            } else {
                println!("column: {} is nested and not normal", name);
                // if data is not normal, represent all data as strings and cast to a series
                Self::stringify_collection(data)
            }
        }
    }

    pub fn build_df(&mut self, name: String, data: TableData) {
        // builds a dataframe from a TableData struct
        println!("creating df {}", name);
        let mut df_data: Vec<Column> = vec![];
        for (field, data) in data.columns.into_iter() {
            let s: Series = DataBase::build_series(field.clone(), data);
            let mut c: Column = s.into_column();
            c.rename(field.into());
            df_data.push(c);
        }
        let df_result = DataFrame::new(df_data);
        if let Ok(df) = df_result {
            self.tables.push(df);
        } else {
            println!("{:?}", df_result);
        }
    }

    pub fn from_norm(norm: Normifier) -> Self {
        // creates a DataBase struct from a populated Normifier
        let mut this_db: DataBase = DataBase::new();
        for (name, data) in norm.tables.into_iter() {
            this_db.build_df(name, data);
        }
        this_db
    }
}
