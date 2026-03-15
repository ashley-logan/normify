use indexmap::{IndexMap, map::Iter};
use serde_json::{Map, Value};
use crate::normalizer::Normifier;
use crate::error::Result;
use crate::dtype::{NormValue, NormArray, NormType};


pub(crate) trait PrimitiveType {
    type RustPrimitive;
}

pub(crate) struct FloatMarker;
pub(crate) struct IntMarker;
pub(crate) struct UintMarker;
pub(crate) struct BoolMarker;
pub(crate) struct StringMarker;

pub(crate) struct NullType;

impl PrimitiveType for FloatMarker {
    type RustPrimitive = f64;
}

impl PrimitiveType for IntMarker {
    type RustPrimitive = i64;
}

impl PrimitiveType for UintMarker {
    type RustPrimitive = u64;
}

impl PrimitiveType for BoolMarker {
    type RustPrimitive = bool;
}

impl PrimitiveType for StringMarker {
    type RustPrimitive = String;
}



pub(crate) fn parse_object(
        records: &mut Normifier,
        t_name: &String,
        obj: &Map<String, Value>,
        p_id: Option<&String>,
        pt_name: Option<&String>,
    ) -> Result<()> {
        // TODO log table name
        // creates a new index map to hold a row of data
        let mut this_record: IndexMap<String, NormValue> = IndexMap::new();
        // creates a new random id for this row
        let this_id = Uuid::now_v7().to_string();
        // this_table.extend_column("id".to_string(), this_id.clone().into());
        this_record.insert("id".to_string(), this_id.clone().into());

        if let (Some(pname), Some(pid)) = (pt_name, p_id) {
            // if the table this row belongs to has a parent table, insert the parent id as a foreign key
            this_record.insert(format!("{}_id", pname), pid.to_owned().into());
        }

        for (k, v) in obj {
            // iterate through each property and its value
            match v {
                Value::Array(arr) => {
                    if arr.is_empty() {
                        this_record.insert(k.to_string(), NormValue::Array(vec![]));
                    }
                    // if the value is an array, this signifies the possible creation of a new table,
                    // where the current table has a one-to-many relationship with the new table
                    else if arr.iter().all(Value::is_object) {
                        // if every item is an object, this value becomes a new table
                        // new table name created from property name
                        let child_table: String = format!("{}_table", k);
                        self.parse_object_array(&child_table, arr, Some(t_name), Some(&this_id))?
                    } else {
                        // if the array is an array of json primitives, normalize the array and insert in row
                        todo!("normalize the array")
                        this_record.insert(k.to_string(), NormValue::from_value(v.to_owned())?);
                    }
                }
                Value::Object(child) => {
                    // if the value is an object, this is a new table
                    // the current table has a one-to-one relationship with the new table
                    let new_tname: String = format!("{}_table", k);
                    self.parse_object(&new_tname, child, Some(&this_id), Some(t_name))?;
                }
                _ => {
                    // if the type if non-nested, just insert it into the row container
                    this_record.insert(k.to_string(), NormValue::from_value(v.to_owned())?);
                }
            }
        }
        // transform and add the row container to the current table
        self.add_record(t_name.to_owned(), this_record);
        Ok(())
    }