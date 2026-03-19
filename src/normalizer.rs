use crate::error::{NormError, Result};
use crate::helpers::flatten_vec;
use indexmap::{IndexMap, map::Iter};
use serde_json::{Map, Value};
use uuid::Uuid;
use crate::models::{NormArray, NormType, DataColumn, NestedArray, IdColumn, Table, NullMarker};



enum Type {
    OneToMany,
    OneToOne,
    ManyToOne,
    ManyToMany,
}
pub struct Relationship {
    from: String,
    to: String,
    variant: Type, 
}

pub struct DataBase {
    pub(crate) tables: IndexMap<String, Table>,
    pub(crate) relationships: Vec<Relationship>
}




impl DataBase {
    fn new() -> Self {
        Self {
            tables: IndexMap::new(),
            relationships: vec![]
        }
    }

    fn push_table(&mut self, name: String, table: Table, relation: Option<Relationship>) {
        self.tables.insert(name, table);
        if let Some(r) = relation {
            self.relationships.push(r);
        }
    }

    fn parse_primitive_array(this_table: &mut Table, arr: &Vec<Value>, k: &String) {
        if arr.is_empty() {
            todo!("insert null since could field could be foreign key or list columns")
        }


        let mut flat_arr: Vec<Value> = vec![];
        flatten_vec(arr, &mut flat_arr); // flatten array to only contain json primitives

        flat_arr.retain(|x| !x.is_null()); // remove nulls


        if flat_arr.iter().any(|x| matches!(x, Value::String(_))) {
            let norm_arr: NormArray<String> = flat_arr.iter().map(|x| x.to_string()).collect();
            this_table.columns.entry(k.to_string()).or_insert(DataColumn::StringListColumn(NestedArray::new())).insert_string_list(norm_arr);
        } else if flat_arr.iter().all(|x| matches!(x, Value::Bool(_))) {
            let norm_arr: NormArray<bool> = flat_arr.iter().map(|x| x.as_bool().unwrap()).collect();
            this_table.columns.entry(k.to_string()).or_insert(DataColumn::BoolListColumn(NestedArray::new())).insert_bool_list(norm_arr);
        } else if flat_arr.iter().all(|x| matches!(x.as_i64(), Some(i))) {
            let norm_arr: NormArray<i64> = flat_arr.iter().map(|x| x.as_i64().unwrap()).collect();
            this_table.columns.entry(k.to_string()).or_insert(DataColumn::IntListColumn(NestedArray::new())).int_push_list(norm_arr);
        } else if flat_arr.iter().all(|x| matches!(x.as_f64(), Some(f))) {
            let norm_arr: NormArray<f64> = flat_arr.iter().map(|x| x.as_f64().unwrap()).collect();
            this_table.columns.entry(k.to_string()).or_insert(DataColumn::FloatListColumn(NestedArray::new())).float_push_list(norm_arr);

        }
    }


    fn parse_object(
        &mut self,
        this_name: &String,
        this_table: &mut Table,
        obj: &Map<String, Value>,
        parent_table: Option<&Table>,
        parent_id: Option<usize>,
        parent_name: Option<&String>,
    ) -> Result<()> {
        // TODO log table name
        // creates a new random id for this row
        let this_id: u64 = this_table.new_id(); // adds a new id to the table's id column and returns it



        if let (Some(pname), Some(pid)) = (parent_name, parent_id) {
            // if this table has a parent table, create the foreign key column if it doesn't exist then add the parent id
            this_table.fk_columns.entry(pname.to_string()).or_insert(IdColumn::new()).man_insert(pid.clone().into());
        }

        for (k, v) in obj {
            // iterate through each property and its value
            match v {
                Value::Array(arr) => {
                    if arr.is_empty() {
                        todo!("insert null since could field could be foreign key or list columns")
                    } 
                    // if the value is an array, this signifies the possible creation of a new table,
                    // where the current table has a one-to-many relationship with the new table
                    else if arr.iter().all(Value::is_object) {
                        // if every item is an object, this value becomes a new table
                        // new table name created from property name
                        let child_name: String = format!("{}_table", k);
                        let mut child_table: &mut Table = self.tables.entry(child_name).or_insert(Table::new());

                        for obj in arr {
                            self.parse_object(&child_name, &mut child_table, obj.as_object().unwrap(), Some(&this_table), Some(&this_id), Some(this_name));
                        }
                    } else {
                        // if the array is an array of json primitives, normalize the array and insert in row
                        Self::parse_primitive_array(this_table, arr, k);
                    }
                    // this_table.columns.insert(k.to_string(), DataColumn::FloatColumn(NormArray::from(vec![f])));

                }
                Value::Object(obj) => {
                    // if the value is an object, this is a new table
                    // the current table has a one-to-one relationship with the new table
                    let child_name: String = format!("{}_table", k);
                    let mut child_table: &mut Table = self.tables.entry(child_name).or_insert(Table::new());

                    self.parse_object(&child_name, &mut child_table, obj, Some(&this_table), Some(&this_id), Some(this_name));
                }
                Value::Bool(b) => {this_table.columns.entry(k.to_string()).or_insert(DataColumn::BoolColumn(NormArray::new())).insert_bool(b.clone());}
                Value::String(s) => {this_table.columns.entry(k.to_string()).or_insert(DataColumn::StringColumn(NormArray::new())).insert_string(s.clone());}
                // Value::Null => {this_table.columns.entry(k.to_string()).or_insert(DataColumn::BoolColumn(NormArray::new())).insert_bool(b.clone());}
                Value::Number(n) => {
                    if let Some(c) = this_table.columns.get_mut(k) {
                        match c {
                            DataColumn::FloatColumn(_) => {c.float_push(n.as_f64().ok_or(NormError::Convert)?);}
                            DataColumn::UintColumn(_) => {c.uint_push(n.as_u64().ok_or(NormError::Convert)?);}
                            _ => {c.int_push(n.as_i64().ok_or(NormError::Convert)?);}
                        };
                    } else if let Some(f)  = n.as_f64() {
                        this_table.columns.insert(k.to_string(), DataColumn::FloatColumn(NormArray::from(vec![f])));
                    } else if let Some(i) = n.as_i64() {
                        this_table.columns.insert(k.to_string(), DataColumn::IntColumn(NormArray::from(vec![i])));
                    } else if let Some(u) = n.as_u64() {
                        this_table.columns.insert(k.to_string(), DataColumn::UintColumn(NormArray::from(vec![u])));
                    } 
                }
                Value::Null => {
                    if let Some(c) = this_table.columns.get_mut(k) {
                        c.null_push();
                    } else {
                        this_table.columns.insert(k.to_string(), DataColumn::UnknownColumn(vec![NullMarker]));
                    }
    
                }
                    
                   
            }
        }
        // transform and add the row container to the current table
        Ok(())
    }

}



impl TableData {
    fn new() -> Self {
        Self {
            columns: IndexMap::new(),
        }
    }
    fn extend_column(&mut self, col_name: String, col_data: NormValue) {
        // pushes a value into its appropriate column vector or creates a new vector
        self.columns
            .entry(col_name)
            .or_insert_with(Vec::new)
            .push(col_data);
    }

    pub fn iter_columns<'a>(&'a self) -> Iter<'a, String, Vec<NormValue>> {
        self.columns.iter()
    }

    pub fn iter_items<'a>(
        &'a self,
    ) -> std::iter::Flatten<indexmap::map::Values<'a, String, Vec<NormValue>>> {
        self.columns.values().flatten()
    }

    fn clean_nulls(&mut self) {
        // removes empty columns and columns that contain exclusively null values
        self.columns.retain(|_, v| v.iter().any(|d| !d.is_null()));
    }
}

impl Normifier {
    pub fn new() -> Self {
        Self {
            tables: IndexMap::new(),
        }
    }

    pub fn add_record(&mut self, table_name: String, record: IndexMap<String, NormValue>) {
        // inserts a row of data into its corresponding table
        let table: &mut TableData = self.tables.entry(table_name).or_insert_with(TableData::new);
        for (field, data) in record {
            table.extend_column(field, data);
        }
    }
    pub fn iter_tables<'a>(&'a self) -> Iter<'a, String, TableData> {
        self.tables.iter()
    }

    pub(crate) fn parse_object(
        &mut self,
        t_name: &String,
        obj: &Map<String, Value>,
        p_id: Option<&String>,
        pt_name: Option<&String>,
    ) -> Result<(), NormError> {
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

    pub(crate) fn parse_object_array(
        &mut self,
        t_name: &String,
        arr: &Vec<Value>,
        p_name: Option<&String>,
        row_id: Option<&String>,
    ) -> Result<(), NormError> {
        for obj in arr {
            // parse each object in the array
            self.parse_object(t_name, obj.as_object().unwrap(), row_id, p_name)?;
        }
        Ok(())
    }

    pub(crate) fn process_root(
        &mut self,
        root_value: Value,
        root_name: String,
    ) -> Result<(), NormError> {
        match root_value {
            Value::Object(root_obj) => {
                self.parse_object(&root_name, &root_obj, None, None)?;
                Ok(())
            }
            Value::Array(arr) => {
                self.parse_object_array(&root_name, &arr, None, None)?;
                Ok(())
            }
            _ => Err(NormError::Parse),
        }
    }

    pub(crate) fn clean_normifier(&mut self) {
        for (_, table) in &mut self.tables {
            table.clean_nulls();
        }
    }
}
