use crate::dtype::{NormValue, NormArray, NormType, DataColumn, Column};
use crate::error::{NormError, Result};
use indexmap::{IndexMap, map::Iter};
use serde_json::{Map, Value};
use uuid::Uuid;

#[derive(Debug)]
pub struct TableData {
    pub(crate) columns: IndexMap<String, Vec<NormValue>>,
}

pub struct Table {
    name: String,
    columns: IndexMap<String, DataColumn>
}

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
    pub(crate) tables: Vec<Table>,
    pub(crate) relationships: Vec<Relationship>
}

impl Table {
    fn new() -> Self {
        Self {
            name: String::new(),
            columns: IndexMap::new()
        }
    }

    fn add(&mut self, field: String, col: DataColumn) {
        self.columns.insert(field, col);
    }

    pub fn insert<T: NormType>(&mut self, field: String, item: T) -> Result<()> {
        self.columns.entry(field).or_insert(NormArray::new::<T>()).push(item);
        for c in &mut self.columns {
            if c.get_field().to_owned() == field {
                match item {
                    NormValue::Float(f) => c.data.insert_float(f.clone())?,
                    NormValue::Bool(b) => c.data.insert_bool(b.clone())?,
                    NormValue::Int(i) => c.data.insert_int(i.clone())?,
                    NormValue::UInt(u) => c.data.insert_uint(u.clone())?,
                    NormValue::String(s) => c.data.insert_string(s.clone())?,
                    NormValue::Null => ()
                };
                return Ok(())
            };
        }
        self.columns.push()
        
    }
}

#[derive(Debug)]
pub struct Normifier {
    pub(crate) tables: IndexMap<String, TableData>,
    // relations: Vec<Relationship>,
}

impl DataBase {
    fn new() -> Self {
        Self {
            tables: vec![],
            relationships: vec![]
        }
    }

    fn add(&mut self, table: Table, relation: Option<Relationship>) {
        self.tables.push(table);
        if let Some(r) = relation {
            self.relationships.push(r);
        }
    }

    fn parse_object(
        &mut self,
        this_table: &Table,
        obj: &Map<String, Value>,
        parent_table: Option<&Table>,
        parent_id: Option<&String>,
        pt_name: Option<&String>,
    ) -> Result<()> {
        // TODO log table name
        // creates a new index map to hold a row of data
        let mut this_record: IndexMap<String, NormValue> = IndexMap::new();
        // creates a new random id for this row
        let this_id = Uuid::now_v7().to_string();
        this_table.columns.entry("id".to_string()).or_insert(DataColumn::StringColumn(NormArray::new())).insert_string(this_id.clone());

        if let (Some(pname), Some(pid)) = (pt_name, parent_id) {
            // if the table this row belongs to has a parent table, insert the parent id as a foreign key
            this_table.columns.entry(format!("{}_id", pname)).or_insert(DataColumn::StringColumn(NormArray::new())).insert_string(pid.clone());
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
                Value::Bool(b) => {this_table.columns.entry(k.to_string()).or_insert(DataColumn::BoolColumn(NormArray::new())).insert_bool(b.clone());}
                Value::String(s) => {this_table.columns.entry(k.to_string()).or_insert(DataColumn::StringColumn(NormArray::new())).insert_string(s.clone());}
                // Value::Null => {this_table.columns.entry(k.to_string()).or_insert(DataColumn::BoolColumn(NormArray::new())).insert_bool(b.clone());}
                Value::Number(n) => {
                    if let Some(c) = this_table.columns.get_mut(k) {
                        match c {
                            DataColumn::FloatColumn(_) => {c.insert_float(n.as_f64()?)}
                            DataColumn::UintColumn(_) => {c.insert_uint(n.as_u64()?)}
                            _ => {c.insert_int(n.as_i64()?)}
                        }
                    } else if let Some(f)  = n.as_f64() {
                        this_table.columns.insert(k.to_string(), DataColumn::FloatColumn(NormArray{vec![f]}))
                    } else if let Some(i) = n.as_i64() {
                        this_table.columns.insert(k.to_string(), DataColumn::IntColumn(NormArray{vec![i]}))
                    } else if let Some(u) = n.as_u64() {
                        this_table.columns.insert(k.to_string(), DataColumn::UintColumn(NormArray{vec![u]}))
                    }
                        
                }
                    
                   
            }
        }
        // transform and add the row container to the current table
        self.add_record(t_name.to_owned(), this_record);
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
