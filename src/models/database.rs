use std::ops::Index;

use crate::error::Result;
use crate::helpers::normalize_arr;
use crate::models::{ColumnType, Item, Table, UnknownArray};
use indexmap::IndexMap;
use serde_json::{Map, Value};

type ObjectIter<'a> = &'a Map<String, Value>;

pub struct Database {
    tables: IndexMap<String, Table>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            tables: IndexMap::new(),
        }
    }

    pub fn contains_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    pub fn get_mut_table<'a>(&'a mut self, name: &str) -> Option<&'a mut Table> {
        self.tables.get_mut(name)
    }

    pub fn get_mut_table_or_create<'a>(&'a mut self, name: &str) -> &'a mut Table {
        self.tables
            .entry(name.to_string())
            .or_insert_with(Table::new)
    }

    pub fn get_mut_or_panic(&mut self, name: &str) -> &mut Table {
        self.tables.get_mut(name).unwrap()
    }

    pub fn add_table(&mut self, name: String) {
        self.tables.insert(name, Table::new());
    }

    pub fn replace_table(&mut self, name: String, table: Table) {
        self.tables.insert(name, table);
    }

    pub fn drop_table(&mut self, name: &String) -> bool {
        self.tables.swap_remove(name).is_some()
    }

    pub fn remove_null_cols(&mut self) {
        let mut empty_tbls: Vec<String> = vec![];
        for (name, tbl) in self.tables.iter_mut() {
            tbl.remove_unknown_cols();
            if tbl.num_data_cols() == 0 {
                empty_tbls.push(name.clone());
            }
        }
        for name in empty_tbls {
            self.tables.swap_remove(name.as_str());
        }
    }

    pub fn iter_tables(&self) -> indexmap::map::Iter<'_, String, Table> {
        self.tables.iter()
    }

    pub(crate) fn parse_obj(
        &mut self,
        table_name: &String,
        obj: ObjectIter,
        parent_id: Option<u64>,
        parent_tname: Option<&String>,
    ) -> Result<()> {
        println!("Parsing new Object");

        if !self.contains_table(table_name) {
            println!("Creating table {}", table_name);
            self.add_table(table_name.to_string());
        }
        let curr_table: &mut Table = self.get_mut_or_panic(table_name); // curr_table = mutable reference to Table: table_name

        let curr_id: u64 = curr_table.new_id(); // push auto generated id for new row and store in curr_id
        println!("New Row in {} table; ID={}", table_name, curr_id);

        if let (Some(pid), Some(pname)) = (parent_id, parent_tname) {
            // if curr_table is a child of Table=parent_name, then push parent id for foreign key column
            // foreign key column is created if needed
            curr_table.insert_fk(pname.to_string(), pid);
            println!("Foreign Key Column for {} appened FK_ID={}", pname, pid);
        }

        let _ = curr_table; // drop mutable reference to working Table

        for (k, v) in obj {
            match v {
                // match on Value variant
                Value::Bool(b) => {
                    println!("Column {} entry is bool {}", k, b.clone());
                    // Bool variant => push Data(bool)
                    self.get_mut_or_panic(table_name)
                        .append_item(k, Item::Data(*b))?;
                }
                Value::String(s) => {
                    println!("Column {} entry is string {}", k, s.clone());
                    // String variant => push Data(String)
                    self.get_mut_or_panic(table_name)
                        .append_item(k, Item::Data(s.clone()))?;
                }
                Value::Null => {
                    println!("Column {} entry is null", k);
                    // Null variant => push null
                    let col = self
                        .get_mut_or_panic(table_name)
                        .get_mut_or_insert(k, Box::new(UnknownArray::new()));
                    col.push_null();
                }
                Value::Number(n) => {
                    let curr_table = self.get_mut_or_panic(table_name);
                    // Number variant => try converting to (impl ItemTrait) number types
                    if let Some(i) = n.as_i64() {
                        println!("Column {} entry is int {}", k, i.clone());
                        // try i64
                        curr_table.append_item(k, Item::Data(i))?;
                    } else if let Some(u) = n.as_u64() {
                        println!("Column {} entry is uint {}", k, u.clone());
                        // try u64
                        curr_table.append_item(k, Item::Data(u))?;
                    } else if let Some(f) = n.as_f64() {
                        println!("Column {} entry is float {}", k, f.clone());
                        // fallback to f64
                        curr_table.append_item(k, Item::Data(f))?;
                    } else {
                        println!("Column {} entry is null", k);
                        // number must be > u64::MAX, for now raise error until implemented

                        curr_table.append_null(k)?;
                    }
                }
                Value::Array(arr) => {
                    println!("Column {} entry is array", k);
                    // Array variant => check inner Value variants

                    if arr.iter().any(Value::is_object) {
                        // array of objects implies a new child table
                        self.get_mut_or_panic(table_name).drop(k); // drops the column if it exists since this is not a column in curr_table
                        let child_name: String = format!("{}_table", k);

                        self.parse_object_array(&child_name, arr, Some(curr_id), Some(table_name))?;
                    } else {
                        // column is a ListArray
                        let n_arr: Box<dyn ColumnType> = normalize_arr(arr)?; // homogenize array and convert innner types to Item<T>

                        let curr_table: &mut Table = self.get_mut_or_panic(table_name);
                        // try downcasting as each array type after normalizing

                        if n_arr.is_unknown() {
                            // if array is empty type cannot be determined
                            let col: &mut Box<dyn ColumnType> =
                                curr_table.get_mut_or_insert(k, Box::new(UnknownArray::new()));
                            col.push_null(); // appropriate sub-array type is appended
                        } else if n_arr.is_bool_column() {
                            curr_table.append_list(k, *n_arr.into_bool_column().unwrap())?;
                        } else if n_arr.is_int_column() {
                            curr_table.append_list(k, *n_arr.into_int_column().unwrap())?;
                            // entry is NormArray<i64>
                        } else if n_arr.is_uint_column() {
                            // entry is NormArray<u64>
                            curr_table.append_list(k, *n_arr.into_uint_column().unwrap())?;
                        } else if n_arr.is_float_column() {
                            // entry is NormArray<f64>
                            curr_table.append_list(k, *n_arr.into_float_column().unwrap())?;
                        } else if n_arr.is_string_column() {
                            // entry is NormArray<String>
                            curr_table.append_list(k, *n_arr.into_string_column().unwrap())?;
                        }
                    }
                }
                Value::Object(child_obj) => {
                    println!("Column {} entry is Object", k);
                    // if the value is an object, this is a new table
                    // the current table has a one-to-one relationship with the new table
                    self.get_mut_or_panic(table_name).drop(k); // drops the column if it exists since this is not a column in curr_table

                    let child_name: String = format!("{}_table", k);
                    self.parse_obj(&child_name, child_obj, Some(curr_id), Some(table_name))?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn parse_object_array(
        &mut self,
        table_name: &String,
        arr: &Vec<Value>,
        parent_id: Option<u64>,
        parent_tname: Option<&String>,
    ) -> Result<()> {
        for v in arr {
            if let Some(obj) = v.as_object() {
                self.parse_obj(table_name, obj, parent_id, parent_tname)?;
            }
        }
        Ok(())
    }
}

impl Index<&str> for Database {
    type Output = Table;

    fn index(&self, index: &str) -> &Self::Output {
        &self.tables[index]
    }
}
