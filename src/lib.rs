// mod database_builder;
mod error;
mod helpers;
mod models;
mod normalizer;
pub use helpers::normalize_arr;
pub use normalizer::Normifier;
pub use serde_json::{Map, Value};

pub use crate::error::{NormError, Result};
pub use crate::models::{
    ColumnType, Database, IdColumn, Item, ItemTrait, NormArray, Table, type_aliases::*,
};
