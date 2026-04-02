// mod database_builder;
pub mod error;
mod helpers;
mod models;
mod normalizer;
pub use helpers::normalize_arr;
pub use normalizer::Normifier;
pub use serde_json::{Map, Value};

pub use models::{ColumnType, Database, IdColumn, Item, ItemTrait, NormArray, Table, type_aliases};
