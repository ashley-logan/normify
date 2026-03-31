use crate::models::{NestedArray, NormArray};

// aliases for norm arrays
pub type BoolColumn = NormArray<bool>;
pub type StringColumn = NormArray<String>;
pub type IntColumn = NormArray<i64>;
pub type UintColumn = NormArray<u64>;
pub type FloatColumn = NormArray<f64>;

// aliases for nested arrays
pub type BoolListColumn = NestedArray<bool>;
pub type StringListColumn = NestedArray<String>;
pub type IntListColumn = NestedArray<i64>;
pub type UintListColumn = NestedArray<u64>;
pub type FloatListColumn = NestedArray<f64>;
