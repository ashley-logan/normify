use crate::models::{NestedArray, NormArray};

// aliases for norm arrays
pub type BoolColumn = NormArray<bool>;
pub type StringColumn = NormArray<String>;
pub type IntColumn = NormArray<i64>;
pub type UintColumn = NormArray<u64>;
pub type FloatColumn = NormArray<f64>;

// aliases for nested arrays
pub type BoolListColumn = NestedArray<BoolColumn>;
pub type StringListColumn = NestedArray<StringColumn>;
pub type IntListColumn = NestedArray<IntColumn>;
pub type UintListColumn = NestedArray<UintColumn>;
pub type FloatListColumn = NestedArray<FloatColumn>;
