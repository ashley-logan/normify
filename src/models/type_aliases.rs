use crate::models::{ArrayTrait, Item, ItemTrait, ListArray, NormArray};

// aliases for norm arrays
pub type BoolColumn = NormArray<bool>;
pub type StringColumn = NormArray<String>;
pub type IntColumn = NormArray<i64>;
pub type UintColumn = NormArray<u64>;
pub type FloatColumn = NormArray<f64>;

// aliases for nested arrays
pub type BoolListColumn = ListArray<BoolColumn>;
pub type StringListColumn = ListArray<StringColumn>;
pub type IntListColumn = ListArray<IntColumn>;
pub type UintListColumn = ListArray<UintColumn>;
pub type FloatListColumn = ListArray<FloatColumn>;
