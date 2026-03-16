pub mod column {
    use crate::error::{NormError, Result};
    use crate::impl_insert;
    use crate::models::{NestedArray, NormArray};
    use derive_more::From;

    #[derive(From)]
    pub enum DataColumn {
        FloatColumn(NormArray<f64>),
        IntColumn(NormArray<i64>),
        UintColumn(NormArray<u64>),
        BoolColumn(NormArray<bool>),
        StringColumn(NormArray<String>),
        FloatListColumn(NestedArray<f64>),
        IntListColumn(NestedArray<i64>),
        UintListColumn(NestedArray<u64>),
        BoolListColumn(NestedArray<bool>),
        StringListColumn(NestedArray<String>),
    }

    impl DataColumn {
        // primitive inserts
        impl_insert!(insert_int, IntColumn, i64);
        impl_insert!(insert_uint, UintColumn, u64);
        impl_insert!(insert_float, FloatColumn, f64);
        impl_insert!(insert_bool, BoolColumn, bool);
        impl_insert!(insert_string, StringColumn, String);
        // list inserts
        impl_insert!(insert_float_list, FloatListColumn, NormArray<f64>);
        impl_insert!(insert_int_list, IntListColumn, NormArray<i64>);
        impl_insert!(insert_uint_list, UintListColumn, NormArray<u64>);
        impl_insert!(insert_bool_list, BoolListColumn, NormArray<bool>);
        impl_insert!(insert_string_list, StringListColumn, NormArray<String>);
    }
}

pub(crate) mod norm_array {
    use crate::models::{ColumnType, DataColumn, Item, NormType};
    pub struct NormArray<T: NormType + PartialEq> {
        items: Vec<Item<T>>,
    }

    impl<T: NormType + PartialEq> NormArray<T> {
        pub(crate) fn new() -> Self {
            Self {
                items: Vec::<Item<T>>::new(),
            }
        }

        pub(crate) fn from(item: Item<T>) -> Self {
            Self { items: vec![item] }
        }
        pub fn push(&mut self, item: Item<T>) {
            self.items.push(item)
        }

        pub fn contains(&self, item: &T) -> bool {
            self.items.contains(item)
        }

        pub fn len(&self) -> usize {
            self.items.len()
        }
    }

    impl<T: NormType + PartialEq> FromIterator<T> for NormArray<T> {
        fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
            let mut arr = NormArray::new();
            for item in iter {
                arr.push(item);
            }
            arr
        }
    }

    impl<T: NormType + PartialEq> IntoIterator for NormArray<T> {
        type Item = T;
        type IntoIter = std::vec::IntoIter<Self::Item>;

        fn into_iter(self) -> Self::IntoIter {
            self.items.into_iter()
        }
    }

    impl<T: NormType + PartialEq> ColumnType for NormArray<T>
    where
        DataColumn: From<NormArray<T>>,
    {
        fn into_enum(self) -> DataColumn {
            DataColumn::from(self)
        }
    }
}

pub(crate) mod id_array {
    use crate::models::ColumnType;
    use crate::models::{DataColumn, NormArray};
    use indexmap::IndexSet;
    pub struct IdColumn(IndexSet<usize>);

    impl IdColumn {
        pub(crate) fn new() -> Self {
            Self(IndexSet::new())
        }

        pub(crate) fn man_insert(&mut self, id: usize) -> bool {
            self.0.insert(id)
        }

        pub(crate) fn auto_insert(&mut self) -> usize {
            let next_id: usize = self.0.len() + 1;
            self.0.insert(next_id);
            next_id
        }
    }

    impl ColumnType for IdColumn {
        fn into_enum(self) -> DataColumn {
            let arr: NormArray<u64> = self.0.into_iter().map(|x| x as u64).collect();
            DataColumn::UintColumn(arr)
        }
    }
}

pub(crate) mod nested_array {
    use crate::models::{ColumnType, DataColumn, NormArray, NormType};
    pub struct NestedArray<T: NormType + PartialEq> {
        lists: Vec<NormArray<T>>,
    }

    impl<T: NormType + PartialEq> NestedArray<T> {
        pub(crate) fn new() -> Self {
            Self { lists: vec![] }
        }

        pub(crate) fn from(arr: NormArray<T>) -> Self {
            Self { lists: vec![arr] }
        }

        pub fn push(&mut self, list: NormArray<T>) {
            self.lists.push(list)
        }

        pub fn len(&self) -> usize {
            self.lists.len()
        }
    }

    impl<T: NormType + PartialEq> FromIterator<NormArray<T>> for NestedArray<T> {
        fn from_iter<I: IntoIterator<Item = NormArray<T>>>(iter: I) -> Self {
            let mut arr = NestedArray::new();
            for item in iter {
                arr.push(item);
            }
            arr
        }
    }

    impl<T: NormType + PartialEq> ColumnType for NestedArray<T>
    where
        DataColumn: From<NestedArray<T>>,
    {
        fn into_enum(self) -> DataColumn {
            DataColumn::from(self)
        }
    }
}
