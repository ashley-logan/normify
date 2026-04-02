use crate::{
    NormArray,
    error::*,
    impl_concrete_cast,
    models::{ColumnType, ItemTrait, UnknownArray, type_aliases::*},
};
use std::{any::Any, fmt::Write};

#[derive(Clone)]
pub struct NestedArray<T: ItemTrait> {
    sub_arrays: Vec<NormArray<T>>,
}

impl<T: ItemTrait + 'static> ColumnType for NestedArray<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn into_string_super(self: Box<Self>) -> Box<NormArray<String>> {
        let mut arr: NormArray<String> = NormArray::new();
        let mut buf = String::new();
        for sub in self.into_iter() {
            buf.clear();
            sub.write_list_fmt(None, &mut buf);
            arr.push_prim(buf.clone());
        }
        Box::new(arr)
    }

    fn write_col_fmt(&self, limit: Option<usize>, buf: &mut dyn Write) {
        let mut lim: usize = limit.unwrap_or(self.len());
        if lim > self.len() {
            lim = self.len();
        }
        writeln!(buf, "").unwrap();
        for i in 0..lim {
            self.sub_arrays[i].write_list_fmt(None, buf);
            writeln!(buf, "").unwrap();
        }
        writeln!(buf, "").unwrap();
    }

    fn write_list_fmt(&self, limit: Option<usize>, buf: &mut dyn Write) {
        let mut lim: usize = limit.unwrap_or(self.len());
        if lim > self.len() {
            lim = self.len();
        }
        writeln!(buf, "[").unwrap();
        for i in 0..lim {
            write!(buf, "\t").unwrap();
            self.sub_arrays[i].write_list_fmt(None, buf);
            writeln!(buf, "").unwrap();
        }
        writeln!(buf, "]").unwrap();
    }

    fn len(&self) -> usize {
        self.sub_arrays.len()
    }

    fn count_data(&self) -> usize {
        self.sub_arrays
            .iter()
            .filter(|&x| x.count_data() > 0)
            .count()
    }

    fn count_nulls(&self) -> usize {
        self.sub_arrays
            .iter()
            .filter(|&x| x.count_data() == 0)
            .count()
    }
    fn is_nested(&self) -> bool {
        true
    }

    fn push_null(&mut self) {
        self.sub_arrays.push(NormArray::<T>::new());
    }

    impl_concrete_cast!(
        as_bool_list_column,
        into_bool_list_column,
        is_bool_list_column,
        BoolListColumn
    );
    impl_concrete_cast!(
        as_string_list_column,
        into_string_list_column,
        is_string_list_column,
        StringListColumn
    );
    impl_concrete_cast!(
        as_int_list_column,
        into_int_list_column,
        is_int_list_column,
        IntListColumn
    );
    impl_concrete_cast!(
        as_uint_list_column,
        into_uint_list_column,
        is_uint_list_column,
        UintListColumn
    );
    impl_concrete_cast!(
        as_float_list_column,
        into_float_list_column,
        is_float_list_column,
        FloatListColumn
    );
}

// impl<T: ItemTrait> From<NormArray<T>> for NestedArray<T> {
//     fn from(value: NormArray<T>) -> Self {
//         Self {
//             sub_arrays: vec![value],
//         }
//     }
// }

impl<T: ItemTrait> From<UnknownArray> for NestedArray<T> {
    fn from(value: UnknownArray) -> Self {
        let mut arr: NestedArray<T> = NestedArray::new();
        for _ in 0..value.count_nulls() {
            arr.push_empty();
        }
        arr
    }
}

impl<T: ItemTrait> NestedArray<T> {
    pub fn new() -> Self {
        Self {
            sub_arrays: Vec::<NormArray<T>>::new(),
        }
    }

    pub fn new_with_nulls(empty_count: usize) -> Self {
        let mut arr = Self::new();
        for _ in 0..empty_count {
            arr.push_empty();
        }
        arr
    }

    pub fn from_arr(arr: NormArray<T>) -> Self {
        Self {
            sub_arrays: vec![arr],
        }
    }

    pub fn push_arr(&mut self, arr: NormArray<T>) {
        self.sub_arrays.push(arr)
    }

    pub fn push_empty(&mut self) {
        self.sub_arrays.push(NormArray::<T>::new());
    }

    pub fn try_downsize(arr: NestedArray<T::Fallback>) -> Result<NestedArray<T>> {
        arr.into_iter().map(NormArray::<T>::try_downsize).collect()
    }
}

impl<T: ItemTrait> std::ops::Index<usize> for NestedArray<T> {
    type Output = NormArray<T>;
    fn index(&self, index: usize) -> &Self::Output {
        &self.sub_arrays[index]
    }
}

impl<T: ItemTrait> std::ops::IndexMut<usize> for NestedArray<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.sub_arrays[index]
    }
}

impl<T: ItemTrait> IntoIterator for NestedArray<T> {
    type Item = NormArray<T>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.sub_arrays.into_iter()
    }
}

impl<T: ItemTrait> FromIterator<NormArray<T>> for NestedArray<T> {
    fn from_iter<I: IntoIterator<Item = NormArray<T>>>(iter: I) -> Self {
        let mut arr = Self::new();
        for sub in iter {
            arr.push_arr(sub);
        }
        arr
    }
}
