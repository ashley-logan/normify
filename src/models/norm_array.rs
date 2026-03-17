use crate::models::{ArrayTrait, NormType};
use crate::models::{Item, NullMarker};

macro_rules! impl_arraytrait_normarray {
    (
        $ty:ty
    ) => {
        impl ArrayTrait for NormArray<$ty> {
            type Inner = $ty;
            fn add_item(&mut self, item: Item<$ty>) {
                self.items.push(item);
            }
        }
    };
}

pub struct NormArray<T: NormType> {
    items: Vec<Item<T>>,
}

impl_arraytrait_normarray!(f64);
impl_arraytrait_normarray!(i64);
impl_arraytrait_normarray!(u64);
impl_arraytrait_normarray!(bool);
impl_arraytrait_normarray!(String);

impl<T: NormType> NormArray<T> {
    pub(crate) fn new() -> Self {
        Self {
            items: Vec::<Item<T>>::new(),
        }
    }

    pub(crate) fn from_item(item: Item<T>) -> Self {
        Self { items: vec![item] }
    }
    pub fn push(&mut self, item: Item<T>) {
        self.items.push(item)
    }

    pub fn contains(&self, item: &Item<T>) -> bool {
        self.items.contains(item)
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }
}

impl<T: NormType> FromIterator<Item<T>> for NormArray<T> {
    fn from_iter<I: IntoIterator<Item = Item<T>>>(iter: I) -> Self {
        let mut arr = NormArray::new();
        for item in iter {
            arr.push(item);
        }
        arr
    }
}

impl<T: NormType> IntoIterator for NormArray<T> {
    type Item = Item<T>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

impl<T: NormType> From<Vec<NullMarker>> for NormArray<T> {
    fn from(v: Vec<NullMarker>) -> Self {
        let mut arr = Self::new();
        for _ in v {
            arr.push(Item::Null);
        }
        arr
    }
}
