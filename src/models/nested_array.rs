use crate::models::ArrayTrait;
use std::any::Any;

#[derive(Clone)]
pub struct ListArray<T: ArrayTrait> {
    lists: Vec<T>,
}

impl<T: ArrayTrait> ArrayTrait for ListArray<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn len(&self) -> usize {
        self.lists.len()
    }

    fn count_data(&self) -> usize {
        self.lists.iter().filter(|&x| x.count_data() > 0).count()
    }

    fn count_nulls(&self) -> usize {
        self.lists.iter().filter(|&x| x.count_data() == 0).count()
    }
}

impl<T: ArrayTrait> ListArray<T> {
    pub fn new() -> Self {
        Self {
            lists: Vec::new::<T>(),
        }
    }

    pub fn from_arr(arr: T) -> Self {
        Self { lists: vec![arr] }
    }

    pub fn push_arr(&mut self, arr: T) {
        self.lists.push(arr)
    }
}
