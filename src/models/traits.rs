macro_rules! impl_normtype {
    ($ty:ty) => {
        impl NormType for $ty {
            fn get_size(self) -> usize {
                size_of::<$ty>()
            }
        }
    };
}

pub trait NormType: PartialEq + Sized {
    fn get_size(self) -> usize;
}

use serde_json::Value;
pub trait ItemTrait: PartialEq + Into<Value> {
    fn as_serde_value(self) -> Value;
}

impl_normtype!(f64);
impl_normtype!(i64);
impl_normtype!(u64);
impl_normtype!(bool);
impl_normtype!(String);

use std::any::Any;
pub trait ArrayTrait {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn len(&self) -> usize;
    fn count_data(&self) -> usize;
    fn count_nulls(&self) -> usize;
}
