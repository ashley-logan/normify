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

impl_normtype!(f64);
impl_normtype!(i64);
impl_normtype!(u64);
impl_normtype!(bool);
impl_normtype!(String);

use crate::models::Item;
pub trait ArrayTrait {
    type Inner: NormType;
    fn add_item(&mut self, item: Item<Self::Inner>);
}
