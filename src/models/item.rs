use crate::models::traits::{ItemTrait, NormType};
use derive_more::{Display, From};
use serde_json::Value;

macro_rules! impl_itemtrait {
    ($ty:ty) => {
        impl ItemTrait for $ty {
            fn as_serde_value(self) -> Value {
                self.into::<Value>()
            }
        }
    };
}
impl_itemtrait!(i64);
impl_itemtrait!(u64);
impl_itemtrait!(f64);
impl_itemtrait!(bool);
impl_itemtrait!(String);

#[derive(PartialEq, From, Clone, Debug, Display)]
pub enum Item<T: ItemTrait> {
    Data(T),
    #[display("null")]
    #[from(ignore)]
    Null,
}

pub struct ListType<T: NormType>(Vec<T>);

pub struct NullMarker;

impl<T: NormType> From<NullMarker> for Item<T> {
    fn from(value: NullMarker) -> Self {
        Item::Null
    }
}
