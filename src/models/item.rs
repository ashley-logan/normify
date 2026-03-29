use crate::models::traits::ItemTrait;
use derive_more::{Display, From};
use serde_json::Value;

#[derive(PartialEq, From, Clone, Debug, Display)]
pub enum Item<T: ItemTrait + Sized> {
    Data(T),
    #[display("null")]
    #[from(ignore)]
    Null,
}

macro_rules! impl_itemtrait {
    ($ty:ty) => {
        impl ItemTrait for $ty {
            fn as_serde_value(self) -> Value {
                self.into()
            }
        }
    };
}
impl_itemtrait!(i64);
impl_itemtrait!(u8);
impl_itemtrait!(u64);
impl_itemtrait!(f64);
impl_itemtrait!(bool);
impl_itemtrait!(String);
