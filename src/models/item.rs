use crate::error::{NormError, Result};
use crate::models::traits::ItemTrait;
use derive_more::{Display, From};
use serde_json::Value;

#[derive(PartialEq, From, Clone, Debug, Display)]
pub enum Item<T: ItemTrait> {
    Data(T),
    #[display("null")]
    #[from(ignore)]
    Null,
}

impl<T: ItemTrait> Item<T> {
    pub fn inner_to_string(self) -> Item<String> {
        let s = self.to_string();
        Item::Data(s)
    }

    pub fn into_option(self) -> Option<T> {
        match self {
            Self::Data(x) => Some(x),
            Self::Null => None,
        }
    }
}

macro_rules! impl_itemtrait {
    ($ty:ty, $fallback:ty) => {
        impl ItemTrait for $ty {
            type Fallback = $fallback;
        }
    };
}
impl_itemtrait!(u8, u128);
impl_itemtrait!(u16, u128);
impl_itemtrait!(u32, u128);
impl_itemtrait!(u64, u128);
impl_itemtrait!(u128, u128);
impl_itemtrait!(i8, i128);
impl_itemtrait!(i16, i128);
impl_itemtrait!(i32, i128);
impl_itemtrait!(i64, i128);
impl_itemtrait!(i128, i128);
impl_itemtrait!(f64, f64);
impl_itemtrait!(bool, bool);
impl_itemtrait!(String, String);
