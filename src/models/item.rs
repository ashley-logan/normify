use derive_more::{Display, From};

use crate::models::traits::NormType;

#[derive(PartialEq, From, Clone, Debug, Display)]
pub enum Item<T: NormType> {
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
