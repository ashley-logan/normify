pub mod norm_value {
    use crate::impl_is_variant;
    use crate::models::NormType;
    use derive_more::{Display, From};
    use std::fmt::Display;

    #[derive(Debug, Clone, Display, PartialEq)]
    #[display("null")]
    pub struct NullType;

    #[derive(Debug, Clone, Display, From, PartialEq)]
    pub enum NormValue {
        // wrapper for json primitives
        String(String),
        Float(f64),
        UInt(u64),
        Int(i64),
        Bool(bool),
    }

    impl NormValue {
        impl_is_variant!(is_string, String);
        impl_is_variant!(is_float, Float);
        impl_is_variant!(is_uint, UInt);
        impl_is_variant!(is_int, Int);
        impl_is_variant!(is_bool, Bool);
    }

    #[derive(PartialEq, From, Clone, Debug)]
    pub enum Item<T: NormType + PartialEq> {
        Data(T),
        Null(NullType),
    }

    impl NormType for f64 {
        fn into_norm(self) -> NormValue {
            NormValue::Float(self)
        }
    }

    impl NormType for i64 {
        fn into_norm(self) -> NormValue {
            NormValue::Int(self)
        }
    }

    impl NormType for u64 {
        fn into_norm(self) -> NormValue {
            NormValue::UInt(self)
        }
    }

    impl NormType for bool {
        fn into_norm(self) -> NormValue {
            NormValue::Bool(self)
        }
    }

    impl NormType for String {
        fn into_norm(self) -> NormValue {
            NormValue::String(self)
        }
    }

    impl NormType for NullType {
        fn into_norm(self) -> NormValue {
            NormValue::Null(self)
        }
    }
    // impl Display for NormValue {
    //     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    //         match self {
    //             Self::Null => write!(f, "null"),
    //             Self::Bool(b) => write!(f, "{}", b),
    //             Self::String(s) => write!(f, "{}", s),
    //             Self::Float(fl) => write!(f, "{}", fl),
    //             Self::UInt(u) => write!(f, "{}", u),
    //             Self::Int(i) => write!(f, "{}", i),
    //         }
    //     }
    // }
}

pub(crate) mod traits {
    use crate::models::{DataColumn, NormValue};
    pub trait NormType: Sized {
        fn into_norm(self) -> NormValue;
    }

    pub trait ColumnType {
        fn into_enum(self) -> DataColumn;
    }
}
