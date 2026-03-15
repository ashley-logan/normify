use crate::dtype::{NormValue, NormType};
use crate::error::{NormError, Result};
use crate::trait_impl::{NullType, PrimitiveType};
use serde_json::{Value, to_string_pretty};

#[macro_export]
macro_rules! impl_insert {
    (
        $method:ident,          // insert method name
        $variant:ident,         // DataColumn variant
        $ty:ty                  // type to push
    ) => {
        pub(crate) fn $method(&mut self, item: $ty) -> Result<()> {
            match self {
                Self::$variant (v) => {
                    v.push(item);
                    Ok(())
                }
                _ => Err(NormError::Insert),
            }
        }
    };
}


pub(crate) fn from_value(value: Value) -> Result<Self> {
        let r: NormValue = match value {
            Value::String(s) => s.into_norm(),
            Value::Null => NormValue::Null,
            Value::Bool(b) => b.into_norm(),
            Value::Number(n) => {
                if let Some(f) = n.as_f64() {
                    f.into_norm()
                } else if let Some(i) = n.as_i64() {
                    i.into_norm()
                } else if let Some(u) = n.as_u64() {
                    u.into_norm()
                } else {
                    return Err(NormError::Convert);
                }
            }
            _ => return Err(NormError::Convert),
        };
        Ok(r)
    }














pub(crate) fn normalize_arr(vec: Vec<Value>) -> Result<NormValue> {
    let mut v: Vec<Value> = vec![];
    flatten_vec(&vec, &mut v); // flattens the array 

    v.retain(|x| !x.is_null() & !x.is_object()); // remove null values and objects

    let mut new_vec: Vec<NormValue> = {
        if v.iter().all(Value::is_boolean) {
            // if boolean array
            v.into_iter().map(|x| x.as_bool().unwrap())

            v.into_iter().map(|x| {
                let r = NormValue::from_value(x)?;
                r
            }).collect()
            // use NormValue's from_value method to auto convert
        } else if v.iter().all(Value::is_number) {
            // if number array
            if v.iter().any(Value::is_f64) {
                // if any number is a float convert all numbers to floats
                v.into_iter()
                    .map(|x| {
                        let r = x.as_f64().ok
                    })
                    .collect()?
            }
            if v.iter().any(|n| n > i64::MAX) {
                v.into_iter()
                    .map(|x| NormValue::from(x.as_u64().ok_or(NormError::Convert)?))
                    .collect::<Vec<NormValue>>()
            } else {
                v.into_iter()
                    .map(|x| NormValue::from(x.as_i64().ok_or(NormError::Convert)?))
                    .collect::<Vec<NormValue>>()
            }
        } else {
            v.into_iter()
                .map(|x| NormValue::from(to_string_pretty(&x)?))
                .collect::<Vec<NormValue>>()
        }
    };
}

// pub(crate) fn flatten_json_array(val: &Value, flat_arr: &mut Vec<Value>) {
//     match val {
//         Value::Array(arr) => {
//             for x in arr {
//                 flatten_json_array(x, flat_arr);
//             }
//         }
//         _ => flat_arr.push(val.clone()),
//     }
// }

pub(crate) fn flatten_vec(vec: &Vec<Value>, flat_arr: &mut Vec<Value>) {
    for x in vec {
        if let Some(arr) = x.as_array() {
            flatten_vec(arr, flat_arr);
        } else {
            flat_arr.push(x.clone());
        }
    }
}

pub fn into_norm_array<>(vec: Vec<T>) -> NormValue {
    let mut inner_arr: Vec<NormValue> = vec![];
    for x in vec {
        inner_arr.push(NormValue::<T>::from(x));
    };
    NormValue::Array(inner_arr)

}
