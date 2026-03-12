use crate::dtype::NormValue;
use crate::error::{NormError, Result};
use serde_json::{Value, to_string_pretty};

pub(crate) fn normalize_arr(vec: Vec<Value>) -> Result<NormValue> {
    let mut v: Vec<Value> = vec![]; // normalized container
    flatten_vec(&vec, &mut v); // flatten the array into normalized container

    v.retain(|x| !x.is_null() & !x.is_object()); // remove null values and objects

    let mut new_vec: Vec<NormValue> = {
        if v.iter().all(Value::is_boolean) {
            v.into_iter()
                .map(NormValue::from_value)
                .collect::<Vec<NormValue>>()
        } else if v.iter().all(Value::is_number) {
            if v.iter().any(Value::is_f64) {
                v.into_iter()
                    .map(|x| NormValue::from(x.as_f64().ok_or(NormError::Convert)?))
                    .collect::<Vec<NormValue>>()
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
