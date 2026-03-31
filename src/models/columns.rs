use crate::models::type_aliases::UintColumn;
use crate::models::{ColumnType, Item};
use crate::{NormArray, impl_concrete_cast};
use indexmap::IndexSet;
use std::fmt::Write;

#[derive(Clone)]
pub struct IdColumn(IndexSet<u64>);

// """
//  __________ __________
// |          |          |
// |  ID_u64  |          |
// |__________|__________|
// |    0     |          |
// |__________|__________|
// |    1     |          |
// |__________|__________|
// |    2     |          |
// |__________|__________|
// |   ...    |
// |__________|
// |   304    |
// |__________|
// |   305    |
// |__________|
// """

impl IdColumn {
    pub(crate) fn new() -> Self {
        Self(IndexSet::new())
    }

    pub(crate) fn from_id(id: u64) -> Self {
        let mut set: IndexSet<u64> = IndexSet::new();
        set.insert(id);
        Self(set)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub(crate) fn man_insert(&mut self, id: u64) -> bool {
        self.0.insert(id)
    }

    pub(crate) fn auto_insert(&mut self) -> u64 {
        // insert a new id based on id column length
        let next_id: u64 = (self.0.len() + 1) as u64;
        self.0.insert(next_id);
        next_id
    }

    pub(crate) fn auto_insert2(&mut self) -> u64 {
        // insert a new id based on id column max (insert 1 if empty)
        let m: &u64 = self.0.iter().max().unwrap_or(&0_u64);
        let new_id: u64 = m.clone() + 1;
        self.0.insert(new_id);
        new_id
    }
}

impl ColumnType for IdColumn {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }

    fn into_string_super(self: Box<Self>) -> Box<NormArray<String>> {
        let mut arr: NormArray<String> = NormArray::new();
        for id in self.0 {
            arr.push_item(Item::Data(id).inner_to_string());
        }
        Box::new(arr)
    }

    fn write_col_fmt(&self, limit: Option<usize>, buf: &mut dyn Write) {
        let mut lim: usize = limit.unwrap_or(self.len());
        if lim > self.len() {
            lim = self.len();
        }
        writeln!(buf, "").unwrap();
        for i in 0..lim {
            writeln!(buf, "{}", self.0[i]).unwrap();
        }
        writeln!(buf, "").unwrap();
    }

    fn write_list_fmt(&self, limit: Option<usize>, buf: &mut dyn Write) {
        let mut lim: usize = limit.unwrap_or(self.len());
        if lim > self.len() {
            lim = self.len();
        }
        write!(buf, "[  ").unwrap();
        for i in 0..lim {
            if i == lim - 1 {
                write!(buf, "{}  ]", self.0[i]).unwrap();
                break;
            }
            write!(buf, "{}, ", self.0[i]).unwrap();
        }
    }

    fn count_data(&self) -> usize {
        self.0.len()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn count_nulls(&self) -> usize {
        0
    }

    fn push_null(&mut self) {}
}

impl From<UintColumn> for IdColumn {
    fn from(value: UintColumn) -> Self {
        let mut idcol: IdColumn = Self::new();
        for id in value {
            if let Item::Data(u) = id {
                idcol.man_insert(u);
            }
        }
        idcol
    }
}
