#[macro_export]
macro_rules! impl_insert {
    (
        $method:ident,          // insert method name
        $variant:ident,         // DataColumn variant
        $ty:ty                  // type to push
    ) => {
        pub(crate) fn $method(&mut self, item: $ty) -> Result<()> {
            match self {
                Self::$variant(v) => {
                    v.push(item);
                    Ok(())
                }
                _ => Err(NormError::Insert),
            }
        }
    };
}

#[macro_export]
macro_rules! impl_is_variant {
    (
        $method:ident,
        $variant:ident
    ) => {
        pub fn $method(&self) -> bool {
            matches!(self, Self::$variant(_))
        }
    };
}
