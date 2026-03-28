#[macro_export]
macro_rules! impl_concrete_cast {
    ($ref_method:ident, $move_method:ident, $is_method:ident, $ty:ty) => {
        fn $ref_method(&mut self) -> Option<&mut $ty> {
            self.as_any_mut().downcast_mut::<$ty>()
        }

        fn $move_method(self: Box<Self>) -> Option<Box<$ty>> {
            self.into_any().downcast::<$ty>().ok()
        }

        fn $is_method(&self) -> bool {
            self.as_any().is::<$ty>()
        }
    };
}
