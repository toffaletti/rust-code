#[feature(globs)];
#[link_args="-lJudy"];

use capi::*;
use std::ptr::{mut_null,to_unsafe_ptr};
use std::cast;
use std::sys::size_of;

pub mod capi {
    use std::libc::{c_void, c_int, c_ulong};
    pub type Pvoid_t = *mut c_void;
    pub type PPvoid_t = *mut Pvoid_t;
    pub type Pcvoid_t = *c_void;
    pub type Word_t = c_ulong;
    pub type PWord_t = *Word_t;

    pub type JU_Errno_t = c_int;

    pub static JU_ERRNO_NONE: JU_Errno_t           = 0;
    pub static JU_ERRNO_FULL: JU_Errno_t           = 1;
    pub static JU_ERRNO_NFMAX: JU_Errno_t          = JU_ERRNO_FULL;
    pub static JU_ERRNO_NOMEM: JU_Errno_t          = 2;
    pub static JU_ERRNO_NULLPPARRAY: JU_Errno_t    = 3;
    pub static JU_ERRNO_NONNULLPARRAY: JU_Errno_t  = 10;
    pub static JU_ERRNO_NULLPINDEX: JU_Errno_t     = 4;
    pub static JU_ERRNO_NULLPVALUE: JU_Errno_t     = 11;
    pub static JU_ERRNO_NOTJUDY1: JU_Errno_t       = 5;
    pub static JU_ERRNO_NOTJUDYL: JU_Errno_t       = 6;
    pub static JU_ERRNO_NOTJUDYSL: JU_Errno_t      = 7;
    pub static JU_ERRNO_UNSORTED: JU_Errno_t       = 12;
    pub static JU_ERRNO_OVERRUN: JU_Errno_t        = 8;
    pub static JU_ERRNO_CORRUPT: JU_Errno_t        = 9;

    pub struct JError_t {
        je_Errno: JU_Errno_t,
        je_ErrID: c_int,
        je_reserved: [Word_t, ..4],
    }
    pub type PJError_t = *mut JError_t;

    impl JError_t {
        pub fn new() -> JError_t {
            JError_t{
                je_Errno: JU_ERRNO_NONE,
                je_ErrID: 0,
                je_reserved: [0, ..4],
            }
        }
    }

    extern {
        pub fn JudyHSGet(array: Pcvoid_t, key: *c_void, size: Word_t) -> PPvoid_t;
        pub fn JudyHSIns(array: PPvoid_t, key: *c_void, size: Word_t, err: PJError_t) -> PPvoid_t;
        pub fn JudyHSDel(array: PPvoid_t, key: *c_void, size: Word_t, err: PJError_t) -> c_int;
        pub fn JudyHSFreeArray(array: PPvoid_t, err: PJError_t) -> Word_t;

        pub fn JudyLIns(array: PPvoid_t, index: Word_t, err: PJError_t) -> PPvoid_t;
        pub fn JudyLDel(array: PPvoid_t, index: Word_t, err: PJError_t) -> c_int;
        pub fn JudyLGet(array: Pcvoid_t, index: Word_t, err: PJError_t) -> PPvoid_t;
        pub fn JudyLCount(array: Pcvoid_t, index1: Word_t, index2: Word_t, err: PJError_t) -> Word_t;
        pub fn JudyLByCount(array: Pcvoid_t, nth: Word_t, pindex: *Word_t, err: PJError_t) -> PPvoid_t;
        pub fn JudyLFreeArray(array: PPvoid_t, err: PJError_t) -> Word_t;
        pub fn JudyLMemUsed(array: Pcvoid_t) -> Word_t;
        pub fn JudyLFirst(array: Pcvoid_t, pindex: *Word_t, err: PJError_t) -> PPvoid_t;
        pub fn JudyLNext(array: Pcvoid_t, pindex: *Word_t, err: PJError_t) -> PPvoid_t;
        pub fn JudyLLast(array: Pcvoid_t, pindex: *Word_t, err: PJError_t) -> PPvoid_t;
        pub fn JudyLPrev(array: Pcvoid_t, pindex: *Word_t, err: PJError_t) -> PPvoid_t;
        pub fn JudyLFirstEmpty(array: Pcvoid_t, pindex: *Word_t, err: PJError_t) -> c_int;
        pub fn JudyLNextEmpty(array: Pcvoid_t, pindex: *Word_t, err: PJError_t) -> c_int;
        pub fn JudyLLastEmpty(array: Pcvoid_t, pindex: *Word_t, err: PJError_t) -> c_int;
        pub fn JudyLPrevEmpty(array: Pcvoid_t, pindex: *Word_t, err: PJError_t) -> c_int;
    }
}

struct JudyL<V> {
    m: Pvoid_t,
}

impl<V> JudyL<V> {
    fn new() -> JudyL<V> {
        JudyL{m: mut_null()}
    }

    #[fixed_stack_segment]
    fn insert(&mut self, index: Word_t, value: ~V) -> bool {
        unsafe {
            let mut err = JError_t::new();
            let v = JudyLIns(&mut self.m, index, &mut err);
            debug2!("err: {:?}", err);
            if v == mut_null() {
                false
            } else if *v != mut_null() {
                false
            } else {
                *v = cast::transmute(value);
                true
            }
        }
    }

    #[fixed_stack_segment]
    fn get<'a>(&'a self, index: Word_t) -> Option<&'a V> {
        unsafe {
            let mut err = JError_t::new();
            let v = JudyLGet(self.m as Pcvoid_t, index, &mut err);
            if v == mut_null() {
                None
            } else {
                Some(cast::transmute(*v))
            }
        }
    }

    #[fixed_stack_segment]
    fn free(&mut self) -> Word_t {
        if self.m != mut_null() {
            unsafe {
                let mut err = JError_t::new();
                JudyLFreeArray(&mut self.m, &mut err)
            }
            //assert!(self.m == mut_null());
        } else {
            0
        }
    }

    fn iter<'a>(&'a self) -> JudyLIterator<'a, V> {
        JudyLIterator{ m: self.m as Pcvoid_t, i: 0, lifetime: None}
    }
}

struct JudyHS<K, V> {
    m: Pvoid_t,
}

impl<K, V> JudyHS<K, V> {
    fn new() -> JudyHS<K, V> {
        JudyHS{m: mut_null()}
    }

    #[fixed_stack_segment]
    fn insert(&mut self, key: K, value: ~V) -> bool {
        unsafe {
            let mut err = JError_t::new();
            let v = JudyHSIns(&mut self.m, to_unsafe_ptr(&key) as Pcvoid_t, size_of::<K>() as Word_t, &mut err);
            debug2!("err: {:?}", err);
            if v == mut_null() {
                false
            } else if *v != mut_null() {
                false
            } else {
                *v = cast::transmute(value);
                true
            }
        }
    }

    #[fixed_stack_segment]
    fn get<'a>(&'a self, key: K) -> Option<&'a V> {
        unsafe {
            let v = JudyHSGet(self.m as Pcvoid_t, to_unsafe_ptr(&key) as Pcvoid_t, size_of::<K>() as Word_t);
            if v == mut_null() {
                None
            } else {
                Some(cast::transmute(*v))
            }
        }
    }

    #[fixed_stack_segment]
    fn free(&mut self) -> Word_t {
        if self.m != mut_null() {
            unsafe { JudyHSFreeArray(&mut self.m, mut_null()) }
            //assert!(self.m == mut_null());
        } else {
            0
        }
    }

}

#[deriving(Clone)]
struct JudyLIterator<'self, V> {
    priv m: Pcvoid_t,
    priv i: Word_t,
    priv lifetime: Option<&'self ()> // FIXME: #5922
}

impl<'self, V> Iterator<(Word_t, &'self V)> for JudyLIterator<'self, V> {

    #[fixed_stack_segment]
    fn next(&mut self) -> Option<(Word_t, &'self V)> {
        unsafe {
            let mut err = JError_t::new();
            let v = JudyLNext(self.m, &self.i, &mut err);
            if v == mut_null() {
                None
            } else {
                Some((self.i, cast::transmute(*v)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_JudyHS() {
        let mut h = JudyHS::<int, int>::new();
        assert!(h.insert(123, ~456));
        match h.get(123) {
            Some(x) => assert_eq!(456, *x),
            None => fail!(),
        }
        assert!(h.free() > 0);
    }

    #[test]
    fn test_JudyL() {
        let mut h = JudyL::<int>::new();
        assert!(h.insert(123, ~456));
        match h.get(123) {
            Some(x) => assert_eq!(456, *x),
            None => fail!(),
        }

        for (i, v) in h.iter() {
            debug2!("i: {:?} v: {:?}", i, v);
        }
        assert!(h.free() > 0);
    }

}
