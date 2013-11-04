// Chase/Lev work-stealing deque
// from Dynamic Circular Work-Stealing Deque
// http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.170.1097&rep=rep1&type=pdf
// and
// Correct and Efﬁcient Work-Stealing for Weak Memory Models
// http://www.di.ens.fr/~zappa/readings/ppopp13.pdf

use std::vec;
use std::unstable::sync::UnsafeArc;
use std::unstable::atomics::{AtomicOption,AtomicUint,AtomicPtr,fence,Relaxed,Release,Acquire,SeqCst};
use std::ptr::{mut_null, to_mut_unsafe_ptr};
use std::cast;
use std::util;

struct Deque<T> {
    top: AtomicUint,
    bottom: AtomicUint,
    array: AtomicPtr<Array<T>>,
}

impl<T> Deque<T> {
    fn new(size: uint) -> Deque<T> {
        unsafe {
            Deque{
                top: AtomicUint::new(0),
                bottom: AtomicUint::new(0),
                array: AtomicPtr::<Array<T>>::new(
                    cast::transmute(~Array::<T>::new((size as f64).ln() as uint))
                    ),
            }
        }
    }

    fn push(&mut self, x: ~T) {
        unsafe {
            let b = self.bottom.load(Relaxed);
            let t = self.top.load(Acquire);
            let mut a = self.array.load(Relaxed);
            if b - t > (*a).size() - 1 {
                let new_array: *mut Array<T> = cast::transmute((*a).grow(t, b));
                a = new_array;
                let ss = (*a).size();
                self.bottom.store(b + ss, Relaxed);
                // TODO: might need to set b = b + ss here also
                cast::transmute::<*mut Array<T>, ~Array<T>>(self.array.swap(new_array, Relaxed));
            }
            (*a).put(b, cast::transmute(x));
            fence(Release);
            self.bottom.store(b + 1, Relaxed);
        }
    }

    fn take(&mut self) -> Option<~T> {
        unsafe {
            let b = self.bottom.load(Relaxed) - 1;
            let a = self.array.load(Relaxed);
            self.bottom.store(b, Relaxed);
            fence(SeqCst);
            let t = self.top.load(Relaxed);
            let size: int = b as int - t as int;
            if size >= 0 {
                // non-empty queue
                let x = (*a).get(b);
                if t == b {
                    if self.top.compare_and_swap(t, t + 1, SeqCst) != t {
                        // failed race
                        self.bottom.store(b + 1, Relaxed);
                        None
                    } else {
                        self.bottom.store(b + 1, Relaxed);
                        Some(cast::transmute(x))
                    }
                } else {
                    Some(cast::transmute(x))
                }
            } else {
                // empty queue
                self.bottom.store(b + 1, Relaxed);
                None
            }
        }
    }

    fn steal(&mut self) -> Result<Option<~T>, bool> {
        unsafe {
            let t = self.top.load(Acquire);
            fence(SeqCst);
            let old_a = self.array.load(Relaxed);
            let b = self.bottom.load(Acquire);
            let a = self.array.load(Relaxed);
            let size = b - t;
            if size <= 0 {
                // empty
                return Ok(None)
            }
            // TODO: i think this (*a) access is still a race...hmmm
            if (size % (*a).size()) == 0 {
                if a == old_a && t == self.top.load(Relaxed) {
                    // empty
                    return Ok(None)
                } else {
                    // abort, failed race
                    return Err(false)
                }
            }
            // non empty
            let x = (*a).get(t);
            if self.top.compare_and_swap(t, t + 1, SeqCst) != t {
                // failed race
                Err(false)
            } else {
                Ok(Some(cast::transmute(x)))
            }
        }
    }
}

struct Array<T> {
    size: AtomicUint,
    buf: AtomicPtr<~[AtomicPtr<T>]>,
}

impl<T> Array<T> {
    fn new(size: uint) -> Array<T> {
        unsafe {
            Array{
                size:AtomicUint::new(size),
                buf: AtomicPtr::<~[AtomicPtr<T>]>::new(cast::transmute(vec::from_fn(1<<size, |_| {
                    AtomicPtr::<T>::new(mut_null())
                })))
            }
        }
    }

    fn grow(&self, top: uint, bottom: uint) -> ~Array<T> {
        let mut a = ~Array::new(self.size.load(Relaxed)+1);
        for i in range(top, bottom) {
            a.put(i, self.get(i));
        }
        a
    }

    fn size(&self) -> uint {
        1<<self.size.load(Relaxed)
    }

    fn put(&mut self, i: uint, v: *mut T) {
        unsafe {
            let buf = self.buf.load(Relaxed);
            do vec::raw::mut_buf_as_slice(buf as *mut AtomicPtr<T>, self.size()) |buf| {
                buf[i % self.size()].store(v, Relaxed);
            }
        }
    }

    fn get(&self, i: uint) -> *mut T {
        unsafe {
            let buf = self.buf.load(Relaxed);
            do vec::raw::mut_buf_as_slice(buf as *mut AtomicPtr<T>, self.size()) |buf| {
                buf[i % self.size()].load(Relaxed)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Deque;

    #[test]
    fn test() {
        let mut q = Deque::new(10);
        q.push(~1);
        assert_eq!(Some(~1), q.take());
        assert_eq!(Ok(None), q.steal());
        q.push(~2);
        assert_eq!(Ok(Some(~2)), q.steal());
    }
}
