use std::vec;
use std::ptr::{mut_null};
use std::unstable::atomics::{AtomicPtr, AtomicUint, fence, SeqCst, Acquire, Release, Relaxed};
use std::unstable::sync::{LittleLock};
use std::cast;

struct Deque<T> {
    array: ~[AtomicPtr<T>],
    mask: uint,
    headIndex: AtomicUint,
    tailIndex: AtomicUint,
    lock: LittleLock,
}

impl<T> Deque<T> {
    fn new(size: uint) -> Deque<T> {
        Deque{
            array: vec::from_fn(size, |_| {
                       AtomicPtr::new(mut_null())
                   }),
            mask: size-1,
            headIndex: AtomicUint::new(0),
            tailIndex: AtomicUint::new(0),
            lock: LittleLock::new()
        }
    }

    pub fn push(&mut self, value: T) {
        let mut tail = self.tailIndex.load(Acquire);
        if tail < self.headIndex.load(Acquire) + self.mask {
            unsafe {
                self.array[tail & self.mask].store(cast::transmute(value), Relaxed);
            }
            self.tailIndex.store(tail+1, Release);
        } else {
            unsafe {
                let value: *mut T =  cast::transmute(value);
                self.lock.lock(|| {
                    let head = self.headIndex.load(Acquire);
                    let count = self.len();
                    if count >= self.mask {
                        let arraySize = self.array.len();
                        let mask = self.mask;
                        let mut newArray = vec::from_fn(arraySize*2, |_| {
                            AtomicPtr::new(mut_null())
                        });
                        for i in range(0, count) {
                            newArray[i].store(self.array[(i+head) & mask].load(SeqCst), Relaxed);
                        }
                        self.array = newArray;
                        self.headIndex.store(0, Release);
                        self.tailIndex.store(count, Release);
                        tail = count;
                        self.mask = (mask * 2) | 1;
                    }
                    self.array[tail & self.mask].store(value, Relaxed);
                    self.tailIndex.store(tail+1, Release);
                });
            }
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        let mut tail = self.tailIndex.load(Acquire);
        if tail == 0 {
            return None
        }
        tail -= 1;
        self.tailIndex.store(tail, Release);
        fence(SeqCst);
        unsafe {
            if self.headIndex.load(Acquire) <= tail {
                Some(cast::transmute(self.array[tail & self.mask].load(Relaxed)))
            } else {
                self.lock.lock(|| {
                    if self.headIndex.load(Acquire) <= tail {
                        Some(cast::transmute(self.array[tail & self.mask].load(Relaxed)))
                    } else {
                        self.tailIndex.store(tail+1, Release);
                        None
                    }
                })
            }
        }
    }

    pub fn steal(&mut self) -> Option<T> {
        // TODO: need to expose rust_try_little_lock
        unsafe {
            self.lock.lock(|| {
                let head = self.headIndex.load(Acquire);
                self.headIndex.store(head+1, Release);
                fence(SeqCst);
                if head < self.tailIndex.load(Acquire) {
                        Some(cast::transmute(self.array[head & self.mask].load(Relaxed)))
                } else {
                    self.headIndex.store(head, Release);
                    None
                }
            })
        }
    }

    pub fn is_empty(&self) -> bool {
        self.headIndex.load(Acquire) >= self.tailIndex.load(Acquire)
    }

    pub fn len(&self) -> uint {
        self.tailIndex.load(Acquire) - self.headIndex.load(Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::Deque;

    #[test]
    fn test() {
        let mut q = Deque::new(10);
        q.push(~1);
        assert_eq!(Some(~1), q.pop());
        assert_eq!(None, q.steal());
        q.push(~2);
        assert_eq!(Some(~2), q.steal());
    }
}
