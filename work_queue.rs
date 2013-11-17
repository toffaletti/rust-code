use std::vec;
use std::unstable::atomics::{atomic_store, atomic_load, AtomicUint, fence, SeqCst, Acquire, Release, Relaxed};
use std::unstable::sync::{UnsafeArc, LittleLock};
use std::cast;

struct Deque<T> {
    priv state: UnsafeArc<State<T>>,
}

impl<T: Send> Deque<T> {
    pub fn with_capacity(capacity: uint) -> Deque<T> {
        Deque{
            state: UnsafeArc::new(State::with_capacity(capacity))
        }
    }

    pub fn push(&mut self, value: T) {
        unsafe { (*self.state.get()).push(value) }
    }

    pub fn pop(&mut self) -> Option<T> {
        unsafe { (*self.state.get()).pop() }
    }

    pub fn steal(&mut self) -> Option<T> {
        unsafe { (*self.state.get()).steal() }
    }

    pub fn is_empty(&mut self) -> bool {
        unsafe { (*self.state.get()).is_empty() }
    }

    pub fn len(&mut self) -> uint {
        unsafe { (*self.state.get()).len() }
    }
}

impl<T: Send> Clone for Deque<T> {
    fn clone(&self) -> Deque<T> {
        Deque {
            state: self.state.clone()
        }
    }
}

struct State<T> {
    array: ~[*mut T],
    mask: uint,
    headIndex: AtomicUint,
    tailIndex: AtomicUint,
    lock: LittleLock,
}

impl<T: Send> State<T> {
    fn with_capacity(size: uint) -> State<T> {
        let mut state = State{
            array: vec::with_capacity(size),
            mask: size-1,
            headIndex: AtomicUint::new(0),
            tailIndex: AtomicUint::new(0),
            lock: LittleLock::new()
        };
        unsafe {
            vec::raw::set_len(&mut state.array, size);
        }
        state
    }

    fn push(&mut self, value: T) {
        let mut tail = self.tailIndex.load(Acquire);
        if tail < self.headIndex.load(Acquire) + self.mask {
            unsafe {
                atomic_store(&mut self.array[tail & self.mask], cast::transmute(value), Relaxed);
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
                        let mut newArray = vec::with_capacity(arraySize*2);
                        vec::raw::set_len(&mut newArray, arraySize*2);
                        for i in range(0, count) {
                            newArray[i] = self.array[(i+head) & mask];
                        }
                        self.array = newArray;
                        self.headIndex.store(0, Release);
                        self.tailIndex.store(count, Release);
                        tail = count;
                        self.mask = (mask * 2) | 1;
                    }
                    atomic_store(&mut self.array[tail & self.mask], value, Relaxed);
                    self.tailIndex.store(tail+1, Release);
                });
            }
        }
    }

    fn pop(&mut self) -> Option<T> {
        let mut tail = self.tailIndex.load(Acquire);
        if tail == 0 {
            return None
        }
        tail -= 1;
        self.tailIndex.store(tail, Release);
        fence(SeqCst);
        unsafe {
            if self.headIndex.load(Acquire) <= tail {
                Some(cast::transmute(atomic_load(&mut self.array[tail & self.mask], Relaxed)))
            } else {
                self.lock.lock(|| {
                    if self.headIndex.load(Acquire) <= tail {
                        Some(cast::transmute(atomic_load(&mut self.array[tail & self.mask], Relaxed)))
                    } else {
                        self.tailIndex.store(tail+1, Release);
                        None
                    }
                })
            }
        }
    }

    fn steal(&mut self) -> Option<T> {
        unsafe {
            match self.lock.try_lock(|| {
                let head = self.headIndex.load(Acquire);
                self.headIndex.store(head+1, Release);
                fence(SeqCst);
                if head < self.tailIndex.load(Acquire) {
                    Some(cast::transmute(atomic_load(&mut self.array[head & self.mask], Relaxed)))
                } else {
                    self.headIndex.store(head, Release);
                    None
                }
            }) {
                Some(T) => T,
                None => None
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.headIndex.load(Acquire) >= self.tailIndex.load(Acquire)
    }

    fn len(&self) -> uint {
        self.tailIndex.load(Acquire) - self.headIndex.load(Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::Deque;

    #[test]
    fn test() {
        let mut q = Deque::with_capacity(10);
        q.push(1);
        assert_eq!(Some(1), q.pop());
        assert_eq!(None, q.steal());
        q.push(2);
        assert_eq!(Some(2), q.steal());
    }

    #[test]
    fn test_grow() {
        let mut q = Deque::with_capacity(2);
        q.push(1);
        assert_eq!(Some(1), q.pop());
        assert_eq!(None, q.steal());
        q.push(2);
        assert_eq!(Some(2), q.steal());
        q.push(3);
        q.push(4);
        assert_eq!(Some(4), q.pop());
        assert_eq!(Some(3), q.pop());
        assert_eq!(None, q.steal());
    }
}
