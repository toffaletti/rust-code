use std::ptr::mut_null;
use std::cast;
use std::unstable::sync::UnsafeArc;
use std::unstable::atomics::{fence,AcqRel};

// https://groups.google.com/forum/#!topic/comp.programming.threads/f18HQB07vZE

struct Queue<T> {
    priv state: UnsafeArc<State<T>>,
}

impl<T: Send> Clone for Queue<T> {
    fn clone(&self) -> Queue<T> {
        Queue {
            state: self.state.clone()
        }
    }
}

impl<T: Send> Queue<T> {
    fn new() -> Queue<T> {
        Queue{state:UnsafeArc::new(State::new())}
    }

    fn push(&mut self, value: T) {
        unsafe { (*self.state.get()).push(value) }
    }

    fn pop(&mut self) -> Option<T> {
        unsafe { (*self.state.get()).pop() }
    }
}

struct Node<T> {
    link: *mut Node<T>,
    value: Option<T>,
}

struct State<T> {
    pad0: [u8, ..64],
    head: *mut Node<T>,
    pad1: [u8, ..64],
    tail: *mut Node<T>,
    prev: *mut Node<T>,
    pad2: [u8, ..64],
}

impl<T> Node<T> {
    fn new() -> Node<T> {
        Node{
            link: mut_null(),
            value: None,
        }
    }
}

impl<T> State<T> {
    fn new() -> State<T> {
        let n: *mut Node<T> = unsafe {
            cast::transmute(~Node::<T>::new())
        };
        State{
            pad0: [0, ..64],
            head: n,
            pad1: [0, ..64],
            tail: n,
            prev: mut_null(),
            pad2: [0, ..64],
        }
    }

    fn push(&mut self, value: T) {
        let n = ~Node::<T>::new();
        unsafe {
            let n = cast::transmute(n);
            (*self.head).value = Some(value);
            // TODO: don't think this is correct.
            fence(AcqRel);
            (*self.head).link = n;
            fence(AcqRel);
            self.head = n;
        }
    }

    fn pop(&mut self) -> Option<T> {
        unsafe {
            let mut next = (*self.tail).link;
            if next != mut_null() {
                let mut cur = self.tail;
                let mut prev = self.prev;
                loop {
                    (*cur).link = prev;
                    prev = cur;
                    cur = next;
                    next = (*cur).link;
                    if next == mut_null() { break }
                }
                self.tail = cur;
                self.prev = prev;
            }
            if self.prev == mut_null() {
                return None;
            }
            let n: ~Node<T> = cast::transmute(self.prev);
            self.prev = n.link;
            return n.value;
        }
    }
}

#[unsafe_destructor]
impl <T: Send> Drop for State<T> {
    fn drop(&mut self) {
        loop {
            match self.pop() {
                None => break,
                Some(_) => (),
            }
        }
        unsafe {
            cast::transmute::<*mut Node<T>, ~Node<T>>(self.head);
        }
    }
}

#[cfg(test)]
mod test {
    use std::task;
    use std::comm;
    use super::Queue;

    #[test]
    fn test() {
        let nmsgs = 10u;
        let mut q = Queue::new();
        for i in range(0, nmsgs) {
            q.push(i);
        }

        let mut i = 0u;
        loop {
            match q.pop() {
                None => {},
                Some(_) => {
                    i += 1;
                    if i == nmsgs { break }
                }
            }
        }
    }

    #[test]
    fn test_threaded() {
        let nmsgs = 100000u;
        let mut q = Queue::new();
        assert_eq!(None, q.pop());

        let (port, chan)  = comm::stream();
        chan.send(q.clone());
        do task::spawn_sched(task::SingleThreaded) {
            let mut q = port.recv();
            for i in range(0, nmsgs) {
                q.push(i);
            }
        }

        let mut i = 0u;
        loop {
            match q.pop() {
                None => {},
                Some(_) => {
                    i += 1;
                    if i == nmsgs { break }
                }
            }
        }
    }
}
