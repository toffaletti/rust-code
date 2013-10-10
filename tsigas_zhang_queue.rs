use std::unstable::sync::UnsafeArc;
use std::vec;
use std::unstable::atomics::{AtomicUint,AtomicPtr,Acquire,Release,Relaxed};
use std::cast;

// TODO: backoff policy for retry loop
// http://www.cse.chalmers.se/~tsigas/papers/spaa01.pdf

struct Queue<T> {
    priv state: UnsafeArc<State<T>>,
}

impl<T: Send+Clone> Queue<T> {
    pub fn push(&mut self, value: ~T) -> bool {
        unsafe {
            let ptr = cast::transmute(value);
            if (*self.state.get()).enqueue(ptr) {
                true
            } else {
                // cast back to ~T so memory is freed
                cast::transmute::<*mut T, ~T>(ptr);
                false
            }
        }
    }

    pub fn pop(&mut self) -> Option<~T> {
        unsafe {
            let p = (*self.state.get()).dequeue();
            if p == State::null0() {
                None
            } else {
                let tmp: ~T = cast::transmute(p);
                Some(tmp)
            }
        }
    }
}

impl<T: Send> Clone for Queue<T> {
    fn clone(&self) -> Queue<T> {
        Queue {
            state: self.state.clone()
        }
    }
}

impl<T: Send> Queue<T> {
    pub fn with_capacity(capacity: uint) -> Queue<T> {
        Queue{state: UnsafeArc::new(
                State::with_capacity(capacity)
                )
        }
    }
}

struct State<T> {
    nodes: ~[AtomicPtr<T>],
    head: AtomicUint,
    tail: AtomicUint,
}

impl<T: Send> State<T> {
    fn with_capacity(capacity: uint) -> State<T> {
        let nodes = do vec::from_fn(capacity+2) |_| {
            AtomicPtr::new(State::null0())
        };
        let mut tmp = State{
            nodes: nodes,
            head: AtomicUint::new(0),
            tail: AtomicUint::new(1),
        };
        tmp.nodes[0].store(State::null1(), Relaxed);
        tmp
    }

    fn maxnum(&self) -> uint {
        self.nodes.len()
    }

    fn null0() -> *mut T {
        0 as *mut T
    }

    fn null1() -> *mut T {
        1 as *mut T
    }

    #[inline]
    fn is_free(p: *mut T) -> bool {
        p == State::null0() || p == State::null1()
    }

    fn enqueue(&mut self, newnode: *mut T) -> bool {
        assert_eq!(0, (newnode as uint) & 1);
        let maxnum = self.maxnum();
        debug2!("maxnum: {:?}", maxnum);
        'retry: loop {
            // read the tail
            let te = self.tail.load(Acquire);
            let mut ate = te;
            let mut tt = self.nodes[ate].load(Relaxed);
            // the next slot of the tail
            let mut temp = (ate + 1) % maxnum;
            debug2!("enter enqueue te: {:?} tt: {:?} temp: {:?}", te, tt, temp);
            debug2!("tt is free: {:?}", State::is_free(tt));
            debug2!("head: {:?}", self.head.load(Relaxed));
            // we want to find the actual tail
            while !State::is_free(tt) {
                // check tail's consistency
                if te != self.tail.load(Relaxed) {
                    continue 'retry
                }
                // if tail meet head,
                // it is possible that queue is full
                if temp == self.head.load(Acquire) {
                    break;
                }
                // now check the next cell
                tt = self.nodes[temp].load(Relaxed);
                ate = temp;
                temp = (temp + 1) % maxnum;
                debug2!("tt: {:?} ate: {:?} temp: {:?}", tt, ate, temp);
            }
            // check tail's consistency
            if te != self.tail.load(Relaxed) {
                continue
            }
            // check whether queue is full
            if temp == self.head.load(Acquire) {
                ate = (temp + 1) % maxnum;
                tt = self.nodes[ate].load(Relaxed);
                debug2!("full check ate: {:?} tt: {:?}", ate, tt);
                // the cell after head is occupied
                if !State::is_free(tt) {
                    // queue is full
                    return false
                }
                // help the dequeue to update head
                debug2!("enqueue updating head to: {:?}", ate);
                self.head.compare_and_swap(temp, ate, Release);
                // try enqueue again
                continue
            }
            let tnew = if tt == State::null1() {
                (newnode as uint | 1u) as *mut T
            } else {
                newnode
            };
            // check the tail consistency
            if te != self.tail.load(Relaxed) {
                continue
            }
            // get the actual tail and try enqueue data
            if self.nodes[ate].compare_and_swap(tt, tnew, Release) == tt {
                // enqueue has succeed
                debug2!("enqueue success ate: {:?} tt: {:?} tnew: {:?}", ate, tt, tnew);
                if temp % 2 == 0 {
                    self.tail.compare_and_swap(te, temp, Release);
                }
                return true
            }
        }
    }

    fn dequeue(&mut self) -> *mut T {
        let maxnum = self.maxnum();
        'retry: loop {
            let th = self.head.load(Acquire);
            // here is the one we want to dequeue
            let mut temp = (th + 1) % maxnum;
            let mut tt = self.nodes[temp].load(Relaxed);
            // find the actual head after this loop
            debug2!("th: {:?} temp: {:?} tt: {:?}", th, temp, tt);
            while State::is_free(tt) {
                // check the head's consistency
                if th != self.head.load(Relaxed) {
                    continue 'retry
                }
                // two consecutive null means empty return
                if temp  == self.tail.load(Acquire) {
                    // two consecutive null means empty return
                    return State::null0()
                }
                temp = (temp + 1) % maxnum; 
                tt = self.nodes[temp].load(Relaxed);
            }
            // check the head's consistency
            if th != self.head.load(Relaxed) {
                continue
            }
            // check whether the queue is empty
            if temp == self.tail.load(Acquire) {
                // help enqueue to update end
                self.tail.compare_and_swap(temp, (temp + 1) % maxnum, Release);
                continue
            }
            let tnull = if (tt as uint) & 1u == 0u {
                State::null1()
            } else {
                State::null0()
            };
            // check the head's consistency
            if th != self.head.load(Relaxed) {
                continue
            }
            // get the actual head, null value means empty
            if self.nodes[temp].compare_and_swap(tt, tnull, Release) == tt {
                if temp % 2 == 0 {
                    self.head.compare_and_swap(th, temp, Release);
                }
                return (tt as uint & -1) as *mut T;
            }
        }
    }
}

fn main() {
    let mut q = Queue::with_capacity(10);
    assert_eq!(None, q.pop());

    for i in range(0, 10) {
        assert!(q.push(~i));
    }

    assert!(!q.push(~0));

    for i in range(0, 10) {
        assert_eq!(Some(~i), q.pop());
    }

    assert_eq!(None, q.pop());
}

#[cfg(test)]
mod tests {
    use std::task;
    use std::comm;
    use super::Queue;

    #[test]
    fn test() {
        let nthreads = 8u;
        let nmsgs = 1000000u;
        let mut q = Queue::with_capacity(nthreads*nmsgs);
        assert_eq!(None, q.pop());

        for _ in range(0, nthreads) {
            let (port, chan)  = comm::stream();
            chan.send(q.clone());
            do task::spawn_sched(task::SingleThreaded) {
                let mut q = port.recv();
                for i in range(0, nmsgs) {
                    assert!(q.push(~i));
                }
            }
        }

        let mut completion_ports = ~[];
        for _ in range(0, nthreads) {
            let (completion_port, completion_chan) = comm::stream();
            completion_ports.push(completion_port);
            let (port, chan)  = comm::stream();
            chan.send(q.clone());
            do task::spawn_sched(task::SingleThreaded) {
                let mut q = port.recv();
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
                completion_chan.send(i);
            }
        }

        for completion_port in completion_ports.iter() {
            assert_eq!(nmsgs, completion_port.recv());
        }
    }
}
