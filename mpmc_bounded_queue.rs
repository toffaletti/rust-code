/* Multi-producer/multi-consumer bounded queue
 * Copyright (c) 2010-2011 Dmitry Vyukov. All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *    1. Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *
 *    2. Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY DMITRY VYUKOV "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL DMITRY VYUKOV OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * The views and conclusions contained in the software and documentation are
 * those of the authors and should not be interpreted as representing official
 * policies, either expressed or implied, of Dmitry Vyukov.
 */

// http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue

use std::unstable::sync::UnsafeArc;
use std::unstable::atomics::{AtomicUint,Relaxed,Release,Acquire};
use std::vec;
use std::uint;

struct Node<T> {
    sequence: AtomicUint,
    value: Option<T>,
}

struct State<T> {
    pad0: [u8, ..64],
    buffer: ~[Node<T>],
    mask: uint,
    pad1: [u8, ..64],
    enqueue_pos: AtomicUint,
    pad2: [u8, ..64],
    dequeue_pos: AtomicUint,
    pad3: [u8, ..64],
}

struct Queue<T> {
    priv state: UnsafeArc<State<T>>,
}

impl<T: Send> State<T> {
    fn with_capacity(capacity: uint) -> State<T> {
        let capacity = if capacity < 2 || (capacity & (capacity - 1)) != 0 {
            if capacity < 2 {
                2u
            } else {
                uint::next_power_of_two(capacity)
            }
        } else {
            capacity
        };
        let buffer = do vec::from_fn(capacity) |i:uint| {
            Node{sequence:AtomicUint::new(i),value:None}
        };
        State{
            pad0: [0, ..64],
            buffer: buffer,
            mask: capacity-1,
            pad1: [0, ..64],
            enqueue_pos: AtomicUint::new(0),
            pad2: [0, ..64],
            dequeue_pos: AtomicUint::new(0),
            pad3: [0, ..64],
        }
    }

    fn push(&mut self, value: T) -> bool {
        let buffer_len = self.buffer.len();
        let mask = self.mask;
        let mut pos = self.enqueue_pos.load(Relaxed);
        loop {
            let node = &mut self.buffer[pos & mask];
            let seq = node.sequence.load(Acquire);
            let diff: i64 = seq as i64 - pos as i64;

            if diff == 0 {
                let enqueue_pos = self.enqueue_pos.compare_and_swap(pos, pos+1, Relaxed);
                if enqueue_pos == pos {
                    node.value = Some(value);
                    node.sequence.store(pos+1, Release);
                    break
                } else {
                    pos = enqueue_pos;
                }
            } else if diff < 0 {
                return false
            } else if pos == 0 && (seq-1) as uint % buffer_len == 0 {
                // handle the case where enqueue_pos has overflowed
                // back to 0 but the queue is full
                return false
            } else {
                pos = self.enqueue_pos.load(Relaxed);
            }
        }
        true
    }

    fn pop(&mut self) -> Option<T> {
        let mask = self.mask;
        let mut pos = self.dequeue_pos.load(Relaxed);
        loop {
            let node = &mut self.buffer[pos & mask];
            let seq = node.sequence.load(Acquire);
            let diff: i64 = seq as i64 - (pos + 1) as i64;
            if diff == 0 || (seq == 0 && pos == uint::max_value) {
                // the part after || handles the case where
                // pos+1 would overflow back to 0 causing
                // diff to be negative and thus dequeue to fail
                // when there is infact data in the queue
                let dequeue_pos = self.dequeue_pos.compare_and_swap(pos, pos+1, Relaxed);
                if dequeue_pos == pos {
                    let value = node.value.take();
                    node.sequence.store(pos + mask + 1, Release);
                    return value
                } else {
                    pos = dequeue_pos;
                }
            } else if diff < 0 {
                return None
            } else {
                pos = self.dequeue_pos.load(Relaxed);
            }
        }
    }
}

impl<T: Send> Queue<T> {
    pub fn with_capacity(capacity: uint) -> Queue<T> {
        Queue{
            state: UnsafeArc::new(State::with_capacity(capacity))
        }
    }

    pub fn push(&mut self, value: T) -> bool {
        unsafe { (*self.state.get()).push(value) }
    }

    pub fn pop(&mut self) -> Option<T> {
        unsafe { (*self.state.get()).pop() }
    }
}

impl<T: Send> Clone for Queue<T> {
    fn clone(&self) -> Queue<T> {
        Queue {
            state: self.state.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::task;
    use std::comm;
    use super::Queue;

    #[test]
    fn test() {
        let nthreads = 8u;
        let nmsgs = 1000u;
        let mut q = Queue::with_capacity(nthreads*nmsgs);
        assert_eq!(None, q.pop());

        for _ in range(0, nthreads) {
            let (port, chan)  = comm::stream();
            chan.send(q.clone());
            do task::spawn_sched(task::SingleThreaded) {
                let mut q = port.recv();
                for i in range(0, nmsgs) {
                    assert!(q.push(i));
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
