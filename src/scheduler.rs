use crate::error::{Error, Result};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

#[derive(Clone, Default)]
pub struct Cancellation(Arc<AtomicBool>);

/// Stage and periodic progress written to stderr when enabled; stdout carries results only.
#[derive(Debug, Clone, Copy, Default)]
pub struct Progress {
    pub enabled: bool,
}

impl Progress {
    pub fn report(&self, message: impl std::fmt::Display) {
        if self.enabled {
            eprintln!("rdedupe: {message}");
        }
    }

    /// Reports every `PERIOD`th item of a stage.
    pub fn tick(&self, stage: &str, count: u64) {
        const PERIOD: u64 = 4096;
        if self.enabled && count % PERIOD == 0 && count > 0 {
            eprintln!("rdedupe: {stage}: {count}");
        }
    }
}

impl Cancellation {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(Error::Cancelled)
        } else {
            Ok(())
        }
    }
}

/// The scan's read buffers: `count` buffers of `size` bytes, allocated once and lent out.
/// A lease returns its buffer on drop; `take` blocks while all are lent (backpressure), so
/// the scan never holds more buffer memory than the budget accounts for.
pub struct BufferPool {
    free: Mutex<Free>,
    available: Condvar,
    size: usize,
}

struct Free {
    buffers: Vec<Vec<u8>>,
    /// Threads blocked in [`BufferPool::take`]. Returning a buffer wakes one only when
    /// this is non-zero: an unconditional `notify_one` costs a system call per return.
    waiting: usize,
}

pub struct Lease<'a> {
    pool: &'a BufferPool,
    buffer: Option<Vec<u8>>,
}

impl BufferPool {
    pub fn new(count: usize, size: usize) -> Self {
        Self {
            free: Mutex::new(Free {
                buffers: (0..count.max(1)).map(|_| vec![0; size]).collect(),
                waiting: 0,
            }),
            available: Condvar::new(),
            size,
        }
    }

    pub fn buffer_size(&self) -> usize {
        self.size
    }

    pub fn take(&self) -> Lease<'_> {
        let mut free = self.free.lock().expect("buffer pool");
        loop {
            if let Some(buffer) = free.buffers.pop() {
                return Lease {
                    pool: self,
                    buffer: Some(buffer),
                };
            }
            free.waiting += 1;
            free = self.available.wait(free).expect("buffer pool");
            free.waiting -= 1;
        }
    }
}

impl std::ops::Deref for Lease<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.buffer.as_deref().expect("leased buffer")
    }
}

impl std::ops::DerefMut for Lease<'_> {
    fn deref_mut(&mut self) -> &mut [u8] {
        self.buffer.as_deref_mut().expect("leased buffer")
    }
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            let mut free = self.pool.free.lock().expect("buffer pool");
            free.buffers.push(buffer);
            if free.waiting > 0 {
                self.pool.available.notify_one();
            }
        }
    }
}

/// Applies `work` to every item of one bounded page using at most `readers` threads.
///
/// Each worker leases one buffer from `pool` for the page, so the page costs at most
/// `readers` buffers (reused across pages and stages) and `readers` concurrently open
/// input descriptors. The caller feeds pages of a fixed maximum size, which bounds the
/// result vector; there is no unbounded queue between discovery and reading. Results are
/// returned in input order.
pub fn bounded_map<T, R, F>(items: &[T], readers: usize, pool: &BufferPool, work: F) -> Vec<R>
where
    T: Sync,
    R: Send,
    F: Fn(&T, &mut [u8]) -> R + Sync,
{
    let workers = readers.max(1).min(items.len().max(1));
    if workers == 1 {
        let mut buffer = pool.take();
        return items.iter().map(|item| work(item, &mut buffer)).collect();
    }
    let next = AtomicUsize::new(0);
    let slots: Mutex<Vec<Option<R>>> = Mutex::new((0..items.len()).map(|_| None).collect());
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                let mut buffer = pool.take();
                loop {
                    let index = next.fetch_add(1, Ordering::SeqCst);
                    if index >= items.len() {
                        break;
                    }
                    // Cancellation is observed inside `work` (it returns an error value), so
                    // workers still fill every slot of the page and the result is total.
                    let result = work(&items[index], &mut buffer);
                    slots.lock().expect("result slots")[index] = Some(result);
                }
            });
        }
    });
    slots
        .into_inner()
        .expect("result slots")
        .into_iter()
        .map(|slot| slot.expect("every index is processed exactly once"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_blocked_take_is_woken_when_a_buffer_returns() {
        let pool = BufferPool::new(1, 16);
        let held = pool.take();
        std::thread::scope(|scope| {
            let waiter = scope.spawn(|| pool.take().len());
            std::thread::sleep(std::time::Duration::from_millis(50));
            drop(held);
            assert_eq!(waiter.join().unwrap(), 16);
        });
    }
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn preserves_order_and_never_exceeds_reader_limit() {
        let active = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let items: Vec<usize> = (0..200).collect();
        let pool = BufferPool::new(4, 4096);
        let out = bounded_map(&items, 3, &pool, |item, buffer| {
            assert_eq!(buffer.len(), 4096);
            let now = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            std::thread::yield_now();
            active.fetch_sub(1, Ordering::SeqCst);
            item * 2
        });
        assert_eq!(out, items.iter().map(|v| v * 2).collect::<Vec<_>>());
        assert!(peak.load(Ordering::SeqCst) <= 3);
    }
}
