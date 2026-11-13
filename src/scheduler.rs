use crate::error::{Error, Result};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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

/// Applies `work` to every item of one bounded page using at most `readers` threads.
///
/// Each worker owns exactly one `buffer_bytes` buffer for its lifetime, so the page costs at
/// most `readers` buffers and `readers` concurrently open input descriptors. The caller
/// feeds pages of a fixed maximum size, which bounds the result vector; there is no
/// unbounded queue between discovery and reading. Results are returned in input order.
pub fn bounded_map<T, R, F>(items: &[T], readers: usize, buffer_bytes: usize, work: F) -> Vec<R>
where
    T: Sync,
    R: Send,
    F: Fn(&T, &mut [u8]) -> R + Sync,
{
    let workers = readers.max(1).min(items.len().max(1));
    if workers == 1 {
        let mut buffer = vec![0; buffer_bytes];
        return items.iter().map(|item| work(item, &mut buffer)).collect();
    }
    let next = AtomicUsize::new(0);
    let slots: Mutex<Vec<Option<R>>> = Mutex::new((0..items.len()).map(|_| None).collect());
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                let mut buffer = vec![0; buffer_bytes];
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
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn preserves_order_and_never_exceeds_reader_limit() {
        let active = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let items: Vec<usize> = (0..200).collect();
        let out = bounded_map(&items, 3, 4096, |item, buffer| {
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
