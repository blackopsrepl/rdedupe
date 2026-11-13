use crate::error::{Error, Result};
use std::fs::File;
use std::os::fd::AsRawFd;

/// `_IOWR(0x94, 54, struct file_dedupe_range)` from `linux/fs.h`. glibc declares the
/// `ioctl` request as `unsigned long`, musl as `int`; the cast keeps the same 32 bits.
const FIDEDUPERANGE: libc::Ioctl = 0xc018_9436_u32 as libc::Ioctl;
const FILE_DEDUPE_RANGE_SAME: i32 = 0;
const FILE_DEDUPE_RANGE_DIFFERS: i32 = 1;

#[repr(C)]
struct FileDedupeRange {
    src_offset: u64,
    src_length: u64,
    dest_count: u16,
    reserved1: u16,
    reserved2: u32,
    info: FileDedupeRangeInfo,
}

#[repr(C)]
struct FileDedupeRangeInfo {
    dest_fd: i64,
    dest_offset: u64,
    bytes_deduped: u64,
    status: i32,
    reserved: u32,
}

/// Outcome of one kernel compare-and-share request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeOutcome {
    /// The kernel verified equality and shared this many bytes (possibly fewer than requested).
    Shared(u64),
    /// The kernel found differing bytes and shared nothing.
    Differs,
}

pub fn dedupe_range(src: &File, dst: &File, offset: u64, length: u64) -> Result<RangeOutcome> {
    let mut request = FileDedupeRange {
        src_offset: offset,
        src_length: length,
        dest_count: 1,
        reserved1: 0,
        reserved2: 0,
        info: FileDedupeRangeInfo {
            dest_fd: dst.as_raw_fd() as i64,
            dest_offset: offset,
            bytes_deduped: 0,
            status: 0,
            reserved: 0,
        },
    };
    let rc = unsafe { libc::ioctl(src.as_raw_fd(), FIDEDUPERANGE, &mut request) };
    if rc < 0 {
        return Err(Error::Capability(format!(
            "FIDEDUPERANGE ioctl failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    match request.info.status {
        FILE_DEDUPE_RANGE_SAME => Ok(RangeOutcome::Shared(request.info.bytes_deduped)),
        FILE_DEDUPE_RANGE_DIFFERS => Ok(RangeOutcome::Differs),
        status if status < 0 => Err(Error::Capability(format!(
            "FIDEDUPERANGE destination failed: {}",
            std::io::Error::from_raw_os_error(-status)
        ))),
        status => Err(Error::Capability(format!(
            "FIDEDUPERANGE returned unknown destination status {status}"
        ))),
    }
}
