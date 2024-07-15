#[cfg(target_arch = "x86_64")]
pub use std::arch::x86_64::{_blsr_u64, _lzcnt_u32, _mm_tzcnt_64};

#[cfg(not(target_arch = "x86_64"))]
pub unsafe fn _mm_tzcnt_64(x: u64) -> i64 {
    x.trailing_zeros() as i64
}

#[cfg(not(target_arch = "x86_64"))]
pub unsafe fn _blsr_u64(x: u64) -> u64 {
    if x == 0 {
        x
    } else {
        x & (!(1 << x.trailing_zeros()))
    }
}

#[cfg(not(target_arch = "x86_64"))]
pub unsafe fn _lzcnt_u32(x: u32) -> u32 {
    x.leading_zeros()
}
