pub(crate) fn write_u8_ref(value: u8, vec8: &mut [u8], pos: &mut usize) {
    vec8[*pos] = value;
    *pos += 1;
}

pub(crate) fn write_u16_ref(value: u16, vec8: &mut [u8], pos: &mut usize) {
    vec8[*pos..(*pos + 2)].copy_from_slice(&value.to_le_bytes());
    *pos += 2;
}

pub(crate) fn write_u32_ref(value: u32, vec8: &mut [u8], pos: &mut usize) {
    vec8[*pos..(*pos + 4)].copy_from_slice(&value.to_le_bytes());
    *pos += 4;
}

pub(crate) fn write_u64_ref(value: u64, vec8: &mut [u8], pos: &mut usize) {
    vec8[*pos..(*pos + 8)].copy_from_slice(&value.to_le_bytes());
    *pos += 8;
}

pub(crate) fn write_u16(value: u16, vec8: &mut [u8], pos: usize) {
    vec8[pos..(pos + 2)].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_u32(value: u32, vec8: &mut [u8], pos: usize) {
    vec8[pos..(pos + 4)].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_u64(value: u64, vec8: &mut [u8], pos: usize) {
    vec8[pos..(pos + 8)].copy_from_slice(&value.to_le_bytes());
}

#[inline]
pub(crate) fn read_u8_ref(vec8: &[u8], pos: &mut usize) -> u8 {
    *pos += 1;
    vec8[*pos - 1]
}

#[inline]
pub(crate) fn read_u16_ref(vec8: &[u8], pos: &mut usize) -> u16 {
    *pos += 2;
    u16::from_le_bytes(vec8[*pos - 2..*pos].try_into().unwrap())
}

#[inline]
pub(crate) fn read_u32_ref(vec8: &[u8], pos: &mut usize) -> u32 {
    *pos += 4;
    u32::from_le_bytes(vec8[*pos - 4..*pos].try_into().unwrap())
}

#[inline]
pub(crate) fn read_u64_ref(vec8: &[u8], pos: &mut usize) -> u64 {
    *pos += 8;
    u64::from_le_bytes(vec8[*pos - 8..*pos].try_into().unwrap())
}

#[inline]
pub(crate) fn read_u8(vec8: &[u8], pos: usize) -> u8 {
    vec8[pos]
}

#[inline]
pub(crate) fn read_u16(vec8: &[u8], pos: usize) -> u16 {
    u16::from_le_bytes(vec8[pos..pos + 2].try_into().unwrap())
}

#[inline]
pub(crate) fn read_u32(vec8: &[u8], pos: usize) -> u32 {
    u32::from_le_bytes(vec8[pos..pos + 4].try_into().unwrap())
}

#[inline]
pub(crate) fn read_u64(vec8: &[u8], pos: usize) -> u64 {
    u64::from_le_bytes(vec8[pos..pos + 8].try_into().unwrap())
}

pub(crate) fn block_copy_mut(
    source: &mut [u8],
    source_offset: usize,
    destination: &mut [u8],
    destination_offset: usize,
    len: usize,
) {
    destination[destination_offset..(destination_offset + len)]
        .copy_from_slice(&source[source_offset..(source_offset + len)]);
}

pub(crate) fn block_copy(
    source: &[u8],
    source_offset: usize,
    destination: &mut [u8],
    destination_offset: usize,
    len: usize,
) {
    destination[destination_offset..(destination_offset + len)]
        .copy_from_slice(&source[source_offset..(source_offset + len)]);
}

#[allow(clippy::mut_from_ref)]
pub(crate) fn cast_byte_ushort_slice(vec8: &[u8]) -> &mut [u16] {
    let a_ptr = vec8.as_ptr() as *mut u16;
    unsafe { std::slice::from_raw_parts_mut(a_ptr, vec8.len() / 2) }
}

#[allow(clippy::mut_from_ref)]
pub(crate) fn cast_byte_ulong_slice(vec8: &[u8]) -> &mut [u64] {
    let a_ptr = vec8.as_ptr() as *mut u64;
    unsafe { std::slice::from_raw_parts_mut(a_ptr, vec8.len() / 8) }
}
