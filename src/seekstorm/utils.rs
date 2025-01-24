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

pub(crate) fn write_i8(value: i8, vec8: &mut [u8], pos: usize) {
    vec8[pos..(pos + 1)].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_i16(value: i16, vec8: &mut [u8], pos: usize) {
    vec8[pos..(pos + 2)].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_i32(value: i32, vec8: &mut [u8], pos: usize) {
    vec8[pos..(pos + 4)].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_i64(value: i64, vec8: &mut [u8], pos: usize) {
    vec8[pos..(pos + 8)].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_f32(value: f32, vec8: &mut [u8], pos: usize) {
    vec8[pos..(pos + 4)].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn write_f64(value: f64, vec8: &mut [u8], pos: usize) {
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
pub(crate) fn read_i8(vec8: &[u8], pos: usize) -> i8 {
    i8::from_le_bytes(vec8[pos..pos + 1].try_into().unwrap())
}

#[inline]
pub(crate) fn read_u16(vec8: &[u8], pos: usize) -> u16 {
    u16::from_le_bytes(vec8[pos..pos + 2].try_into().unwrap())
}

#[inline]
pub(crate) fn read_i16(vec8: &[u8], pos: usize) -> i16 {
    i16::from_le_bytes(vec8[pos..pos + 2].try_into().unwrap())
}

#[inline]
pub(crate) fn read_u32(vec8: &[u8], pos: usize) -> u32 {
    u32::from_le_bytes(vec8[pos..pos + 4].try_into().unwrap())
}

#[inline]
pub(crate) fn read_i32(vec8: &[u8], pos: usize) -> i32 {
    i32::from_le_bytes(vec8[pos..pos + 4].try_into().unwrap())
}

#[inline]
pub(crate) fn read_u64(vec8: &[u8], pos: usize) -> u64 {
    u64::from_le_bytes(vec8[pos..pos + 8].try_into().unwrap())
}

#[inline]
pub(crate) fn read_i64(vec8: &[u8], pos: usize) -> i64 {
    i64::from_le_bytes(vec8[pos..pos + 8].try_into().unwrap())
}

#[inline]
pub(crate) fn read_f32(vec8: &[u8], pos: usize) -> f32 {
    f32::from_le_bytes(vec8[pos..pos + 8].try_into().unwrap())
}

#[inline]
pub(crate) fn read_f64(vec8: &[u8], pos: usize) -> f64 {
    f64::from_le_bytes(vec8[pos..pos + 8].try_into().unwrap())
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

pub(crate) fn truncate(s: &str, max_chars: usize) -> &str {
    match s.char_indices().nth(max_chars) {
        None => s,
        Some((idx, _)) => &s[..idx],
    }
}
