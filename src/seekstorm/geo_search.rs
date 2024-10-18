use std::cmp::Ordering;

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::{_pdep_u64, _pext_u64};

use crate::{
    index::DistanceUnit,
    search::{Point, SortOrder},
};

#[inline]
fn encode_morton_64_bit(x: u32) -> u64 {
    let mut x = x as u64;
    x = (x | (x << 32)) & 0x00000000ffffffff;
    x = (x | (x << 16)) & 0x0000FFFF0000FFFF;
    x = (x | (x << 8)) & 0x00FF00FF00FF00FF;
    x = (x | (x << 4)) & 0x0F0F0F0F0F0F0F0F;
    x = (x | (x << 2)) & 0x3333333333333333;
    x = (x | (x << 1)) & 0x5555555555555555;
    x
}

/// encode 2D-coordinate (lat/lon) into 64-bit Morton code
/// This method is lossy/quantized as two f64 coordinate values are mapped to a single u64 Morton code!
/// The z-value of a point in multidimensions is simply calculated by interleaving the binary representations of its coordinate values.
#[inline]
pub fn encode_morton_2_d(point: &Point) -> u64 {
    let x_u32 = ((point[0] * 10_000_000.0) as i32) as u32;
    let y_u32 = ((point[1] * 10_000_000.0) as i32) as u32;
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if is_x86_feature_detected!("bmi2") {
            return unsafe {
                _pdep_u64(x_u32.into(), 0x5555555555555555)
                    | _pdep_u64(y_u32.into(), 0xAAAAAAAAAAAAAAAA)
            };
        }
    }

    (encode_morton_64_bit(y_u32) << 1) | encode_morton_64_bit(x_u32)
}

#[inline]
fn decode_morton_64_bit(code: u64) -> u64 {
    let mut x = code & 0x5555555555555555;
    x = (x ^ (x >> 1)) & 0x3333333333333333;
    x = (x ^ (x >> 2)) & 0x0F0F0F0F0F0F0F0F;
    x = (x ^ (x >> 4)) & 0x00FF00FF00FF00FF;
    x = (x ^ (x >> 8)) & 0x0000FFFF0000FFFF;
    x = (x ^ (x >> 16)) & 0x00000000FFFFFFFF;
    x
}

/// decode 64-bit Morton code into 2D-coordinate (lat/lon)
/// This method is lossy/quantized as a single u64 Morton code is converted to two f64 coordinate values!
#[inline]
pub fn decode_morton_2_d(code: u64) -> Point {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if is_x86_feature_detected!("bmi2") {
            let x_u32 = unsafe { _pext_u64(code, 0x5555555555555555) as u32 };
            let y_u32 = unsafe { _pext_u64(code, 0xAAAAAAAAAAAAAAAA) as u32 };

            return vec![
                (x_u32 as i32) as f64 / 10_000_000.0,
                (y_u32 as i32) as f64 / 10_000_000.0,
            ];
        };
    }

    let x_u32 = decode_morton_64_bit(code) as u32;
    let y_u32 = decode_morton_64_bit(code >> 1) as u32;

    vec![
        (x_u32 as i32) as f64 / 10_000_000.0,
        (y_u32 as i32) as f64 / 10_000_000.0,
    ]
}

#[inline]
fn simplified_distance(point1: &Point, point2: &Point) -> f64 {
    let x = (point2[1] - point1[1]) * f64::cos(DEG2RAD * (point1[0] + point2[0]) / 2.0);
    let y = point2[0] - point1[0];

    x * x + y * y
}

/// Comparison of the distances between two morton encoded positions and a base position
pub fn morton_ordering(
    morton1: u64,
    morton2: u64,
    base_point: &Point,
    order: &SortOrder,
) -> Ordering {
    let point1 = decode_morton_2_d(morton1);
    let point2 = decode_morton_2_d(morton2);

    let distance1 = simplified_distance(&point1, base_point);
    let distance2 = simplified_distance(&point2, base_point);

    if order == &SortOrder::Descending {
        distance1
            .partial_cmp(&distance2)
            .unwrap_or(core::cmp::Ordering::Equal)
    } else {
        distance2
            .partial_cmp(&distance1)
            .unwrap_or(core::cmp::Ordering::Equal)
    }
}

const EARTH_RADIUS_KM: f64 = 6371.0087714;
const EARTH_RADIUS_MI: f64 = 3_958.761_315_801_475;
const DEG2RAD: f64 = 0.017_453_292_519_943_295;

/// calculates distance in kilometers or miles between two 2D-coordinates using Euclidian distance (Pythagoras theorem) with Equirectangular approximation.
#[inline]
pub fn euclidian_distance(point1: &Point, point2: &Point, unit: &DistanceUnit) -> f64 {
    let x = DEG2RAD * (point2[1] - point1[1]) * f64::cos(DEG2RAD * (point1[0] + point2[0]) / 2.0);
    let y = DEG2RAD * (point2[0] - point1[0]);

    (if *unit == DistanceUnit::Kilometers {
        EARTH_RADIUS_KM
    } else {
        EARTH_RADIUS_MI
    }) * (x * x + y * y).sqrt()
}

/// Converts a Point and a distance radius into a range of morton_codes for geo search range filtering.
/// The conversion is lossy due to coordinate to Morton code rounding errors and Equirectangular approximation of Euclidian distance.
pub fn point_distance_to_morton_range(
    point: &Point,
    distance: f64,
    unit: &DistanceUnit,
) -> std::ops::Range<u64> {
    let earth_radius = if *unit == DistanceUnit::Kilometers {
        EARTH_RADIUS_KM
    } else {
        EARTH_RADIUS_MI
    };
    let lat_delta = distance / (DEG2RAD * earth_radius);
    let lon_delta = distance / (DEG2RAD * earth_radius * f64::cos(DEG2RAD * point[0]));
    let morton_min = encode_morton_2_d(&vec![point[0] - lat_delta, point[1] - lon_delta]);
    let morton_max = encode_morton_2_d(&vec![point[0] + lat_delta, point[1] + lon_delta]);

    morton_min..morton_max
}
