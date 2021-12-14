use generic_array::{ArrayLength, GenericArray};
use num::pow;
use std::cmp;
use std::cmp::Ordering;
use std::ops;

use crate::utils::shorten;

use sha2::{Digest, Sha256};

#[derive(PartialEq, Eq, Clone, std::hash::Hash, Copy)]
pub struct Arru8<N: ArrayLength<u8> + Eq>
where
    N::ArrayType: Copy,
{
    //big endian!
    pub arr: GenericArray<u8, N>,
}

pub type Hash = Arru8<<Sha256 as Digest>::OutputSize>;
pub type Key = Hash;

impl Hash {
    pub fn hash(s: &String) -> Self {
        let mut hasher = Sha256::default();

        hasher.update(s);

        let result = hasher.finalize();

        Hash::new(result)
    }

    pub fn from_addr(addr: std::net::SocketAddr) -> Self {
        Hash::hash(&addr.to_string())
    }
}

impl<N> std::fmt::LowerHex for Arru8<N>
where
    N: ArrayLength<u8> + Eq,
    N::ArrayType: Copy,
{
    fn fmt(&self, fmtr: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        for byte in self.arr.iter() {
            write!(fmtr, "{:02x}", byte)?;
        }
        Ok(())
    }
}

impl<N> TryFrom<String> for Arru8<N>
where
    N: ArrayLength<u8> + Eq,
    N::ArrayType: Copy,
{
    type Error = hex::FromHexError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        let hex_vec = hex::decode(s)?;
        let mut arr: GenericArray<u8, N> = GenericArray::default();

        if hex_vec.len() == N::to_usize() {
            hex_vec.iter().enumerate().for_each(|(i, x)| {
                arr[i] = *x;
            });
            Ok(Self::new(arr))
        } else {
            Err(hex::FromHexError::InvalidStringLength)
        }
    }
}

impl<N> std::fmt::Display for Arru8<N>
where
    N: ArrayLength<u8> + Eq,
    N::ArrayType: Copy,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        if N::to_usize() < 32 {
            write!(f, "{}", self.as_hex_string())
        } else {
            write!(f, "{}", shorten(&self.as_hex_string()))
        }
    }
}

impl<N> std::fmt::Debug for Arru8<N>
where
    N: ArrayLength<u8> + Eq,
    N::ArrayType: Copy,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        if N::to_usize() < 32 {
            write!(f, "Arru8<{}> {{ {:?} }}", N::to_usize(), self.arr)
        } else {
            write!(f, "Hash {{ {} }}", shorten(&self.as_hex_string()))
        }
    }
}

impl<N> Arru8<N>
where
    N: ArrayLength<u8> + Eq,
    N::ArrayType: Copy,
{
    pub fn new(arr: GenericArray<u8, N>) -> Self {
        Arru8 { arr }
    }

    fn max() -> Self {
        let mut max_array = GenericArray::default();
        for n in &mut max_array {
            *n = u8::MAX;
        }
        Arru8::new(max_array)
    }

    fn one() -> Self {
        let mut one = GenericArray::default();
        one[0] = 1;
        Arru8::new(one)
    }

    fn halfway() -> Self {
        let mut halfway = GenericArray::default();
        halfway[N::to_usize() - 1] = 128;
        Arru8::new(halfway)
    }

    pub fn two_to_the_power_of(k: usize) -> Self {
        let mut res = GenericArray::default();
        let m = k % 8;
        let idx = (k - m) / 8;
        res[idx] = pow(2u8, m);
        Arru8::new(res)
    }

    fn cyclic_sub(self, rhs: Self) -> Self {
        if self.ge(&rhs) {
            self - rhs
        } else {
            Self::max() - (rhs - self - Self::one())
        }
    }

    pub fn cyclic_add(self, rhs: Self) -> Self {
        if self > rhs {
            rhs.cyclic_add(self)
        } else {
            if self <= Self::max() - rhs {
                self + rhs
            } else {
                self - (Self::max() - rhs + Self::one())
            }
        }
    }

    pub fn cyclic_distance(&self, rhs: &Self) -> Self {
        let lhs = self.clone(); // there has to be a better way...
        let d1 = self.clone().cyclic_sub(rhs.clone());
        let d2 = rhs.clone().cyclic_sub(lhs);

        if d1 <= d2 {
            d1
        } else {
            d2
        }
    }

    pub fn as_hex_string(&self) -> String {
        format!("{:02x}", self)
    }

    pub fn in_cyclic_range(&self, anticlockwise_bound: &Self, clockwise_bound: &Self) -> bool {
        match anticlockwise_bound.cmp(&clockwise_bound) {
            Ordering::Less => (anticlockwise_bound <= self) && (self <= clockwise_bound),
            Ordering::Greater => !self.in_cyclic_range(clockwise_bound, anticlockwise_bound),
            Ordering::Equal => true,
        }
    }
}

impl<N> cmp::PartialOrd for Arru8<N>
where
    N: ArrayLength<u8> + Eq + Copy,
    <N as ArrayLength<u8>>::ArrayType: Copy,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<N> cmp::Ord for Arru8<N>
where
    N: ArrayLength<u8> + Eq + Copy,
    <N as ArrayLength<u8>>::ArrayType: Copy,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for (n1, n2) in self.arr.iter().zip(other.arr.iter()).rev() {
            if n1 != n2 {
                return n1.cmp(n2);
            }
        }

        cmp::Ordering::Equal
    }
}

impl<N> ops::Sub for Arru8<N>
where
    N: ArrayLength<u8> + Eq + Copy,
    <N as ArrayLength<u8>>::ArrayType: Copy,
{
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        let mut borrow = 0;

        let mut result: Self = Arru8::new(GenericArray::default());

        for (i, (n1, n2)) in self.arr.iter().zip(rhs.arr.iter()).enumerate() {
            let d: u8;

            if *n1 == 0 && borrow == 1 {
                // special case where we have to borrow from a 0
                // the current 0 borrows from the next one to become 256
                // and lends 1 to the previous one, becoming 255. my brain hurts.
                d = 255 - n2;
                borrow = 1;
            } else {
                let borrowed_n1 = n1 - (borrow * 1);
                if &borrowed_n1 >= n2 {
                    d = borrowed_n1 - n2;
                    borrow = 0;
                } else {
                    // we do this instead of simply n2 + 256 - borrowed_n1
                    // to ensure we stay within the bounds of u8:
                    d = 255 - (n2 - borrowed_n1) + 1;
                    borrow = 1;
                }
            }

            result.arr[i] = d;
        }

        result
    }
}

fn carry_add(a: u8, b: u8, carry: u8) -> (u8, u8) {
    assert!(carry == 1 || carry == 0);
    // holy branching, batman!
    if a > b {
        return carry_add(b, a, carry);
    }

    if carry == 1 {
        if a == 255 {
            return (255, 1);
        } else {
            return carry_add(a + 1, b, 0);
        }
    }

    if a <= 255 - b {
        (a + b, 0)
    } else {
        (a - (255 - b + 1), 1)
    }
}

impl<N> ops::Add for Arru8<N>
where
    N: ArrayLength<u8> + Eq + Copy,
    <N as ArrayLength<u8>>::ArrayType: Copy,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        let mut carry: u8 = 0;
        let mut result: Self = Arru8::new(GenericArray::default());

        for (i, (a, b)) in self.arr.into_iter().zip(rhs.arr.into_iter()).enumerate() {
            let (s, new_carry) = carry_add(a, b, carry);

            result.arr[i] = s;
            carry = new_carry;
        }

        assert!(carry == 0, "Overflow!");

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use generic_array::arr;

    #[test]
    fn test_sub3() {
        let n1 = Arru8::new(arr![u8; 231, 45, 186]);
        let n2 = Arru8::new(arr![u8; 134, 251, 76]);
        let d = Arru8::new(arr![u8; 97, 50, 109]);

        assert_eq!(n1 - n2, d);
    }

    #[test]
    fn test_sub1() {
        let n1 = Arru8::new(arr![u8;  255]);
        let n2 = Arru8::new(arr![u8;  48]);
        let d = Arru8::new(arr![u8; 255 - 48]);

        assert_eq!(n1 - n2, d);
    }

    #[test]
    fn test_sub2() {
        let n1 = Arru8::new(arr![u8;  1, 1, 255]);
        let n2 = Arru8::new(arr![u8;  255, 255, 1]);
        let d = Arru8::new(arr![u8; 2, 1, 253]);

        assert_eq!(n1 - n2, d);
    }

    #[test]
    fn test_sub3_eq() {
        let n1 = Arru8::new(arr![u8; 231, 45, 186]);
        let n2 = Arru8::new(arr![u8; 231, 45, 186]);
        let zero = Arru8::new(arr![u8; 0,0,0]);

        assert_eq!(n1 - n2, zero);
    }

    #[test]
    fn test_sub4() {
        let n1 = Arru8::new(arr![u8; 231, 45, 186]);
        let n2 = Arru8::new(arr![u8; 230, 45, 185]);
        let zero = Arru8::new(arr![u8; 1,0,1]);

        assert_eq!(n1 - n2, zero);
    }

    #[test]
    fn test_sub5() {
        let n1 = Arru8::new(arr![u8; 1, 45, 255]);
        let n2 = Arru8::new(arr![u8; 255, 45, 1]);
        let zero = Arru8::new(arr![u8; 2, 255, 253]);

        assert_eq!(n1 - n2, zero);
    }

    #[test]
    fn test_sub6() {
        let n1 = Arru8::new(arr![u8; 1, 0, 2]);
        let n2 = Arru8::new(arr![u8; 2, 0, 1]);
        let zero = Arru8::new(arr![u8; 255, 255, 0]);

        assert_eq!(n1 - n2, zero);
    }

    #[test]
    fn test_sub7() {
        let n1 = Arru8::new(arr![u8; 1, 0, 0, 2]);
        let n2 = Arru8::new(arr![u8; 2, 0, 0, 1]);
        let zero = Arru8::new(arr![u8; 255, 255, 255, 0]);

        assert_eq!(n1 - n2, zero);
    }

    #[test]
    fn test_ge() {
        let n1 = Arru8::new(arr![u8; 1, 0, 0, 2]);
        let n2 = Arru8::new(arr![u8; 2, 0, 0, 1]);

        assert!(n1 > n2);
    }

    #[test]
    fn test_cyclic_sub() {
        let n1 = Arru8::new(arr![u8;  48]);
        let n2 = Arru8::new(arr![u8;  134]);
        let d = Arru8::new(arr![u8; 86]);

        assert_eq!(n1.cyclic_distance(&n2), d);
    }

    #[test]
    fn test_cyclic_dist1() {
        let n1 = Arru8::new(arr![u8;  255]);
        let n2 = Arru8::new(arr![u8;  0]);

        let d = Arru8::new(arr![u8; 1]);

        assert_eq!(n1.cyclic_distance(&n2), d);
    }

    #[test]
    fn test_cyclic_dist2() {
        let n1 = Arru8::new(arr![u8;  255, 255, 255, 255, 255, 255, 255]);
        let n2 = Arru8::new(arr![u8;  0, 0, 0, 0, 0, 0, 0]);

        let d = Arru8::new(arr![u8; 1, 0, 0, 0, 0, 0, 0]);

        assert_eq!(n1.cyclic_distance(&n2), d);
    }

    #[test]
    fn test_cyclic_dist3() {
        let n1 = Arru8::new(arr![u8;  8, 56, 133, 7, 201]);
        let n2 = Arru8::new(arr![u8;  58, 23, 133, 6, 52]);

        let d = Arru8::new(arr![u8; 50, 223, 255, 254, 106]);

        assert_eq!(n1.cyclic_distance(&n2), d);
    }

    #[test]
    fn test_cyclic_dist_eq() {
        let n1 = Arru8::new(arr![u8;  251, 54, 6]);
        let n2 = Arru8::new(arr![u8;  251, 54, 6]);

        let d = Arru8::new(arr![u8; 0, 0, 0]);

        assert_eq!(n1.cyclic_distance(&n2), d);
    }

    #[test]
    fn test_cyclic_dist4() {
        let n2 = Arru8::new(arr![u8;  16, 112]);
        let n1 = Arru8::new(arr![u8;  208, 139]);

        let d = Arru8::new(arr![u8; 192, 27]);

        assert_eq!(n1.cyclic_distance(&n2), d);
    }

    #[test]
    fn test_in_cyclic_range1() {
        let n_1 = Arru8::new(arr![u8;  16, 112]);
        let n_2 = Arru8::new(arr![u8;  208, 139]);

        let n_m = Arru8::new(arr![u8; 192, 118]);

        assert!(n_m.in_cyclic_range(&n_1, &n_2));
    }

    #[test]
    fn test_in_cyclic_range2() {
        let n_1 = Arru8::new(arr![u8;  16, 255]);
        let n_2 = Arru8::new(arr![u8;  208, 4]);

        let n_m = Arru8::new(arr![u8; 192, 1]);

        assert!(n_m.in_cyclic_range(&n_1, &n_2));
        assert!(!n_m.in_cyclic_range(&n_2, &n_1));
    }

    #[test]
    fn test_in_cyclic_range3() {
        let n_1 = Arru8::new(arr![u8;  16, 255]);
        let n_2 = Arru8::new(arr![u8;  208, 4]);

        let n_m = Arru8::new(arr![u8; 192, 45]);

        assert!(!n_m.in_cyclic_range(&n_1, &n_2));
    }
    #[test]
    fn test_in_cyclic_range4() {
        let n_1 = Arru8::new(arr![u8;  16, 243]);
        let n_2 = Arru8::new(arr![u8;  16, 243]);

        let n_m = Arru8::new(arr![u8; 192, 45]);

        assert!(n_m.in_cyclic_range(&n_1, &n_2));
    }

    #[test]
    fn test_in_cyclic_range5() {
        let n_1 = Arru8::new(arr![u8;  16, 243]);
        let n_2 = Arru8::new(arr![u8;  16, 243]);

        let n_m = Arru8::new(arr![u8; 16, 243]);

        assert!(n_m.in_cyclic_range(&n_1, &n_2));
    }

    #[test]
    fn test_in_cyclic_range6() {
        let n_1 = Arru8::new(arr![u8;  16, 243]);
        let n_2 = Arru8::new(arr![u8;  16, 243]);

        let n_m = Arru8::new(arr![u8; 17, 243]);

        assert!(n_m.in_cyclic_range(&n_1, &n_2));
    }

    #[test]
    fn test_2pow() {
        assert_eq!(
            Arru8::<U3>::two_to_the_power_of(0),
            Arru8::new(arr![u8; 1, 0, 0])
        );
        assert_eq!(
            Arru8::<U3>::two_to_the_power_of(1),
            Arru8::new(arr![u8; 2, 0, 0])
        );
        assert_eq!(
            Arru8::<U3>::two_to_the_power_of(2),
            Arru8::new(arr![u8; 4, 0, 0])
        );
        assert_eq!(
            Arru8::<U3>::two_to_the_power_of(7),
            Arru8::new(arr![u8; 128, 0, 0])
        );
        assert_eq!(
            Arru8::<U3>::two_to_the_power_of(8),
            Arru8::new(arr![u8; 0, 1, 0])
        );
        assert_eq!(
            Arru8::<U3>::two_to_the_power_of(9),
            Arru8::new(arr![u8; 0, 2, 0])
        );
        assert_eq!(
            Arru8::<U3>::two_to_the_power_of(16),
            Arru8::new(arr![u8; 0, 0, 1])
        );
        assert_eq!(
            Arru8::<U3>::two_to_the_power_of(23),
            Arru8::new(arr![u8; 0, 0, 128])
        );
    }

    #[test]
    fn test_carry_add() {
        assert_eq!(carry_add(1, 1, 0), (2, 0));
        assert_eq!(carry_add(1, 1, 1), (3, 0));

        assert_eq!(carry_add(200, 60, 0), (4, 1));
        assert_eq!(carry_add(200, 60, 1), (5, 1));
        assert_eq!(carry_add(60, 200, 0), (4, 1));
        assert_eq!(carry_add(60, 200, 1), (5, 1));

        assert_eq!(carry_add(0, 0, 0), (0, 0));
        assert_eq!(carry_add(0, 0, 1), (1, 0));

        assert_eq!(carry_add(255, 255, 0), (254, 1));
        assert_eq!(carry_add(255, 255, 1), (255, 1));

        assert_eq!(carry_add(0, 254, 1), (255, 0));
    }

    #[test]
    fn test_add() {
        assert_eq!(
            Arru8::new(arr![u8; 0, 0, 2]) + Arru8::new(arr![u8; 0, 0, 1]),
            Arru8::new(arr![u8; 0, 0, 3])
        );

        assert_eq!(
            Arru8::new(arr![u8; 200, 0, 2]) + Arru8::new(arr![u8; 60, 0, 1]),
            Arru8::new(arr![u8; 4, 1, 3])
        );

        assert_eq!(
            Arru8::new(arr![u8; 255, 255, 254]) + Arru8::new(arr![u8; 255, 255, 0]),
            Arru8::new(arr![u8; 254, 255, 255])
        );
    }

    #[test]
    fn test_cyclic_add() {
        assert_eq!(
            Arru8::new(arr![u8; 0, 0, 2]).cyclic_add(Arru8::new(arr![u8; 0, 0, 1])),
            Arru8::new(arr![u8; 0, 0, 3])
        );

        assert_eq!(
            Arru8::new(arr![u8; 1, 2, 200]).cyclic_add(Arru8::new(arr![u8; 4, 10, 60])),
            Arru8::new(arr![u8; 5, 12, 4])
        );

        assert_eq!(
            Arru8::new(arr![u8; 0, 0, 255]).cyclic_add(Arru8::new(arr![u8; 0, 0, 1])),
            Arru8::new(arr![u8; 0, 0, 0])
        );

        assert_eq!(
            Arru8::new(arr![u8; 255, 255, 255]).cyclic_add(Arru8::new(arr![u8; 255, 255, 255])),
            Arru8::new(arr![u8; 254, 255, 255])
        );
    }

    extern crate quickcheck;
    use generic_array::typenum::{Unsigned, U3};
    use quickcheck::*;

    impl<N> Arbitrary for Arru8<N>
    where
        N: ArrayLength<u8> + Eq + Copy,
        <N as ArrayLength<u8>>::ArrayType: Copy,
    {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut arr = GenericArray::default();
            for n in arr.iter_mut() {
                *n = u8::arbitrary(g);
            }

            Arru8::<_> { arr: arr.into() }
        }
    }

    quickcheck! {
        fn symmetric(h1: Arru8<U3>, h2: Arru8<U3>) -> bool {
            let h1c = h1.clone();
            let h2c = h2.clone();
            h1.cyclic_distance(&h2c) == h2.cyclic_distance(&h1c)
        }

        fn smaller_than_half(h1: Arru8<U3>, h2: Arru8<U3>) -> bool {
            let d = h1.cyclic_distance(&h2);
            let mut half_max: [u8; 3]= [0; 3];
            half_max[U3::to_usize() - 1] = 128;

            d <= Arru8::<U3> {arr: half_max.into()}
        }

        fn hex_parse_format(h1: Arru8<U3>) -> bool {
            let hex_s = h1.as_hex_string();
            h1 == hex_s.try_into().unwrap()
        }

        fn hex_parse_format_hash(h1: Hash) -> bool {
            let hex_s = h1.as_hex_string();
            h1 == hex_s.try_into().unwrap()
        }
    }
}

impl Into<String> for Key {
    fn into(self) -> String {
        self.as_hex_string()
    }
}