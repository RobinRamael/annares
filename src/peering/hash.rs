use generic_array::{ArrayLength, GenericArray};
use std::cmp;
use std::ops;

use crate::peering::utils::shorten;

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
        write!(f, "{}", self.as_hex_string())
    }
}

impl<N> std::fmt::Debug for Arru8<N>
where
    N: ArrayLength<u8> + Eq,
    N::ArrayType: Copy,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        if N::to_usize() < 32 {
            write!(
                f,
                "Arru8<{}> {{ {} }}",
                N::to_usize(),
                &self.as_hex_string()
            )
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

    fn cyclic_sub(self, rhs: Self) -> Self {
        if self.ge(&rhs) {
            self - rhs
        } else {
            Self::max() - (rhs - self - Self::one())
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
