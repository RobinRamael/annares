use generic_array::{arr, ArrayLength, GenericArray};
use std::cmp;
use std::ops;

#[derive(PartialEq, Eq, Debug)]
struct Arr3u8<N: ArrayLength<u8>> {
    //big endian!
    arr: GenericArray<u8, N>,
}

impl<N> Arr3u8<N>
where
    N: ArrayLength<u8>,
{
    fn new(arr: GenericArray<u8, N>) -> Self {
        Arr3u8 { arr }
    }
}

impl<N> cmp::PartialOrd for Arr3u8<N>
where
    N: ArrayLength<u8> + std::cmp::PartialEq,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.arr.partial_cmp(&other.arr)
    }
}

impl<N> ops::Sub for Arr3u8<N>
where
    N: ArrayLength<u8>,
{
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        // assert!(self >= rhs);

        let mut borrow = 0;

        let mut result: Self = Arr3u8::new(GenericArray::default());

        for (i, (n1, n2)) in self.arr.iter().zip(rhs.arr.iter()).enumerate() {
            let borrowed_n1 = n1 - (borrow * 1);

            let d: u8;

            if n1 >= n2 {
                d = borrowed_n1 - n2;
                borrow = 0;
            } else {
                d = 255 - (n2 - borrowed_n1) + 1;
                borrow = 1;
            }
            result.arr[i] = d as u8;
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_dist3() {
        let n1 = Arr3u8::new(arr![u8; 231, 45, 186]);
        let n2 = Arr3u8::new(arr![u8; 134, 251, 76]);
        let d = Arr3u8::new(arr![u8; 97, 50, 109]);

        assert_eq!(n1 - n2, d);
    }

    #[test]
    fn test_dist3_eq() {
        let n1 = Arr3u8::new(arr![u8; 231, 45, 186]);
        let n2 = Arr3u8::new(arr![u8; 231, 45, 186]);
        let zero = Arr3u8::new(arr![u8; 0,0,0]);

        assert_eq!(n1 - n2, zero);
    }
}
