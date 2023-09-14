use arrow2::{array::PrimitiveArray, trusted_len::TrustedLen, types::NativeType};

// stealing from polars

pub(crate) fn merge_native<'a, T>(
    a: &'a PrimitiveArray<T>,
    b: &'a PrimitiveArray<T>,
    merge_indicator: &[bool],
) -> PrimitiveArray<T>
where
    T: NativeType + 'static,
{
    let total_len = a.len() + b.len();
    let mut a = a.into_iter();
    let mut b = b.into_iter();

    let iter = merge_indicator.iter().map(|a_indicator| {
        if *a_indicator {
            a.next().unwrap()
        } else {
            b.next().unwrap()
        }
    }).map(|o| o.copied());

    // Safety: length is correct
    let iter = TrustMyLength::new(iter, total_len);
    PrimitiveArray::from_trusted_len_iter(iter)
}

pub(crate) fn merge_dedup_native<'a, T>(
    a: &'a PrimitiveArray<T>,
    b: &'a PrimitiveArray<T>,
    merge_indicator: &[bool],
) -> PrimitiveArray<T>
where
    T: NativeType + 'static,
{
    let total_len = a.len() + b.len();
    let mut a = a.into_iter().peekable();
    let mut b = b.into_iter().peekable();

    let iter = merge_indicator.iter().map(move |a_indicator| {
        if *a_indicator {
            let next = a.next().flatten();
            if b.peek().is_some() && b.peek().unwrap().copied().as_ref() == next {
                b.next(); // we can skip this one
            }
            next
        } else {
            let next = b.next().flatten();
            if a.peek().is_some() && a.peek().unwrap().copied().as_ref() == next {
                a.next(); // we can skip this one
            }
            next
        }
    }).map(|o| o.copied());

    println!("total len: {}", total_len);

    // Safety: length is correct
    let iter = TrustMyLength::new(iter, total_len);
    PrimitiveArray::from_trusted_len_iter(iter)
}

pub(crate) fn get_merge_indicator<T>(
    mut a_iter: impl TrustedLen<Item = T>,
    mut b_iter: impl TrustedLen<Item = T>,
) -> Vec<bool>
where
    T: PartialOrd + Default + Copy,
{
    const A_INDICATOR: bool = true;
    const B_INDICATOR: bool = false;

    let a_len = a_iter.size_hint().0;
    let b_len = b_iter.size_hint().0;
    if a_len == 0 {
        return vec![true; b_len];
    };
    if b_len == 0 {
        return vec![false; a_len];
    }

    let mut current_a = T::default();
    let cap = a_len + b_len;
    let mut out = Vec::with_capacity(cap);

    let mut current_b = b_iter.next().unwrap();

    for a in &mut a_iter {
        current_a = a;
        if a <= current_b {
            // safety
            // we pre-allocated enough
            out.push(A_INDICATOR);
            continue;
        }
        out.push(B_INDICATOR);

        loop {
            if let Some(b) = b_iter.next() {
                current_b = b;
                if b >= a {
                    out.push(A_INDICATOR);
                    break;
                }
                out.push(B_INDICATOR);
                continue;
            }
            // b is depleted fill with a indicator
            let remaining = cap - out.len();
            out.extend(std::iter::repeat(A_INDICATOR).take(remaining));
            return out;
        }
    }
    if current_a < current_b {
        out.push(B_INDICATOR);
    }
    // check if current value already is added
    if *out.last().unwrap() == A_INDICATOR {
        out.push(B_INDICATOR);
    }
    // take remaining
    out.extend(b_iter.map(|_| B_INDICATOR));
    assert_eq!(out.len(), b_len + a_len);

    out
}

#[derive(Clone)]
pub struct TrustMyLength<I: Iterator<Item = J>, J> {
    iter: I,
    len: usize,
}

impl<I, J> TrustMyLength<I, J>
where
    I: Iterator<Item = J>,
{
    #[inline]
    pub fn new(iter: I, len: usize) -> Self {
        Self { iter, len }
    }
}

impl<I, J> Iterator for TrustMyLength<I, J>
where
    I: Iterator<Item = J>,
{
    type Item = J;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

unsafe impl<I, J> TrustedLen for TrustMyLength<I, J> where I: Iterator<Item = J> {}