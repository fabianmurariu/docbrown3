use std::{cmp, marker::Send};

use crate::*;
use async_recursion::async_recursion;
use futures::join;

pub async fn bridge<I, C>(par_iter: I, consumer: C) -> C::Result
where
    I: AsyncIndexedIterator,
    C: Consumer<I::Item>,
{
    let len = par_iter.len();
    return par_iter.with_producer(Callback { len, consumer }).await;

    struct Callback<C> {
        len: usize,
        consumer: C,
    }

    #[async_trait]
    impl<C, I: Send> SourceCallback<I> for Callback<C>
    where
        C: Consumer<I>,
    {
        type Output = C::Result;
        async fn callback<P>(self, producer: P) -> C::Result
        where
            P: Source<Item = I> + Send,
        {
            bridge_producer_consumer(self.len, producer, self.consumer).await
        }
    }
}

pub async fn bridge_producer_consumer<P, C>(len: usize, producer: P, consumer: C) -> C::Result
where
    P: Source,
    C: Consumer<P::Item>,
{
    let splitter = LengthSplitter::new(producer.min_len(), producer.max_len(), len);
    return helper(len, splitter, producer, consumer).await;

    #[async_recursion]
    async fn helper<P, C>(
        len: usize,
        mut splitter: LengthSplitter,
        producer: P,
        consumer: C,
    ) -> C::Result
    where
        P: Source + Send,
        C: Consumer<P::Item>,
    {
        if consumer.full().await {
            let folder = consumer.into_folder();
            folder.complete().await
        } else if splitter.try_split(len) {
            let mid = len / 2;
            let (left_producer, right_producer) = producer.split_at(mid);
            let (left_consumer, right_consumer, reducer) = consumer.split_at(mid).await;

            let left_fut = helper(mid, splitter, left_producer, left_consumer);

            let right_fut = helper(len - mid, splitter, right_producer, right_consumer);

            let (left_result, right_result) = join!(left_fut, right_fut);

            reducer.reduce(left_result, right_result).await
        } else {
            let fold_with = producer.fold_with(consumer.into_folder()).await;
            fold_with.complete().await
        }
    }
}

/// A splitter controls the policy for splitting into smaller work items.
///
/// Thief-splitting is an adaptive policy that starts by splitting into
/// enough jobs for every worker thread, and then resets itself whenever a
/// job is actually stolen into a different thread.
#[derive(Clone, Copy)]
struct Splitter {
    /// The `splits` tell us approximately how many remaining times we'd
    /// like to split this job.  We always just divide it by two though, so
    /// the effective number of pieces will be `next_power_of_two()`.
    splits: usize,
}

impl Splitter {
    #[inline]
    fn new() -> Splitter {
        Splitter {
            splits: crate::current_num_threads(),
        }
    }

    #[inline]
    fn try_split(&mut self, stolen: bool) -> bool {
        let Splitter { splits } = *self;

        if stolen {
            // This job was stolen!  Reset the number of desired splits to the
            // thread count, if that's more than we had remaining anyway.
            self.splits = cmp::max(crate::current_num_threads(), self.splits / 2);
            true
        } else if splits > 0 {
            // We have splits remaining, make it so.
            self.splits /= 2;
            true
        } else {
            // Not stolen, and no more splits -- we're done!
            false
        }
    }
}

/// The length splitter is built on thief-splitting, but additionally takes
/// into account the remaining length of the iterator.
#[derive(Clone, Copy)]
struct LengthSplitter {
    inner: Splitter,

    /// The smallest we're willing to divide into.  Usually this is just 1,
    /// but you can choose a larger working size with `with_min_len()`.
    min: usize,
}

impl LengthSplitter {
    /// Creates a new splitter based on lengths.
    ///
    /// The `min` is a hard lower bound.  We'll never split below that, but
    /// of course an iterator might start out smaller already.
    ///
    /// The `max` is an upper bound on the working size, used to determine
    /// the minimum number of times we need to split to get under that limit.
    /// The adaptive algorithm may very well split even further, but never
    /// smaller than the `min`.
    #[inline]
    fn new(min: usize, max: usize, len: usize) -> LengthSplitter {
        let mut splitter = LengthSplitter {
            inner: Splitter::new(),
            min: cmp::max(min, 1),
        };

        // Divide the given length by the max working length to get the minimum
        // number of splits we need to get under that max.  This rounds down,
        // but the splitter actually gives `next_power_of_two()` pieces anyway.
        // e.g. len 12345 / max 100 = 123 min_splits -> 128 pieces.
        let min_splits = len / cmp::max(max, 1);

        // Only update the value if it's not splitting enough already.
        if min_splits > splitter.inner.splits {
            splitter.inner.splits = min_splits;
        }

        splitter
    }

    #[inline]
    fn try_split(&mut self, len: usize) -> bool {
        // If splitting wouldn't make us too small, try the inner splitter.
        len / 2 >= self.min && self.inner.try_split(false)
    }
}
