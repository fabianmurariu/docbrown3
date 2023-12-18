use std::num::NonZeroUsize;

use async_trait::async_trait;

pub mod arrays;
mod plumbing;

trait IntoAsyncIterator {
    type Iter: AsyncIndexedIterator<Item = Self::Item>;
    type Item: Send;

    fn into_async_iter(self) -> Self::Iter;
}

#[async_trait]
trait AsyncIndexedIterator {
    type Item: Send;
    async fn drive<C: Consumer<Self::Item>>(self, consumer: C) -> C::Result;

    async fn with_producer<CB: SourceCallback<Self::Item> + Send>(self, callback: CB)
        -> CB::Output;

    fn len(&self) -> usize;
}

#[async_trait]
trait SourceCallback<T: Send>: Sized {
    type Output: Send;

    async fn callback<P>(self, producer: P) -> Self::Output
    where
        P: Source<Item = T> + Send;
}

#[async_trait]
trait Source: Sized + Send{
    /// The type of item that will be produced by this producer once
    /// it is converted into an iterator.
    type Item: Send;

    /// The type of iterator we will become.
    type IntoIter: Iterator<Item = Self::Item> + DoubleEndedIterator + ExactSizeIterator + Send;

    /// Convert `self` into an iterator; at this point, no more parallel splits
    /// are possible.
    fn into_iter(self) -> Self::IntoIter;

    /// Split into two producers; one produces items `0..index`, the
    /// other `index..N`. Index must be less than or equal to `N`.
    fn split_at(self, index: usize) -> (Self, Self);

    async fn fold_with<F>(self, folder: F) -> F
    where
        F: Folder<Self::Item> + Send,
    {
        folder.consume_iter(self.into_iter()).await
    }

    fn min_len(&self) -> usize {
        1
    }

    fn max_len(&self) -> usize {
        1
    }
}

#[async_trait]
trait Folder<Item: Send>: Sized {
    type Result;

    async fn consume(self, item: Item) -> Self;

    async fn complete(self) -> Self::Result;

    async fn full(&self) -> bool;

    async fn consume_iter<I>(mut self, items: I) -> Self
    where
        I: IntoIterator<Item = Item> + Send,
        <I as IntoIterator>::IntoIter: Send,
    {
        let mut folder = self;
        for item in items {
            folder = folder.consume(item).await;
            let is_full = folder.full();
            if is_full.await {
                break;
            }
        }
        folder
    }
}

#[async_trait]
trait Reducer<Result> {
    /// Reduce two final results into one; this is executed after a
    /// split.
    async fn reduce(self, left: Result, right: Result) -> Result;
}

#[async_trait]
trait Consumer<Item: Sized + Send>: Send + Sized {
    /// The type of folder that this consumer can be converted into.
    type Folder: Folder<Item, Result = Self::Result> + Send;

    /// The type of reducer that is produced if this consumer is split.
    type Reducer: Reducer<Self::Result> + Send;

    /// The type of result that this consumer will ultimately produce.
    type Result: Send;

    fn into_folder(self) -> Self::Folder;

    async fn full(&self) -> bool;

    async fn split_at(self, index: usize) -> (Self, Self, Self::Reducer);
}

fn current_num_threads() -> usize {
    std::thread::available_parallelism()
        .unwrap_or(unsafe { NonZeroUsize::new_unchecked(1) })
        .get()
}
