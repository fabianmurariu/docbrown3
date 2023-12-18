use std::marker::Send;

use crate::*;

impl<'data, T: Sync + Send + 'data> IntoAsyncIterator for &'data Vec<T> {
    type Item = &'data T;
    type Iter = Iter<'data, T>;

    fn into_async_iter(self) -> Self::Iter {
        todo!()
    }
}

pub struct Iter<'data, T: Sync> {
    slice: &'data [T],
}

impl<'data, T: Sync> Clone for Iter<'data, T> {
    fn clone(&self) -> Self {
        Iter { ..*self }
    }
}

#[async_trait]
impl<'data, T: Sync + Send + 'data> AsyncIndexedIterator for Iter<'data, T> {
    type Item = &'data T;

    async fn drive<C: Consumer<Self::Item>>(self, consumer: C) -> C::Result {
        todo!()
    }

    async fn with_producer<CB: SourceCallback<Self::Item> + Send>(self, callback: CB) -> CB::Output {
        callback.callback(IterSource { slice: self.slice }).await
    }

    fn len(&self) -> usize {
        self.slice.len()
    }
}

struct IterSource<'data, T: Sync> {
    slice: &'data [T],
}

impl<'data, T: Sync> Source for IterSource<'data, T> {
    type Item = &'data T;
    type IntoIter = std::slice::Iter<'data, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.slice.iter()
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        let (left, right) = self.slice.split_at(index);
        (IterSource { slice: left }, IterSource { slice: right })
    }
}


