use std::path::Path;

use crate::{BufferManager, time_indexed_events::IndexedEvents};

struct TimedItems {
    num_item_buckets: usize,
    bm: BufferManager,
}

impl TimedItems {
    fn new<P: AsRef<Path>>(path: P, phys_size: usize, virt_size: usize) -> Self {
        let bm = BufferManager::new(phys_size, virt_size, path)
            .expect("Failed to create buffer manager");
        let num_item_buckets = bm.virt_count() / 4; // 25% of the pages are where we store the the items the rest are for overflow
        Self { bm, num_item_buckets }
    }

    pub fn append(&mut self, item: usize, time: i64) -> bool {
        let bucket = item % self.num_item_buckets;
        let page = self.bm.get_page_mut(bucket).expect("Failed to get page");
        // get the bytes then cast the page into IndexedEvents using bytemuck and append the item and time
        let mut bytes = page.bytes;
        let events = bytemuck::cast_mut::<_, IndexedEvents>(&mut bytes);
        events.append(item, time)
    }
}

#[cfg(test)]
mod test{
    
}