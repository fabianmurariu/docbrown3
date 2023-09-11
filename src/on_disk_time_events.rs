use std::{collections::HashSet, ops::DerefMut, path::Path, sync::Arc, array::IntoIter};

use rand::Rng;

use crate::{time_indexed_events::IndexedEvents, BufferManager, PageEntryMut, PID};

struct Items {
    num_item_buckets: usize,
    overflow_page_start: usize,
    bm: Arc<BufferManager>,
}

impl Items {
    fn new<P: AsRef<Path>>(path: P, phys_size: usize, virt_size: usize) -> Self {
        let bm = BufferManager::new(virt_size, phys_size, path)
            .expect("Failed to create buffer manager");
        let num_item_buckets = bm.virt_count() / 4; // 25% of the pages are where we store the the items the rest are for overflow
        Self {
            bm: Arc::new(bm),
            num_item_buckets,
            overflow_page_start: num_item_buckets,
        }
    }

    fn append_to_page(&self, page: &mut PageEntryMut<'_>, row: usize, col: i64) -> bool {
        let data_page = page.deref_mut();
        let events = bytemuck::cast_mut::<_, IndexedEvents>(data_page.deref_mut());
        let out = events.append(row, col);
        println!(
            "append_to_page: {}, {}, cap: {}, append_ok: {}",
            row,
            col,
            events.capacity(),
            out
        );
        out
    }

    pub fn append(&mut self, row: usize, col: i64) -> bool {
        // FIXME: we need a better way of knowing when stuff is full
        let pid = row % self.num_item_buckets;

        let mut page = self.bm.get_page_mut(pid).expect("Failed to get page");
        let mut overflow_page = None;
        let mut seen_pages = HashSet::from_iter(vec![pid]);

        let mut append_ok = self.append_to_page(&mut page, row, col);

        let cap = page.view_as::<IndexedEvents>().capacity();
        println!("{}, {}, cap: {}, append_ok: {}", row, col, cap, append_ok);

        if !append_ok {
            while overflow_page.is_none() {
                // check if we have an overflow page that is not full
                let indexed_page = page.view_as::<IndexedEvents>();
                if let Some(o_page) = indexed_page.overflow_page() {
                    let next_page = self.bm.get_page_mut(o_page).expect("Failed to get page");

                    if !next_page.view_as::<IndexedEvents>().is_full() {
                        overflow_page = Some(next_page);
                        break;
                    } else {
                        page = next_page;
                        seen_pages.insert(o_page);
                        overflow_page = None;
                        // loop back
                    }
                } else {
                    // pick a random page between overflow_page_start and bm.virtual_count();
                    let pick_a_page = self.random_page_except(&seen_pages);
                    let next_page = self
                        .bm
                        .get_page_mut(pick_a_page)
                        .expect("Failed to alloc page");
                    if !next_page.view_as::<IndexedEvents>().is_full() {
                        overflow_page = Some(next_page);
                        break;
                    } else {
                        page = next_page;
                        seen_pages.insert(pick_a_page);
                        overflow_page = None;
                        // loop back
                    }
                }
            }

            if page.view_as::<IndexedEvents>().overflow_page().is_none() {
                page.view_mut_as::<IndexedEvents>()
                    .set_overflow_page(overflow_page.as_ref().map(|p| p.pid).unwrap());
            }

            // end of the chain write the values in an overflow page
            if let Some(mut overflow_page) = overflow_page {
                append_ok = self.append_to_page(&mut overflow_page, row, col);
            }
        }

        append_ok
    }

    fn random_page_except(&self, seen: &HashSet<PID>) -> PID {
        let mut rng = rand::thread_rng();
        loop {
            let pick_a_page = rng.gen_range(self.overflow_page_start..self.bm.virt_count());
            if !seen.contains(&pick_a_page) {
                return pick_a_page;
            }
        }
    }

    pub fn iter_pages(&self, item: usize) -> impl Iterator<Item = PID> + '_ {
        let pid = item % self.num_item_buckets;

        itertools::unfold(Some(pid), |pid| {
            if let Some(current_pid) = pid.clone() {
                *pid = self
                    .bm
                    .get_page(current_pid)
                    .expect("Failed to get page")
                    .view_as::<IndexedEvents>()
                    .overflow_page();
                Some(current_pid)
            } else {
                None
            }
        })
    }

    pub fn iter(&self, item: usize) -> impl Iterator<Item = IterEvents> + '_ {
        self.iter_pages(item).map(move |pid| {
            IterEvents {
                events: self.bm.clone(),
                item,
                pid,
            }
        })
    }
}

struct IterEvents {
    events: Arc<BufferManager>,
    item: usize,
    pid: PID,
}

impl IntoIterator for IterEvents{
    type Item = (usize, i64);
    type IntoIter = Box<dyn Iterator<Item = (usize, i64)>>;

    fn into_iter(self) -> Self::IntoIter {
        let page = self.events.get_page(self.pid).expect("Failed to get page");
        let events = page.view_as::<IndexedEvents>();
        let item = self.item;
        let iter = events.timestamps_for_row(self.pid).map(move |col| (item, col));
        Box::new(iter)
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use crate::{time_indexed_events::TIME_EVENTS_LEN, KB, MB};

    use super::*;

    #[test]
    fn append_items_in_2_pages_no_overflow() {
        let mut items = Items::new("items.bm", 12 * KB, 32 * KB);

        // check num_item_buckets
        assert_eq!(items.num_item_buckets, 2);

        for i in 0..TIME_EVENTS_LEN {
            assert!(items.append(i, i as i64));
        }

        // even numbers go to bucket 0
        for i in (0..TIME_EVENTS_LEN).into_iter().filter(|i| i % 2 == 0) {
            let pages = items.iter_pages(i).collect_vec();
            assert_eq!(pages, vec![0]);
        }

        // odd numbers goto bucket 1
        for i in (0..TIME_EVENTS_LEN).into_iter().filter(|i| i % 2 == 1) {
            let pages = items.iter_pages(i).collect_vec();
            assert_eq!(pages, vec![1]);
        }
    }

    #[test]
    fn append_items_in_2_pages_with_overflow() {
        let mut items = Items::new("items.bm", 12 * KB, 32 * KB);

        // check num_item_buckets
        assert_eq!(items.num_item_buckets, 2);

        for i in 0..(2 * TIME_EVENTS_LEN) + 2 {
            assert!(items.append(i, i as i64));
        }

        // even numbers go to bucket 0
        for i in (0..TIME_EVENTS_LEN).into_iter().filter(|i| i % 2 == 0) {
            let pages = items.iter_pages(i).collect_vec();
            assert_eq!(pages.len(), 2);
            assert_eq!(&pages[0], &0);
            assert!(&pages[1] >= &2);
        }

        // odd numbers goto bucket 1
        for i in (0..TIME_EVENTS_LEN).into_iter().filter(|i| i % 2 == 1) {
            let pages = items.iter_pages(i).collect_vec();
            assert_eq!(pages.len(), 2);
            assert_eq!(&pages[0], &1);
            assert!(&pages[1] >= &2);
        }
    }
}
