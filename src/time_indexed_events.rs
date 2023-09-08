use std::mem::size_of;

use bytemuck::{Pod, Zeroable};

use crate::PAGE_SIZE;

// this should fit in the page size 4096.. somehow
const TIME_EVENTS_LEN: usize =
    ((PAGE_SIZE - size_of::<[u8; 256]>() - size_of::<usize>() - size_of::<usize>()) / 2) / 8;

// this is an append log of items at times that are sorted by (item, time) in the index field
// on every insertion, the index is updated to reflect the new order
#[repr(C)]
#[derive(Pod, Zeroable, Copy, Clone, Debug)]
pub struct IndexedEvents {
    overflow: usize,
    cap: usize, // the capacity of this page
    items: [usize; TIME_EVENTS_LEN],
    times: [i64; TIME_EVENTS_LEN],
    index: [u8; 256], // the index into items and times ordered by (item, time)
}

impl IndexedEvents {
    pub fn append(&mut self, item: usize, time: i64) -> bool {
        // append item and time and keep the position
        if self.cap < TIME_EVENTS_LEN {
            self.items[self.cap] = item;
            self.times[self.cap] = time;

            // insert into index at the correct position keeping the index sorted by (item, time)

            let pos = self.find_index_pos(&item, &time);

            // insert the item into the index
            self.index.copy_within(pos..self.cap, pos + 1);
            self.index[pos] = self.cap as u8;

            self.cap += 1;
            true
        } else {
            false
        }
    }

    fn find_index_pos(&self, item: &usize, time: &i64) -> usize {
        self.index[0..self.cap]
            .binary_search_by(|probe| {
                let i = *probe as usize;
                let probe_time = self.times[i];
                let probe_item = self.items[i];

                probe_item.cmp(item).then(probe_time.cmp(time))
            })
            .unwrap_or_else(|not_found| not_found)
    }

    pub fn iter(&self) -> impl Iterator<Item = (usize, i64)> + '_ {
        self.index
            .iter()
            .take(self.cap)
            .map(move |&i| (self.items[i as usize], self.times[i as usize]))
    }

    pub fn timestamps_for_item(&self, item: usize) -> impl Iterator<Item = i64> + '_ {
        self.index
            .iter()
            .take(self.cap)
            .filter(move |&&i| self.items[i as usize] == item)
            .map(move |&i| self.times[i as usize])
    }
}

#[cfg(test)]
mod test {
    use std::mem::size_of;

    use bytemuck::Zeroable;

    use crate::{time_indexed_events::IndexedEvents, PAGE_SIZE};

    #[test]
    fn what_size_am_i() {
        let events: IndexedEvents = Zeroable::zeroed();
        println!(
            "TimeEvents size: {}, u8 max {}, len: {}",
            std::mem::size_of::<IndexedEvents>(),
            u8::MAX,
            events.items.len()
        );
        assert!(size_of::<IndexedEvents>() <= PAGE_SIZE);
    }

    #[test]
    fn insert_one() {
        let mut events: IndexedEvents = Zeroable::zeroed();
        assert!(events.append(1, 1));
        assert_eq!(events.cap, 1);
        assert_eq!(events.items[0], 1);
        assert_eq!(events.times[0], 1);
        assert_eq!(events.index[0], 0);

        let mut iter = events.iter();
        assert_eq!(iter.next(), Some((1, 1)));
        assert_eq!(iter.next(), None);

        let mut iter = events.timestamps_for_item(1);
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn insert_2_items_same_timestamp() {
        let mut events: IndexedEvents = Zeroable::zeroed();
        assert!(events.append(1, 1));
        assert!(events.append(2, 1));
        assert_eq!(events.cap, 2);
        assert_eq!(events.items[0], 1);
        assert_eq!(events.times[0], 1);
        assert_eq!(events.index[0], 0);
        assert_eq!(events.items[1], 2);
        assert_eq!(events.times[1], 1);
        assert_eq!(events.index[1], 1);

        let mut iter = events.iter();
        assert_eq!(iter.next(), Some((1, 1)));
        assert_eq!(iter.next(), Some((2, 1)));
        assert_eq!(iter.next(), None);

        let mut iter = events.timestamps_for_item(1);
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.next(), None);

        let mut iter = events.timestamps_for_item(2);
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn insert_2_items_2_timestamps_each() {
        let mut events: IndexedEvents = Zeroable::zeroed();
        assert!(events.append(1, 1));
        assert!(events.append(2, 1));
        assert!(events.append(1, 2));
        assert!(events.append(2, 2));
        assert_eq!(events.cap, 4);

        let actual = events.iter().collect::<Vec<_>>();
        assert_eq!(actual, vec![(1, 1), (1, 2), (2, 1), (2, 2)]);

        let actual = events.timestamps_for_item(1).collect::<Vec<_>>();
        assert_eq!(actual, vec![1, 2]);

        let actual = events.timestamps_for_item(2).collect::<Vec<_>>();
        assert_eq!(actual, vec![1, 2]);
    }
}
