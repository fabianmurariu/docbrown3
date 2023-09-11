use std::mem::size_of;

use bytemuck::{Pod, Zeroable};

use crate::PAGE_SIZE;

// this should fit in the page size 4096.. somehow
pub const TIME_EVENTS_LEN: usize =
    ((PAGE_SIZE - size_of::<[u8; 256]>() - size_of::<usize>() - size_of::<usize>()) / 2) / 8;

// this is an append log of rows at cols that are sorted by (rows, cols) in the index field
// on every insertion, the index is updated to reflect the new order
// this is in essence graph like thing with u0 -> [u0, u1, u2] u1 -> [u9, u8], u2 -> [u7, u6, u5, u4, u3]
#[repr(C,align(4096))]
#[derive(Pod, Zeroable, Copy, Clone, Debug)]
pub struct IndexedEvents {
    overflow: usize, // address where the overflow page is
    cap: usize,      // the capacity of this page
    rows: [usize; TIME_EVENTS_LEN],
    cols: [i64; TIME_EVENTS_LEN],
    index: [u8; 256], // the index into rows and cols ordered by (rows and cols)
}

impl IndexedEvents {

    pub fn capacity(&self) -> usize {
        self.cap
    }

    pub fn overflow_page(&self) -> Option<usize> {
        if self.overflow == 0 { // the first page can't be an overflow
            None
        } else {
            Some(self.overflow)
        }
    }

    pub fn set_overflow_page(&mut self, overflow: usize) {
        self.overflow = overflow;
    }

    pub fn is_full(&self) -> bool {
        self.cap == TIME_EVENTS_LEN
    }

    pub fn append(&mut self, row: usize, time: i64) -> bool {
        // append row and time and keep the position
        if self.cap < TIME_EVENTS_LEN {
            self.rows[self.cap] = row;
            self.cols[self.cap] = time;

            // insert into index at the correct position keeping the index sorted by (row, col)

            let pos = self.find_index_pos(&row, &time);

            // insert the row into the index
            self.index.copy_within(pos..self.cap, pos + 1);
            self.index[pos] = self.cap as u8;

            self.cap += 1;
            true
        } else {
            false
        }
    }

    fn find_index_pos(&self, row: &usize, time: &i64) -> usize {
        self.index[0..self.cap]
            .binary_search_by(|probe| {
                let i = *probe as usize;
                let probe_time = self.cols[i];
                let probe_row = self.rows[i];

                probe_row.cmp(row).then(probe_time.cmp(time))
            })
            .unwrap_or_else(|not_found| not_found)
    }

    pub fn iter(&self) -> impl Iterator<Item = (usize, i64)> + '_ {
        self.index
            .iter()
            .take(self.cap)
            .map(move |&i| (self.rows[i as usize], self.cols[i as usize]))
    }

    pub fn timestamps_for_row(&self, row: usize) -> impl Iterator<Item = i64> + '_ {
        self.index
            .iter()
            .take(self.cap)
            .filter(move |&&i| self.rows[i as usize] == row)
            .map(move |&i| self.cols[i as usize])
    }
}

#[cfg(test)]
mod test {
    use std::{mem::{size_of, align_of}, ops::DerefMut};

    use bytemuck::Zeroable;

    use crate::{time_indexed_events::IndexedEvents, PAGE_SIZE, DataPage};

    #[test]
    fn what_size_am_i() {
        let events: IndexedEvents = Zeroable::zeroed();
        println!(
            "TimeEvents size: {}, u8 max {}, len: {}",
            std::mem::size_of::<IndexedEvents>(),
            u8::MAX,
            events.rows.len()
        );
        assert!(size_of::<IndexedEvents>() <= PAGE_SIZE);
    }

    #[test]
    fn insert_one() {
        let mut events: IndexedEvents = Zeroable::zeroed();
        assert!(events.append(1, 1));
        assert_eq!(events.cap, 1);
        assert_eq!(events.rows[0], 1);
        assert_eq!(events.cols[0], 1);
        assert_eq!(events.index[0], 0);

        let mut iter = events.iter();
        assert_eq!(iter.next(), Some((1, 1)));
        assert_eq!(iter.next(), None);

        let mut iter = events.timestamps_for_row(1);
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn insert_2_items_same_timestamp() {
        let mut bytes = DataPage::new();

        let mut events = bytemuck::cast_mut::<_, IndexedEvents>(bytes.deref_mut());
        assert!(events.append(1, 1));
        assert!(events.append(2, 1));
        assert_eq!(events.cap, 2);
        assert_eq!(events.rows[0], 1);
        assert_eq!(events.cols[0], 1);
        assert_eq!(events.index[0], 0);
        assert_eq!(events.rows[1], 2);
        assert_eq!(events.cols[1], 1);
        assert_eq!(events.index[1], 1);

        let mut iter = events.iter();
        assert_eq!(iter.next(), Some((1, 1)));
        assert_eq!(iter.next(), Some((2, 1)));
        assert_eq!(iter.next(), None);

        let mut iter = events.timestamps_for_row(1);
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.next(), None);

        let mut iter = events.timestamps_for_row(2);
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

        let actual = events.timestamps_for_row(1).collect::<Vec<_>>();
        assert_eq!(actual, vec![1, 2]);

        let actual = events.timestamps_for_row(2).collect::<Vec<_>>();
        assert_eq!(actual, vec![1, 2]);
    }
}
