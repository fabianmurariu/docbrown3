mod merge;

use arrow2::{
    array::{
        Array, ListArray, MutableListArray, MutablePrimitiveArray, MutableStructArray,
        PrimitiveArray, StructArray,
    },
    datatypes::{DataType, Field},
};
use itertools::Itertools;

#[derive(Debug)]
pub struct GraphFragment {
    items: PrimitiveArray<u64>,
    outbound: ListArray<i32>, // struct {v_id, e_id}
    inbound: ListArray<i32>,  // struct {v_id, e_id}
}

type MPArr<T> = MutablePrimitiveArray<T>;

#[repr(transparent)]
struct MutEdgePair<'a>(&'a mut MutableStructArray);

impl<'a> MutEdgePair<'a> {
    fn add_pair(&mut self, v: u64, e: u64) {
        self.0.value::<MPArr<u64>>(0).unwrap().push(Some(v));
        self.0.value::<MPArr<u64>>(1).unwrap().push(Some(e));
        self.0.push(true);
    }
}

impl<'a> From<&'a mut MutableStructArray> for MutEdgePair<'a> {
    fn from(arr: &'a mut MutableStructArray) -> Self {
        Self(arr)
    }
}

#[repr(transparent)]
struct EdgePair<'a>(&'a StructArray);

impl<'a> From<&'a StructArray> for EdgePair<'a> {
    fn from(arr: &'a StructArray) -> Self {
        Self(arr)
    }
}

impl <'a> EdgePair<'a> {
    pub fn from_arr(arr: Box<dyn Array>) -> Option<Self> {
        let a = arr.as_any().downcast_ref::<StructArray>()?;
        Some(Self::from(a))
    }

    fn vs(&self) -> &'a PrimitiveArray<u64> {
        self.0.values()[0]
            .as_any()
            .downcast_ref::<PrimitiveArray<u64>>()
            .unwrap()
    }

    fn es(&self) -> &'a PrimitiveArray<u64> {
        self.0.values()[1]
            .as_any()
            .downcast_ref::<PrimitiveArray<u64>>()
            .unwrap()
    }

    fn iter(&self) -> impl Iterator<Item = (&u64, &u64)> + '_ {
        self.vs().into_iter().flatten().zip(self.es().into_iter().flatten())
    }
}

enum IngestEdgeType {
    Outbound((u64, u64, u64)),
    Inbound((u64, u64, u64)),
}

impl IngestEdgeType {
    fn vertex(&self) -> u64 {
        match self {
            IngestEdgeType::Outbound((v, _, _)) => *v,
            IngestEdgeType::Inbound((v, _, _)) => *v,
        }
    }
}

pub enum Dir {
    Outbound,
    Inbound,
    Both,
}

impl GraphFragment {
    fn lift_adj_list(&self, row: usize, dir: Dir) -> Option<StructArray> {
        let adj_list = match dir {
            Dir::Outbound => &self.outbound,
            Dir::Inbound => &self.inbound,
            Dir::Both => todo!(),
        };
        let adj_list = adj_list.value(row);
        let adj_list = adj_list.as_any().downcast_ref::<StructArray>();
        adj_list.cloned()
    }

    pub fn edges(&self, node: u64, dir: Dir) -> Option<Box<dyn Iterator<Item = (u64, u64)>>> {
        match dir {
            Dir::Outbound | Dir::Inbound => self
                .items
                .values()
                .as_slice()
                .binary_search(&node)
                .ok()
                .and_then(|row| self.lift_adj_list(row, dir))
                .map(|arr| {
                    let v = arr.values()[0]
                        .as_any()
                        .downcast_ref::<PrimitiveArray<u64>>()
                        .unwrap()
                        .clone();
                    let e = arr.values()[1]
                        .as_any()
                        .downcast_ref::<PrimitiveArray<u64>>()
                        .unwrap()
                        .clone();
                    let iter: Box<dyn Iterator<Item = (u64, u64)>> = Box::new(
                        v.into_iter()
                            .filter_map(|x| x)
                            .zip(e.into_iter().filter_map(|x| x)),
                    );
                    iter
                }),
            Dir::Both => {
                let out = self.edges(node, Dir::Outbound)?;
                let inb = self.edges(node, Dir::Inbound)?;
                Some(Box::new(out.merge_by(inb, |(v1, _), (v2, _)| v1 < v2)))
            }
        }
    }

    pub fn from_sorted_triplets<I: IntoIterator<Item = (u64, u64, u64)>>(
        outbound: I,
        inbound: I,
    ) -> Self {
        let fields = vec![
            Field::new("v", DataType::UInt64, false),
            Field::new("e", DataType::UInt64, false),
        ];

        let mut items = MPArr::<u64>::new();

        // arrays for outbound
        let out_v = Box::new(MPArr::<u64>::new());
        let out_e = Box::new(MPArr::<u64>::new());
        let out_inner =
            MutableStructArray::new(DataType::Struct(fields.clone()), vec![out_v, out_e]);

        let mut outbound_arr = MutableListArray::<i32, MutableStructArray>::new_with_field(
            out_inner, "outbound", true,
        );

        // arrays for inbound
        let in_v = Box::new(MPArr::<u64>::new());
        let in_e = Box::new(MPArr::<u64>::new());
        let in_inner = MutableStructArray::new(DataType::Struct(fields), vec![in_v, in_e]);

        let mut inbound_arr =
            MutableListArray::<i32, MutableStructArray>::new_with_field(in_inner, "inbound", true);

        let in_iter: Box<dyn Iterator<Item = IngestEdgeType>> =
            Box::new(inbound.into_iter().map(|t| IngestEdgeType::Inbound(t)));
        let out_iter = Box::new(outbound.into_iter().map(|t| IngestEdgeType::Outbound(t)));

        let iter = [in_iter, out_iter]
            .into_iter()
            .kmerge_by(|a, b| a.vertex() < b.vertex());

        let mut cur = None;
        for edge in iter {
            let vertex = edge.vertex();
            if cur.is_none() {
                // happens once
                cur = Some(vertex);
            }

            if cur != Some(vertex) {
                items.push(cur);

                outbound_arr.try_push_valid().expect("push valid"); // one row done
                inbound_arr.try_push_valid().expect("push valid"); // one row done

                cur = Some(vertex);
            }

            match edge {
                IngestEdgeType::Outbound((_, dst, e)) => {
                    let mut row: MutEdgePair = outbound_arr.mut_values().into();
                    row.add_pair(dst, e);
                }
                IngestEdgeType::Inbound((_, dst, e)) => {
                    let mut row: MutEdgePair = inbound_arr.mut_values().into();
                    row.add_pair(dst, e);
                }
            }
            // let mut row: VecEdgePair = outbound_arr.mut_values().into();
            // row.add_pair(dst, e);
        }

        items.push(cur);
        outbound_arr.try_push_valid().expect("push valid"); // last row;
        inbound_arr.try_push_valid().expect("push valid"); // last row;

        Self {
            items: items.into(),
            outbound: outbound_arr.into(),
            inbound: inbound_arr.into(),
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn adj(&self, node: usize) -> Box<dyn Array> {
        self.outbound.value(node)
    }

    pub fn merge(&self, other: &GraphFragment) -> GraphFragment {
        // get the merge indicator for the items column
        // in parallel merge the outbound and inbound columns
        // make sure both the outbound item ids as well as the edge ids are merged

        let row_indicators = merge::get_merge_indicator(
            self.items.values().iter().copied(),
            other.items.values().iter().copied(),
        );

        // need the same stuff as from_sorted_triplets

        // let lists = row_indicators.iter().map(|indicator| {
        //     if *indicator {
        //         let next_item = self_items.next().flatten();
        //         let next_out = self_out.next().flatten();
        //         if next_item == other_items.peek().unwrap().copied().as_ref() {
        //             println!("merge lists for {:?}", next_item);
        //             let next_other_out = other_out.next().flatten();
        //             // we need to merge these two lists into one and eliminate the duplicates
        //         }
        //         next_item
        //     } else {
        //         let next_item = other_items.next().flatten();
        //         if next_item == self_items.peek().unwrap().copied().as_ref() {
        //             println!("merge lists for {:?}", next_item);
        //         }
        //         next_item
        //     }
        // });

        let mut self_items = self.items.iter().peekable();
        let mut other_items = other.items.iter().peekable();

        let mut self_out = self.outbound.into_iter();
        let mut other_out = other.outbound.into_iter();

        let cap = self.items.len() + other.items.len();

        let mut items = MPArr::<u64>::with_capacity(cap);

        for indicator in row_indicators.iter() {
            if *indicator {
                let next_item = self_items.next().flatten().copied();
                if other_items.peek().is_some() && other_items.peek().unwrap().copied() == next_item
                {
                    let next_other_out = other_out.next().flatten();
                    let next_self_out = self_out.next().flatten();
                    println!("merge lists for {:?}", next_item);
                    // we need to merge these two lists into one and eliminate the duplicates
                } else {
                    items.push(next_item); // next item is different no need to merge lists
                }
            } else {
                let next_item = other_items.next().flatten().copied();
                items.push(next_item);
            }
        }

        // every time we pick an item via the indicator we can peek onto the other iterator and check if
        // it has the same value then we can merge the two lists

        todo!("items, outbound and inbound columns")
    }

    fn merge_lists(
        left: Option<Box<dyn Array>>,
        right: Option<Box<dyn Array>>,
        into: &mut MutableListArray<i32, MutableStructArray>,
    ) -> Option<()> {
        let lhs = EdgePair::from_arr(left?)?;
        let rhs = EdgePair::from_arr(right?)?;

        let pairs = lhs.iter().merge_by(rhs.iter(), |(v1, _), (v2, _)| v1 < v2).dedup();

        let mut row: MutEdgePair = into.mut_values().into();

        for (v, e) in pairs {
            row.add_pair(*v, *e);
        }
        into.try_push_valid().expect("push valid");

        Some(())
    }
}

#[cfg(test)]
mod test {

    use arrow2::{
        array::UInt64Array,
        compute::{
            merge_sort::{self, merge_sort},
            sort::SortOptions,
        },
    };

    use super::*;

    #[test]
    fn load_sorted_adj_lists_then_query_neighbours() {
        let inbound = vec![(1, 1, 2), (1, 4, 3), (2, 2, 3), (9, 3, 4), (13, 3, 5)];
        let outbound = vec![
            (1, 2, 0),
            (1, 3, 1),
            (2, 3, 2),
            (2, 4, 3),
            (6, 5, 4),
            (9, 5, 7),
        ];
        let csr = GraphFragment::from_sorted_triplets(outbound, inbound);
        assert_eq!(csr.len(), 5);

        let actual = csr.edges(1, Dir::Outbound).unwrap().collect::<Vec<_>>();
        let expected = vec![(2, 0), (3, 1)];
        assert_eq!(actual, expected);

        let actual = csr.edges(1, Dir::Inbound).unwrap().collect::<Vec<_>>();
        let expected = vec![(1, 2), (4, 3)];
        assert_eq!(actual, expected);

        let actual = csr.edges(1, Dir::Both).unwrap().collect::<Vec<_>>();
        let expected = vec![(1, 2), (2, 0), (3, 1), (4, 3)];
        assert_eq!(actual, expected);

        for arr in csr.outbound.into_iter() {
            if let Some(arr) = arr {
                let arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
                let vs = arr.values()[0]
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();

                let es = arr.values()[1]
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                println!("{:?} {:?}", es, vs);
            }
        }
    }

    #[test]
    fn test_merge_arrays() {
        let p1 = UInt64Array::from_slice([1, 2, 3, 4, 6]);
        let p2 = UInt64Array::from_slice([4, 5, 6]);

        let indicator = merge::get_merge_indicator(p1.clone().into_iter(), p2.clone().into_iter());
        println!("{:?}", indicator);
        let merged = merge::merge_native(&p1, &p2, &indicator[..]);
        println!("{:?}", merged);

        let merged = merge::merge_dedup_native(&p1, &p2, &indicator[..]);
        println!("{:?}", merged);
    }
}
