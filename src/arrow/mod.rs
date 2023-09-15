mod merge;

use std::iter::Peekable;

use arrow2::{
    array::{
        Array, ListArray, MutableListArray, MutablePrimitiveArray, MutableStructArray,
        PrimitiveArray, StructArray,
    },
    datatypes::{DataType, Field},
    trusted_len::TrustedLen,
};
use itertools::Itertools;

#[derive(Debug, PartialEq, Clone)]
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

    fn add_vals<I: TrustedLen<Item = u64>>(&mut self, vs: I, es: I) {
        self.0
            .value::<MPArr<u64>>(0)
            .unwrap()
            .extend_trusted_len_values(vs);
        self.0
            .value::<MPArr<u64>>(1)
            .unwrap()
            .extend_trusted_len_values(es);

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

impl<'a> EdgePair<'a> {
    pub fn from_arr(arr: &'a Box<dyn Array>) -> Option<Self> {
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
        self.vs()
            .into_iter()
            .flatten()
            .zip(self.es().into_iter().flatten())
    }
}

enum IngestEdgeType<'a> {
    Outbound(&'a (u64, u64, u64)),
    Inbound(&'a (u64, u64, u64)),
}

impl<'a> IngestEdgeType<'a> {
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

    pub fn from_sorted_triplets<'a, I: Iterator<Item = &'a (u64, u64, u64)>>(
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
                    row.add_pair(*dst, *e);
                }
                IngestEdgeType::Inbound((_, dst, e)) => {
                    let mut row: MutEdgePair = inbound_arr.mut_values().into();
                    row.add_pair(*dst, *e);
                }
            }
            // let mut row: VecEdgePair = outbound_arr.mut_values().into();
            // row.add_pair(dst, e);
        }

        if cur.is_some() {
            items.push(cur);
            outbound_arr.try_push_valid().expect("push valid"); // last row;
            inbound_arr.try_push_valid().expect("push valid"); // last row;
        }

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

    fn merge_items(&self, indicators: &[bool], other: &GraphFragment) -> PrimitiveArray<u64> {
        let cap = self.items.len() + other.items.len();

        let mut self_items = self.items.iter().peekable();
        let mut other_items = other.items.iter().peekable();

        let mut items = MPArr::<u64>::with_capacity(cap);

        // items column
        for indicator in indicators.iter() {
            if *indicator {
                self.advance_iter(&mut self_items, &mut other_items, &mut items)
            } else {
                self.advance_iter(&mut other_items, &mut self_items, &mut items)
            }
        }

        items.into()
    }

    fn merge_adj_list(
        &self,
        indicators: &[bool],
        name: &str,
        other: &GraphFragment,

        self_adj: &mut dyn Iterator<Item = Option<Box<dyn Array>>>,
        other_adj: &mut dyn Iterator<Item = Option<Box<dyn Array>>>,
    ) -> ListArray<i32> {
        let mut self_items = self.items.iter().peekable();
        let mut other_items = other.items.iter().peekable();

        let fields = vec![
            Field::new("v", DataType::UInt64, false),
            Field::new("e", DataType::UInt64, false),
        ];

        let v_arr = Box::new(MPArr::<u64>::new());
        let e_arr = Box::new(MPArr::<u64>::new());
        let inner = MutableStructArray::new(DataType::Struct(fields.clone()), vec![v_arr, e_arr]);

        let mut list_arr =
            MutableListArray::<i32, MutableStructArray>::new_with_field(inner, name, true);

        for indicator in indicators.iter() {
            if *indicator {
                self.advance_iter_and_push_adj_list(
                    &mut self_items,
                    &mut other_items,
                    self_adj,
                    other_adj,
                    &mut list_arr,
                );
            } else {
                self.advance_iter_and_push_adj_list(
                    &mut other_items,
                    &mut self_items,
                    other_adj,
                    self_adj,
                    &mut list_arr,
                );
            }
        }
        list_arr.into()
    }

    pub fn merge(&self, other: &GraphFragment) -> GraphFragment {
        if other.len() == 0 {
            return self.clone();
        }

        if self.len() == 0 {
            return other.clone();
        }

        let row_indicators = merge::get_merge_indicator(
            self.items.values().iter().copied(),
            other.items.values().iter().copied(),
        );

        let items = self.merge_items(&row_indicators[..], other);

        let mut self_outbound = self.outbound.iter().peekable();
        let mut other_outbound = other.outbound.iter().peekable();

        let mut self_inbound = self.inbound.iter().peekable();
        let mut other_inbound = other.inbound.iter().peekable();

        let outbound = self.merge_adj_list(
            &row_indicators[..],
            "outbound",
            other,
            &mut self_outbound,
            &mut other_outbound,
        );

        let inbound = self.merge_adj_list(
            &row_indicators[..],
            "inbound",
            other,
            &mut self_inbound,
            &mut other_inbound,
        );

        GraphFragment {
            items,
            outbound,
            inbound,
        }
    }

    fn advance_iter_and_push_adj_list<'a, I: Iterator<Item = Option<&'a u64>>>(
        &self,
        left_items: &mut Peekable<I>,
        right_items: &mut Peekable<I>,

        left_out: &mut dyn Iterator<Item = Option<Box<dyn Array>>>,
        right_out: &mut dyn Iterator<Item = Option<Box<dyn Array>>>,

        outbound_arr: &mut MutableListArray<i32, MutableStructArray>,
    ) {
        let next_item = left_items.next().flatten().copied();
        if right_items.peek().is_some() && right_items.peek().unwrap().copied() == next_item {
            let next_other_out = right_out.next().flatten();
            let next_self_out = left_out.next().flatten();
            Self::merge_lists(next_self_out, next_other_out, outbound_arr);
        } else {
            let outbound = left_out.next().flatten();
            Self::append_list(outbound, outbound_arr);
        }
    }

    fn advance_iter<'a, I: Iterator<Item = Option<&'a u64>>>(
        &self,
        left_items: &mut Peekable<I>,
        right_items: &mut Peekable<I>,
        items: &mut MPArr<u64>,
    ) {
        let next_item = left_items.next().flatten().copied();
        if right_items.peek().is_some() && right_items.peek().unwrap().copied() == next_item {
            // advance the other iterator to remove dups
            right_items.next();
        }

        if next_item.is_some() {
            items.push(next_item);
        }
    }

    fn merge_lists(
        left: Option<Box<dyn Array>>,
        right: Option<Box<dyn Array>>,
        into: &mut MutableListArray<i32, MutableStructArray>,
    ) -> Option<()> {
        let l = left?;
        let r = right?;
        let lhs = EdgePair::from_arr(&l)?;
        let rhs = EdgePair::from_arr(&r)?;

        let pairs = lhs
            .iter()
            .merge_by(rhs.iter(), |(v1, _), (v2, _)| v1 < v2)
            .dedup();

        let mut row: MutEdgePair = into.mut_values().into();

        for (v, e) in pairs {
            row.add_pair(*v, *e);
        }
        into.try_push_valid().expect("push valid");

        Some(())
    }

    fn append_list(
        list: Option<Box<dyn Array>>,
        into: &mut MutableListArray<i32, MutableStructArray>,
    ) -> Option<()> {
        let list = list?;
        let list = EdgePair::from_arr(&list)?;
        let mut row: MutEdgePair = into.mut_values().into();
        let (_, vs, _) = list.vs().clone().into_inner();
        let (_, es, _) = list.es().clone().into_inner();
        row.add_vals(vs.into_iter(), es.into_iter());

        into.try_push_valid().expect("push valid");
        Some(())
    }
}

#[cfg(test)]
mod test {

    use arrow2::array::UInt64Array;

    use super::*;

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn merge_graph_fragments(
            inbound_g1 in any::<Vec<(u64, u64, u64)>>().prop_map(|mut v| {v.sort(); v}),
            inbound_g2 in any::<Vec<(u64, u64, u64)>>().prop_map(|mut v| {v.sort(); v}),
            outbound_g1 in any::<Vec<(u64, u64, u64)>>().prop_map(|mut v| {v.sort(); v}),
            outbound_g2 in any::<Vec<(u64, u64, u64)>>().prop_map(|mut v| {v.sort(); v}),
        ) {
            let csr1 = GraphFragment::from_sorted_triplets(outbound_g1.iter(), inbound_g1.iter());
            let csr2 = GraphFragment::from_sorted_triplets(outbound_g2.iter(), inbound_g2.iter());

            let actual = csr1.merge(&csr2);

            let inbound_iter = Box::new(inbound_g1.iter().merge_by(inbound_g2.iter(), |(v1,_,_),(v2,_,_)|v1<v2).dedup());
            let outbound_iter:Box<dyn Iterator<Item = &(u64, u64, u64)>> = Box::new(outbound_g1.iter().merge_by(outbound_g2.iter(), |(v1,_,_),(v2,_,_)|v1<v2).dedup());

            let expected = GraphFragment::from_sorted_triplets(outbound_iter, inbound_iter);

            assert_eq!(actual, expected)

        }
    }

    proptest! {
        #[test]
        fn merge_items(
            inbound_g1 in any::<Vec<(u64, u64, u64)>>().prop_map(|mut v| {v.sort(); v}),
            inbound_g2 in any::<Vec<(u64, u64, u64)>>().prop_map(|mut v| {v.sort(); v}),
            outbound_g1 in any::<Vec<(u64, u64, u64)>>().prop_map(|mut v| {v.sort(); v}),
            outbound_g2 in any::<Vec<(u64, u64, u64)>>().prop_map(|mut v| {v.sort(); v}),
        ) {
            let expected = [
                &inbound_g1[..],
                &inbound_g2[..],
                &outbound_g1[..],
                &outbound_g2[..],
            ]
            .into_iter()
            .kmerge_by(|(b, _, _), (a, _, _)| b < a)
            .map(|(v, _, _)| v)
            .dedup()
            .copied()
            .map(Some)
            .collect::<Vec<_>>();

            let csr1 = GraphFragment::from_sorted_triplets(outbound_g1.iter(), inbound_g1.iter());
            let csr2 = GraphFragment::from_sorted_triplets(outbound_g2.iter(), inbound_g2.iter());

            let row_indicators = merge::get_merge_indicator(
                csr1.items.values().iter().copied(),
                csr2.items.values().iter().copied(),
            );

            let items = csr1.merge_items(&row_indicators[..], &csr2);

            assert_eq!(items, PrimitiveArray::from(expected))

        }
    }

    #[test]
    fn merge_empty_graph_fragments() {
        let csr1 = GraphFragment::from_sorted_triplets([].iter(), [].iter());
        let csr2 = GraphFragment::from_sorted_triplets([].iter(), [].iter());

        let actual = csr1.merge(&csr2);

        assert_eq!(actual, csr1)
    }

    #[test]
    fn merge_graph_fragments_1_vertex_outbound_2() {
        let csr1 = GraphFragment::from_sorted_triplets([(1, 2, 0)].iter(), [].iter());
        let csr2 = GraphFragment::from_sorted_triplets([(1, 3, 1)].iter(), [].iter());

        let expected =
            GraphFragment::from_sorted_triplets([(1, 2, 0), (1, 3, 1)].iter(), [].iter());

        let actual = csr1.merge(&csr2);

        assert_eq!(actual, expected)
    }

    #[test]
    fn merge_graph_fragments_1_vertex_outbound_from_2nd() {
        let csr1 = GraphFragment::from_sorted_triplets([].iter(), [].iter());
        let csr2 = GraphFragment::from_sorted_triplets([(0, 0, 0)].iter(), [].iter());

        let expected = GraphFragment::from_sorted_triplets([(0, 0, 0)].iter(), [].iter());

        let actual = csr1.merge(&csr2);

        assert_eq!(actual, expected)
    }

    #[test]
    fn merge_graph_fragments_1_vertex_both_lists() {
        let csr1 = GraphFragment::from_sorted_triplets([(1, 2, 0)].iter(), [(1, 9, 2)].iter());
        let csr2 = GraphFragment::from_sorted_triplets([(1, 3, 1)].iter(), [(1, 13, 4)].iter());

        let expected = GraphFragment::from_sorted_triplets(
            [(1, 2, 0), (1, 3, 1)].iter(),
            [(1, 9, 2), (1, 13, 4)].iter(),
        );

        let actual = csr1.merge(&csr2);

        assert_eq!(actual, expected)
    }

    #[test]
    fn merge_graph_fragments_2_vertices_both_lists_not_overlapping() {
        let csr1 = GraphFragment::from_sorted_triplets([(1, 2, 0)].iter(), [(1, 9, 2)].iter());
        let csr2 = GraphFragment::from_sorted_triplets([(2, 3, 1)].iter(), [(2, 13, 4)].iter());

        let expected = GraphFragment::from_sorted_triplets(
            [(1, 2, 0), (2, 3, 1)].iter(),
            [(1, 9, 2), (2, 13, 4)].iter(),
        );

        let actual = csr1.merge(&csr2);

        assert_eq!(actual, expected)
    }

    #[test]
    fn merge_graph_fragments_2_vertices_both_lists_overlapping() {
        let csr1 =
            GraphFragment::from_sorted_triplets([(1, 2, 0), (1, 3, 1)].iter(), [(1, 9, 2)].iter());
        let csr2 =
            GraphFragment::from_sorted_triplets([(1, 3, 1), (2, 7, 3)].iter(), [(1, 13, 4)].iter());

        let expected = GraphFragment::from_sorted_triplets(
            [(1, 2, 0), (1, 3, 1), (2, 7, 3)].iter(),
            [(1, 9, 2), (1, 13, 4)].iter(),
        );

        let actual = csr1.merge(&csr2);

        assert_eq!(actual, expected)
    }

    #[test]
    fn merge_outbound_in_csr1_and_csr2() {
        let inbound = vec![(1, 2, 0), (1, 3, 1), (1, 4, 2)];
        let outbound = vec![(1, 2, 0), (1, 3, 1), (1, 4, 2)];
        let csr1 = GraphFragment::from_sorted_triplets(outbound.iter(), inbound.iter());

        let inbound = vec![(1, 4, 2), (1, 5, 3), (1, 6, 4)];
        let outbound = vec![(1, 4, 2), (1, 5, 3), (1, 6, 4)];
        let csr2 = GraphFragment::from_sorted_triplets(outbound.iter(), inbound.iter());

        let fields = vec![
            Field::new("v", DataType::UInt64, false),
            Field::new("e", DataType::UInt64, false),
        ];
        let out_v = Box::new(MPArr::<u64>::new());
        let out_e = Box::new(MPArr::<u64>::new());
        let out_inner =
            MutableStructArray::new(DataType::Struct(fields.clone()), vec![out_v, out_e]);

        let mut outbound_arr = MutableListArray::<i32, MutableStructArray>::new_with_field(
            out_inner, "outbound", true,
        );

        for (left, right) in csr1.outbound.into_iter().zip(csr2.outbound.into_iter()) {
            GraphFragment::merge_lists(left, right, &mut outbound_arr);
        }

        let arr: ListArray<i32> = outbound_arr.into();
        let arr = arr.value(0);
        let arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
        let vs = arr.values()[0]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let es = arr.values()[1]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        // now assert

        let actual = vs.into_iter().flatten().copied().collect::<Vec<_>>();
        let expected = vec![2, 3, 4, 5, 6];
        assert_eq!(actual, expected);

        let actual = es.into_iter().flatten().copied().collect::<Vec<_>>();
        let expected = vec![0, 1, 2, 3, 4];
        assert_eq!(actual, expected);
    }

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
        let csr = GraphFragment::from_sorted_triplets(outbound.iter(), inbound.iter());
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
