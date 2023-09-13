use arrow2::{
    array::{
        Array, ListArray, MutableArray, MutableListArray, MutablePrimitiveArray,
        MutableStructArray, PrimitiveArray, StructArray,
    },
    datatypes::{DataType, Field},
    types::NativeType,
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
struct VecEdgePair<'a>(&'a mut MutableStructArray);

impl<'a> VecEdgePair<'a> {
    fn add_pair(&mut self, v: u64, e: u64) {
        self.0.value::<MPArr<u64>>(0).unwrap().push(Some(v));
        self.0.value::<MPArr<u64>>(1).unwrap().push(Some(e));
        self.0.push(true);
    }
}

impl<'a> From<&'a mut MutableStructArray> for VecEdgePair<'a> {
    fn from(arr: &'a mut MutableStructArray) -> Self {
        Self(arr)
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

impl GraphFragment {
    pub fn neighbours_iter(&self, node: u64) -> Option<Box<dyn Iterator<Item = (u64, u64)>>> {
        self.items
            .values()
            .as_slice()
            .binary_search(&node)
            .ok()
            .and_then(|idx| {
                let arr = self
                    .outbound
                    .value(idx)
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .cloned();
                arr
            })
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
            })
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
                    let mut row: VecEdgePair = outbound_arr.mut_values().into();
                    row.add_pair(dst, e);
                }
                IngestEdgeType::Inbound((_, dst, e)) => {
                    let mut row: VecEdgePair = inbound_arr.mut_values().into();
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
}

#[cfg(test)]
mod test {

    use arrow2::{
        array::{Int32Array, StructArray},
        datatypes::{DataType, Field},
    };

    use super::*;

    #[test]
    fn test() {
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
        println!("graph fragment {:?}", csr);
        assert_eq!(csr.len(), 5);

        // // the adjacency list of node 1 is [2, 3]
        // let adj = csr.adj(1);

        // let neighbours = Int32Array::from_slice(&[2, 3]).boxed();
        // let edge_ids = Int32Array::from_slice(&[0, 1]).boxed();

        // let fields = vec![
        //     Field::new("v", DataType::Int64, false),
        //     Field::new("e", DataType::Int64, false),
        // ];

        // let edges =
        //     StructArray::new(DataType::Struct(fields), vec![neighbours, edge_ids], None).boxed();

        // assert_eq!(adj, edges)
    }
}
