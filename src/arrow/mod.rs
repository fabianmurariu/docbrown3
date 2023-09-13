use arrow2::{
    array::{
        Array, ListArray, MutableArray, MutableListArray, MutablePrimitiveArray,
        MutableStructArray, PrimitiveArray,
    },
    datatypes::{DataType, Field},
    types::NativeType,
};
use itertools::Itertools;

#[derive(Debug)]
pub struct GraphFragment {
    items: PrimitiveArray<u64>,
    outbound: ListArray<i32>, // struct {v_id, e_id}
                              // inbound: ListArray<i32>,  // struct {v_id, e_id}
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
    pub fn from_sorted_triplets<I: IntoIterator<Item = (u64, u64, u64)>>(
        outbound: I,
        inbound: I,
    ) -> Self {
        let fields = vec![
            Field::new("v", DataType::UInt64, false),
            Field::new("e", DataType::UInt64, false),
        ];

        let mut items = MPArr::<u64>::new();

        let out_v = Box::new(MPArr::<u64>::new());
        let out_e = Box::new(MPArr::<u64>::new());
        let out_inner = MutableStructArray::new(DataType::Struct(fields), vec![out_v, out_e]);

        let mut outbound_arr = MutableListArray::<i32, MutableStructArray>::new_with_field(
            out_inner, "outbound", true,
        );

        let in_iter: Box<dyn Iterator<Item = IngestEdgeType>> =
            Box::new(inbound.into_iter().map(|t| IngestEdgeType::Inbound(t)));
        let out_iter = Box::new(outbound.into_iter().map(|t| IngestEdgeType::Outbound(t)));

        let iter = [in_iter, out_iter]
            .into_iter()
            .kmerge_by(|a, b| a.vertex() < b.vertex());

        let mut cur = None;
        for (src, dst, e) in outbound {
            if cur.is_none() {
                // happens once
                cur = Some(src);
            }

            if cur != Some(src) {
                items.push(cur);
                outbound_arr.try_push_valid().expect("push valid"); // one row done
                cur = Some(src);
            }

            let mut row: VecEdgePair = outbound_arr.mut_values().into();
            row.add_pair(dst, e);
        }

        items.push(cur);
        outbound_arr.try_push_valid().expect("push valid"); // last row;

        Self {
            items: items.into(),
            outbound: outbound_arr.into(),
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
        assert_eq!(csr.len(), 4);

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
