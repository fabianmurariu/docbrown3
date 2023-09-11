use std::{fs::File, io::BufReader, ops::Deref};

use csv::ReaderBuilder;
use docbrown3::{BufferManager, GB, MB};
use flate2::read::GzDecoder;
use heed::{
    types::{OwnedType, Str},
    Database, EnvOpenOptions,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct BTC {
    src: String,
    dst: String,
}

fn main() {
    let file_path = std::env::args().nth(1).expect("Please provide a file path");
    let input = std::env::args()
        .nth(2)
        .expect("Please provide a file path to read from");
    let mut bm =
        BufferManager::new(16 * GB, 100 * MB, file_path).expect("Failed to create buffer manager");

    let total_pages = bm.virt_count();
    println!("Total pages: {}", total_pages);

    // read line by line the bitcoin dataset via CSV with Gzip compression
    // create a hashmap from the address to the ordered position on a vector

    // Open the gzipped file
    let file = File::open(input).expect("Failed to open file");
    let gz_decoder = GzDecoder::new(file);
    let reader = BufReader::new(gz_decoder);

    // Create a CSV reader with Serde support
    let mut rdr = ReaderBuilder::new().has_headers(false).from_reader(reader);

    let env = EnvOpenOptions::new()
        .open("global_vertex_mapping.mdb")
        .expect("Failed to open env");

    let db: Database<Str, OwnedType<usize>> =
        env.create_database(None).expect("Failed to create db");

    let mut id_count = 0;
    // Iterate over each record and deserialize into MyStruct
    for result in rdr.deserialize::<BTC>().take(10) {
        let record: BTC = result.expect("Failed to deserialize");

        // find the src and dst in the db
        let mut txn = env.write_txn().expect("Failed to create write txn");

        let src_id = db.get(&txn, &record.src).expect("Failed to get src id");
        
        if let None = src_id {
            db.put(&mut txn, &record.src, &id_count)
                .expect("Failed to put src id");
            id_count += 1;
        }

        let dst_id = db.get(&txn, &record.dst).expect("Failed to get dst id");

        if let None = dst_id {
            db.put(&mut txn, &record.dst, &id_count)
                .expect("Failed to put dst id");
            id_count += 1;
        }

        println!("{:?}", record);
    }
}
