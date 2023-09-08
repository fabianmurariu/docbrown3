use std::ops::Deref;

use docbrown3::{BufferManager, MB};

fn main() {
    let file_path = std::env::args().nth(1).expect("Please provide a file path");
    let mut bm =
        BufferManager::new(256 * MB, 16 * MB, file_path).expect("Failed to create buffer manager");

    let total_pages = bm.virt_count();
    println!("Total pages: {}", total_pages);

    for pid in 0..50000 {
        let page = bm.get_page(pid).expect("Failed to get page");
        let data = page.deref();

        
        println!("Page {} is {:?}", pid, page.len());
    }
}
