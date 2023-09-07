use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fs::{File, OpenOptions},
    io,
    ops::{Deref, DerefMut},
    os::unix::prelude::OpenOptionsExt,
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
};

use bytemuck::{Pod, Zeroable};
use memmap2::{MmapMut, MmapOptions};
use parking_lot::{
    lock_api::{RwLockReadGuard, RwLockWriteGuard},
    RwLock,
};
use rio::Completion;

const PAGE_SIZE: usize = 4096;

#[derive(Debug, Pod, Clone, Copy, Zeroable)]
#[repr(C, align(4096))] // <-- align to page size
pub struct DataPage {
    bytes: [u8; PAGE_SIZE],
}

type PID = usize;

struct BufferManager {
    virt_size: usize,
    phys_size: usize,

    virt_count: usize,
    phys_count: usize,

    resident_set: HashSet<PID>,

    mmap: MmapMut,
    pages_ptr: *mut DataPage,

    phys_used_count: AtomicUsize,
    alloc_count: AtomicUsize,

    page_slots: Vec<PageSlot>,

    io_context: IOContext,
}

const GB: usize = 1024 * 1024 * 1024;
const MB: usize = 1024 * 1024;
const EVICT_BATCH: usize = 32;

impl BufferManager {
    pub fn new<P: AsRef<Path>>(
        virt_size: usize,
        phys_size: usize,
        path: P,
    ) -> Result<Self, io::Error> {
        assert!(virt_size > 0 && phys_size > 0 && virt_size >= phys_size); // make this a result

        let virt_alloc_size = virt_size + (1 << 16);

        let mut mmap = MmapOptions::new().len(virt_alloc_size).map_anon()?;
        let pages_ptr: *mut DataPage = mmap.as_mut_ptr() as *mut DataPage;

        let virt_count = virt_size / PAGE_SIZE;

        let mut page_slots = Vec::with_capacity(virt_count);
        for _ in 0..virt_count {
            page_slots.push(PageSlot::default());
        }

        let io_context = IOContext::new(path)?;

        Ok(Self {
            virt_size: virt_alloc_size,
            phys_size,

            virt_count,
            phys_count: phys_size / PAGE_SIZE,

            resident_set: HashSet::new(),

            mmap,
            pages_ptr,

            phys_used_count: AtomicUsize::new(0),
            alloc_count: AtomicUsize::new(0),

            page_slots,

            io_context,
        })
    }

    fn ensure_free_pages(&mut self) -> Result<(), io::Error>{
        let phys_used_count = self.phys_used_count.load(Ordering::Relaxed);
        let limit = (self.phys_count as f64 * 0.95).round();
        println!("phys_used_count: {phys_used_count}, limit: {limit}");

        if phys_used_count >= limit as usize {
            self.force_evict()?;
        }
        Ok(())
    }

    fn evict_pages(&mut self, pages: Vec<PID>) -> Result<(), io::Error> {
        let mut complete_writes = Vec::with_capacity(pages.len());

        for pid in pages {
            // take exclusive lock on each page we're evicting
            if let Some(mut guard) = self.page_slots[pid].lock.try_write() {
                let start = pid * PAGE_SIZE;
                let end = start + PAGE_SIZE;
                println!("writing page {} to disk at {start}..{end}", pid);
                {
                    let b: &[u8; PAGE_SIZE] = (&self.mmap[start..end]).try_into().unwrap();
                    let comp = self.io_context.write_at(start, b);


                    complete_writes.push((start, comp));
                }
                self.resident_set.remove(&pid);
                guard.take();
            }
        }
        let mut evicted_count = 0;
        // wait for all the IO to happen
        for (start, comp) in complete_writes {
            comp.wait()?;
            // tell the OS we're done with this page
            self.mmap.advise_range(memmap2::Advice::DontNeed, start, PAGE_SIZE)?;
            evicted_count += 1;
        }
        self.phys_used_count.fetch_sub(evicted_count, Ordering::Relaxed);
        Ok(())
    }

    pub fn force_evict(&mut self) -> Result<(), io::Error> {
        let mut evicted = 0;
        let mut evict_candidates = Vec::with_capacity(EVICT_BATCH);
        for pid in self.resident_set.iter() {
            // acquire the exclusive lock for this page (means no one else is using it)
            if let Some(_) = self.page_slots[*pid].lock.try_write() {
                evict_candidates.push(*pid);
                evicted += 1;
            }
            if evicted >= EVICT_BATCH {
                break;
            }
        }
        self.evict_pages(evict_candidates)
    }

    fn alloc_page(&mut self) -> Result<PageEntry<'_>, io::Error> {
        self.ensure_free_pages();
        let pid = self.alloc_count.fetch_add(1, Ordering::Relaxed);
        assert!(pid <= self.virt_count);
        self.load_page(pid)?;

        let read_guard = self.page_slots[pid].lock.read();

        Ok(PageEntry::new(pid, self, read_guard))
    }

    pub fn get_page_mut(&mut self, pid: PID) -> Result<PageEntryMut<'_>, io::Error> {
        // this is more or less alloc_page but we know what PID we want
        self.load_page(pid)?;
        let write_guard = self.page_slots[pid].lock.write();

        Ok(PageEntryMut::new(pid, self, write_guard))
    }

    pub fn get_page(&mut self, pid: PID) -> Result<PageEntry<'_>, io::Error> {
        // this is more or less alloc_page but we know what PID we want
        self.load_page(pid)?;
        let write_guard = self.page_slots[pid].lock.read();

        Ok(PageEntry::new(pid, self, write_guard))
    }

    fn load_page(&mut self, pid: PID) -> Result<(), io::Error> {
        self.ensure_free_pages()?;
        let mut slot = self.page_slots[pid].lock.write();
        if slot.is_none() {
            self.phys_used_count.fetch_add(1, Ordering::Relaxed);

            // we need to read the page from the file into memory
            let start = pid * PAGE_SIZE;
            let end = start + PAGE_SIZE;
            println!("reading page {} from disk at {start}..{end}", pid);
            {
                let b: &mut [u8; PAGE_SIZE] = (&mut self.mmap[start..end]).try_into().unwrap();
                self.io_context.read_at(start, b)?;
            }
            // done loading things into memory now we can cast the page
            let page = unsafe { self.pages_ptr.add(pid) };
            slot.replace(page);
            self.resident_set.insert(pid);
        }
        Ok(())
    }

    pub fn alloc_count(&self) -> usize {
        self.alloc_count.load(Ordering::Relaxed)
    }

    pub fn phys_used_count(&self) -> usize {
        self.phys_used_count.load(Ordering::Relaxed)
    }

    pub fn is_resident(&self, pid: PID) -> bool {
        self.resident_set.contains(&pid)
    }
}

struct IOContext {
    ring: rio::Rio,
    file: File,
}

impl IOContext {
    fn new<P: AsRef<Path>>(path: P) -> Result<Self, io::Error> {
        let ring = rio::new()?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            // .custom_flags(libc::O_DIRECT)
            .open(path)?;
        Ok(Self { ring, file })
    }

    fn read_at(&self, offset: usize, b: &mut [u8; PAGE_SIZE]) -> Result<usize, io::Error> {
        let completion = self.ring.read_at(&self.file, b, offset as u64);
        completion.wait()
    }

    fn write_at<'a>(&'a self, offset: usize, b: &'a [u8; PAGE_SIZE]) -> Completion<'a, usize> {
        self.ring.write_at(&self.file, b, offset as u64)
    }
}

struct PageSlot {
    lock: parking_lot::RwLock<Option<*mut DataPage>>,
}

impl Default for PageSlot {
    fn default() -> Self {
        Self {
            lock: RwLock::new(None),
        }
    }
}

pub struct PageEntry<'a> {
    pid: PID,
    bm: &'a BufferManager,
    read_guard: RwLockReadGuard<'a, parking_lot::RawRwLock, Option<*mut DataPage>>,
}

impl Deref for PageEntry<'_> {
    type Target = DataPage;

    fn deref(&self) -> &Self::Target {
        unsafe {
            let page = self.read_guard.as_ref().unwrap();
            &**page
        }
    }
}

pub struct PageEntryMut<'a> {
    pid: PID,
    bm: &'a BufferManager,
    write_guard: RwLockWriteGuard<'a, parking_lot::RawRwLock, Option<*mut DataPage>>,
}

impl Deref for PageEntryMut<'_> {
    type Target = DataPage;

    fn deref(&self) -> &Self::Target {
        unsafe {
            let page = self.write_guard.as_ref().unwrap();
            &**page
        }
    }
}

impl DerefMut for PageEntryMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            let page = self.write_guard.as_mut().unwrap();
            &mut **page
        }
    }
}

impl<'a> PageEntryMut<'a> {
    fn new(
        pid: PID,
        bm: &'a BufferManager,
        write_guard: RwLockWriteGuard<'a, parking_lot::RawRwLock, Option<*mut DataPage>>,
    ) -> Self {
        Self {
            pid,
            bm,
            write_guard,
        }
    }
}

impl<'a> PageEntry<'a> {
    fn new(
        pid: PID,
        bm: &'a BufferManager,
        read_guard: RwLockReadGuard<'a, parking_lot::RawRwLock, Option<*mut DataPage>>,
    ) -> Self {
        Self {
            pid,
            bm,
            read_guard,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn make_a_buffer_manager() {
        let mut buffer_manager = BufferManager::new(16 * MB, 4 * MB, "/tmp/bm").unwrap();
        let a_bit_extra = 1 << 16;
        assert_eq!(buffer_manager.virt_size, 16 * MB + a_bit_extra);
        assert_eq!(buffer_manager.mmap.len(), 16 * MB + a_bit_extra);

        // here goes nothing
        buffer_manager.alloc_page().unwrap();

        assert!(buffer_manager.is_resident(0));
        assert!(!buffer_manager.is_resident(1));

        // here goes nothing again!
        buffer_manager.alloc_page().unwrap();

        assert!(buffer_manager.is_resident(0));
        assert!(buffer_manager.is_resident(1));
        assert!(!buffer_manager.is_resident(2));

        assert!(buffer_manager.alloc_count() == 2);
    }

    #[test]
    fn bm_get_mut_page_change_then_read_page_check_change() {
        let mut buffer_manager = BufferManager::new(16 * MB, 4 * MB, "/tmp/bm").unwrap();
        {
            let mut page = buffer_manager.get_page_mut(0).unwrap();
            for i in 0..PAGE_SIZE {
                page.bytes[i] = i as u8;
            }
        }

        let page = buffer_manager.get_page(0).unwrap();
        for i in 0..PAGE_SIZE {
            assert_eq!(page.bytes[i], i as u8);
        }
    }

    #[test]
    fn bm_modify_3_pages_force_evict_read_pages_and_check_data_is_valid(){
        let mut buffer_manager = BufferManager::new(16 * MB, 4 * MB, "/tmp/bm").unwrap();
        {
            let mut page = buffer_manager.get_page_mut(0).unwrap();
            for i in 0..PAGE_SIZE {
                page.bytes[i] = i as u8;
            }
        }
        {
            let mut page = buffer_manager.get_page_mut(1).unwrap();
            for i in 0..PAGE_SIZE {
                page.bytes[i] = i as u8;
            }
        }
        {
            let mut page = buffer_manager.get_page_mut(2).unwrap();
            for i in 0..PAGE_SIZE {
                page.bytes[i] = i as u8;
            }
        }
        buffer_manager.force_evict().unwrap();

        // check phys_used_count
        assert_eq!(buffer_manager.phys_used_count(), 0);
        
        {
            let page = buffer_manager.get_page(0).unwrap();
            for i in 0..PAGE_SIZE {
                assert_eq!(page.bytes[i], i as u8);
            }
        }
        {
            let page = buffer_manager.get_page(1).unwrap();
            for i in 0..PAGE_SIZE {
                assert_eq!(page.bytes[i], i as u8);
            }
        }
        {
            let page = buffer_manager.get_page(2).unwrap();
            for i in 0..PAGE_SIZE {
                assert_eq!(page.bytes[i], i as u8);
            }
        }
    }

}