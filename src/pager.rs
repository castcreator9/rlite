use crate::constants::{PAGE_SIZE, ROW_SIZE, ROWS_PER_PAGE, TABLE_MAX_PAGES};
use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

#[derive(Debug)]
pub enum PageError {
    Io(std::io::Error),
    TryFromIntError(std::num::TryFromIntError),
    FetchOutOfBounds(usize),
}

impl From<std::io::Error> for PageError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<std::num::TryFromIntError> for PageError {
    fn from(value: std::num::TryFromIntError) -> Self {
        Self::TryFromIntError(value)
    }
}

impl std::fmt::Display for PageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "IO Error: {err}"),
            Self::FetchOutOfBounds(i) => {
                write!(
                    f,
                    "Tried to fetch page out of bounds. {i} > {TABLE_MAX_PAGES}"
                )
            }
            Self::TryFromIntError(err) => write!(f, "TryFromIntError: {err}"),
        }
    }
}

pub struct Pager {
    file: File,
    file_length: usize,
    pages: Box<[Option<[u8; PAGE_SIZE]>; TABLE_MAX_PAGES]>,
}

impl Pager {
    fn new(filename: impl AsRef<Path>) -> Result<Self, PageError> {
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(filename)?;
        //let file_length = file.seek(SeekFrom::End(0))?;
        let file_length = usize::try_from(file.seek(SeekFrom::End(0))?)?;

        Ok(Self {
            file,
            file_length,
            pages: Box::new(std::array::from_fn(|_| None)),
        })
    }

    fn get_page(&mut self, page_num: usize) -> Result<&mut [u8], PageError> {
        if page_num > TABLE_MAX_PAGES {
            return Err(PageError::FetchOutOfBounds(page_num));
        }

        if self.pages[page_num].is_none() {
            let mut page = [0u8; PAGE_SIZE];
            let mut num_pages = self.file_length / PAGE_SIZE;

            if !(self.file_length).is_multiple_of(PAGE_SIZE) {
                num_pages += 1;
            }

            if page_num <= num_pages {
                self.file
                    .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))?;
                let _ = self.file.read(&mut page)?;
            }
            self.pages[page_num] = Some(page);
        }

        Ok(self.pages[page_num].as_mut().unwrap())
    }

    fn flush(&mut self, page_num: usize, size: usize) {
        if let Some(page) = self.pages[page_num] {
            self.file
                .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                .expect("Unable to seek in flush");
            self.file
                .write_all(&page[..size])
                .expect("Unable to write in flush");
        }
    }
}

pub struct Table {
    pub num_rows: usize,
    pager: Pager,
}

impl Table {
    pub fn new(filename: impl AsRef<Path>) -> Result<Self, PageError> {
        let pager = Pager::new(filename)?;
        let num_rows = pager.file_length / ROW_SIZE;

        Ok(Self { num_rows, pager })
    }

    // Will panic for out of bounds
    pub fn row_slot(&mut self, row_num: usize) -> &mut [u8] {
        let page_num = row_num / ROWS_PER_PAGE;
        let page = self.pager.get_page(page_num).unwrap();

        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;

        &mut page[byte_offset..byte_offset + ROW_SIZE]
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        let pager = &mut self.pager;
        let num_full_pages = self.num_rows / ROWS_PER_PAGE;

        for i in 0..num_full_pages {
            if pager.pages[i].is_none() {
                continue;
            }
            pager.flush(i, PAGE_SIZE);
            pager.pages[i] = None;
        }

        // Partial pages
        let num_aditional_rows = self.num_rows % ROWS_PER_PAGE;
        if num_aditional_rows > 0 {
            let page_num = num_full_pages;
            if pager.pages[page_num].is_some() {
                pager.flush(page_num, num_aditional_rows * ROW_SIZE);
                pager.pages[page_num] = None;
            }
        }

        for page in pager.pages.iter_mut() {
            *page = None;
        }
    }
}
