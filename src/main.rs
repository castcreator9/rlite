#![allow(dead_code)]

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;
const ID_SIZE: usize = std::mem::size_of::<u32>();
const USERNAME_SIZE: usize = std::mem::size_of::<[u8; 32]>();
const EMAIL_SIZE: usize = std::mem::size_of::<[u8; 255]>();
const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

fn read_input() -> String {
    use std::io::Write;
    let mut line = String::new();

    print!("db> ");
    let _ = std::io::stdout()
        .flush()
        .map_err(|e| println!("Unable to flush prompt: {e}"));
    let _ = std::io::stdin()
        .read_line(&mut line)
        .map_err(|e| println!("Unable to read line: {e}"));
    line.trim().to_string()
}

enum MetaCommandError<'a> {
    UnrecognizedCommand { meta: &'a str, input: &'a str },
}

impl std::fmt::Display for MetaCommandError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetaCommandError::UnrecognizedCommand { meta, input } => {
                write!(f, "Unrecognized command: '{meta}' in '{input}'")
            }
        }
    }
}

enum PrepareError<'a> {
    UnrecognizedStatement { statement: &'a str, input: &'a str },
    InvalidInput { input: &'a str },
    InvalidId { id: &'a str, input: &'a str },
    NegativeNumber { id: &'a str, input: &'a str },
    UsernameTooLong { username: &'a str, input: &'a str },
    EmailTooLong { email: &'a str, input: &'a str },
}

impl std::fmt::Display for PrepareError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrepareError::UnrecognizedStatement { statement, input } => {
                write!(f, "Unrecognized statement: '{statement}' in '{input}'.")
            }
            PrepareError::InvalidInput { input } => {
                write!(f, "Invalid input: '{input}'.")
            }
            PrepareError::InvalidId { id, input } => {
                write!(
                    f,
                    "Invalid id: '{id}' in '{input}'.\nId has to be a positive integer."
                )
            }
            PrepareError::NegativeNumber { id, input } => {
                write!(
                    f,
                    "Invalid id: '{id} in '{input}'.\nId has to be a positive integer."
                )
            }
            PrepareError::UsernameTooLong { username, input } => {
                write!(
                    f,
                    "Invalid username: '{}' in '{}'.\nMaximum valid size: {}.\nUsername's size: {}",
                    username,
                    input,
                    COLUMN_USERNAME_SIZE,
                    username.len()
                )
            }
            PrepareError::EmailTooLong { email, input } => {
                write!(
                    f,
                    "Invalid email: '{}' in '{}'.\nMaximum valid size: {}.\nEmail's size: {}",
                    email,
                    input,
                    COLUMN_EMAIL_SIZE,
                    email.len()
                )
            }
        }
    }
}

enum ExecuteError {
    TableFull,
}

impl std::fmt::Display for ExecuteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecuteError::TableFull => write!(f, "Table full."),
        }
    }
}

fn do_meta_command(input: &str) -> Result<(), MetaCommandError<'_>> {
    if input.starts_with(".exit") {
        return Ok(());
    }

    let meta = input.split_whitespace().next().unwrap_or_default();
    Err(MetaCommandError::UnrecognizedCommand { meta, input })
}

enum StatementType {
    Insert,
    Select,
}

struct Row {
    id: u32,
    username: [u8; COLUMN_USERNAME_SIZE],
    email: [u8; COLUMN_EMAIL_SIZE],
}

impl Row {
    fn deserialize(bytes: &[u8]) -> Self {
        let mut id_bytes = [0u8; ID_SIZE];
        let mut username_bytes = [0u8; USERNAME_SIZE];
        let mut email_bytes = [0u8; EMAIL_SIZE];

        id_bytes.copy_from_slice(&bytes[ID_OFFSET..ID_OFFSET + ID_SIZE]);
        username_bytes.copy_from_slice(&bytes[USERNAME_OFFSET..USERNAME_OFFSET + USERNAME_SIZE]);
        email_bytes.copy_from_slice(&bytes[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_SIZE]);
        let id = u32::from_le_bytes(id_bytes);

        Self {
            id,
            username: username_bytes,
            email: email_bytes,
        }
    }

    fn serialize(&self) -> [u8; ROW_SIZE] {
        let mut bytes = [0u8; ROW_SIZE];

        let id = self.id.to_le_bytes();
        bytes[ID_OFFSET..ID_OFFSET + ID_SIZE].copy_from_slice(&id);
        bytes[USERNAME_OFFSET..USERNAME_OFFSET + USERNAME_SIZE].copy_from_slice(&self.username);
        bytes[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_SIZE].copy_from_slice(&self.email);

        bytes
    }

    fn print(&self) {
        let username = String::from_utf8_lossy(&self.username);
        let email = String::from_utf8_lossy(&self.email);
        println!("({} {} {})", self.id, username, email);
    }
}

// Username and Email has to be ASCII character to fit in their size
// 32b and 255b al input has to be 1 char = 1 byte
struct Statement {
    stype: StatementType,
    row: Option<Row>,
}

impl Statement {
    /// Parses a statement from input text.
    ///
    /// Expected format:
    /// `insert <id> <username> <email>`
    /// or
    /// `select`
    fn prepare_statement(input: &str) -> Result<Self, PrepareError<'_>> {
        let mut parts = input.split_whitespace();
        let statement = parts.next().ok_or(PrepareError::InvalidInput { input })?;

        let row = if statement == "insert" {
            let id = parts
                .next()
                .ok_or(PrepareError::InvalidInput { input })
                .and_then(|v| {
                    if v.starts_with('-') {
                        return Err(PrepareError::NegativeNumber { id: v, input });
                    }
                    v.parse::<u32>()
                        .map_err(|_| PrepareError::InvalidId { id: v, input })
                })?;
            let username = parts
                .next()
                .ok_or(PrepareError::InvalidInput { input })
                .and_then(|s| {
                    let mut bytes = [0u8; USERNAME_SIZE];
                    if s.len() > COLUMN_USERNAME_SIZE {
                        return Err(PrepareError::UsernameTooLong { username: s, input });
                    }
                    bytes[..s.len()].copy_from_slice(s.as_bytes());
                    Ok(bytes)
                })?;
            let email = parts
                .next()
                .ok_or(PrepareError::InvalidInput { input })
                .and_then(|s| {
                    let mut bytes = [0u8; EMAIL_SIZE];
                    if s.len() > COLUMN_EMAIL_SIZE {
                        return Err(PrepareError::EmailTooLong { email: s, input });
                    }
                    bytes[..s.len()].copy_from_slice(s.as_bytes());
                    Ok(bytes)
                })?;

            Some(Row {
                id,
                username,
                email,
            })
        } else {
            None
        };

        let stype = match statement {
            "insert" => StatementType::Insert,
            "select" => StatementType::Select,
            _ => return Err(PrepareError::UnrecognizedStatement { statement, input }),
        };

        Ok(Self { stype, row })
    }
}

struct Table {
    num_rows: usize,
    pages: Box<[Option<[u8; PAGE_SIZE]>; TABLE_MAX_PAGES]>,
}

impl Table {
    fn new() -> Self {
        Self {
            num_rows: 0,
            pages: Box::new(std::array::from_fn(|_| None)),
        }
    }

    fn row_slot(&mut self, row_num: usize) -> &mut [u8] {
        let page_num = row_num / ROWS_PER_PAGE;
        let page = self.pages[page_num].get_or_insert([0u8; PAGE_SIZE]);

        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;

        &mut page[byte_offset..byte_offset + ROW_SIZE]
    }
}

fn execute_insert(statement: &Statement, table: &mut Table) -> Result<(), ExecuteError> {
    if table.num_rows >= TABLE_MAX_ROWS {
        return Err(ExecuteError::TableFull);
    }

    let page = table.row_slot(table.num_rows);
    if let Some(row) = &statement.row {
        page.copy_from_slice(&row.serialize());
    }
    table.num_rows += 1;

    Ok(())
}

fn execute_select(table: &mut Table) {
    for i in 0..table.num_rows {
        let row = table.row_slot(i);
        Row::deserialize(row).print();
    }
}

fn execute_statement(statement: &Statement, table: &mut Table) -> Result<(), ExecuteError> {
    match statement.stype {
        StatementType::Insert => execute_insert(statement, table),
        StatementType::Select => {
            execute_select(table);
            Ok(())
        }
    }
}

fn main() {
    let mut table = Table::new();
    loop {
        let input = read_input();

        if input.starts_with('.') {
            match do_meta_command(&input) {
                Ok(()) => break,
                Err(e) => {
                    println!("{e}");
                    continue;
                }
            }
        }

        let statement = match Statement::prepare_statement(&input) {
            Ok(s) => s,
            Err(e) => {
                println!("{e}");
                continue;
            }
        };

        match execute_statement(&statement, &mut table) {
            Ok(()) => println!("Executed."),
            Err(e) => println!("{e}"),
        }
    }
}
