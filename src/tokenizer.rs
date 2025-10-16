use crate::constants::{
    COLUMN_EMAIL_SIZE, COLUMN_USERNAME_SIZE, EMAIL_OFFSET, EMAIL_SIZE, ID_OFFSET, ID_SIZE,
    ROW_SIZE, USERNAME_OFFSET, USERNAME_SIZE,
};

pub enum MetaCommandError<'a> {
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

pub enum PrepareError<'a> {
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

pub fn do_meta_command(input: &str) -> Result<(), MetaCommandError<'_>> {
    if input.starts_with(".exit") {
        return Ok(());
    }

    let meta = input.split_whitespace().next().unwrap_or_default();
    Err(MetaCommandError::UnrecognizedCommand { meta, input })
}

pub enum StatementType {
    Insert,
    Select,
}

pub struct Row {
    id: u32,
    username: [u8; COLUMN_USERNAME_SIZE],
    email: [u8; COLUMN_EMAIL_SIZE],
}

impl Row {
    pub fn deserialize(bytes: &[u8]) -> Self {
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

    pub fn serialize(&self) -> [u8; ROW_SIZE] {
        let mut bytes = [0u8; ROW_SIZE];

        let id = self.id.to_le_bytes();
        bytes[ID_OFFSET..ID_OFFSET + ID_SIZE].copy_from_slice(&id);
        bytes[USERNAME_OFFSET..USERNAME_OFFSET + USERNAME_SIZE].copy_from_slice(&self.username);
        bytes[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_SIZE].copy_from_slice(&self.email);

        bytes
    }

    pub fn print(&self) {
        let username = String::from_utf8_lossy(&self.username);
        let email = String::from_utf8_lossy(&self.email);
        println!("({} {} {})", self.id, username, email);
    }
}

// Username and Email has to be ASCII character to fit in their size
// 32b and 255b al input has to be 1 char = 1 byte
pub struct Statement {
    pub stype: StatementType,
    pub row: Option<Row>,
}

impl Statement {
    /// Parses a statement from input text.
    ///
    /// Expected format:
    /// `insert <id> <username> <email>`
    /// or
    /// `select`
    pub fn prepare_statement(input: &str) -> Result<Self, PrepareError<'_>> {
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
