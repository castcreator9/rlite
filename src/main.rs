#![allow(dead_code)]
mod constants;
mod pager;
mod tokenizer;

use constants::TABLE_MAX_ROWS;
use pager::Table;
use tokenizer::{Row, Statement, StatementType, do_meta_command};

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

fn execute_insert(statement: &Statement, table: &mut Table) -> Result<(), ExecuteError> {
    if table.num_rows >= TABLE_MAX_ROWS {
        return Err(ExecuteError::TableFull);
    }

    let page = table.row_slot(table.num_rows);
    if let Some(row) = &statement.row {
        page.copy_from_slice(&row.serialize());
        table.num_rows += 1;
    }

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
    let args = std::env::args().collect::<Vec<String>>();
    if args.len() < 2 {
        println!("Must supply a database filename.");
        std::process::exit(0);
    }

    let mut table = match Table::new(&args[1]) {
        Ok(t) => t,
        Err(e) => panic!("{e}"),
    };

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
