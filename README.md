# Rustic SQL

## Overview

**SQL Rústico** is a basic SQL engine implemented in Rust. It processes SQL-like commands (`INSERT`, `UPDATE`, `DELETE`, `SELECT`) using tables stored as CSV files. The first row in each CSV represents column names, while subsequent rows contain data records.

This project is designed to provide an educational understanding of SQL processing and Rust's memory management principles.

## Features

### Supported Commands
- **`INSERT`**: Add new records.
- **`UPDATE`**: Modify existing records.
- **`DELETE`**: Remove records.
- **`SELECT`**: Read data with:
  - **WHERE** filters.
  - Comparison operators (`=`, `<`, `>`, etc.).
  - Boolean logic (`AND`, `OR`, `NOT`).
  - Sorting (`ORDER BY`).

### Input Format
Run the program with:
```bash
cargo run -- <path/to/tables> "<SQL query>"
```
## Requirements
- Rust (latest stable version).
- Compatible with Unix/Linux.
- No external crates or unsafe code.
- Code must pass ```cargo fmt``` and ```cargo clippy```.

