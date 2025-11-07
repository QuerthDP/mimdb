# MIMDB - Columnar Analytical Database Library

MIMDB is a Rust library for working with columnar analytical data storage. It provides a custom columnar file format with compression and optimizations for analytical processing.

## Features

### File Format
- **Two column data types**: 64-bit signed integers (int64) and variable-length strings (VARCHAR)
- **Int64 compression**: Variable Length Encoding (VLE) + Delta Encoding + ZSTD
- **VARCHAR compression**: LZ4 with length prefixes
- **Tabular data**: all columns have the same length (number of rows)
- **File header**: column metadata, versioning, magic bytes

### In-Memory Representation
- **Columnar data structure** optimized for CPU performance
- **Cache-friendly layout** for better data locality
- **Type safety** using Rust enums

### Serialization and Deserialization
- **File writing** with automatic compression
- **File reading** with decompression and validation
- **Data integrity verification**

### Analytical Metrics
- **Average values** for int64 columns
- **ASCII character count** for VARCHAR columns
- **Summaries** for each column

## Code Structure

```rust
// Main data types
pub enum ColumnType { Int64, Varchar }
pub enum ColumnData { Int64(Vec<i64>), Varchar(Vec<String>) }
pub struct Table { columns: HashMap<String, ColumnData>, row_count: usize }

// File format
struct FileHeader { version, column_count, row_count, columns }
struct ColumnMeta { name, column_type, compressed_size, uncompressed_size, row_count }
```

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
mimdb = "0.1.0"
```

### Creating a table
```rust
use mimdb::{Table, ColumnData};

let mut table = Table::new();
table.add_column("id".to_string(), ColumnData::Int64(vec![1, 2, 3, 4, 5]))?;
table.add_column("name".to_string(), ColumnData::Varchar(vec!["Alice".to_string(), "Bob".to_string()]))?;
```

### Serialization
```rust
table.save_to_file("data.mimdb")?;
```

### Deserialization
```rust
let loaded_table = Table::load_from_file("data.mimdb")?;
```

### Computing metrics
```rust
let averages = table.calculate_int_averages();
let ascii_counts = table.calculate_ascii_counts();
table.print_metrics();
```

## Building and Running

```bash
# Build the library
cargo build

# Run tests
cargo test
```

## Demo

The demo program creates a sample table with 10 rows and 5 columns:
- **int64 columns**: `id`, `score`, `delta_test`
- **varchar columns**: `name`, `job_title`

Then it:
1. Serializes the table to file `example_table.mimdb`
2. Calculates and displays metrics
3. Deserializes from file
4. Verifies data integrity
5. Compares metrics before and after

## Technologies

- **Rust 2024 Edition** - memory safety and performance
- **ZSTD** - compression for numeric data
- **LZ4** - fast compression for text data
- **Serde + Bincode** - metadata serialization
- **Anyhow** - error handling

## Optimizations

1. **Delta Encoding** - reduces size of numeric data with similar values
2. **Variable Length Encoding** - reduces size of small numbers
3. **Zigzag Encoding** - efficient encoding of negative numbers
4. **Columnar layout** - better memory locality for analytical operations
5. **Algorithmic compression** - ZSTD for numbers, LZ4 for text

## Performance

Example results for a 10x5 table:
- **Uncompressed size**: ~400 bytes of data + metadata
- **File size**: 575 bytes (includes header, metadata and compressed data)
- **Compression**: ~30% space savings for example data

## Extensions

The project can be extended with:
- Additional data types (float, boolean, timestamp)
- Column indexes
- Parallel processing
- REST API endpoints
- Advanced statistical metrics
- Query language

## License

This project is intended for educational use only by employees and students of MIMUW.