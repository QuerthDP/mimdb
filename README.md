# MIMDB - Columnar Analytical Database

MIMDB is a columnar analytical database system implemented in Rust. It provides a REST API for database operations, persistent storage with compression, and Docker support for easy deployment.

## Features

### Database System
- **REST API**: Full HTTP API for database operations (tables, queries, results)
- **Metastore**: Persistent metadata storage surviving restarts
- **COPY operations**: Load CSV files into tables
- **SELECT operations**: Query all rows from a table
- **Atomic operations**: Data changes are visible only after completion

### File Format
- **Two column data types**: 64-bit signed integers (INT64) and variable-length strings (VARCHAR)
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
- **Batch processing** for memory-efficient handling of large datasets
- **Configurable batch sizes** to optimize memory usage vs. performance
- **Streaming decompression** for processing data larger than available RAM

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

## Quick Start

### Building

```bash
# Build the project
make build

# Build release version
make release

# Build only the server
make server
```

### Running the Server

#### Local Development

```bash
# Run the server locally (release mode)
make run

# Or with cargo directly
cargo run --bin server -- --port 3000 --data-dir ./mimdb_data
```

The server will start on `http://localhost:3000` (by default).

#### Docker Deployment

```bash
# Build Docker image
make docker

# Run Docker container
make docker-run

# Or manually with custom paths:
docker run -d \
    --name mimdb \
    -p 3000:3000 \
    -v /path/to/csv/files:/data \
    -v mimdb_data:/app/mimdb_data \
    mimdb:latest
```

**Volumes:**
- `/data` - Mount your CSV files here for COPY operations
- `/app/mimdb_data` - Database storage (metadata and data files)

**Important:** The `sourceFilepath` in COPY queries must use the container path (`/data/...`), not your local path.

## REST API

The server implements a REST API according to the `api/dbmsInterface.yaml` OpenAPI specification.

### API Documentation

The server provides interactive API documentation via Swagger UI:
- **Swagger UI**: `http://localhost:3000/swagger-ui`
- **OpenAPI JSON**: `http://localhost:3000/api-docs/openapi.json`

### Table Operations

#### List all tables
```bash
curl http://localhost:3000/tables
```

#### Get table details
```bash
curl http://localhost:3000/table/{tableId}
```

#### Create a table
```bash
curl -X PUT http://localhost:3000/table \
  -H "Content-Type: application/json" \
  -d '{
    "name": "users",
    "columns": [
      {"name": "id", "type": "INT64"},
      {"name": "name", "type": "VARCHAR"}
    ]
  }'
```

#### Delete a table
```bash
curl -X DELETE http://localhost:3000/table/{tableId}
```

### Query Operations

#### List all queries
```bash
curl http://localhost:3000/queries
```

#### Get query details
```bash
curl http://localhost:3000/query/{queryId}
```

#### Execute COPY query (load CSV into table)
```bash
curl -X POST http://localhost:3000/query \
  -H "Content-Type: application/json" \
  -d '{
    "queryDefinition": {
      "sourceFilepath": "/data/users.csv",
      "destinationTableName": "users",
      "doesCsvContainHeader": false
    }
  }'
```

#### Execute SELECT query
```bash
curl -X POST http://localhost:3000/query \
  -H "Content-Type: application/json" \
  -d '{
    "queryDefinition": {
      "tableName": "users"
    }
  }'
```

### Result Operations

#### Get query result
```bash
curl http://localhost:3000/result/{queryId}

# With row limit
curl -X GET http://localhost:3000/result/{queryId} \
  -H "Content-Type: application/json" \
  -d '{"rowLimit": 100}'
```

#### Get query error (for failed queries)
```bash
curl http://localhost:3000/error/{queryId}
```

### System Information

```bash
curl http://localhost:3000/system/info
```

## Example Workflow

```bash
# 1. Create a table
TABLE_ID=$(curl -s -X PUT http://localhost:3000/table \
  -H "Content-Type: application/json" \
  -d '{
    "name": "employees",
    "columns": [
      {"name": "id", "type": "INT64"},
      {"name": "name", "type": "VARCHAR"},
      {"name": "salary", "type": "INT64"}
    ]
  }' | tr -d '"')

echo "Created table: $TABLE_ID"

# 2. Prepare CSV file (in /data directory when using Docker)
echo "1,Alice,50000" > /data/employees.csv
echo "2,Bob,60000" >> /data/employees.csv
echo "3,Charlie,55000" >> /data/employees.csv

# 3. Load CSV data into table
COPY_QUERY_ID=$(curl -s -X POST http://localhost:3000/query \
  -H "Content-Type: application/json" \
  -d '{
    "queryDefinition": {
      "sourceFilepath": "/data/employees.csv",
      "destinationTableName": "employees",
      "doesCsvContainHeader": false
    }
  }' | tr -d '"')

echo "COPY query ID: $COPY_QUERY_ID"

# 4. Check query status
curl http://localhost:3000/query/$COPY_QUERY_ID

# 5. Select all data from table
SELECT_QUERY_ID=$(curl -s -X POST http://localhost:3000/query \
  -H "Content-Type: application/json" \
  -d '{
    "queryDefinition": {
      "tableName": "employees"
    }
  }' | tr -d '"')

echo "SELECT query ID: $SELECT_QUERY_ID"

# 6. Get results
curl http://localhost:3000/result/$SELECT_QUERY_ID
```

## Library Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
mimdb = "0.2.0"
```

### Creating a table
```rust
use mimdb::{Table, ColumnData};

let mut table = Table::new();
table.add_column("id".to_string(), ColumnData::Int64(vec![1, 2, 3, 4, 5]))?;
table.add_column("name".to_string(), ColumnData::Varchar(vec![
    "Alice".to_string(),
    "Bob".to_string(),
    "Charlie".to_string(),
    "Diana".to_string(),
    "Eve".to_string()
]))?;
```

### Serialization
```rust
table.serialize("data.mimdb")?;
```

### Deserialization
```rust
let loaded_table = Table::deserialize("data.mimdb")?;
```

### Computing metrics
```rust
let averages = table.calculate_int_averages();
let ascii_counts = table.calculate_ascii_counts();
table.print_metrics();
```

### Batch processing for large datasets
```rust
use mimdb::serialization::BatchConfig;

// Default batch processing (100k rows per batch)
table.serialize_with_config("large_data.mimdb", &BatchConfig::default())?;

// Custom batch size for memory-constrained environments
let config = BatchConfig::new(50_000);
table.serialize_with_config("large_data.mimdb", &config)?;

// Load with batch processing
let loaded_table = Table::deserialize_with_config("large_data.mimdb", &config)?;
```

## Running Tests

```bash
# Run all tests
make test

# Or with cargo
cargo test
```

## Tools

The project includes several command-line utilities in the `mimdb/bin/` directory. See the [binaries README](mimdb/bin/README.md) for detailed documentation.

### MIMDB File Loader

The `loader` utility allows you to inspect and analyze existing MIMDB files from the command line.

```bash
# Build the loader tool
cargo build --bin loader

# Analyze a MIMDB file
./target/debug/loader examples/data/simple_example.mimdb
```

## Examples

The library includes three examples in the `examples/` directory:

1. **`simple_usage.rs`** - Comprehensive demonstration showing all library features including table creation, serialization, deserialization, and data integrity verification
2. **`data_analysis.rs`** - Advanced analytics example demonstrating statistical analysis and character analysis capabilities
3. **`batch_processing.rs`** - Demonstrates memory-efficient processing of large datasets (10M+ rows) using configurable batch sizes

See the [examples README](examples/README.md) for detailed information about each example.

## License

This project is intended for educational use only by employees and students of MIMUW.