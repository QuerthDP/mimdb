# MIMDB Binaries

This directory contains the executable binaries for the MIMDB database system.

## Server

The main binary is the MIMDB REST API server, which provides a full HTTP interface for database operations.

### Starting the Server

```bash
# Start with default settings (port 3000, data in ./mimdb_data)
cargo run --bin server

# Start on a custom port
cargo run --bin server -- --port 8080

# Start with a custom data directory
cargo run --bin server -- --data-dir /path/to/data

# Combine options
cargo run --bin server -- --port 8080 --data-dir /var/lib/mimdb
```

### Command Line Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--port` | `-p` | Port to listen on | `3000` |
| `--data-dir` | `-d` | Data directory path | `./mimdb_data` |
| `--help` | `-h` | Show help message | - |

### Features

- **REST API**: Full HTTP API conforming to `api/dbmsInterface.yaml` OpenAPI specification
- **Swagger UI**: Interactive API documentation at `http://localhost:3000/swagger-ui` (by default)
- **Persistent Storage**: Metadata and data files survive server restarts
- **Structured Logging**: Request logging via Tower middleware with tracing support

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/tables` | GET | List all tables |
| `/table/{id}` | GET | Get table details |
| `/table` | PUT | Create a new table |
| `/table/{id}` | DELETE | Delete a table |
| `/queries` | GET | List all queries |
| `/query/{id}` | GET | Get query status |
| `/query` | POST | Submit a query (COPY or SELECT) |
| `/result/{id}` | GET | Get query results |
| `/error/{id}` | GET | Get query error details |
| `/system/info` | GET | Get system information |

### Docker Deployment

```bash
# Build and run with Docker
make docker
make docker-run

# Or manually
docker run -d \
    --name mimdb \
    -p 3000:3000 \
    -v /path/to/csv/files:/data \
    -v mimdb_data:/app/mimdb_data \
    mimdb:latest
```

---

## Utility Binaries

### Loader

A command-line utility for inspecting MIMDB files.

```bash
cargo run --bin loader -- examples/data/simple_example.mimdb
```

Displays file information, table metrics, and column details.

### Generate Examples

Generates example data files for testing and demonstration.

```bash
cargo run --bin generate_examples
```

Creates files in `examples/data/`:
- `simple_example.mimdb` - Basic dataset (5 rows)
- `employee_example.mimdb` - Employee data (8 rows)
- `sales_example.mimdb` - Sales transactions (20 rows)
- `student_grades_example.mimdb` - Student grades (12 rows)
- `large_dataset_example.mimdb` - Performance testing (10M rows)
- `edge_cases_example.mimdb` - Edge cases and special characters (9 rows)

Each `.mimdb` file has a corresponding `.txt` file with metadata and statistics.
