# Public Interface Tests (PIT)

Test suite for validating the database public interface.

## Running Tests

By default, tests connect to an existing database running on `localhost:8080` (Docker is not started).

### Basic Usage

```bash
# Run all tests
go test ./tests -v

# Run specific test category
go test ./tests -run "TestFunctional" -v
```

### Configuration Options

Tests can be configured via command-line flags or environment variables. Flags take precedence over environment variables.

#### Flags and Environment Variables

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `-db-image` | `DB_IMAGE` | `isbd-mimuw-db:latest` | Docker image name |
| `-db-hostname` | `DB_HOSTNAME` | `localhost` | Database hostname |
| `-db-port` | `DB_PORT` | `8080` | Database port |
| `-db-run-docker` | `DB_RUN_DOCKER` | `false` | Start Docker container |
| `-async` | `TEST_ASYNC` | `false` | Run tests in async mode |
| `-db-memory` | `DB_MEMORY` | `0` | DB memory in bytes for stress tests |

### Usage Examples

```bash
# Connect to custom hostname and port
go test ./tests -db-hostname 192.168.1.100 -db-port 5432 -v

# Start Docker with a custom image
go test ./tests -db-run-docker true -db-image my-custom-db:v1 -v

# Use environment variables
DB_HOSTNAME=mydb.local DB_PORT=8080 go test ./tests -v

# Run specific tests
go test ./tests -run "SystemInfo" -v
go test ./tests -run "TableCreation/TableEmpty" -v
```

## Async Mode

By default, tests run synchronously (submit query → wait → assert). In async mode, all queries are submitted first, then results are verified.

```bash
# Enable async mode via flag
go test ./tests -async -v

# Enable async mode via environment variable
TEST_ASYNC=true go test ./tests -v
```

Use it to make tests faster.
When debugging a problem, it's better to run sequentially.
It excludes concerrency problems.

## Stress Tests

Stress tests verify database behavior with large data volumes (sorting, CSE). They are **skipped by default** unless `-db-memory` is set.
Set memory accessigle by your DB to validate it's capabilities.

### Running Stress Tests

```bash
# Run with 10MB memory limit
go test ./tests -run "TestStress" -db-memory 10485760 -v

# Run with 100MB memory limit  
go test ./tests -run "TestStress" -db-memory 104857600 -v

# Using environment variable (bytes)
DB_MEMORY=10485760 go test ./tests -run "TestStress" -v
```

### Available Stress Tests

| Test | Description |
|------|-------------|
| `TestStress_SortLargeData` | Sort data 2x larger than DB memory (ASC) |
| `TestStress_SortLargeData_Descending` | Sort data 2x larger than DB memory (DESC) |
| `TestStress_SortMultipleColumns` | Sort by two columns, total 2x memory |
| `TestStress_Incremental` | Progressive tests: 0.5x, 1x, 1.5x, 2x memory |
| `TestStress_CSE` | CSE test with 11 identical expressions |
| `TestStress_CSE_SelectOnly` | CSE test with 10 expressions in SELECT |

### CSE (Common Subexpression Elimination) Tests

CSE tests verify whether the database can recognize and optimize repeated expressions.

```bash
# Run CSE tests with 10MB memory
go test ./tests -run "TestStress_CSE" -db-memory 10485760 -v

# Run all CSE tests
go test ./tests -run "CSE" -db-memory 10485760 -v
```

**How CSE tests work:**
- Single expression targets ~10% of available memory
- Expression is repeated 10-11 times in the query
- Without CSE: needs 100-110% memory → should fail/be slow
- With CSE: needs only 10% memory → should pass quickly

Example query structure:
```sql
SELECT id, STRLEN(concat_expr) + STRLEN(concat_expr) + ... (10x)
FROM stress_rows 
WHERE STRLEN(concat_expr) > 0
```

## Test Categories

| Category | Pattern | Description |
|----------|---------|-------------|
| System Info | `TestSystemInfo` | Basic connectivity |
| Table Creation | `TestTableCreation` | Schema operations |
| Query Validation | `TestQueryValidation` | SQL validation rules |
| Functional | `TestFunctional` | Operator/function correctness |
| Stress | `TestStress` | Large data handling |

```bash
# Run only validation tests
go test ./tests -run "TestQueryValidation" -v

# Run only functional tests  
go test ./tests -run "TestFunctional" -v

# Run stress tests (requires -db-memory)
go test ./tests -run "TestStress" -db-memory 10485760 -v

# Combine async mode with stress tests
go test ./tests -run "TestStress" -db-memory 10485760 -async -v
```
