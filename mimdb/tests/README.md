# MIMDB Test Suite

This directory contains comprehensive tests for the MIMDB columnar database library.

## Test Structure

### Test Files

- **`api_tests.rs`** - End-to-end REST API tests (Public Interface Testing)
  - System info endpoint (`test_system_info`)
  - Table operations: list, create, get, delete (`test_list_tables_empty`, `test_create_table`, `test_get_table_by_id`, `test_delete_table`)
  - Table validation: duplicate names, duplicate columns, empty names, no columns (`test_create_table_duplicate_name`, `test_create_table_duplicate_columns`, `test_empty_table_name`, `test_table_with_no_columns`)
  - Query operations: list, SELECT, COPY (`test_list_queries_empty`, `test_select_query_on_empty_table`, `test_copy_and_select_full_workflow`)
  - COPY edge cases: with header, nonexistent file, multiple operations (`test_copy_with_header`, `test_copy_nonexistent_file`, `test_multiple_copy_operations`)
  - Result handling: row limits (`test_result_with_row_limit`)
  - Persistence across restarts (`test_persistence_across_restarts`)
  - Error handling: invalid JSON, nonexistent tables (`test_invalid_json_request`, `test_select_nonexistent_table`)
  - Query status tracking (`test_query_status_completed`, `test_queries_list_after_operations`)

- **`serialization_tests.rs`** - Tests for serialization and deserialization functionality
  - Basic serialization/deserialization (`test_basic_serialization`)
  - Empty table handling (`test_empty_table_serialization`)
  - Single row tables (`test_single_row_serialization`)
  - Large dataset testing (`test_large_dataset_serialization`)
  - Special character handling (`test_special_characters_serialization`)
  - Extreme value testing (`test_extreme_values_serialization`)
  - Multiple cycle testing (`test_multiple_cycles`)
  - Mixed column types (`test_mixed_column_types`)
  - Invalid file format handling (`test_invalid_file_format`)
  - Corrupted file handling (`test_corrupted_file_handling`)

- **`integrity_tests.rs`** - Tests for data integrity verification
  - Checksum-based integrity verification (`test_data_integrity_with_checksums`)
  - Compression data integrity (`test_compression_data_integrity`)
  - Boundary condition testing (`test_boundary_conditions_integrity`)
  - Incremental integrity testing (`test_incremental_integrity`)
  - Concurrent access patterns (`test_concurrent_access_patterns`)
  - Memory vs disk consistency (`test_memory_disk_consistency`)

- **`integration_tests.rs`** - Comprehensive integration tests
  - End-to-end functionality testing (`test_comprehensive_integration`)
  - Edge cases integration (`test_edge_cases_integration`)
  - Performance and scalability testing (`test_performance_integration`)
  - Cross-compatibility testing (`test_cross_compatibility`)
  - Stress testing with unusual patterns (`test_stress_patterns`)

### Test Data

Test data is automatically generated during test execution using temporary directories. The tests use the `tempfile` crate to create isolated test environments that are cleaned up automatically.

## Running Tests

### Run All Tests
```bash
cargo test
```

### Run Specific Test Categories
```bash
# API tests
cargo test api

# Serialization tests
cargo test serialization

# Integrity tests
cargo test integrity

# Integration tests
cargo test integration
```

### Run Tests with Output
```bash
cargo test -- --nocapture
```

### Run Tests in Parallel
```bash
cargo test -- --test-threads=4
```

## Test Coverage

The test suite covers:

1. **REST API (Public Interface Testing)**
   - Table CRUD operations
   - Query submission and execution
   - COPY and SELECT query workflows
   - Result retrieval with row limits
   - Error responses and validation
   - Persistence across server restarts
   - System information endpoints

2. **Serialization/Deserialization**
   - Basic round-trip operations
   - Edge cases (empty tables, single rows)
   - Large datasets (10,000+ rows)
   - Special characters and Unicode
   - Extreme integer values
   - Multiple data types

3. **Data Integrity**
   - Checksum verification
   - Statistical property preservation
   - Compression/decompression integrity
   - Boundary value testing
   - Cross-platform consistency

4. **File Operations**
   - Format consistency
   - Corruption detection
   - Cross-loading verification

5. **Performance**
   - Scalability testing
   - Compression effectiveness
   - Load/save performance
   - Memory usage patterns

6. **Edge Cases**
   - Empty data structures
   - Maximum/minimum values
   - Special character handling
   - Invalid file formats
   - Corrupted data recovery

## Test Data

The test suite includes various data patterns:

- **Sequential data** - Good for delta compression
- **Random data** - Challenging for compression
- **Repetitive data** - Excellent compression ratio
- **Mixed patterns** - Real-world scenarios
- **Edge cases** - Boundary conditions
- **Large datasets** - Performance testing

## Expected Results
All tests should pass in a properly functioning MIMDB implementation. The tests verify:

- Data integrity is maintained across all operations
- File format is consistent and recoverable
- Performance scales appropriately with data size
- Edge cases are handled gracefully
- Compression is effective and reversible

## Adding New Tests

To add new tests:

1. Choose the appropriate test file based on test category
2. Follow existing test patterns and naming conventions
3. Include both positive and negative test cases
4. Add comprehensive assertions
5. Use temporary directories for file operations
6. Clean up resources properly

## Dependencies

Test dependencies are managed in `mimdb/Cargo.toml`:

- `tempfile` - For temporary directory creation
- Standard Rust testing framework

## Performance Notes

- Integration tests may take longer due to large dataset generation
- Serialization tests create temporary files that are automatically cleaned
- Memory usage tests may require adequate system resources
- All tests use temporary directories to avoid filesystem pollution