# Generating Test Data Files

The MIMDB project includes example data files for testing and demonstration purposes. These files can be generated using the included binary utility.

## Project Structure

The project is organized with a clean separation between library code, binaries, and examples:

```
mimdb/
├── src/          # Library source code
│   └── lib.rs    # Main library implementation
├── bin/          # Binary executables
│   └── generate_examples.rs  # Test data generator
├── tests/        # Integration tests
└── Cargo.toml    # Package configuration

examples/
└── data/         # Generated .mimdb and .txt example files
```

## Generating Example Files

To generate all example data files, run:
```bash
cargo run --bin generate_examples
```

This will create files in the `examples/data/` directory:
- `.mimdb` files containing example datasets in the MIMDB binary format
- `.txt` files describing the datasets with metadata and statistics

## Generated Files

The following example datasets will be created:

1. **simple_example.mimdb** - Basic dataset with IDs and names (5 rows)
2. **employee_example.mimdb** - Employee dataset with salaries and departments (8 rows)
3. **sales_example.mimdb** - Sales transaction data (20 rows)
4. **student_grades_example.mimdb** - Student grade records (12 rows)
5. **large_dataset_example.mimdb** - Large dataset for performance testing (10,000,000 rows)
6. **edge_cases_example.mimdb** - Edge cases with extreme values and special characters (9 rows)

Each dataset also has a corresponding `.txt` file in the same directory that describes the dataset characteristics, row counts, column information, and file sizes.

## Binary Structure

The `generate_examples` binary is located in the `bin/` directory and is configured as a standalone executable in `Cargo.toml`. This separation keeps the binary code separate from the library code and integration tests.

### Key Features:
- **Clean separation**: Binary utilities in `bin/`, library code in `src/`, tests in `tests/`
- **Self-contained**: The binary includes all necessary logic for generating test data
- **Reusable**: Can be run independently or as part of CI/CD pipelines

## Usage in Examples and Tests

These generated files are used by the example programs and integration tests to demonstrate MIMDB functionality and verify file format compatibility across different scenarios. The files can be regenerated at any time by running the binary utility.

## Development

To modify the test data generation:
1. Edit `bin/generate_examples.rs`
2. Run `cargo run --bin generate_examples` to regenerate files
3. Run `cargo test` to verify all tests still pass