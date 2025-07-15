# Test Script Directory

This directory contains organized test files for the Rust Solo All-DB project.

## Directory Structure

```
test_script/
├── csv/                    # CSV test files
│   ├── test_data.csv      # Basic CSV with headers (comma-separated)
│   ├── test_semicolon.csv # Semicolon-separated CSV
│   ├── tab_delimited.tsv  # Tab-separated values
│   ├── pipe_with_errors.csv # Pipe-separated with malformed rows
│   ├── no_headers.csv     # CSV without headers
│   └── products.csv       # Sample products database
└── cli/                   # CLI test scripts
    ├── basic_load_test.txt      # Basic CSV loading
    ├── semicolon_test.txt       # Semicolon delimiter test
    ├── tab_test.txt            # Tab delimiter test
    ├── recovery_test.txt       # Error recovery mode test
    ├── no_headers_test.txt     # No headers test
    ├── products_test.txt       # Products database test
    └── comprehensive_test.txt   # Full ETL feature test
```

## Test Files Description

### CSV Files

1. **test_data.csv** - Basic comma-separated file with name,age headers
2. **test_semicolon.csv** - Semicolon-separated file with name;age;city;active
3. **tab_delimited.tsv** - Tab-separated file with employee data
4. **pipe_with_errors.csv** - Pipe-separated file with intentional parsing errors
5. **no_headers.csv** - CSV file without header row
6. **products.csv** - Sample product database with multiple columns

### CLI Scripts

1. **basic_load_test.txt** - Tests basic CSV loading functionality
2. **semicolon_test.txt** - Tests custom delimiter support
3. **tab_test.txt** - Tests tab delimiter support
4. **recovery_test.txt** - Tests error recovery mode
5. **no_headers_test.txt** - Tests no-headers mode
6. **products_test.txt** - Tests with realistic product data
7. **comprehensive_test.txt** - Full test suite covering all features

## Running Tests

### Individual Test Scripts
```bash
# Basic CSV loading test
cargo run --bin rustdb < test_script/cli/basic_load_test.txt

# Semicolon delimiter test
cargo run --bin rustdb < test_script/cli/semicolon_test.txt

# Recovery mode test
cargo run --bin rustdb < test_script/cli/recovery_test.txt
```

### Comprehensive Test Suite
```bash
# Run all ETL features
cargo run --bin rustdb < test_script/cli/comprehensive_test.txt
```

### Unit Tests
```bash
# Run all ETL unit tests
cargo test etl --lib

# Run specific test
cargo test test_csv_loading_with_recovery_mode
```

## Test Features Covered

- ✅ **Custom Delimiters**: Comma, semicolon, tab, pipe
- ✅ **Error Recovery**: Skip malformed rows, continue processing
- ✅ **Header Handling**: With/without headers
- ✅ **Column Selection**: Specify key/value columns
- ✅ **Progress Reporting**: Batch processing with status updates
- ✅ **Database Operations**: Insert, get, delete, compact, stats
- ✅ **Memory Management**: Batch processing for large files
- ✅ **Parallel Processing**: Multi-threaded CSV loading

## Expected Outputs

### Success Cases
- Records loaded successfully with count
- Database statistics showing inserted records
- Query results for inserted keys

### Error Cases (Recovery Mode)
- Malformed rows reported with line numbers
- Success rate percentage displayed
- Partial loading with error summary

## Adding New Tests

1. **CSV Files**: Add to `test_data/csv/` directory
2. **CLI Scripts**: Add to `test_data/cli_scripts/` directory
3. **Follow naming convention**: `feature_test.txt` for CLI scripts
4. **Update this README**: Document new test files and their purpose
