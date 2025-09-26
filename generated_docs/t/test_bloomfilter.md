# test_bloomfilter

## Location
src/test/modules/test_bloomfilter/test_bloomfilter.c: 113 - 138

## Overview
SQL-callable entry point function that performs comprehensive Bloom filter testing with configurable parameters and validation.

## Definition
```c
Datum test_bloomfilter(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the main entry point for PostgreSQL's Bloom filter testing module, designed to be called from SQL. It provides a comprehensive testing framework that can run multiple test iterations with specified parameters to validate Bloom filter performance and correctness.

The function accepts four parameters through PostgreSQL's function argument interface: power (for memory sizing), nelements (number of elements to test), seed (for reproducible random behavior), and tests (number of test iterations to run). It includes robust parameter validation to ensure all inputs are within acceptable ranges - power must be between 23 and 32 inclusive, tests must be positive, and nelements must be non-negative.

For each test iteration, it calls `create_and_test_bloom` to perform the actual Bloom filter creation, population, and false positive rate measurement. This design allows for statistical validation across multiple test runs, which is important for probabilistic data structures like Bloom filters.

## Parameters / Member Variables
- `power`: Memory size parameter (must be 23-32 inclusive); determines filter memory as 2^power bits  
- `nelements`: Number of elements to add to the filter and test against (must be non-negative)
- `seed`: Random seed for reproducible test results
- `tests`: Number of test iterations to run (must be positive)

## Dependencies
- Functions called/Symbols referenced:
  - create_and_test_bloom (performs the actual Bloom filter testing)
  - PG_GETARG_INT32 (extracts integer arguments from PostgreSQL function call)
  - PG_GETARG_INT64 (extracts 64-bit integer arguments)
  - PG_RETURN_VOID (returns void result to PostgreSQL)
  - elog (error and debug logging)
- Called from (representative examples):
  - SQL queries calling the test_bloomfilter() function
  - create_and_test_bloom (appears to be a circular reference in the output, but this is likely the PG_FUNCTION_INFO_V1 macro registration)

## Notes and Other Information
- This is a PostgreSQL extension function that can be called from SQL as `SELECT test_bloomfilter(power, nelements, seed, tests)`
- Parameter validation ensures power is within the practical range of 23-32 (8MB to 512MB approximately)
- The function runs multiple test iterations, allowing statistical validation of Bloom filter behavior
- Uses PostgreSQL's standard function interface macros (PG_FUNCTION_ARGS, PG_GETARG_*, PG_RETURN_VOID)
- Debug logging tracks progress through multiple test iterations
- Designed to emit WARNINGs when false positive rates exceed the 1% threshold
- The function enables automated testing of Bloom filter performance across different configurations
- Returns void, with results communicated through PostgreSQL's logging system