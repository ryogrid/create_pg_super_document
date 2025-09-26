# test_radixtree

## Location
src/test/modules/test_radixtree/test_radixtree.c: 446 - 473

## Overview
A PostgreSQL function that serves as the main test driver for the adaptive radix tree implementation, executing comprehensive tests across different node classes, tree configurations, and usage patterns.

## Definition
```c
Datum test_radixtree(PG_FUNCTION_ARGS)
```

## Detailed Description
The `test_radixtree` function is a comprehensive test suite for PostgreSQL's adaptive radix tree implementation. It orchestrates multiple testing scenarios to validate the correctness and performance of the radix tree data structure across various configurations:

1. **Empty Tree Testing**: Tests operations on an empty radix tree to ensure proper behavior with no data
2. **Node Class Testing**: Systematically tests each radix tree node class (node-4, node-16-lo, node-16-hi, node-48, node-256) with different configurations
3. **Multi-level Tree Testing**: Tests trees with different depths (single level, two levels, maximum levels)
4. **Key Order Testing**: Tests both ascending and descending key insertion patterns
5. **Random Testing**: Performs stress testing with random key insertion, lookup, and deletion patterns

The function uses a predefined array `rt_node_class_tests` that defines test parameters for each node class, including the class name and the number of keys needed to exercise that particular node type. For each node class, it runs tests with different shift values (0, 8, and maximum shift) to create trees of varying depths, and tests both ascending and descending key insertion orders.

## Parameters / Member Variables
This function takes standard PostgreSQL function arguments but does not use any specific parameters:
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - `test_empty`: Tests radix tree operations on an empty tree
  - `test_basic`: Tests basic radix tree operations (insert, lookup, delete, iterate) for specific node classes
  - `test_random`: Performs randomized stress testing of radix tree operations
  - `lengthof`: Macro to get the length of the `rt_node_class_tests` array
  - `BITS_PER_BYTE`: Constant for calculating maximum shift value
  - `PG_RETURN_VOID`: PostgreSQL macro to return void from a function
- Called from (representative examples):
  - `rt_num_entries`: Used as a PostgreSQL function accessible via SQL

## Notes and Other Information
- This is a test module specifically designed for validating the adaptive radix tree implementation in PostgreSQL
- The test suite covers both local memory and shared memory configurations (controlled by `TEST_SHARED_RT` compilation flag)
- The function tests trees with different levels by using bit-shifting on keys to create sparse key distributions
- The maximum shift calculation `(sizeof(uint64) - 1) * BITS_PER_BYTE` determines the deepest possible tree structure
- Each node class test runs multiple scenarios: single-level trees, two-level trees, and maximum-depth trees
- The test validates both ascending and descending key insertion patterns to ensure proper handling of different access patterns
- Located in `src/test/modules/test_radixtree/test_radixtree.c:446-473`
- The function is registered as a PostgreSQL SQL-callable function via `PG_FUNCTION_INFO_V1(test_radixtree)`