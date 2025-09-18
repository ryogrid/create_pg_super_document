# sanity_check_array

## Location
src/test/modules/test_tidstore/test_tidstore.c: 136 - 149

## Overview
A static validation function that performs sanity checks on PostgreSQL ArrayType structures to ensure they meet specific requirements for tidstore testing operations.

## Definition


## Detailed Description
This function validates that an ArrayType structure meets the requirements for use in tidstore test operations. It performs two critical validation checks:

1. **Null value validation**: Ensures the array does not contain any NULL values, as NULL TIDs would be invalid for tidstore operations
2. **Dimensionality validation**: Ensures the array is either empty or one-dimensional, as multi-dimensional arrays are not supported in the tidstore test framework

The function uses PostgreSQL's error reporting mechanism to throw appropriate errors with specific error codes when validation fails, ensuring that invalid arrays are caught early in the testing process.

## Parameters / Member Variables
- : Pointer to the ArrayType structure to validate

## Dependencies
- Functions called/Symbols referenced:
  - ARR_HASNULL (macro to check if array has null bitmap)
  - array_contains_nulls (function to check for actual null values)
  - ARR_NDIM (macro to get array dimensions)
  - ereport (for error reporting)
  - errcode (for error code specification)
  - errmsg (for error message formatting)
- Called from (representative examples):
  - do_set_block_offsets

## Notes and Other Information
- This is a static helper function used internally within the test_tidstore module
- The function throws PostgreSQL errors rather than returning error codes, following PostgreSQL's exception handling patterns
- The dual check for nulls (ARR_HASNULL && array_contains_nulls) ensures both metadata and actual content are validated
- Error codes used follow PostgreSQL standards: ERRCODE_NULL_VALUE_NOT_ALLOWED and ERRCODE_DATA_EXCEPTION
- Essential for maintaining data integrity in tidstore test operations where invalid TIDs could cause system corruption