# hash_numeric

## Location
src/backend/utils/adt/numeric.c: 2713 - 2792

## Overview
Computes a hash value for PostgreSQL numeric data types, ensuring that numerically equal values produce the same hash regardless of their internal representation.

## Definition


## Detailed Description
This function generates a 32-bit hash value for numeric types used in hash tables, hash joins, and other hash-based operations. It implements careful normalization to ensure that numerically equivalent values (such as 1.0 and 1.00) produce identical hash values, which is essential for correctness in hash-based algorithms.

The function strips leading and trailing zeros from the digit representation and excludes the scale from the hash calculation, since values with different scales but equal numeric value must hash to the same result. The weight (effective position of decimal point) is incorporated via XOR to distinguish between values that differ only in magnitude.

## Parameters / Member Variables
-  (PG_GETARG_NUMERIC(0)): The numeric value to be hashed

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC (parameter extraction)
  - NUMERIC_IS_SPECIAL (special value detection)
  - NUMERIC_WEIGHT (decimal point position)
  - NUMERIC_DIGITS (digit array access)
  - NUMERIC_NDIGITS (digit count)
  - [hash_any](hash_any.md) (generic binary hash function)
  - PG_RETURN_UINT32/PG_RETURN_DATUM (return value macros)
- Called from (representative examples):
  - [JsonbHashScalarValue](../J/JsonbHashScalarValue.md) (JSONB numeric hashing)

## Notes and Other Information
- Returns hash value 0 for special values (NaN, infinity) to ensure consistent behavior
- Returns hash value -1 for zero values regardless of their representation
- Excludes scale from hash calculation to maintain equality semantics
- Leading and trailing zeros are stripped to normalize representation
- The weight is XORed with the digit hash to incorporate decimal point position
- Critical for performance and correctness of hash-based operations on numeric data
- Used internally by PostgreSQL's hash join and hash aggregation algorithms