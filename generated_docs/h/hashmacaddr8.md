# hashmacaddr8

## Location
src/backend/utils/adt/mac8.c: 395 - 402

## Overview
The hashmacaddr8 function is a hash support function for creating hash indexes on PostgreSQL's 8-byte MAC address (macaddr8) data type.

## Definition


## Detailed Description
This function computes a hash value for a macaddr8 (8-byte MAC address) value, enabling the use of hash indexes and hash joins for this data type. The function uses PostgreSQL's generic hash_any function to compute a hash over the entire 8-byte MAC address structure. This hash function is essential for the performance of hash-based operations like hash joins, hash aggregation, and hash indexing on macaddr8 columns.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: The macaddr8 value to hash (key)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MACADDR8_P (macro to extract macaddr8 argument)
  - [hash_any](hash_any.md) (generic hash function for arbitrary byte sequences)
  - sizeof (operator to get size of macaddr8 type)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is typically registered as the hash support function for macaddr8 data type in PostgreSQL's system catalogs
- The hash is computed over the entire 8-byte MAC address structure for consistent results
- Returns a 32-bit hash value as a Datum
- Essential for hash-based query execution plans involving macaddr8 columns
- Part of PostgreSQL's MAC address data type support introduced for 8-byte MAC addresses