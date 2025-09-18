# ScalarArrayOpExprHashEntry

## Location
src/backend/executor/execExprInterp.c: 189 - 194

## Overview
ScalarArrayOpExprHashEntry is a hash table entry structure used to optimize scalar-array operations (like 'x = ANY(array)') by caching frequently accessed array elements during EEOP_HASHED_SCALARARRAYOP expression evaluation.

## Definition


## Detailed Description
This structure represents individual entries in a hash table designed to accelerate scalar-array operations, particularly when the same array is accessed repeatedly with different scalar values. Instead of performing linear searches through arrays on each evaluation, PostgreSQL builds a hash table using ScalarArrayOpExprHashEntry elements to store array values with their precomputed hash values.

The structure is used with PostgreSQL's simplehash.h hash table implementation, where each array element becomes a hash entry for O(1) lookup performance. This optimization is especially beneficial for large arrays or frequently executed queries with IN/NOT IN clauses containing constant arrays.

## Parameters / Member Variables
- : The Datum value representing the array element being stored in the hash table
- : Internal hash table status field used by simplehash.h implementation for managing entry states
- hash: hash table empty: Precomputed hash value for the key, cached to avoid recalculating during hash operations

## Dependencies
- Functions called/Symbols referenced:
  - Datum (PostgreSQL's universal data type)
  - Uses simplehash.h infrastructure (SH_ELEMENT_TYPE macro)
- Called from (representative examples):
  - [ExecEvalHashedScalarArrayOp](../E/ExecEvalHashedScalarArrayOp.md) (creates and populates hash table with these entries)
  - saophash_insert (inserts entries during hash table construction)
  - saophash_lookup (searches for entries during scalar comparison)

## Notes and Other Information
- This structure is specifically designed for use with PostgreSQL's generic hash table implementation (lib/simplehash.h)
- Only used in the EEOP_HASHED_SCALARARRAYOP execution path for optimized scalar-array operations
- Hash table is built once per expression evaluation and reused for subsequent lookups within the same query execution
- The hash table excludes NULL values from the array - NULL handling is done separately in the evaluation logic
- Memory for hash entries is allocated in the per-query memory context to persist across multiple evaluations of the same expression