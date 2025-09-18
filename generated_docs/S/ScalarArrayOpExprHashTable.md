# ScalarArrayOpExprHashTable

## Location
[src/backend/executor/execExprInterp.c:211-217](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L211-L217)

## Overview
ScalarArrayOpExprHashTable is a comprehensive data structure that encapsulates all components needed for optimized hash-based scalar-array operations, serving as the complete execution context for EEOP_HASHED_SCALARARRAYOP expression evaluation.

## Definition


## Detailed Description
This structure represents the complete execution environment for optimized scalar-array operations in PostgreSQL's expression evaluator. It encapsulates both the hash table containing array elements and all the necessary function call infrastructure for hash computation and element comparison.

The hash table is built once when first evaluating an expression like 'scalar = ANY(const_array)' and then reused for subsequent evaluations of the same expression. This provides significant performance improvements over linear array scanning, especially for large arrays or frequently executed queries.

The structure serves as the private_data context passed to hash table operations, allowing hash and comparison functions to access the necessary PostgreSQL function call infrastructure.

## Parameters / Member Variables
- : Pointer to the underlying simplehash.h hash table (saophash_hash) that stores the array elements
- : Back-reference to the ExprEvalStep that owns this hash table, providing access to the original expression context
- : Function manager info for the hash function used to compute hash values for array elements
- : Pre-initialized function call data structure used when calling the hash function, includes argument slots and metadata

## Dependencies
- Functions called/Symbols referenced:
  - saophash_hash (the underlying hash table type from simplehash.h)
  - ExprEvalStep (parent expression step structure)
  - [FmgrInfo](../F/FmgrInfo.md) (PostgreSQL function manager info)
  - [FunctionCallInfoBaseData](../F/FunctionCallInfoBaseData.md) (function call argument and result data)
- Called from (representative examples):
  - [ExecEvalHashedScalarArrayOp](../E/ExecEvalHashedScalarArrayOp.md) (creates, populates, and uses the hash table)
  - [saop_element_hash](../s/saop_element_hash.md) (accesses hash_fcinfo_data and hash_finfo for computing hash values)
  - [saop_hash_element_match](../s/saop_hash_element_match.md) (accesses op field to get comparison function info)

## Notes and Other Information
- Memory for this structure is allocated in the per-query memory context to persist across multiple evaluations
- The hash table is populated with ScalarArrayOpExprHashEntry elements during first evaluation
- Only supports OR semantics (IN/NOT IN clauses), unlike the general scalar array operation which supports AND semantics
- Hash table construction excludes NULL values - these are handled separately in the evaluation logic
- The structure integrates PostgreSQL's type-specific hash and comparison functions with the generic hash table implementation
- Performance optimization that can provide substantial speedup for large constant arrays in WHERE clauses