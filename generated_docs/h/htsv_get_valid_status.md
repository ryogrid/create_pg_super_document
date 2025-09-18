# htsv_get_valid_status

## Location
[src/backend/access/heap/pruneheap.c:960-998](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/pruneheap.c#L960-L998)

## Overview
htsv_get_valid_status is a safety guard function that validates and converts cached heap tuple visibility status values to ensure they have been properly computed before use.

## Definition
static inline HTSV_Result htsv_get_valid_status(int status)

## Detailed Description
This function serves as a defensive programming measure in the heap pruning subsystem. During pruning operations, tuple visibility is calculated once per tuple and cached in the PruneState.htsv array for performance reasons. This helper function provides a safe way to access those cached visibility results by:

1. **Validation**: Asserting that the status value falls within the valid range of HTSV_Result enum values
2. **Type Safety**: Converting the integer status back to the proper HTSV_Result enum type
3. **Debug Protection**: Helping detect bugs where visibility status is accessed before being computed

The function is designed to catch programming errors during development where visibility status might be accessed before it has been calculated by heap_prune_satisfies_vacuum.

## Parameters / Member Variables
- : Integer representation of the cached HTSV_Result status value

## Dependencies
- Functions called/Symbols referenced:
  - HEAPTUPLE_DEAD (enum constant for range checking)
  - HEAPTUPLE_DELETE_IN_PROGRESS (enum constant for range checking)  
  - HTSV_Result (return type enum)
  - Assert (debugging macro)
- Called from (representative examples):
  - [heap_prune_chain](heap_prune_chain.md)

## Notes and Other Information
- Static inline function for performance in debug builds
- Part of the PruneState visibility caching mechanism
- Assertion will only fire in debug builds (USE_ASSERT_CHECKING)
- Guards against accessing uninitialized visibility status values
- Essential for maintaining the integrity of the once-per-tuple visibility calculation optimization
- Return type conversion from int to HTSV_Result enum provides type safety