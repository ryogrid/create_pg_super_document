# TidStoreCreateLocal

## Location
[src/backend/access/common/tidstore.c:165-212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tidstore.c#L165-L212)

## Overview
Creates a local TidStore instance that stores tuple identifiers (TIDs) in a radix tree structure, designed for efficient storage and retrieval of TID sets within a single process.

## Definition


## Detailed Description
TidStoreCreateLocal creates a TidStore for local (non-shared) use within a single backend process. The function allocates memory for the TidStore structure and creates an appropriate memory context for the underlying radix tree storage. The TidStore lives in the current memory context at the time of creation, while the TID storage itself lives in a child memory context called rt_context.

The function optimizes memory allocation by choosing an appropriate maxBlockSize based on the max_bytes hint, ensuring efficient memory usage and reducing space wastage from over-allocation. Depending on the insert_only parameter, it creates either a BumpContext (for insert-only workloads) or an AllocSetContext (for general use with insertions and deletions).

## Parameters / Member Variables
- : A hint for the maximum expected memory usage, used to cap the memory block size to reduce space wastage (not an enforced limit)
- : Boolean flag indicating whether the TidStore will only be used for insertions (true) or will also need deletions (false)

## Dependencies
- Functions called/Symbols referenced:
  - 
  - 
  - 
  - 
  - 
  - 
  - 
- Called from (representative examples):
  -  (src/backend/access/heap/vacuumlazy.c:2881)
  -  (src/backend/access/heap/vacuumlazy.c:2920)
  -  (src/test/modules/test_tidstore/test_tidstore.c:120)

## Notes and Other Information
- The max_bytes parameter is only a hint for memory optimization and is not internally enforced
- Callers must monitor actual memory usage through TidStoreMemoryUsage() if they need to enforce limits
- The choice between BumpContext and AllocSetContext depends on the intended usage pattern: BumpContext is more efficient for insert-only scenarios
- The maxBlockSize is automatically adjusted to be no larger than 1/16 of max_bytes to optimize memory allocation patterns