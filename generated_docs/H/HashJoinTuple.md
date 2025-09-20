# HashJoinTuple

## Location
[src/include/nodes/execnodes.h:2186-2186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2186-L2186)

## Overview
HashJoinTuple is a typedef for a pointer to HashJoinTupleData structure, representing individual tuples stored in the hash table during hash join operations.

## Definition

```c
typedef struct HashJoinTupleData *HashJoinTuple;
```
## Detailed Description
HashJoinTuple represents individual tuples stored in PostgreSQL's hash join hash table. Each tuple is part of a linked list within a hash bucket, allowing for collision resolution through chaining. The structure contains the computed hash value for efficient comparison and a pointer to the next tuple in the same bucket. The actual tuple data follows the header in MinimalTuple format. The union for the next pointer supports both regular (unshared) hash joins and parallel hash joins using shared memory through dynamic shared area (DSA) pointers.

## Parameters / Member Variables
- : Union containing pointer to next tuple in the same hash bucket, supporting both regular pointers (unshared) and DSA pointers (shared) for parallel execution
- : Pre-computed 32-bit hash code for this tuple, used for efficient hash comparisons during probing
- Tuple data: The actual tuple data follows the structure header in MinimalTuple format on a MAXALIGN boundary (not explicitly declared in struct)

## Dependencies
- Functions called/Symbols referenced:
  - HashJoinTupleData (actual structure being pointed to)
  - dsa_pointer (for parallel hash join shared memory support)
  - MinimalTuple (for efficient tuple storage format)
- Called from (representative examples):
  - ExecHashTableInsert (inserts tuples into hash table)
  - [ExecScanHashBucket](../E/ExecScanHashBucket.md) (scans hash bucket for matching tuples)
  - ExecHashIncreaseNumBatches (redistributes tuples during batch expansion)
  - ExecParallelHashTableInsert (parallel hash table insertion)
  - ExecHashSkewTableInsert (inserts into skew buckets for optimization)

## Notes and Other Information
- This structure is the fundamental building block of hash join hash tables, designed for memory efficiency
- The union supports both single-process and parallel hash joins - unshared for regular execution, shared for parallel workers
- Hash values are pre-computed and stored to avoid recalculation during hash table probing
- Tuple data uses MinimalTuple format which omits system columns for space efficiency
- The linked list design handles hash collisions efficiently through separate chaining
- Memory alignment (MAXALIGN) ensures optimal performance for tuple data access
- The structure is defined in src/include/executor/hashjoin.h and referenced in src/include/nodes/execnodes.h at line 2186
- Used extensively in hash join operations for building and probing the hash table during the build and probe phases