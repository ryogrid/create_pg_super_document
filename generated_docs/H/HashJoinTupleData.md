# HashJoinTupleData

## Location
src/include/executor/hashjoin.h: 78 - 88

## Overview
HashJoinTupleData is a fundamental data structure that represents an individual tuple (row) stored in PostgreSQL's hash join hash table, containing the tuple's hash value and a pointer to the next tuple in the same hash bucket.

## Definition

```c
typedef struct HashJoinTupleData
{
	/* link to next tuple in same bucket */
	union
	{
		struct HashJoinTupleData *unshared;
		dsa_pointer shared;
	}			next;
	uint32		hashvalue;		/* tuple's hash code */
	/* Tuple data, in MinimalTuple format, follows on a MAXALIGN boundary */
}			HashJoinTupleData;
```
## Detailed Description
HashJoinTupleData serves as the core building block for hash join operations in PostgreSQL's executor. Each tuple from the inner relation of a hash join is wrapped in this structure and stored in hash buckets within the hash table. The structure implements a chained hash table design where colliding tuples (those with the same hash bucket) are linked together via the  pointer.

The structure supports both shared and unshared memory configurations through a union in the  field -  for single-process hash joins and  (using dsa_pointer) for parallel hash joins where multiple processes need to access the same hash table data.

The actual tuple data is stored immediately after the HashJoinTupleData header in MinimalTuple format, aligned on a MAXALIGN boundary for optimal memory access performance.

## Parameters / Member Variables
- : Union containing the link to the next tuple in the same hash bucket
  - : Direct pointer to the next HashJoinTupleData structure for single-process joins
  - : DSA (Dynamic Shared Area) pointer for accessing the next tuple in parallel hash joins
- : The 32-bit hash code computed for this tuple, used for bucket assignment and comparison optimization

## Dependencies
- Functions called/Symbols referenced:
  - dsa_pointer (for parallel hash join support)
  - HashJoinTupleData (self-reference for linked list structure)
- Called from (representative examples):
  - HJTUPLE_OVERHEAD (macro that calculates overhead size)
  - HashJoinTableData (container structure that manages collections of these tuples)
  - HashJoinTuple (typedef alias for pointer to this structure)

## Notes and Other Information
- The tuple data following the header is stored in MinimalTuple format to minimize memory overhead
- Memory alignment on MAXALIGN boundary ensures efficient access on different architectures  
- The union design allows the same structure to work efficiently in both serial and parallel execution contexts
- Hash values are pre-computed and stored to avoid recalculation during bucket operations and join matching
- This structure is allocated in the HashTableContext memory context for proper resource management during query execution