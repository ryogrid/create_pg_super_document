# AggStatePerHashData

## Location
[src/include/executor/nodeAgg.h:309-322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/nodeAgg.h#L309-L322)

## Overview
AggStatePerHashData represents per-hashtable state for hash-based aggregation, supporting both simple hashed aggregation and grouping sets with hashing by maintaining one instance per grouping set.

## Definition

```c
typedef struct AggStatePerHashData
{
	TupleHashTable hashtable;	/* hash table with one entry per group */
	TupleHashIterator hashiter; /* for iterating through hash table */
	TupleTableSlot *hashslot;	/* slot for loading hash table */
	FmgrInfo   *hashfunctions;	/* per-grouping-field hash fns */
	Oid		   *eqfuncoids;		/* per-grouping-field equality fns */
	int			numCols;		/* number of hash key columns */
	int			numhashGrpCols; /* number of columns in hash table */
	int			largestGrpColIdx;	/* largest col required for hashing */
	AttrNumber *hashGrpColIdxInput; /* hash col indices in input slot */
	AttrNumber *hashGrpColIdxHash;	/* indices in hash table tuples */
	Agg		   *aggnode;		/* original Agg node, for numGroups etc. */
}			AggStatePerHashData;
```
## Detailed Description
AggStatePerHashData manages the state for hash-based aggregation processing, providing efficient grouping through hash table operations. When processing grouping sets with hashing, the system maintains one instance of this structure per grouping set. For simple hashed aggregation without grouping sets, only a single instance is used.

The structure contains the core hash table infrastructure including the table itself, iteration support, and slot management for tuple operations. It maintains column mapping information to efficiently translate between input tuple positions and hash table positions, enabling flexible column arrangements during hash key construction.

The hash and equality functions are stored per grouping field, allowing for type-specific optimized operations during hash table lookups and comparisons.

## Parameters / Member Variables
- `hashtable`: TupleHashTable storing one entry per group with aggregate state values
- `hashiter`: TupleHashIterator for efficient iteration through all hash table entries
- `*hashslot`: TupleTableSlot used for loading and manipulating hash table entries
- `*hashfunctions`: Array of FmgrInfo structures containing hash functions for each grouping field
- `*eqfuncoids`: Array of OIDs for equality functions corresponding to each grouping field
- `numCols`: Total number of columns used as hash keys
- `numhashGrpCols`: Number of columns actually stored in the hash table structure
- `largestGrpColIdx`: Index of the largest column required for hashing operations (optimization hint)
- `*hashGrpColIdxInput`: Array mapping hash key column indices to positions in input tuple slots
- `*hashGrpColIdxHash`: Array mapping hash key column indices to positions in hash table tuples
- `*aggnode`: Pointer to the original Agg node providing metadata like estimated number of groups
## Dependencies
- Functions called/Symbols referenced:
  - [TupleHashTable](../T/TupleHashTable.md)
  - TupleHashIterator
  - [Agg](Agg.md)
- Called from (representative examples):
  - [ExecInitAgg](../E/ExecInitAgg.md)
  - [AggStatePerHash](AggStatePerHash.md)

## Notes and Other Information
This structure is central to PostgreSQL's hash-based aggregation strategy, which provides excellent performance for queries with moderate to large numbers of groups when sufficient memory is available. The column index mappings (hashGrpColIdxInput and hashGrpColIdxHash) enable flexible tuple layouts and optimize memory access patterns during hash operations. The separation of hash and equality functions allows for type-specific optimizations while maintaining generality across different data types used in grouping operations.