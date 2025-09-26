# HashSkewBucket

## Location
[src/include/executor/hashjoin.h:113-117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/hashjoin.h#L113-L117)

## Overview
HashSkewBucket is a specialized hash table structure designed to optimize PostgreSQL hash joins by handling highly frequent hash values (Most Common Values) separately from the main hash table to improve performance and reduce disk I/O.

## Definition

```c
typedef struct HashSkewBucket
{
	uint32		hashvalue;		/* common hash value */
	HashJoinTuple tuples;		/* linked list of inner-relation tuples */
} HashSkewBucket;
```
## Detailed Description
HashSkewBucket implements a skew optimization strategy for hash joins when the outer relation has a significantly non-uniform distribution. Rather than allowing highly frequent hash values to overwhelm regular hash buckets, these common values are identified (typically from the outer relation's Most Common Values statistics) and their corresponding inner relation tuples are stored in separate skew buckets.

This optimization is particularly effective because it treats tuples with skewed hash values as part of the first batch, ensuring they never get spilled to disk. The skew hashtable is memory-limited to SKEW_HASH_MEM_PERCENT of the total join memory allocation. When this limit is approached, the system reduces the number of MCVs being specially treated.

The design focuses on optimizing for the outer relation's distribution because: (1) the outer relation is typically larger, providing greater I/O savings, and (2) the planner tends to place more uniformly distributed relations on the inner side, making outer relation skew more likely and impactful.

## Parameters / Member Variables
- : The 32-bit hash value that is common among all tuples stored in this skew bucket, corresponding to a Most Common Value from the outer relation
- : Head pointer to a linked list of HashJoinTuple structures containing all inner-relation tuples that hash to this particular value

## Dependencies
- Functions called/Symbols referenced:
  - [HashJoinTuple](HashJoinTuple.md) (for storing the linked list of matching tuples)
- Called from (representative examples):
  - [ExecChooseHashTableSize](../E/ExecChooseHashTableSize.md) (for memory size calculations)
  - [ExecHashBuildSkewHash](../E/ExecHashBuildSkewHash.md) (for constructing and populating skew buckets)
  - [ExecHashRemoveNextSkewBucket](../E/ExecHashRemoveNextSkewBucket.md) (for removing processed buckets)
  - [ExecHashTableResetMatchFlags](../E/ExecHashTableResetMatchFlags.md) (for resetting match status)
  - SKEW_BUCKET_OVERHEAD (macro for calculating memory overhead)
  - HashJoinTableData (container structure managing collections of skew buckets)

## Notes and Other Information
- Skew buckets are allocated from the same memory context as the main hash table but are accounted separately for memory management
- The optimization is most beneficial for joins where a small number of values account for a large percentage of the outer relation's tuples
- Tuples in skew buckets are processed during the first batch execution, avoiding the overhead of temporary file I/O
- The skew hash table size is dynamically adjusted based on available memory and the number of MCVs requiring special treatment
- This structure is particularly important for star-schema and similar analytical workloads where dimension tables often have highly skewed key distributions