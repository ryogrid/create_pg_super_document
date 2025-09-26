# ExecHashBuildSkewHash

## Location
[src/backend/executor/nodeHash.c:2382-2534](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L2382-L2534)

## Overview
Sets up skew optimization for hash joins by creating specialized hash buckets for the most common values (MCVs) of the outer relation's join key to improve hash table performance.

## Definition

```c
static void
ExecHashBuildSkewHash(HashJoinTable hashtable, Hash *node, int mcvsToUse)
```
## Detailed Description
ExecHashBuildSkewHash implements a performance optimization technique for hash joins called "skew optimization." When the outer relation's join key has highly skewed data distribution (some values appear much more frequently than others), normal hash table performance can degrade due to bucket collisions and uneven distribution.

This function creates a separate skew hash table with dedicated buckets for the most common values (MCVs) identified by the query planner. The skew hash table uses open addressing with power-of-2 sizing and is allocated in the hashtable's batch context for automatic cleanup.

The function retrieves statistics from the system catalog (pg_statistic) to identify MCVs and their frequencies. It only proceeds if the total frequency of MCVs exceeds SKEW_MIN_OUTER_FRACTION to ensure the optimization is worthwhile. Skew buckets are created in order of decreasing MCV frequency, which is important for proper bucket removal during memory pressure.

## Parameters / Member Variables
- : The HashJoinTable structure being optimized with skew buckets
- : Hash node containing skew optimization metadata (skewTable, skewColumn, skewInherit)
- : Maximum number of MCV values to create skew buckets for, based on available memory

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache3 (retrieves statistics from pg_statistic)
  - get_attstatsslot (extracts MCV values and frequencies)
  - pg_nextpower2_32 (calculates optimal hash table size)
  - MemoryContextAllocZero (allocates skew bucket arrays)
  - FunctionCall1Coll (computes hash values for MCVs)
  - free_attstatsslot (releases statistics slot)
- Called from:
  - ExecHashTableCreate (during hash table initialization)

## Notes and Other Information
- Only activates when the planner has identified a valid skewTable OID
- Requires sufficient memory to allocate at least one skew bucket
- Uses open addressing hash table with power-of-2 sizing plus extra bits to reduce collisions
- Memory allocation occurs in batch context for automatic cleanup after first batch
- Critical that skew buckets are created in decreasing MCV frequency order for proper removal during memory pressure
- Skew optimization is abandoned if MCV frequency sum is below SKEW_MIN_OUTER_FRACTION threshold
- Handles hash collisions between different MCVs by allowing bucket sharing