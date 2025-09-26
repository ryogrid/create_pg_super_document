# Hash

## Location
[src/include/nodes/plannodes.h:1197-1211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L1197-L1211)

## Overview
The Hash node is the build-side component of hash joins that creates and populates an in-memory hash table from tuples produced by its child plan, optimized for efficient probing during the join operation.

## Definition

```c
typedef struct Hash
{
	Plan		plan;

	/*
	 * List of expressions to be hashed for tuples from Hash's outer plan,
	 * needed to put them into the hashtable.
	 */
	List	   *hashkeys;		/* hash keys for the hashjoin condition */
	Oid			skewTable;		/* outer join key's table OID, or InvalidOid */
	AttrNumber	skewColumn;		/* outer join key's column #, or zero */
	bool		skewInherit;	/* is outer join rel an inheritance tree? */
	/* all other info is in the parent HashJoin node */
	Cardinality rows_total;		/* estimate total rows if parallel_aware */
} Hash;
```
## Detailed Description
The Hash node implements the build phase of hash join operations. It processes all tuples from its child plan, computes hash values for the join keys, and stores the tuples in an in-memory hash table structure. This hash table is subsequently used by its parent HashJoin node during the probe phase to find matching tuples efficiently.

Key operational characteristics:
- Executes completely before the probe phase begins (blocking operation)
- Creates an optimally-sized hash table based on cardinality estimates
- Supports both regular and skewed data distributions through skew optimization
- Can operate in parallel environments with shared hash tables
- Handles memory management through batching when data exceeds available memory
- Uses specialized hash functions for different data types

The node includes optimization features for handling skewed data distributions, where certain hash values occur much more frequently than others, which can degrade hash table performance.

## Parameters / Member Variables
- `plan`: Base Plan structure containing common plan node information
- `*hashkeys`: List of expressions that will be evaluated and hashed for each tuple to determine hash table placement
- `skewTable`: OID of the table containing the join key column for skew optimization (InvalidOid if not applicable)
- `skewColumn`: Column number of the join key in the outer relation for skew optimization (zero if not applicable)
- `skewInherit`: Boolean indicating whether the outer join relation involves inheritance tables
- `rows_total`: Estimated total number of rows across all parallel workers (used in parallel-aware plans)
## Dependencies
- Functions called/Symbols referenced:
  - [Plan](../P/Plan.md) (base structure)
  - [List](../L/List.md)
  - Oid
  - AttrNumber
  - Cardinality
- Called from (representative examples):
  - [ExecInitHash](../E/ExecInitHash.md)
  - [ExecHashTableCreate](../E/ExecHashTableCreate.md)
  - [ExecHashBuildSkewHash](../E/ExecHashBuildSkewHash.md)
  - [create_hashjoin_plan](../c/create_hashjoin_plan.md)
  - [make_hash](../m/make_hash.md)

## Notes and Other Information
- The Hash node always serves as the inner (build) side of hash join operations
- [Hash](Hash.md) table sizing is critical for performance and is determined using statistics and memory constraints
- Skew optimization uses Most Common Values (MCV) statistics to handle data distribution problems
- In parallel hash joins, multiple workers can build different portions of the same logical hash table
- Memory management includes automatic batching when the hash table exceeds work_mem limits
- The node supports both parallel-aware and parallel-oblivious execution modes
- [Hash](Hash.md) functions are chosen based on the data types of the join keys
- The hash table structure includes collision resolution through chaining