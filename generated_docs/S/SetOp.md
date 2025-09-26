# SetOp

## Location
[src/include/nodes/plannodes.h:1217-1245](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L1217-L1245)

## Overview
The SetOp node implements SQL set operations (INTERSECT, INTERSECT ALL, EXCEPT, EXCEPT ALL) by processing sorted or hashed input to find matching or non-matching tuples between datasets.

## Definition

```c
typedef struct SetOp
{
	Plan		plan;

	/* what to do, see nodes.h */
	SetOpCmd	cmd;

	/* how to do it, see nodes.h */
	SetOpStrategy strategy;

	/* number of columns to check for duplicate-ness */
	int			numCols;

	/* their indexes in the target list */
	AttrNumber *dupColIdx pg_node_attr(array_size(numCols));

	/* equality operators to compare with */
	Oid		   *dupOperators pg_node_attr(array_size(numCols));
	Oid		   *dupCollations pg_node_attr(array_size(numCols));

	/* where is the flag column, if any */
	AttrNumber	flagColIdx;

	/* flag value for first input relation */
	int			firstFlag;

	/* estimated number of groups in input */
	long		numGroups;
} SetOp;
```
## Detailed Description
The SetOp node implements SQL set operations by comparing tuples from different input sources. It operates on pre-processed input where tuples from different relations are distinguished by a flag column, allowing the node to determine which set operation to apply.

The node supports two execution strategies:
- **SETOP_SORTED**: Requires pre-sorted input and processes tuples in order, comparing consecutive groups
- **SETOP_HASHED**: Uses an internal hash table to group and count tuples, suitable for unsorted input

Set operations implemented:
- **INTERSECT**: Returns tuples that appear in both input sets
- **INTERSECT ALL**: Returns tuples with their minimum occurrence count across input sets  
- **EXCEPT**: Returns tuples from the first set that don't appear in the second set
- **EXCEPT ALL**: Returns tuples from the first set, subtracting their occurrence counts in the second set

The flag column mechanism allows the node to distinguish which input relation each tuple originated from, enabling proper set operation semantics.

## Parameters / Member Variables
- : Base Plan structure containing common plan node information
- : Specifies which set operation to perform (INTERSECT, INTERSECT_ALL, EXCEPT, EXCEPT_ALL)
- : Execution strategy (SETOP_SORTED for sorted input, SETOP_HASHED for hash-based processing)
- : Number of columns to compare when determining tuple equality
- : Array of target list column indexes to use for equality comparisons
- : Array of equality operator OIDs for comparing corresponding columns
- : Array of collation OIDs for locale-specific equality comparisons
- : Index of the flag column that identifies which input relation each tuple came from
- : Flag value used to identify tuples from the first input relation
- : Estimated number of distinct tuple groups in the input (for optimization)

## Dependencies
- Functions called/Symbols referenced:
  - [Plan](../P/Plan.md) (base structure)
  - [SetOpCmd](SetOpCmd.md)
  - SetOpStrategy
  - AttrNumber
  - Oid
- Called from (representative examples):
  - [ExecInitSetOp](../E/ExecInitSetOp.md)
  - [ExecSetOp](../E/ExecSetOp.md)
  - [ExecReScanSetOp](../E/ExecReScanSetOp.md)
  - [create_setop_plan](../c/create_setop_plan.md)
  - [make_setop](../m/make_setop.md)

## Notes and Other Information
- The SetOp node expects its input to already be tagged with flag values distinguishing different source relations
- [Hash](../H/Hash.md)-based strategy is more memory-intensive but doesn't require pre-sorted input
- Sorted strategy requires less memory but needs the input to be sorted on the comparison columns
- The node handles NULL values according to SQL standard set operation semantics
- For ALL variants, the node maintains occurrence counts to implement proper multiset semantics
- Performance depends heavily on the chosen strategy and the distribution of input data
- The flag column approach allows efficient implementation of set operations with a single node type