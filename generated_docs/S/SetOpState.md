# SetOpState

## Location
[src/include/nodes/execnodes.h:2781-2797](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2781-L2797)

## Overview
SetOpState is the execution state structure for SetOp nodes in PostgreSQL's executor, managing the runtime state for set operations like UNION, INTERSECT, and EXCEPT.

## Definition

```c
typedef struct SetOpState
{
	PlanState	ps;				/* its first field is NodeTag */
	ExprState  *eqfunction;		/* equality comparator */
	Oid		   *eqfuncoids;		/* per-grouping-field equality fns */
	FmgrInfo   *hashfunctions;	/* per-grouping-field hash fns */
	bool		setop_done;		/* indicates completion of output scan */
	long		numOutput;		/* number of dups left to output */
	/* these fields are used in SETOP_SORTED mode: */
	SetOpStatePerGroup pergroup;	/* per-group working state */
	HeapTuple	grp_firstTuple; /* copy of first tuple of current group */
	/* these fields are used in SETOP_HASHED mode: */
	TupleHashTable hashtable;	/* hash table with one entry per group */
	MemoryContext tableContext; /* memory context containing hash table */
	bool		table_filled;	/* hash table filled yet? */
	TupleHashIterator hashiter; /* for iterating through hash table */
} SetOpState;
```
## Detailed Description
SetOpState maintains the execution state for SetOp nodes, which implement SQL set operations (UNION, INTERSECT, EXCEPT). The structure supports two execution strategies: sorted mode for pre-sorted input and hashed mode for unsorted input. It handles duplicate detection and counting, which is essential for implementing set operation semantics correctly. The state tracks progress through the operation and maintains the necessary data structures for both execution modes.

## Parameters / Member Variables
-   PID TTY          TIME CMD
14145 ?        00:00:00 bash
14172 ?        00:00:00 ps
21784 ?        00:00:00 dbus-daemon: Base PlanState structure containing common executor node information
- : Expression state for the equality comparison function
- : Array of OIDs for per-grouping-field equality functions
- : Function manager info for per-grouping-field hash functions
- : Boolean flag indicating whether the output scan is complete
- : Count of duplicate tuples remaining to be output
- : Per-group working state (used in SETOP_SORTED mode)
- : Copy of the first tuple in the current group (sorted mode)
- : Hash table with one entry per tuple group (hashed mode)
- : Memory context containing the hash table
- : Boolean indicating whether the hash table has been populated
- : Iterator for traversing the hash table during output

## Dependencies
- Functions called/Symbols referenced:
  - [SetOpStatePerGroup](SetOpStatePerGroup.md)
  - [TupleHashTable](../T/TupleHashTable.md)
  - TupleHashIterator
- Called from (representative examples):
  - [ExecSetOp](../E/ExecSetOp.md)
  - [ExecInitSetOp](../E/ExecInitSetOp.md)
  - [ExecEndSetOp](../E/ExecEndSetOp.md)
  - [setop_retrieve_direct](../s/setop_retrieve_direct.md)
  - [setop_fill_hash_table](../s/setop_fill_hash_table.md)

## Notes and Other Information
- Supports both sorted and hashed execution strategies for different input conditions
- The structure is more complex than a simple Unique operation because it must count duplicates for proper set operation semantics
- Memory management is carefully handled with dedicated memory contexts for hash table operations
- Essential for implementing SQL standard set operations with correct duplicate handling and performance optimization