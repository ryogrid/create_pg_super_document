# RecursiveUnionState

## Location
[src/include/nodes/execnodes.h:1508-1521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L1508-L1521)

## Overview
RecursiveUnionState is the runtime state structure for executing recursive UNION queries, managing the iterative computation of recursive CTEs (Common Table Expressions).

## Definition

```c
typedef struct RecursiveUnionState
{
	PlanState	ps;				/* its first field is NodeTag */
	bool		recursing;
	bool		intermediate_empty;
	Tuplestorestate *working_table;
	Tuplestorestate *intermediate_table;
	/* Remaining fields are unused in UNION ALL case */
	Oid		   *eqfuncoids;		/* per-grouping-field equality fns */
	FmgrInfo   *hashfunctions;	/* per-grouping-field hash fns */
	MemoryContext tempContext;	/* short-term context for comparisons */
	TupleHashTable hashtable;	/* hash table for tuples already seen */
	MemoryContext tableContext; /* memory context containing hash table */
} RecursiveUnionState;
```
## Detailed Description
RecursiveUnionState manages the execution of recursive UNION operations, which are used to implement recursive CTEs. It maintains working tables for the iterative computation process, where each iteration processes the results from the previous iteration. The structure supports both UNION ALL (which allows duplicates) and UNION (which eliminates duplicates using hash tables).

## Parameters / Member Variables

13357 ?        00:00:00 bash
13405 ?        00:00:00 ps
21784 ?        00:00:00 dbus-daemon: Base PlanState structure containing common executor node fields
- : Boolean flag indicating whether we have finished scanning the non-recursive term and are now in the recursive phase
- : Boolean flag indicating whether the intermediate_table is currently empty (used to detect termination)
- : Tuplestore containing the working table that gets scanned by the recursive term in each iteration
- : Tuplestore containing the current recursive output, which becomes the next generation of the working table
- : Array of equality function OIDs for each grouping field (unused in UNION ALL case)
- : Array of hash function information for each grouping field (unused in UNION ALL case)
- : Temporary memory context used for short-term comparisons during duplicate elimination
- : Hash table for tracking tuples already seen to eliminate duplicates (unused in UNION ALL case)
- : Memory context containing the hash table data structures

## Dependencies
- Functions called/Symbols referenced:
  - Tuplestorestate
  - [TupleHashTable](../T/TupleHashTable.md)
- Called from (representative examples):
  - ExecRecursiveUnion
  - ExecInitRecursiveUnion
  - ExecEndRecursiveUnion
  - ExecReScanRecursiveUnion

## Notes and Other Information
- Essential for implementing SQL recursive CTEs and hierarchical queries
- The working_table and intermediate_table are swapped between iterations to implement the recursive computation
- For UNION ALL queries, the hash table fields are unused, providing better performance when duplicates are acceptable
- The recursion terminates when intermediate_table becomes empty after an iteration
- Memory contexts are carefully managed to avoid memory leaks during long-running recursive queries