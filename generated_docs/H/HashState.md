# HashState

## Location
[src/include/nodes/execnodes.h:2744-2767](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2744-L2767)

## Overview
HashState is the execution state structure for Hash nodes in PostgreSQL's executor, maintaining the hash table and related information for hash joins and other hash-based operations.

## Definition

```c
typedef struct HashState
{
	PlanState	ps;				/* its first field is NodeTag */
	HashJoinTable hashtable;	/* hash table for the hashjoin */
	List	   *hashkeys;		/* list of ExprState nodes */

	/*
	 * In a parallelized hash join, the leader retains a pointer to the
	 * shared-memory stats area in its shared_info field, and then copies the
	 * shared-memory info back to local storage before DSM shutdown.  The
	 * shared_info field remains NULL in workers, or in non-parallel joins.
	 */
	SharedHashInfo *shared_info;

	/*
	 * If we are collecting hash stats, this points to an initially-zeroed
	 * collection area, which could be either local storage or in shared
	 * memory; either way it's for just one process.
	 */
	HashInstrumentation *hinstrument;

	/* Parallel hash state. */
	struct ParallelHashJoinState *parallel_state;
} HashState;
```
## Detailed Description
HashState represents the runtime state for Hash executor nodes in PostgreSQL. It extends PlanState to provide hash-specific functionality and maintains the hash table used during hash join operations. The structure supports both single-process and parallel hash joins, with special handling for shared memory coordination in parallel scenarios. It tracks hash statistics for performance monitoring and optimization purposes.

## Parameters / Member Variables
-   PID TTY          TIME CMD
12693 ?        00:00:00 bash
12720 ?        00:00:00 ps
21784 ?        00:00:00 dbus-daemon: Base PlanState structure containing common executor node information
- : The actual hash table data structure used for hash join operations
- : List of ExprState nodes representing the hash key expressions
- : Pointer to shared memory statistics area (leader process only in parallel joins)
- : Hash instrumentation data for collecting performance statistics
- : State information for coordinating parallel hash join execution

## Dependencies
- Functions called/Symbols referenced:
  - [HashJoinTable](HashJoinTable.md)
  - [SharedHashInfo](../S/SharedHashInfo.md)
  - [HashInstrumentation](HashInstrumentation.md)
  - ParallelHashJoinState
- Called from (representative examples):
  - [MultiExecHash](../M/MultiExecHash.md)
  - [ExecInitHash](../E/ExecInitHash.md)
  - ExecEndHash
  - [ExecHashJoinImpl](../E/ExecHashJoinImpl.md)

## Notes and Other Information
- The structure is specifically designed to handle both sequential and parallel hash operations
- In parallel hash joins, only the leader process maintains shared_info, while workers have this field as NULL
- Hash instrumentation can use either local or shared memory depending on the execution context
- The structure is central to PostgreSQL's hash join implementation and performance monitoring