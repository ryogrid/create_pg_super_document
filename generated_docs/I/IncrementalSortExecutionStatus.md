# IncrementalSortExecutionStatus

## Location
[src/include/nodes/execnodes.h:2387-2388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2387-L2388)

## Overview
An enumeration that tracks the current execution phase of an incremental sort operation, managing state transitions between loading and reading phases for both full and prefix sorting.

## Definition

```c
typedef struct IncrementalSortState
{
	ScanState	ss;				/* its first field is NodeTag */
	bool		bounded;		/* is the result set bounded? */
	int64		bound;			/* if bounded, how many tuples are needed */
	bool		outerNodeDone;	/* finished fetching tuples from outer node */
	int64		bound_Done;		/* value of bound we did the sort with */
	IncrementalSortExecutionStatus execution_status;
	int64		n_fullsort_remaining;
	Tuplesortstate *fullsort_state; /* private state of tuplesort.c */
	Tuplesortstate *prefixsort_state;	/* private state of tuplesort.c */
	/* the keys by which the input path is already sorted */
	PresortedKeyData *presorted_keys;

	IncrementalSortInfo incsort_info;

	/* slot for pivot tuple defining values of presorted keys within group */
	TupleTableSlot *group_pivot;
	TupleTableSlot *transfer_tuple;
	bool		am_worker;		/* are we a worker? */
	SharedIncrementalSortInfo *shared_info; /* one entry per worker */
} IncrementalSortState;
```
## Detailed Description
IncrementalSortExecutionStatus manages the execution state machine of PostgreSQL's incremental sort algorithm. Incremental sort is an optimization that leverages existing sort order in input data - when data is already sorted by a prefix of the required sort keys, it can sort smaller groups incrementally rather than performing a full sort. The enum tracks whether the executor is currently loading data into sort states or reading sorted results, and whether it's operating on the full sort or just the prefix sort portion.

## Parameters / Member Variables
- : Loading phase for full sort - collecting tuples that need complete sorting across all sort keys
- : Loading phase for prefix sort - collecting tuples within a group that shares the same prefix key values
- : Reading phase for full sort - returning sorted tuples from the complete sort operation
- : Reading phase for prefix sort - returning sorted tuples from within a prefix group

## Dependencies
- Functions called/Symbols referenced: (None - this is a simple enumeration)
- Called from (representative examples):
  - [IncrementalSortState](IncrementalSortState.md) (used as execution_status field at execnodes.h:2396)
  - nodeIncrementalSort.c:ExecIncrementalSort() (state transitions throughout)
  - nodeIncrementalSort.c:switchToPresortedPrefixMode() (assignments at lines 418, 454)
  - nodeIncrementalSort.c:ExecInitIncrementalSort() (initialization at line 995)

## Notes and Other Information
This enum is central to the incremental sort execution model, which optimizes sorting performance when input data has partial ordering. The state transitions follow a pattern where the executor alternates between LOAD and READ phases, switching between FULLSORT and PREFIXSORT modes based on the detection of group boundaries in the presorted prefix columns. The algorithm's efficiency comes from avoiding full re-sorting of data that already has useful ordering properties.