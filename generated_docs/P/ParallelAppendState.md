# ParallelAppendState

## Location
[src/backend/executor/nodeAppend.c:69-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAppend.c#L69-L82)

## Overview
ParallelAppendState is a shared state structure used for coordinating parallel execution in parallel-aware Append nodes, providing synchronization mechanisms for multiple worker processes to select and execute subplans.

## Definition

```c
struct ParallelAppendState
{
	LWLock		pa_lock;		/* mutual exclusion to choose next subplan */
	int			pa_next_plan;	/* next plan to choose by any worker */

	/*
	 * pa_finished[i] should be true if no more workers should select subplan
	 * i.  for a non-partial plan, this should be set to true as soon as a
	 * worker selects the plan; for a partial plan, it remains false until
	 * some worker executes the plan to completion.
	 */
	bool		pa_finished[FLEXIBLE_ARRAY_MEMBER];
};
```
## Detailed Description
ParallelAppendState serves as the coordination mechanism for parallel Append node execution in PostgreSQL's query processing system. This structure is shared across multiple worker processes to ensure proper distribution and synchronization of subplan execution.

The structure implements a work-stealing approach where workers coordinate to select the next available subplan for execution. The lightweight lock (LWLock) ensures thread-safe access to the shared state, while the next plan counter and finished array track execution progress across all subplans.

The design distinguishes between partial and non-partial plans: non-partial plans are marked as finished immediately when selected by a worker (since only one worker should execute them), while partial plans remain available until completely executed by some worker.

## Parameters / Member Variables
- `pa_lock`: LWLock providing mutual exclusion for thread-safe access when workers choose the next subplan to execute
- `pa_next_plan`: Integer counter indicating the next subplan index that should be selected by any available worker
- `pa_finished[FLEXIBLE_ARRAY_MEMBER]`: Flexible array of boolean flags where each element indicates whether subplan i should no longer be selected by workers - set immediately for non-partial plans, set only upon completion for partial plans

## Dependencies
- Functions called/Symbols referenced:
  - [LWLock](../L/LWLock.md)
  - FLEXIBLE_ARRAY_MEMBER

- Called from (representative examples):
  - [ExecAppendEstimate](../E/ExecAppendEstimate.md)
  - [ExecAppendInitializeDSM](../E/ExecAppendInitializeDSM.md)
  - [ExecAppendReInitializeDSM](../E/ExecAppendReInitializeDSM.md)  
  - [choose_next_subplan_for_leader](../c/choose_next_subplan_for_leader.md)
  - [choose_next_subplan_for_worker](../c/choose_next_subplan_for_worker.md)
  - [AppendState](../A/AppendState.md) (referenced in execnodes.h)

## Notes and Other Information
- Located in src/backend/executor/nodeAppend.c:69-82
- This structure is allocated in dynamic shared memory (DSM) to be accessible across parallel worker processes
- The flexible array member allows the structure to accommodate varying numbers of subplans
- Critical for implementing PostgreSQL's parallel query execution capabilities for Append nodes
- Ensures work distribution efficiency while preventing race conditions in parallel execution scenarios