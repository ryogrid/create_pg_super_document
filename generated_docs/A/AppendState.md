# AppendState

## Location
[src/include/nodes/execnodes.h:1434-1435](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L1434-L1435)

## Overview
AppendState is an execution state structure for Append nodes in PostgreSQL's executor, which manages the sequential and parallel execution of multiple child plans, commonly used for UNION operations and partitioned table access.

## Definition
```c
struct AppendState
{
    PlanState       ps;                         /* its first field is NodeTag */
    PlanState     **appendplans;                /* array of PlanStates for my inputs */
    int             as_nplans;
    int             as_whichplan;
    bool            as_begun;                   /* false means need to initialize */
    Bitmapset      *as_asyncplans;              /* asynchronous plans indexes */
    int             as_nasyncplans;             /* # of asynchronous plans */
    AsyncRequest  **as_asyncrequests;          /* array of AsyncRequests */
    TupleTableSlot **as_asyncresults;          /* unreturned results of async plans */
    int             as_nasyncresults;           /* # of valid entries in as_asyncresults */
    bool            as_syncdone;                /* true if all synchronous plans done in asynchronous mode */
    int             as_nasyncremain;            /* # of remaining asynchronous plans */
    Bitmapset      *as_needrequest;             /* asynchronous plans needing a new request */
    struct WaitEventSet *as_eventset;          /* WaitEventSet for file descriptor wait events */
    int             as_first_partial_plan;      /* Index of first partial plan */
    ParallelAppendState *as_pstate;             /* parallel coordination info */
    Size            pstate_len;                 /* size of parallel coordination info */
    struct PartitionPruneState *as_prune_state;
    bool            as_valid_subplans_identified;   /* is as_valid_subplans valid? */
    Bitmapset      *as_valid_subplans;
    Bitmapset      *as_valid_asyncplans;        /* valid asynchronous plans indexes */
    bool            (*choose_next_subplan)(AppendState *);
};
```

## Detailed Description
AppendState coordinates the execution of multiple child plans in sequence, supporting both synchronous and asynchronous execution modes. It's fundamental to PostgreSQL's ability to handle UNION operations, partitioned table scans, and inheritance hierarchies. The structure supports advanced features like runtime partition pruning, parallel execution across multiple workers, and asynchronous plan execution for foreign data wrappers. It maintains detailed state for tracking which subplans are active, completed, or pending, and provides sophisticated coordination mechanisms for parallel append operations.

## Parameters / Member Variables
- `ps`: Base PlanState structure containing common execution state fields
- `appendplans`: Array of PlanState pointers for all child plans to be executed
- `as_nplans`: Total number of child plans in the appendplans array
- `as_whichplan`: Index of the currently executing child plan
- `as_begun`: Boolean flag indicating whether execution has been initialized
- `as_asyncplans`: Bitmapset identifying which child plans support asynchronous execution
- `as_nasyncplans`: Count of asynchronous plans in the execution tree
- `as_asyncrequests`: Array of AsyncRequest structures for managing asynchronous plan execution
- `as_asyncresults`: Array of TupleTableSlot pointers holding unreturned results from async plans
- `as_nasyncresults`: Number of valid entries currently stored in as_asyncresults
- `as_syncdone`: Boolean indicating whether all synchronous plans have completed in async mode
- `as_nasyncremain`: Count of asynchronous plans that still have work remaining
- `as_needrequest`: Bitmapset identifying async plans that need new requests issued
- `as_eventset`: WaitEventSet used for managing file descriptor events during async execution
- `as_first_partial_plan`: Index in appendplans array where partial plans begin (for parallel execution)
- `as_pstate`: Pointer to ParallelAppendState for coordinating parallel worker execution
- `pstate_len`: Size of the parallel coordination state structure
- `as_prune_state`: Pointer to PartitionPruneState for runtime partition elimination
- `as_valid_subplans_identified`: Boolean indicating whether valid subplan identification is complete
- `as_valid_subplans`: Bitmapset of subplans identified as valid for execution after pruning
- `as_valid_asyncplans`: Bitmapset of valid asynchronous plans after pruning
- `choose_next_subplan`: Function pointer to strategy for selecting the next subplan to execute

## Dependencies
- Functions called/Symbols referenced:
  - [PlanState](../P/PlanState.md) (inherited base structure)
  - [ParallelAppendState](../P/ParallelAppendState.md) (for parallel coordination)
  - PartitionPruneState (for runtime pruning)
  - [AsyncRequest](AsyncRequest.md) (for asynchronous execution)
  - TupleTableSlot (for result storage)
  - [WaitEventSet](../W/WaitEventSet.md) (for event management)
  - [Bitmapset](../B/Bitmapset.md) (for plan tracking)
- Called from (representative examples):
  - [ExecAppend](../E/ExecAppend.md)
  - [ExecInitAppend](../E/ExecInitAppend.md)
  - [ExecEndAppend](../E/ExecEndAppend.md)
  - [ExecReScanAppend](../E/ExecReScanAppend.md)
  - [choose_next_subplan_locally](../c/choose_next_subplan_locally.md)
  - [choose_next_subplan_for_leader](../c/choose_next_subplan_for_leader.md)
  - [choose_next_subplan_for_worker](../c/choose_next_subplan_for_worker.md)
  - [ExecAppendAsyncBegin](../E/ExecAppendAsyncBegin.md)
  - [ExecAppendAsyncGetNext](../E/ExecAppendAsyncGetNext.md)

## Notes and Other Information
AppendState represents one of PostgreSQL's most sophisticated execution state structures, enabling complex query execution patterns across multiple data sources. The asynchronous execution support allows PostgreSQL to efficiently query foreign tables and other remote data sources by overlapping I/O operations. The parallel execution capabilities enable efficient utilization of multiple CPU cores for large UNION operations and partitioned table scans. The runtime partition pruning integration allows PostgreSQL to skip unnecessary partitions during execution, significantly improving performance for partitioned table operations. The choose_next_subplan function pointer provides flexibility in subplan selection strategies, supporting different execution patterns for local vs. parallel execution contexts.