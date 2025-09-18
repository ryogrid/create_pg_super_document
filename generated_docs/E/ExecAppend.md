# ExecAppend

## Location
src/backend/executor/nodeAppend.c: 288 - 385

## Overview
The main execution function for Append nodes that handles iteration over multiple subplans, supporting both synchronous and asynchronous execution modes.

## Definition


## Detailed Description
ExecAppend is the core execution function for PostgreSQL's Append node executor. It implements a sophisticated iteration strategy that can handle both synchronous and asynchronous subplan execution. The function operates in a continuous loop, cycling through available subplans until all are exhausted.

Key execution phases:
1. **Initialization Check**: On first call after Init or ReScan, performs setup including async subplan initialization
2. **Subplan Selection**: Uses a pluggable strategy (choose_next_subplan function pointer) to select the next subplan to execute
3. **Tuple Retrieval**: Attempts to get tuples from both async and sync subplans
4. **Event Handling**: Manages async events and waits for completion when necessary
5. **Termination**: Returns NULL when all subplans are exhausted

The function optimizes performance by returning tuples directly from subplans without copying through the Append node's result slot.

## Parameters / Member Variables
- : The PlanState (cast to AppendState) containing the append execution context

## Dependencies
- Functions called/Symbols referenced:
  - castNode (for type casting to AppendState)
  - ExecClearTuple (for returning empty results)
  - ExecAppendAsyncBegin (for async subplan initialization)
  - ExecAppendAsyncGetNext (for async tuple retrieval)
  - ExecAppendAsyncEventWait (for async event processing)
  - ExecProcNode (for synchronous subplan execution)
  - TupIsNull (for null tuple checking)
  - bms_is_empty (for bitmap set operations)
  - CHECK_FOR_INTERRUPTS (for query cancellation support)
- Called from (representative examples):
  - ExecInitAppend (set as ExecProcNode function pointer)
  - PostgreSQL executor framework through function pointer calls

## Notes and Other Information
- The function supports both synchronous and asynchronous execution modes simultaneously
- Tuples are returned directly from subplans without intermediate copying for performance
- The function handles partition pruning by working with dynamically determined valid subplans  
- Async execution allows for parallel processing of subplans that support it
- The execution strategy is pluggable via the choose_next_subplan function pointer
- Query interruption is supported through CHECK_FOR_INTERRUPTS() macro calls