# ExecProjectSet

## Location
[src/backend/executor/nodeProjectSet.c:42-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeProjectSet.c#L42-L138)

## Overview
ExecProjectSet is the main execution function for ProjectSet plan nodes that handles evaluation of target lists containing set-returning functions (SRFs).

## Definition


## Detailed Description
ExecProjectSet manages the execution of ProjectSet nodes, which are responsible for projecting tuples that contain set-returning functions. The function operates in two main modes:

1. **Continuation mode**: When there are still pending tuples from a previous SRF evaluation ( is true), it attempts to project another tuple from the same input.

2. **New input mode**: When no pending tuples exist, it retrieves new input tuples from the outer plan and projects SRFs from them.

The function handles the complex lifecycle of SRFs by maintaining state about whether more tuples are expected from the current input tuple. It continues processing until either a valid result tuple is produced or no more input tuples are available.

## Parameters / Member Variables
- : The plan state node, which is cast to ProjectSetState internally

## Dependencies
- Functions called/Symbols referenced:
  - [ExecProjectSRF](ExecProjectSRF.md)
  - ResetExprContext  
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - ExecProcNode
  - TupIsNull
  - outerPlanState
- Called from (representative examples):
  - [ExecInitProjectSet](ExecInitProjectSet.md) (assigned as the ExecProcNode function)

## Notes and Other Information
- The function includes interrupt checking via CHECK_FOR_INTERRUPTS()
- Memory management is carefully handled with separate contexts for per-tuple and argument evaluation
- The function loops until it finds an input tuple that produces at least one output row
- Designed to handle the complex semantics of set-returning functions in PostgreSQL's execution engine