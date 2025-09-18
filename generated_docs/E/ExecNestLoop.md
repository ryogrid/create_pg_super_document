# ExecNestLoop

## Location
src/backend/executor/nodeNestloop.c: 60 - 261

## Overview
ExecNestLoop executes a nested loop join operation between outer and inner relations, returning tuples that satisfy join and other qualification conditions.

## Definition


## Detailed Description
ExecNestLoop implements the core nested loop join algorithm in PostgreSQL's executor. It performs a nested iteration where for each tuple from the outer relation, it scans through all tuples in the inner relation to find matches based on join conditions.

The function operates in a continuous loop, maintaining state through the NestLoopState structure. When it needs a new outer tuple, it fetches one from the outer plan and resets the inner scan. For each outer tuple, it processes inner tuples until either a qualifying join tuple is found or the inner relation is exhausted.

The algorithm supports various join types including inner joins, left outer joins, and anti-joins. For outer joins, when no matching inner tuple is found, it generates a result tuple with null values for inner attributes. For anti-joins, it only returns outer tuples that have no matches in the inner relation.

The function also handles parameterized nested loops where outer tuple values are passed as parameters to the inner scan through the nestParams mechanism, enabling more efficient execution of correlated subqueries.

## Parameters / Member Variables
- : The PlanState node containing execution state information for the nested loop join

## Dependencies
- Functions called/Symbols referenced:
  - ExecProcNode: Gets next tuple from outer/inner plan
  - ExecQual: Evaluates join and other qualification expressions  
  - ExecProject: Projects result tuple using projection info
  - [ExecReScan](ExecReScan.md): Rescans inner plan when starting with new outer tuple
  - ResetExprContext: Resets per-tuple expression evaluation memory
  - TupIsNull: Checks if tuple slot is null
  - slot_getattr: Extracts attribute value from tuple slot
  - [bms_add_member](../b/bms_add_member.md): Adds parameter to changed parameter bitmap
  - InstrCountFiltered1/InstrCountFiltered2: Updates instrumentation counters
- Called from (representative examples):
  - [ExecInitNestLoop](ExecInitNestLoop.md): During node initialization and execution

## Notes and Other Information
- Uses ENL1_printf debug macros for tracing execution flow
- Handles CHECK_FOR_INTERRUPTS() to allow query cancellation
- Maintains nl_NeedNewOuter and nl_MatchedOuter flags to track join state
- Supports single_match optimization for semi-joins
- Uses ecxt_outertuple and ecxt_innertuple in expression context for qualification evaluation
- Memory management through ResetExprContext() prevents memory leaks in long-running joins
- Supports instrumentation for monitoring filtered tuple counts