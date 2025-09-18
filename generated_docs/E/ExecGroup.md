# ExecGroup

## Location
[src/backend/executor/nodeGroup.c:36-160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGroup.c#L36-L160)

## Overview
ExecGroup is the main execution function for PostgreSQL's Group plan node that returns one representative tuple for each group of matching input tuples based on grouping columns.

## Definition


## Detailed Description
ExecGroup implements the core logic for SQL GROUP BY operations at the executor level. It processes pre-sorted input tuples and identifies groups by comparing consecutive tuples using equality functions on the grouping columns. For each distinct group, it returns the first tuple of that group after applying any HAVING clause qualifications and projections.

The function operates in two phases:
1. **Initialization phase**: On first call, it acquires the first input tuple and determines if it should be returned
2. **Main processing loop**: Iterates through input tuple groups, skipping over tuples that belong to the current group until finding the start of a new group

The algorithm relies on the input being pre-sorted by the grouping columns, which is ensured by the planner placing appropriate Sort nodes in the plan tree.

## Parameters / Member Variables
- : The PlanState containing the GroupState node and execution context

## Dependencies
- Functions called/Symbols referenced:
  - [GroupState](../G/GroupState.md) (cast target)
  - TupIsNull (tuple checking)
  - ExecProcNode (child node execution)
  - outerPlanState (access child plan)
  - ExecCopySlot (tuple copying)
  - ExecQual (HAVING clause evaluation)
  - ExecProject (result projection)
  - ExecQualAndReset (group equality testing)
  - InstrCountFiltered1 (instrumentation)
- Called from (representative examples):
  - [ExecInitGroup](ExecInitGroup.md) (as assigned ExecProcNode function)

## Notes and Other Information
- The function assumes input tuples are pre-sorted by grouping columns
- Uses the ScanTupleSlot to hold the first tuple of each group for comparison
- Implements HAVING clause filtering through the node's qual expression
- Returns NULL when all input groups have been processed
- The grp_done flag prevents further processing after completion
- Memory management is handled through ExecQualAndReset for per-tuple contexts