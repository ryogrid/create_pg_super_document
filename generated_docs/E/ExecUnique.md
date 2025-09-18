# ExecUnique

## Location
src/backend/executor/nodeUnique.c: 46 - 113

## Overview
ExecUnique is the main execution function for the UNIQUE plan node that eliminates duplicate tuples from a sorted input stream, returning only the first occurrence of each distinct tuple.

## Definition


## Detailed Description
ExecUnique implements duplicate elimination by processing tuples from its outer subplan in a loop. The function assumes that input tuples arrive in sorted order, which allows for efficient duplicate detection by comparing consecutive tuples. When a new tuple is fetched from the subplan, it is compared against the previously returned tuple using equality functions. If the tuples are identical, the new tuple is discarded and the next tuple is fetched. If they differ, or if this is the first tuple, it is saved as the result and returned to the caller. The function handles the end-of-input condition by returning NULL when the outer subplan is exhausted.

## Parameters / Member Variables
- : The PlanState structure containing execution state information for the UNIQUE node, cast to UniqueState internally

## Dependencies
- Functions called/Symbols referenced:
  - castNode: Cast pstate to UniqueState
  - outerPlanState: Get the outer plan state
  - ExecProcNode: Execute the outer subplan to get next tuple
  - TupIsNull: Check if tuple slot is empty
  - ExecClearTuple: Clear the result tuple slot
  - ExecQualAndReset: Execute equality comparison between tuples
  - ExecCopySlot: Copy tuple from source to result slot
- Called from:
  - ExecInitUnique: During node initialization to set up the execution function

## Notes and Other Information
- Requires input tuples to be sorted for correct duplicate elimination
- Only returns the first tuple of each group of duplicates
- Uses equality functions (eqfunction) stored in UniqueState for tuple comparison
- Must copy result tuples because the source subplan may reuse tuple slots
- Handles interrupts via CHECK_FOR_INTERRUPTS() for query cancellation support