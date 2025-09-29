# ExecReScanUnique

## Location
[src/backend/executor/nodeUnique.c:175-188](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeUnique.c#L175-L188)

## Overview
ExecReScanUnique resets the UNIQUE plan node to restart execution from the beginning, clearing its result tuple state and conditionally rescanning its outer subplan.

## Definition
void ExecReScanUnique(UniqueState *node)

## Detailed Description
ExecReScanUnique handles the rescan operation for UNIQUE plan nodes, which is necessary when the executor needs to restart execution from the beginning (such as in nested loops). The function performs two key operations: first, it clears the result tuple slot to ensure that the first input tuple from the restarted subplan will be returned (since UNIQUE nodes return the first occurrence of each distinct tuple group). Second, it conditionally rescans the outer subplan based on whether the subplan has changed parameters (chgParam). If chgParam is NULL, indicating no parameter changes, the function explicitly calls ExecReScan on the outer plan. If chgParam is not NULL, the rescan is deferred because the subplan will automatically be rescanned when ExecProcNode is next called on it.

## Parameters / Member Variables
- : Pointer to the UniqueState structure representing the UNIQUE node to be rescanned

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState: Get the outer plan state from UniqueState
  - [ExecClearTuple](ExecClearTuple.md): Clear the result tuple slot to reset state
  - [ExecReScan](ExecReScan.md): Recursively rescan the outer subplan if no parameter changes
- Called from:
  - [ExecReScan](ExecReScan.md): During query rescan operations
  - nodeUnique.h: Header declaration

## Notes and Other Information
- Must clear result tuple slot to ensure correct behavior on rescan - first tuple must always be returned
- Implements PostgreSQL's parameter change optimization - defers subplan rescan if parameters changed
- Essential for correctness in nested loop joins and other scenarios requiring multiple scans
- Part of the standard executor rescan protocol used throughout the executor system
- The chgParam check is a performance optimization to avoid unnecessary rescans

## Simplified Source

```c
void ExecReScanUnique(UniqueState *node) {
    PlanState *outerPlan = outerPlanState(node);

    // Clear result tuple so first input tuple is returned
    ExecClearTuple(node->ps.ps_ResultTupleSlot);

    // Rescan outer plan if no parameter changes
    // (if parameters changed, plan will be rescanned automatically)
    if (outerPlan->chgParam == NULL)
        ExecReScan(outerPlan);
}
```