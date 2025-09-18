# ExecCloseResultRelations

## Location
[src/backend/executor/execMain.c:1516-1575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L1516-L1575)

## Overview
ExecCloseResultRelations closes any relations that have been opened for ResultRelInfos during query execution, ensuring proper cleanup of result relations and their associated resources.

## Definition
```c
void ExecCloseResultRelations(EState *estate)
```

## Detailed Description
ExecCloseResultRelations is responsible for closing relations that were opened specifically for result operations during query execution. The function handles two main categories of relations:

1. **Result Relations with Indexes**: For relations in `es_opened_result_relations`, it closes the indexes first using ExecCloseIndices, then handles any ancestor result relations. Ancestor relations with RTI (RangeTableIndex) > 0 are skipped as they will be closed by ExecCloseRangeTableRelations, while stub relations (RTI = 0) are closed directly.

2. **Trigger Target Relations**: For relations in `es_trig_target_relations` that were opened by ExecGetTriggerResultRel(), it closes them directly. These are "dummy" ResultRelInfo structures that don't have indexes, so no index cleanup is needed.

The function ensures that all result-related relations are properly closed to prevent resource leaks, while avoiding duplicate closes through careful coordination with ExecCloseRangeTableRelations.

## Parameters / Member Variables
- `estate`: Pointer to the EState containing lists of opened result relations (`es_opened_result_relations`) and trigger target relations (`es_trig_target_relations`) that need to be closed

## Dependencies
- Functions called/Symbols referenced:
  - [ExecCloseIndices](ExecCloseIndices.md) (closes indexes for result relations)
  - table_close (closes individual relation descriptors with NoLock)
- Called from:
  - [ExecEndPlan](ExecEndPlan.md) (main execution cleanup)
  - [CopyFrom](../C/CopyFrom.md) (COPY command cleanup)
  - [afterTriggerInvokeEvents](../a/afterTriggerInvokeEvents.md) (trigger event cleanup)
  - [EvalPlanQualEnd](EvalPlanQualEnd.md) (EPQ cleanup)
  - ResetPerTupleExprContext (expression context reset)

## Notes and Other Information
- This function is part of the executor cleanup sequence and works in coordination with ExecCloseRangeTableRelations
- Uses NoLock when closing relations, indicating that proper locking was handled during the opening phase
- Carefully distinguishes between regular ancestor relations (closed elsewhere) and stub relations (closed here)
- Includes assertions to verify that trigger target relations are "dummy" ResultRelInfos without indexes
- Critical for preventing relation descriptor leaks in complex queries involving result relations
- The separation between result relations and range table relations allows for modular cleanup handling