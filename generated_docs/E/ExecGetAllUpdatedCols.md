# ExecGetAllUpdatedCols

## Location
[src/backend/executor/execUtils.c:1324-1343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L1324-L1343)

## Overview
Returns a bitmap representing all columns being updated during an UPDATE operation, including both explicitly updated columns and generated columns that need recalculation.

## Definition


## Detailed Description
This function provides a comprehensive view of all columns that will be modified during an UPDATE operation by combining two sources of updated columns:

1. **Explicitly updated columns**: Columns directly specified in the UPDATE statement
2. **Generated columns**: Columns that must be recalculated because they depend on explicitly updated columns

The function creates a union of bitmaps returned by ExecGetUpdatedCols() and ExecGetExtraUpdatedCols(), providing the complete set of columns that will change during the operation. This comprehensive view is essential for trigger processing, constraint checking, and lock mode determination.

The function allocates the result bitmap in the per-tuple memory context, which has a short lifespan tied to the processing of a single tuple. Callers that need longer-lived bitmaps must copy the result to an appropriate memory context.

## Parameters / Member Variables
- : ResultRelInfo structure for the target relation
- : Executor state containing execution context and memory management information

## Dependencies
- Functions called/Symbols referenced:
  - GetPerTupleMemoryContext: Retrieves the per-tuple memory context for short-lived allocations
  - [ExecGetUpdatedCols](ExecGetUpdatedCols.md): Gets bitmap of explicitly updated columns
  - [ExecGetExtraUpdatedCols](ExecGetExtraUpdatedCols.md): Gets bitmap of generated columns that need updating
  - [bms_union](../b/bms_union.md): Combines two bitmaps into a single unified bitmap
- Called from (representative examples):
  - [ExecBSUpdateTriggers](ExecBSUpdateTriggers.md): For determining which columns changed when firing BEFORE statement triggers
  - [ExecASUpdateTriggers](ExecASUpdateTriggers.md): For AFTER statement trigger processing
  - [ExecBRUpdateTriggersNew](ExecBRUpdateTriggersNew.md): For BEFORE row trigger processing with new tuple values
  - [ExecARUpdateTriggers](ExecARUpdateTriggers.md): For AFTER row trigger processing
  - [ExecUpdateLockMode](ExecUpdateLockMode.md): For determining appropriate locking based on updated columns

## Notes and Other Information
- The returned bitmap is allocated in per-tuple memory context and has limited lifespan
- Callers must copy the bitmap to a longer-lived context if persistence is needed
- Essential for trigger systems that need to know all affected columns, not just explicitly updated ones
- Critical for lock mode optimization - understanding the complete set of affected columns helps determine minimal necessary locking
- Combines both user-specified updates and system-generated updates (generated columns) into a single comprehensive view
- Used extensively by the trigger system to ensure proper trigger firing based on complete column change information