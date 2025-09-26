# ExecAggCopyTransValue

## Location
[src/backend/executor/execExprInterp.c:5070-5118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L5070-L5118)

## Overview
ExecAggCopyTransValue manages memory for aggregate transition values, ensuring proper copying and cleanup of pass-by-reference data in the aggregation context.

## Definition
Datum ExecAggCopyTransValue(AggState *aggstate, AggStatePerTrans pertrans, Datum newValue, bool newValueIsNull, Datum oldValue, bool oldValueIsNull)

## Detailed Description
This function handles the complex memory management requirements for aggregate transition values, particularly for pass-by-reference data types. It ensures that new transition values are stored in the appropriate aggregation context rather than the per-tuple context, while properly cleaning up old transition values. The function has sophisticated logic for handling expanded objects, determining whether to copy data or reuse existing objects based on their memory context. It implements optimization strategies for aggregate-aware transition functions that manage their own memory efficiently.

## Parameters / Member Variables
- aggstate: AggState pointer containing the overall aggregation state
- pertrans: AggStatePerTrans pointer containing per-transition function state information
- newValue: Datum representing the new transition value to be stored
- newValueIsNull: boolean indicating whether the new value is NULL
- oldValue: Datum representing the old transition value to be cleaned up
- oldValueIsNull: boolean indicating whether the old value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - DatumIsReadWriteExpandedObject
  - [DatumGetEOHP](../D/DatumGetEOHP.md)
  - [MemoryContextGetParent](../M/MemoryContextGetParent.md)
  - [datumCopy](../d/datumCopy.md)
  - [DeleteExpandedObject](../D/DeleteExpandedObject.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [pfree](../p/pfree.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)
- Called from (representative examples):
  - [ExecAggPlainTransByRef](ExecAggPlainTransByRef.md)
  - [advance_transition_function](../a/advance_transition_function.md)
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (in JIT compilation context)

## Notes and Other Information
- Implements sophisticated memory management for expanded objects in aggregation contexts
- Optimizes for aggregate-aware transition functions that manage memory efficiently
- Changes current memory context as a side effect of its operation
- Ensures proper cleanup of old transition values to prevent memory leaks
- Handles both expanded and non-expanded object types appropriately
- Located in src/backend/executor/execExprInterp.c at lines 5070-5118