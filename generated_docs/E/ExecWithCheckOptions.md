# ExecWithCheckOptions

## Location
[src/backend/executor/execMain.c:2053-2215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L2053-L2215)

## Overview
Validates WITH CHECK OPTION constraints and row-level security policies for tuples, handling different constraint types with appropriate error reporting based on security context.

## Definition
```c
void ExecWithCheckOptions(WCOKind kind, ResultRelInfo *resultRelInfo,
                         TupleTableSlot *slot, EState *estate)
```

## Detailed Description
ExecWithCheckOptions enforces WITH CHECK OPTION constraints and row-level security (RLS) policies during data modification operations. The function evaluates expressions associated with specific constraint types (views, RLS insert/update/merge/conflict checks) and generates context-appropriate error messages when violations occur. For view constraints, it provides detailed tuple information in error messages, while for RLS violations, it deliberately omits tuple details for security reasons. The function handles tuple format conversion for partitioned tables and supports multiple constraint evaluation passes for different WCO kinds.

## Parameters / Member Variables
- `kind`: WCOKind enumeration specifying which type of WITH CHECK OPTION constraints to evaluate (view, RLS insert, update, merge, or conflict checks)
- `resultRelInfo`: ResultRelInfo structure containing the relation metadata and lists of WITH CHECK OPTION constraints and their compiled expressions
- `slot`: TupleTableSlot containing the tuple to be validated against the specified constraints
- `estate`: Execution state providing access to per-tuple expression context and column modification tracking

## Dependencies
- Functions called/Symbols referenced:
  - GetPerTupleExprContext
  - ExecQual
  - [build_attrmap_by_name_if_req](../b/build_attrmap_by_name_if_req.md)
  - [execute_attr_map_slot](../e/execute_attr_map_slot.md)
  - [MakeTupleTableSlot](../M/MakeTupleTableSlot.md)
  - [ExecGetInsertedCols](ExecGetInsertedCols.md)
  - [ExecGetUpdatedCols](ExecGetUpdatedCols.md)
  - [bms_union](../b/bms_union.md)
  - [ExecBuildSlotValueDescription](ExecBuildSlotValueDescription.md)
- Called from (representative examples):
  - [ExecInsert](ExecInsert.md)
  - [ExecBatchInsert](ExecBatchInsert.md)
  - [ExecUpdateAct](ExecUpdateAct.md)
  - [ExecUpdateEpilogue](ExecUpdateEpilogue.md)
  - [ExecOnConflictUpdate](ExecOnConflictUpdate.md)
  - [ExecMergeMatched](ExecMergeMatched.md)

## Notes and Other Information
- Designed for multiple invocations with different WCOKind values to handle all constraint types in a single modification operation
- Provides detailed tuple information for view constraint violations but omits sensitive data for RLS violations
- Handles tuple format conversion for partitioned tables to ensure consistent error reporting
- Uses appropriate error codes: ERRCODE_WITH_CHECK_OPTION_VIOLATION for view constraints and ERRCODE_INSUFFICIENT_PRIVILEGE for RLS policy violations
- Supports various RLS policy types including insert, update, merge, and conflict resolution scenarios
- NULL or FALSE expression evaluation results in constraint violation, following PostgreSQL's constraint semantics