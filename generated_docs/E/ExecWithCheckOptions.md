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
  - [ExecQual](ExecQual.md)
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

## Simplified Source

```c
void
ExecWithCheckOptions(WCOKind kind, ResultRelInfo *resultRelInfo,
                     TupleTableSlot *slot, EState *estate)
{
    Relation rel = resultRelInfo->ri_RelationDesc;
    TupleDesc tupdesc = RelationGetDescr(rel);
    ExprContext *econtext;
    ListCell *l1, *l2;

    // Set up expression context for constraint evaluation
    econtext = GetPerTupleExprContext(estate);
    econtext->ecxt_scantuple = slot;

    // Check each WITH CHECK OPTION constraint
    forboth(l1, resultRelInfo->ri_WithCheckOptions,
            l2, resultRelInfo->ri_WithCheckOptionExprs)
    {
        WithCheckOption *wco = (WithCheckOption *) lfirst(l1);
        ExprState *wcoExpr = (ExprState *) lfirst(l2);

        // Skip constraints not matching the requested kind
        if (wco->kind != kind)
            continue;

        // Evaluate the constraint expression
        if (!ExecQual(wcoExpr, econtext))
        {
            // Handle constraint violation based on type
            switch (wco->kind)
            {
                case WCO_VIEW_CHECK:
                    // For view constraints, provide detailed error info
                    handle_view_check_violation(wco, resultRelInfo, slot,
                                                 estate, rel, tupdesc);
                    break;

                case WCO_RLS_INSERT_CHECK:
                case WCO_RLS_UPDATE_CHECK:
                    // For RLS policies, report generic security violation
                    report_rls_policy_violation(wco, "new row violates row-level security policy");
                    break;

                case WCO_RLS_MERGE_UPDATE_CHECK:
                case WCO_RLS_MERGE_DELETE_CHECK:
                    report_rls_policy_violation(wco, "target row violates row-level security policy (USING expression)");
                    break;

                case WCO_RLS_CONFLICT_CHECK:
                    report_rls_policy_violation(wco, "new row violates row-level security policy (USING expression)");
                    break;

                default:
                    elog(ERROR, "unrecognized WCO kind: %u", wco->kind);
                    break;
            }
        }
    }
}

// Helper function to handle view constraint violations
static void
handle_view_check_violation(WithCheckOption *wco, ResultRelInfo *resultRelInfo,
                             TupleTableSlot *slot, EState *estate,
                             Relation rel, TupleDesc tupdesc)
{
    char *val_desc;
    Bitmapset *modifiedCols;

    // Handle partition mapping if needed
    if (resultRelInfo->ri_RootResultRelInfo)
    {
        // Map slot to root relation format and get modified columns
        slot = map_partition_slot_to_root(slot, resultRelInfo);
        modifiedCols = get_root_modified_cols(resultRelInfo, estate);
        rel = resultRelInfo->ri_RootResultRelInfo->ri_RelationDesc;
    }
    else
    {
        modifiedCols = bms_union(ExecGetInsertedCols(resultRelInfo, estate),
                                 ExecGetUpdatedCols(resultRelInfo, estate));
    }

    // Build detailed error description
    val_desc = ExecBuildSlotValueDescription(RelationGetRelid(rel), slot,
                                             tupdesc, modifiedCols, 64);

    ereport(ERROR,
            (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION),
             errmsg("new row violates check option for view \"%s\"", wco->relname),
             val_desc ? errdetail("Failing row contains %s.", val_desc) : 0));
}

// Helper function to report RLS policy violations
static void
report_rls_policy_violation(WithCheckOption *wco, const char *message)
{
    if (wco->polname != NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("%s \"%s\" for table \"%s\"", message, wco->polname, wco->relname)));
    else
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("%s for table \"%s\"", message, wco->relname)));
}
```