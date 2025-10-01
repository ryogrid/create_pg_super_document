# show_modifytable_info

## Location
[src/backend/commands/explain.c:4172-4383](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L4172-L4383)

## Overview
show_modifytable_info is a static function that displays detailed information for ModifyTable nodes in PostgreSQL's EXPLAIN output, including target tables, foreign data wrapper details, and conflict resolution information.

## Definition
```c
static void show_modifytable_info(ModifyTableState *mtstate, List *ancestors, ExplainState *es)
```

## Detailed Description
This function serves three main objectives for ModifyTable operations (INSERT, UPDATE, DELETE, MERGE) in EXPLAIN output: (1) identify actual target tables when there are multiple targets or they differ from the nominal target, (2) allow foreign data wrappers (FDWs) to display additional information about foreign targets, and (3) show information about ON CONFLICT handling and MERGE operation statistics. The function handles complex scenarios like partitioned tables, foreign tables, and provides detailed instrumentation data when EXPLAIN ANALYZE is used.

## Parameters / Member Variables
- `mtstate`: Pointer to the ModifyTableState containing execution state information
- `ancestors`: List of ancestor plan nodes for context in qualification display
- `es`: Pointer to ExplainState structure controlling output format and options

## Dependencies
- Functions called/Symbols referenced:
  - [ExplainOpenGroup](../E/ExplainOpenGroup.md)
  - [ExplainCloseGroup](../E/ExplainCloseGroup.md)
  - [ExplainIndentText](../E/ExplainIndentText.md)
  - [ExplainTargetRel](../E/ExplainTargetRel.md)
  - [ExplainPropertyText](../E/ExplainPropertyText.md)
  - [ExplainPropertyList](../E/ExplainPropertyList.md)
  - [ExplainPropertyFloat](../E/ExplainPropertyFloat.md)
  - [show_upper_qual](show_upper_qual.md)
  - [show_instrumentation_count](show_instrumentation_count.md)
  - [get_rel_name](../g/get_rel_name.md)
  - [list_nth](../l/list_nth.md)
  - [InstrEndLoop](../I/InstrEndLoop.md)
  - outerPlanState
- Called from (representative examples):
  - [ExplainNode](../E/ExplainNode.md)

## Notes and Other Information
- This is a static function, only accessible within the explain.c file
- Handles all DML operation types: INSERT, UPDATE, DELETE, and MERGE
- Provides special handling for ON CONFLICT clauses in INSERT operations
- Shows detailed tuple statistics for MERGE operations when EXPLAIN ANALYZE is used
- Supports foreign data wrapper integration through ExplainForeignModify callbacks
- Automatically labels target tables when there are multiple targets or they differ from nominal targets
- Provides comprehensive instrumentation data including conflict resolution statistics and tuple counts
- Part of PostgreSQL's advanced query execution plan explanation system

## Simplified Source

```c
static void
show_modifytable_info(ModifyTableState *mtstate, List *ancestors, ExplainState *es)
{
    ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
    const char *operation;
    const char *foperation;
    bool labeltargets;
    List *idxNames = NIL;

    // Determine operation type strings
    switch (node->operation)
    {
        case CMD_INSERT:
            operation = "Insert";
            foperation = "Foreign Insert";
            break;
        case CMD_UPDATE:
            operation = "Update";
            foperation = "Foreign Update";
            break;
        case CMD_DELETE:
            operation = "Delete";
            foperation = "Foreign Delete";
            break;
        case CMD_MERGE:
            operation = "Merge";
            foperation = "Foreign Merge";
            break;
        default:
            operation = "???";
            foperation = "Foreign ???";
            break;
    }

    // Decide whether to explicitly label target relations
    labeltargets = (mtstate->mt_nrels > 1 ||
                   (mtstate->mt_nrels == 1 &&
                    mtstate->resultRelInfo[0].ri_RangeTableIndex != node->nominalRelation));

    if (labeltargets)
        ExplainOpenGroup("Target Tables", "Target Tables", false, es);

    // Process each target relation
    for (int j = 0; j < mtstate->mt_nrels; j++)
    {
        ResultRelInfo *resultRelInfo = mtstate->resultRelInfo + j;
        FdwRoutine *fdwroutine = resultRelInfo->ri_FdwRoutine;

        if (labeltargets)
        {
            ExplainOpenGroup("Target Table", NULL, true, es);

            // Add operation type decoration for text mode
            if (es->format == EXPLAIN_FORMAT_TEXT)
            {
                ExplainIndentText(es);
                appendStringInfoString(es->str, fdwroutine ? foperation : operation);
            }

            // Show target relation
            ExplainTargetRel((Plan *) node, resultRelInfo->ri_RangeTableIndex, es);

            if (es->format == EXPLAIN_FORMAT_TEXT)
            {
                appendStringInfoChar(es->str, '\n');
                es->indent++;
            }
        }

        // Allow FDW to add custom information
        if (!resultRelInfo->ri_usesFdwDirectModify &&
            fdwroutine != NULL &&
            fdwroutine->ExplainForeignModify != NULL)
        {
            List *fdw_private = (List *) list_nth(node->fdwPrivLists, j);
            fdwroutine->ExplainForeignModify(mtstate, resultRelInfo, fdw_private, j, es);
        }

        if (labeltargets)
        {
            if (es->format == EXPLAIN_FORMAT_TEXT)
                es->indent--;
            ExplainCloseGroup("Target Table", NULL, true, es);
        }
    }

    // Collect ON CONFLICT arbiter index names
    foreach(lst, node->arbiterIndexes)
    {
        char *indexname = get_rel_name(lfirst_oid(lst));
        idxNames = lappend(idxNames, indexname);
    }

    // Show ON CONFLICT information
    if (node->onConflictAction != ONCONFLICT_NONE)
    {
        ExplainPropertyText("Conflict Resolution",
                           node->onConflictAction == ONCONFLICT_NOTHING ? "NOTHING" : "UPDATE",
                           es);

        if (idxNames)
            ExplainPropertyList("Conflict Arbiter Indexes", idxNames, es);

        // Show conflict filter if present
        if (node->onConflictWhere)
        {
            show_upper_qual((List *) node->onConflictWhere, "Conflict Filter",
                           &mtstate->ps, ancestors, es);
            show_instrumentation_count("Rows Removed by Conflict Filter", 1, &mtstate->ps, es);
        }

        // Show conflict resolution statistics for EXPLAIN ANALYZE
        if (es->analyze && mtstate->ps.instrument)
        {
            InstrEndLoop(outerPlanState(mtstate)->instrument);

            double total = outerPlanState(mtstate)->instrument->ntuples;
            double other_path = mtstate->ps.instrument->ntuples2;
            double insert_path = total - other_path;

            ExplainPropertyFloat("Tuples Inserted", NULL, insert_path, 0, es);
            ExplainPropertyFloat("Conflicting Tuples", NULL, other_path, 0, es);
        }
    }
    else if (node->operation == CMD_MERGE)
    {
        // Show MERGE operation statistics for EXPLAIN ANALYZE
        if (es->analyze && mtstate->ps.instrument)
        {
            InstrEndLoop(outerPlanState(mtstate)->instrument);

            double total = outerPlanState(mtstate)->instrument->ntuples;
            double insert_path = mtstate->mt_merge_inserted;
            double update_path = mtstate->mt_merge_updated;
            double delete_path = mtstate->mt_merge_deleted;
            double skipped_path = total - insert_path - update_path - delete_path;

            if (es->format == EXPLAIN_FORMAT_TEXT)
            {
                if (total > 0)
                {
                    ExplainIndentText(es);
                    appendStringInfoString(es->str, "Tuples:");
                    if (insert_path > 0)
                        appendStringInfo(es->str, " inserted=%.0f", insert_path);
                    if (update_path > 0)
                        appendStringInfo(es->str, " updated=%.0f", update_path);
                    if (delete_path > 0)
                        appendStringInfo(es->str, " deleted=%.0f", delete_path);
                    if (skipped_path > 0)
                        appendStringInfo(es->str, " skipped=%.0f", skipped_path);
                    appendStringInfoChar(es->str, '\n');
                }
            }
            else
            {
                ExplainPropertyFloat("Tuples Inserted", NULL, insert_path, 0, es);
                ExplainPropertyFloat("Tuples Updated", NULL, update_path, 0, es);
                ExplainPropertyFloat("Tuples Deleted", NULL, delete_path, 0, es);
                ExplainPropertyFloat("Tuples Skipped", NULL, skipped_path, 0, es);
            }
        }
    }

    if (labeltargets)
        ExplainCloseGroup("Target Tables", "Target Tables", false, es);
}
```