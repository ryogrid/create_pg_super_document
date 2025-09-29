# InitPlan

## Location
[src/backend/executor/execMain.c:826-1018](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L826-L1018)

## Overview
Initializes the query execution plan by opening files, allocating storage, setting up execution state, and preparing all necessary data structures for query execution.

## Definition

```c
static void
InitPlan(QueryDesc *queryDesc, int eflags)
```
## Detailed Description
This comprehensive initialization function prepares the PostgreSQL executor for query execution. It performs several critical setup operations in sequence:

1. **Permission Checking**: Validates that the user has required permissions for all relations involved in the query
2. **Range Table Initialization**: Sets up the executor's range table with relation information
3. **Row Marking Setup**: Configures row locking mechanisms for SELECT FOR UPDATE/SHARE queries
4. **Subplan Initialization**: Prepares all subplans and initplans with appropriate execution flags
5. **Main Plan Tree Initialization**: Recursively initializes the entire plan node tree
6. **Junk Filter Setup**: For SELECT queries, creates filters to remove internal columns from result tuples

The function handles different query types and execution modes, setting appropriate flags for subplans based on whether they need rewinding capability, and properly configures row marking for various locking strengths.

## Parameters / Member Variables
- : Pointer to QueryDesc structure containing:
  - : Type of SQL command being executed
  - : The planned statement with execution plan
  - : Executor state for managing execution context
  - : Tuple descriptor for result tuples (set by this function)
  - : Root plan state node (set by this function)
- : Execution flags controlling execution behavior (EXEC_FLAG_REWIND, EXEC_FLAG_BACKWARD, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecCheckPermissions](../E/ExecCheckPermissions.md)
  - [ExecInitRangeTable](../E/ExecInitRangeTable.md)
  - [ExecGetRangeTableRelation](../E/ExecGetRangeTableRelation.md)
  - [ExecInitNode](../E/ExecInitNode.md)
  - [ExecGetResultType](../E/ExecGetResultType.md)
  - [ExecInitExtraTupleSlot](../E/ExecInitExtraTupleSlot.md)
  - [ExecInitJunkFilter](../E/ExecInitJunkFilter.md)
  - [CheckValidRowMarkRel](../C/CheckValidRowMarkRel.md)
  - [exec_rt_fetch](../e/exec_rt_fetch.md)
  - [bms_is_member](../b/bms_is_member.md)
- Called from (representative examples):
  - [standard_ExecutorStart](../s/standard_ExecutorStart.md)

## Notes and Other Information
- This is a static function only called from within execMain.c during executor startup
- The function does not return a value; it modifies the queryDesc structure in place
- Row marking is only set up for relations that actually need physical table access
- Subplans are initialized before the main plan tree to ensure proper dependency resolution
- Execution flags are carefully managed to avoid unnecessary overhead in subplans
- The junk filter is only created for SELECT queries when the target list contains junk attributes
- Parent row marks are ignored at runtime as they are only needed during planning
- The function ensures all necessary memory allocations are performed in appropriate memory contexts

## Simplified Source

```c
static void InitPlan(QueryDesc *queryDesc, int eflags)
{
    CmdType operation = queryDesc->operation;
    PlannedStmt *plannedstmt = queryDesc->plannedstmt;
    Plan *plan = plannedstmt->planTree;
    List *rangeTable = plannedstmt->rtable;
    EState *estate = queryDesc->estate;
    PlanState *planstate;
    TupleDesc tupType;
    ListCell *l;
    int i;

    // Check permissions for all relations
    ExecCheckPermissions(rangeTable, plannedstmt->permInfos, true);

    // Initialize the range table
    ExecInitRangeTable(estate, rangeTable, plannedstmt->permInfos);
    estate->es_plannedstmt = plannedstmt;

    // Set up row marking if needed
    if (plannedstmt->rowMarks)
    {
        estate->es_rowmarks = (ExecRowMark **)
            palloc0(estate->es_range_table_size * sizeof(ExecRowMark *));

        foreach(l, plannedstmt->rowMarks)
        {
            PlanRowMark *rc = (PlanRowMark *) lfirst(l);
            Oid relid;
            Relation relation;
            ExecRowMark *erm;

            if (rc->isParent)  // Skip parent rowmarks
                continue;

            relid = exec_rt_fetch(rc->rti, estate)->relid;

            // Open relation based on mark type
            switch (rc->markType)
            {
                case ROW_MARK_EXCLUSIVE:
                case ROW_MARK_NOKEYEXCLUSIVE:
                case ROW_MARK_SHARE:
                case ROW_MARK_KEYSHARE:
                case ROW_MARK_REFERENCE:
                    relation = ExecGetRangeTableRelation(estate, rc->rti);
                    break;
                case ROW_MARK_COPY:
                    relation = NULL;  // No physical access needed
                    break;
                default:
                    elog(ERROR, "unrecognized markType: %d", rc->markType);
                    relation = NULL;
                    break;
            }

            if (relation)
                CheckValidRowMarkRel(relation, rc->markType);

            // Create and initialize ExecRowMark
            erm = (ExecRowMark *) palloc(sizeof(ExecRowMark));
            erm->relation = relation;
            erm->relid = relid;
            erm->rti = rc->rti;
            erm->prti = rc->prti;
            erm->rowmarkId = rc->rowmarkId;
            erm->markType = rc->markType;
            erm->strength = rc->strength;
            erm->waitPolicy = rc->waitPolicy;
            erm->ermActive = false;
            ItemPointerSetInvalid(&(erm->curCtid));
            erm->ermExtra = NULL;

            estate->es_rowmarks[erm->rti - 1] = erm;
        }
    }

    // Initialize tuple table and EPQ state
    estate->es_tupleTable = NIL;
    estate->es_epq_active = NULL;

    // Initialize subplans first
    Assert(estate->es_subplanstates == NIL);
    i = 1;  // Subplan indices start from 1
    foreach(l, plannedstmt->subplans)
    {
        Plan *subplan = (Plan *) lfirst(l);
        PlanState *subplanstate;
        int sp_eflags;

        // Set appropriate flags for subplans
        sp_eflags = eflags & ~(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);
        if (bms_is_member(i, plannedstmt->rewindPlanIDs))
            sp_eflags |= EXEC_FLAG_REWIND;

        subplanstate = ExecInitNode(subplan, estate, sp_eflags);
        estate->es_subplanstates = lappend(estate->es_subplanstates, subplanstate);
        i++;
    }

    // Initialize the main plan tree
    planstate = ExecInitNode(plan, estate, eflags);

    // Get result tuple descriptor
    tupType = ExecGetResultType(planstate);

    // Set up junk filter for SELECT queries if needed
    if (operation == CMD_SELECT)
    {
        bool junk_filter_needed = false;
        ListCell *tlist;

        // Check if any target list entries are junk
        foreach(tlist, plan->targetlist)
        {
            TargetEntry *tle = (TargetEntry *) lfirst(tlist);
            if (tle->resjunk)
            {
                junk_filter_needed = true;
                break;
            }
        }

        if (junk_filter_needed)
        {
            JunkFilter *j;
            TupleTableSlot *slot;

            slot = ExecInitExtraTupleSlot(estate, NULL, &TTSOpsVirtual);
            j = ExecInitJunkFilter(planstate->plan->targetlist, slot);
            estate->es_junkFilter = j;

            // Use cleaned tuple type for results
            tupType = j->jf_cleanTupType;
        }
    }

    queryDesc->tupDesc = tupType;
    queryDesc->planstate = planstate;
}
```