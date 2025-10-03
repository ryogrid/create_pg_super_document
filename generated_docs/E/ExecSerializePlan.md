# ExecSerializePlan

## Location
[src/backend/executor/execParallel.c:145-228](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execParallel.c#L145-L228)

## Overview
Creates a serialized representation of a query execution plan to be sent to parallel worker processes in PostgreSQL's parallel query execution framework.

## Definition

```c
static char *
ExecSerializePlan(Plan *plan, EState *estate)
```
## Detailed Description
ExecSerializePlan prepares a query plan for parallel execution by creating a serialized copy that can be transmitted to worker processes. The function performs several critical transformations to ensure the plan is suitable for parallel execution:

1. **Plan Copying**: Creates a deep copy of the original plan to avoid modifying the master's plan structure
2. **Target List Modification**: Sets all target entries'  flags to false, preventing workers from filtering out columns that may be needed by the master process
3. **PlannedStmt Construction**: Builds a minimal PlannedStmt wrapper with essential metadata for executor initialization
4. **Subplan Safety Filtering**: Only includes parallel-safe subplans, leaving NULL placeholders for unsafe ones to maintain proper indexing
5. **Serialization**: Converts the prepared PlannedStmt to a string representation using PostgreSQL's node serialization system

The function ensures that worker processes receive only the information they need while maintaining safety constraints for parallel execution.

## Parameters / Member Variables
- `*plan`: The Plan node to be serialized for parallel execution
- `*estate`: The executor state containing runtime information including range tables and parameter types
## Dependencies
- Functions called/Symbols referenced:
  - copyObject
  - makeNode
  - [pgstat_get_my_query_id](../p/pgstat_get_my_query_id.md)
  - [nodeToString](../n/nodeToString.md)
  - lfirst_node
  - [lappend](../l/lappend.md)
- Called from:
  - [ExecInitParallelPlan](ExecInitParallelPlan.md)

## Notes and Other Information
- The function creates a "dummy" PlannedStmt with minimal required fields since workers only need basic executor initialization data
- Parallel-unsafe subplans are explicitly excluded to prevent workers from attempting to execute non-parallel-aware operations
- The resjunk flag modification is described as "sort of a hack" in the comments, indicating this is a workaround for the executor's automatic junk filtering behavior
- Located in src/backend/executor/execParallel.c:145-228

## Simplified Source

```c
static char *ExecSerializePlan(Plan *plan, EState *estate)
{
    PlannedStmt *pstmt;
    ListCell *lc;

    // Create a copy to avoid modifying the original
    plan = copyObject(plan);

    // Clear resjunk flags to preserve all columns for workers
    foreach(lc, plan->targetlist)
    {
        TargetEntry *tle = lfirst_node(TargetEntry, lc);
        tle->resjunk = false;
    }

    // Create minimal PlannedStmt for worker execution
    pstmt = makeNode(PlannedStmt);
    pstmt->commandType = CMD_SELECT;
    pstmt->queryId = pgstat_get_my_query_id();
    pstmt->planTree = plan;
    pstmt->rtable = estate->es_range_table;
    pstmt->permInfos = estate->es_rteperminfos;
    pstmt->paramExecTypes = estate->es_plannedstmt->paramExecTypes;

    // Initialize other required fields with defaults
    pstmt->hasReturning = false;
    pstmt->canSetTag = true;
    pstmt->parallelModeNeeded = false;
    pstmt->resultRelations = NIL;
    pstmt->appendRelations = NIL;
    pstmt->rowMarks = NIL;
    pstmt->relationOids = NIL;
    pstmt->invalItems = NIL;
    pstmt->utilityStmt = NULL;

    // Copy only parallel-safe subplans, leaving NULL for unsafe ones
    pstmt->subplans = NIL;
    foreach(lc, estate->es_plannedstmt->subplans)
    {
        Plan *subplan = (Plan *) lfirst(lc);
        if (subplan && !subplan->parallel_safe)
            subplan = NULL;  // Leave hole for unsafe subplans
        pstmt->subplans = lappend(pstmt->subplans, subplan);
    }

    // Return serialized representation
    return nodeToString(pstmt);
}
```