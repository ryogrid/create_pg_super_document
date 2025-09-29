# init_execution_state

## Location
[src/backend/executor/functions.c:464-582](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L464-L582)

## Overview
Sets up per-query execution state records for a SQL function by processing parsed query trees, planning them, and creating execution state structures for each command.

## Definition
```c
static List *
init_execution_state(List *queryTree_list,
                     SQLFunctionCachePtr fcache,
                     bool lazyEvalOK)
```

## Detailed Description
This function processes a list of parsed and rewritten query trees for a SQL function, converting them into execution states. It handles both regular queries (which need planning via pg_plan_query) and utility commands (which require no planning). The function validates commands for use within SQL functions, enforcing restrictions like prohibiting client COPY operations and transaction commands. It also implements lazy evaluation optimization for SELECT statements that return the function result.

## Parameters / Member Variables
- `queryTree_list`: List of Lists containing parsed and rewritten query trees, with sublist structure denoting original query boundaries
- `fcache`: Pointer to SQL function cache containing function metadata and configuration
- `lazyEvalOK`: Boolean indicating whether lazy evaluation is permitted for the final SELECT statement

## Dependencies
- Functions called/Symbols referenced:
  - [pg_plan_query](../p/pg_plan_query.md)
  - makeNode
  - [CommandIsReadOnly](../C/CommandIsReadOnly.md)
  - [CreateCommandName](../C/CreateCommandName.md)
  - [palloc](../p/palloc.md)
  - [lappend](../l/lappend.md)
  - lfirst_node
- Called from (representative examples):
  - [init_sql_fcache](init_sql_fcache.md)

## Notes and Other Information
- Creates execution_state structures linked in sequential order for each query
- Enforces function safety by rejecting client COPY and transaction commands
- Respects readonly function constraints by checking CommandIsReadOnly
- Implements lazy evaluation for final SELECT statements when conditions allow
- Marks the last canSetTag query as setting the function result
- Returns NIL if no queries provided, otherwise returns list of execution state chains

## Simplified Source

```c
static List *init_execution_state(List *queryTree_list,
                                  SQLFunctionCachePtr fcache,
                                  bool lazyEvalOK)
{
    List *eslist = NIL;
    execution_state *lasttages = NULL;

    // Process each query sublist
    foreach(lc1, queryTree_list) {
        List *qtlist = lfirst_node(List, lc1);
        execution_state *firstes = NULL;
        execution_state *preves = NULL;

        // Process each query in the sublist
        foreach(lc2, qtlist) {
            Query *queryTree = lfirst_node(Query, lc2);
            PlannedStmt *stmt;
            execution_state *newes;

            // Step 1: Plan the query
            if (queryTree->commandType == CMD_UTILITY) {
                // Utility commands don't need planning
                stmt = makeNode(PlannedStmt);
                stmt->commandType = CMD_UTILITY;
                stmt->canSetTag = queryTree->canSetTag;
                stmt->utilityStmt = queryTree->utilityStmt;
                // Copy other metadata
            } else {
                // Plan regular queries
                stmt = pg_plan_query(queryTree, fcache->src, CURSOR_OPT_PARALLEL_OK, NULL);
            }

            // Step 2: Validate command for function context
            if (stmt->commandType == CMD_UTILITY) {
                // Reject client COPY commands
                if (IsA(stmt->utilityStmt, CopyStmt) &&
                    ((CopyStmt *) stmt->utilityStmt)->filename == NULL) {
                    ereport(ERROR, "cannot COPY to/from client in an SQL function");
                }

                // Reject transaction commands
                if (IsA(stmt->utilityStmt, TransactionStmt)) {
                    ereport(ERROR, "%s is not allowed in an SQL function",
                           CreateCommandName(stmt->utilityStmt));
                }
            }

            // Check readonly function constraints
            if (fcache->readonly_func && !CommandIsReadOnly(stmt)) {
                ereport(ERROR, "%s is not allowed in a non-volatile function",
                       CreateCommandName((Node *) stmt));
            }

            // Step 3: Create execution state record
            newes = (execution_state *) palloc(sizeof(execution_state));

            // Link into chain
            if (preves) {
                preves->next = newes;
            } else {
                firstes = newes;
            }

            // Initialize execution state
            newes->next = NULL;
            newes->status = F_EXEC_START;
            newes->setsResult = false;
            newes->lazyEval = false;
            newes->stmt = stmt;
            newes->qd = NULL;

            // Track last statement that can set result
            if (queryTree->canSetTag) {
                lasttages = newes;
            }

            preves = newes;
        }

        // Add this query chain to the result list
        eslist = lappend(eslist, firstes);
    }

    // Step 4: Configure result-setting and lazy evaluation
    if (lasttages && fcache->junkFilter) {
        lasttages->setsResult = true;

        // Enable lazy evaluation for final SELECT if possible
        if (lazyEvalOK &&
            lasttages->stmt->commandType == CMD_SELECT &&
            !lasttages->stmt->hasModifyingCTE) {
            fcache->lazyEval = lasttages->lazyEval = true;
        }
    }

    return eslist;
}
```