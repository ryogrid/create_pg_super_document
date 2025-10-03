# pg_plan_queries

## Location
[src/backend/tcop/postgres.c:976-1016](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L976-L1016)

## Overview
Generates execution plans for a list of already-rewritten queries, handling both regular optimizable statements and utility commands to produce a list of PlannedStmt nodes.

## Definition

```c
List *
pg_plan_queries(List *querytrees, const char *query_string, int cursorOptions,
				ParamListInfo boundParams)
```
## Detailed Description
This function serves as a batch planning interface that processes multiple queries at once, which is common when dealing with complex SQL statements that may be rewritten into multiple query trees. The function handles two distinct types of queries:

1. **Utility Commands (CMD_UTILITY)**: For DDL statements and other utility commands, the function creates a simple wrapper PlannedStmt node without invoking the planner. It preserves important metadata from the original query including:
   - Command type and tag setting capability
   - Utility statement reference
   - Statement location and length information
   - Query identifier for tracking

2. **Regular Queries**: For DML statements (SELECT, INSERT, UPDATE, DELETE), the function delegates to  to perform full cost-based optimization planning.

The function iterates through each query in the input list, determines the appropriate planning strategy, and builds a corresponding list of PlannedStmt nodes that can be executed by PostgreSQL's executor.

## Parameters / Member Variables
- `*querytrees`: List of rewritten Query structures ready for planning
- `*query_string`: Original SQL query string for logging and error reporting
- `cursorOptions`: Cursor-specific options affecting plan generation
- `boundParams`: Parameter values for prepared statement planning
## Dependencies
- Functions called/Symbols referenced:
  - [pg_plan_query](pg_plan_query.md)
  - makeNode
  - lfirst_node
  - [lappend](../l/lappend.md)
- Called from (representative examples):
  - [exec_simple_query](../e/exec_simple_query.md)
  - [execute_sql_string](../e/execute_sql_string.md)
  - [BuildCachedPlan](../B/BuildCachedPlan.md)

## Notes and Other Information
- Essential for handling multi-statement queries and complex rewrite scenarios
- Efficiently batches planning operations while maintaining individual query context
- Preserves all necessary metadata for utility commands without unnecessary planning overhead
- Critical component in PostgreSQL's query processing pipeline, bridging rewriting and execution phases
- Located in src/backend/tcop/postgres.c:976-1016
- The function maintains the order of queries in the input list in the output planned statement list
- Utility commands bypass the planner entirely, improving performance for DDL operations
- Used extensively in prepared statement caching and extension SQL execution contexts

## Simplified Source

```c
// Simplified version of pg_plan_queries
List *
pg_plan_queries(List *querytrees, const char *query_string, int cursorOptions,
                ParamListInfo boundParams) {
    List *planned_statements = NIL;
    ListCell *query_cell;

    // Process each query in the input list
    foreach(query_cell, querytrees) {
        Query *query = lfirst_node(Query, query_cell);
        PlannedStmt *planned_stmt;

        // Check if this is a utility command (DDL, etc.)
        if (query->commandType == CMD_UTILITY) {
            // Utility commands don't need planning - create wrapper node
            planned_stmt = makeNode(PlannedStmt);
            planned_stmt->commandType = CMD_UTILITY;
            planned_stmt->canSetTag = query->canSetTag;
            planned_stmt->utilityStmt = query->utilityStmt;
            planned_stmt->stmt_location = query->stmt_location;
            planned_stmt->stmt_len = query->stmt_len;
            planned_stmt->queryId = query->queryId;
        } else {
            // Regular DML queries - invoke the planner
            planned_stmt = pg_plan_query(query, query_string, cursorOptions,
                                       boundParams);
        }

        // Add the planned statement to our result list
        planned_statements = lappend(planned_statements, planned_stmt);
    }

    return planned_statements;
}
```

Key simplifications made:
- Used more descriptive variable names (planned_statements, query_cell, planned_stmt)
- Added clear comments explaining the two main execution paths
- Maintained the essential logic flow: iterate through queries, handle utility vs regular commands differently
- Preserved all critical operations and metadata copying
- Focused on the core algorithm without changing functionality