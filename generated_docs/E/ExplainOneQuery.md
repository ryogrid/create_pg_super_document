# ExplainOneQuery

## Location
[src/backend/commands/explain.c:428-454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L428-L454)

## Overview
ExplainOneQuery prints the execution plan for a single Query, serving as a dispatch function that handles both utility statements and plannable queries.

## Definition

```c
static void
ExplainOneQuery(Query *query, int cursorOptions,
				IntoClause *into, ExplainState *es,
				const char *queryString, ParamListInfo params,
				QueryEnvironment *queryEnv)
```
## Detailed Description
ExplainOneQuery is a central dispatch function in the EXPLAIN command processing pipeline. It determines how to handle different types of queries and delegates to appropriate specialized functions. For utility statements (DDL, DCL, etc.), it calls ExplainOneUtility since the planner cannot handle these statement types. For plannable queries (DML statements like SELECT, INSERT, UPDATE, DELETE), it provides a hook mechanism for advisor plugins through ExplainOneQuery_hook, falling back to standard_ExplainOneQuery if no hook is installed.

This design provides extensibility for query analysis plugins while maintaining the standard PostgreSQL explain functionality. The function acts as a routing mechanism that ensures each query type is handled by the appropriate explain logic.

## Parameters / Member Variables
- `*query`: Query structure to be explained
- `cursorOptions`: Cursor options flags (like CURSOR_OPT_PARALLEL_OK) affecting plan generation
- `*into`: IntoClause for CREATE TABLE AS statements, NULL for regular queries
- `*es`: ExplainState containing formatting options and output buffer
- `*queryString`: Original query string for context in error messages and logging
- `params`: ParamListInfo containing parameter values for parameterized queries
- `*queryEnv`: QueryEnvironment providing additional query execution context
## Dependencies
- Functions called/Symbols referenced:
  - [ExplainOneUtility](ExplainOneUtility.md)
  - [standard_ExplainOneQuery](../s/standard_ExplainOneQuery.md)
  - ExplainOneQuery_hook (function pointer)
  - CMD_UTILITY (enum value)
- Called from (representative examples):
  - [ExplainQuery](ExplainQuery.md)
  - [ExplainOneUtility](ExplainOneUtility.md) (for nested utility statements)

## Notes and Other Information
- Static function, only accessible within explain.c
- Provides hook mechanism for extensibility via ExplainOneQuery_hook
- Handles the fundamental distinction between utility and plannable statements
- The 'into' parameter is specifically for CREATE TABLE AS statement handling
- Cursor options affect whether parallel query execution is considered during planning

## Simplified Source

```c
static void ExplainOneQuery(Query *query, int cursorOptions,
                           IntoClause *into, ExplainState *es,
                           const char *queryString, ParamListInfo params,
                           QueryEnvironment *queryEnv) {
    // Utility statements can't be planned, handle separately
    if (query->commandType == CMD_UTILITY) {
        ExplainOneUtility(query->utilityStmt, into, es, queryString, params, queryEnv);
        return;
    }

    // Check for advisor plugin hook first
    if (ExplainOneQuery_hook) {
        // Let plugin handle the query explanation
        (*ExplainOneQuery_hook)(query, cursorOptions, into, es,
                               queryString, params, queryEnv);
    } else {
        // Use standard PostgreSQL explain logic
        standard_ExplainOneQuery(query, cursorOptions, into, es,
                                queryString, params, queryEnv);
    }
}
```