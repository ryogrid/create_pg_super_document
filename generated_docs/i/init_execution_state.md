# init_execution_state

## Location
src/backend/executor/functions.c: 464 - 582

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
  - pg_plan_query
  - makeNode
  - CommandIsReadOnly
  - CreateCommandName
  - palloc
  - lappend
  - lfirst_node
- Called from (representative examples):
  - init_sql_fcache

## Notes and Other Information
- Creates execution_state structures linked in sequential order for each query
- Enforces function safety by rejecting client COPY and transaction commands
- Respects readonly function constraints by checking CommandIsReadOnly
- Implements lazy evaluation for final SELECT statements when conditions allow
- Marks the last canSetTag query as setting the function result
- Returns NIL if no queries provided, otherwise returns list of execution state chains