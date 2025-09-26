# drop_unnamed_stmt

## Location
[src/backend/tcop/postgres.c:2877-2901](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L2877-L2901)

## Overview
Releases any existing unnamed prepared statement by dropping its cached plan source.

## Definition
```c
static void drop_unnamed_stmt(void)
```

## Detailed Description
This function manages the cleanup of unnamed prepared statements in PostgreSQL's prepared statement system. In PostgreSQL, clients can create prepared statements either with explicit names or as unnamed statements. The unnamed prepared statement is a special case where the server maintains at most one unnamed statement per session.\n\nThe function performs safe cleanup by:\n1. Checking if an unnamed statement currently exists (unnamed_stmt_psrc \!= NULL)\n2. Temporarily storing the plan source pointer\n3. Clearing the global unnamed_stmt_psrc pointer to NULL first (paranoia protection)\n4. Calling DropCachedPlan to actually release the cached plan resources\n\nThis approach prevents dangling pointer issues that could occur if an error happens during the cleanup process.\n\n## Parameters / Member Variables\n- This function takes no parameters\n- Uses global variable `unnamed_stmt_psrc` (CachedPlanSource pointer)\n\n## Dependencies\n- Functions called/Symbols referenced:\n  - [CachedPlanSource](../C/CachedPlanSource.md) (structure type for cached plan sources)\n  - [DropCachedPlan](../D/DropCachedPlan.md) (function to release cached plan resources)\n- Called from (representative examples):\n  - [exec_simple_query](../e/exec_simple_query.md) (in src/backend/tcop/postgres.c:1059)\n  - [exec_parse_message](../e/exec_parse_message.md) (in src/backend/tcop/postgres.c:1457)\n  - [PostgresMain](../P/PostgresMain.md) (in src/backend/tcop/postgres.c:4894)\n\n## Notes and Other Information\n- This is a static function within postgres.c, making it internal to the query execution module\n- Implements safe cleanup pattern by nullifying pointer before calling cleanup function\n- Part of PostgreSQL's prepared statement management system\n- The unnamed prepared statement is a session-level resource that needs proper cleanup\n- Called during query execution cleanup, new statement preparation, and session termination\n- The \"paranoia\" comment indicates defensive programming to prevent use-after-free errors