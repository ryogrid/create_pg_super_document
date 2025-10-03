# disable_statement_timeout

## Location
[src/backend/tcop/postgres.c:5254-5258](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L5254-L5258)

## Overview
A static utility function that disables the currently active statement timeout to prevent query termination due to time limits.

## Definition

```c
static void
disable_statement_timeout(void)
```
## Detailed Description
The  function is a simple but crucial utility function in PostgreSQL's query execution system that conditionally disables the statement timeout mechanism. It first checks if a statement timeout is currently active using , and if so, calls  to deactivate it.

This function is typically called at strategic points during query processing where the statement timeout should be temporarily suspended, such as when completing transactions or handling certain types of queries that may legitimately take longer than the configured timeout period. The function ensures that queries are not prematurely terminated when they reach critical execution phases where interruption could leave the database in an inconsistent state.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - : Checks if a specific timeout type is currently active
  - : Disables a specified timeout with optional cleanup
  - : Timeout type constant for statement execution timeouts

- Called from (representative examples):
  - : During simple query execution (src/backend/tcop/postgres.c:1333)
  - : During prepared statement execution (src/backend/tcop/postgres.c:2307)
  - : When finishing transaction commands (src/backend/tcop/postgres.c:2801)

## Notes and Other Information
- This function is defined as static, meaning it has internal linkage and is only accessible within the postgres.c compilation unit
- The function is part of PostgreSQL's timeout management system, which helps prevent runaway queries from consuming excessive resources
- It's typically called during critical phases of query execution where statement timeouts should not interrupt processing
- The second parameter  passed to  indicates that no special cleanup is required when disabling the timeout
- Located in src/backend/tcop/postgres.c at lines 5254-5258, this function is part of the core query processing infrastructure

## Simplified Source

```c
// Simplified version of disable_statement_timeout
static void
disable_statement_timeout(void)
{
    // Check if statement timeout is currently active
    if (get_timeout_active(STATEMENT_TIMEOUT)) {
        // Disable the statement timeout without special cleanup
        disable_timeout(STATEMENT_TIMEOUT, false);
    }
}
```

Key simplifications made:
- Added descriptive comments to explain each step
- Function is already very simple, so minimal simplification was needed
- Preserved the essential conditional check and timeout disabling logic
- The original function is already quite readable and concise