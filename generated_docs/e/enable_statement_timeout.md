# enable_statement_timeout

## Location
[src/backend/tcop/postgres.c:5232-5253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L5232-L5253)

## Overview
This function conditionally starts or manages the statement timeout timer based on current configuration settings and transaction state, ensuring statements don't run longer than the configured time limit.

## Definition
```c
static void enable_statement_timeout(void)
```

## Detailed Description
`enable_statement_timeout` manages the statement timeout mechanism by intelligently starting, maintaining, or disabling timeout timers based on the current configuration. The function first checks if a statement timeout is configured and whether it should take precedence over any transaction timeout. If the statement timeout is enabled and not already active, it starts a new timeout timer. If the statement timeout is disabled or should not be active, it disables any currently running statement timeout. The function implements an optimization where it avoids restarting timers that are already running, reducing the overhead of timeout management while maintaining reasonable timeout accuracy.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - Assert (assertion macro for debugging)
  - xact_started (global variable indicating if transaction has started)
  - StatementTimeout (global variable containing statement timeout value in milliseconds)
  - TransactionTimeout (global variable containing transaction timeout value)
  - get_timeout_active (function to check if a timeout is currently active)
  - enable_timeout_after (function to start a timeout timer)
  - disable_timeout (function to stop a timeout timer)
  - STATEMENT_TIMEOUT (timeout type constant)

- Called from (representative examples):
  - start_xact_command (transaction command initiation)

## Notes and Other Information
- Must be called within an active transaction (enforced by assertion)
- Implements timeout precedence logic: statement timeout takes precedence when it's shorter than transaction timeout
- Avoids unnecessary timer restarts for performance optimization
- Part of PostgreSQL's query cancellation and resource management system
- Critical for preventing runaway queries and ensuring system responsiveness
- Works in conjunction with the broader timeout infrastructure
- Helps enforce query execution time limits for both individual statements and entire transactions
- Essential for multi-tenant environments and preventing resource monopolization