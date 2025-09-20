# psql_setup_cancel_handler

## Location
[src/bin/psql/common.c:313-323](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L313-L323)

## Overview
psql_setup_cancel_handler is a function that initializes the cancellation signal handler for psql by registering the psql-specific cancel callback.

## Definition

```c
void
psql_setup_cancel_handler(void)
```
## Detailed Description
psql_setup_cancel_handler serves as a wrapper function that sets up the cancellation signal handling mechanism for psql. It delegates to the generic setup_cancel_handler() function, passing psql_cancel_callback as the specific callback to be invoked when a cancellation signal (such as SIGINT from Ctrl+C) is received. This function is part of psql's initialization process and ensures that the application can handle user interruption requests gracefully. The separation of concerns allows psql to use the common PostgreSQL signal handling infrastructure while providing its own custom cancellation behavior.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [setup_cancel_handler](../s/setup_cancel_handler.md) (generic signal handler setup function)
  - [psql_cancel_callback](psql_cancel_callback.md) (psql-specific cancellation callback)
- Called from (representative examples):
  - Startup code in src/bin/psql/startup.c
  - Referenced in src/bin/psql/common.h header

## Notes and Other Information
- This function acts as a bridge between psql-specific cancellation handling and generic PostgreSQL signal infrastructure
- Should be called during psql initialization to enable proper interrupt handling
- Works in conjunction with psql_cancel_callback to provide complete cancellation support
- Part of the layered signal handling architecture in PostgreSQL client applications
- Ensures consistent signal handling behavior across different PostgreSQL client tools