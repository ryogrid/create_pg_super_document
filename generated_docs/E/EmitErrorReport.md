# EmitErrorReport

## Location
[src/backend/utils/error/elog.c:1687-1745](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L1687-L1745)

## Overview
Outputs the top-of-stack error message to the appropriate destinations (server log, client, or custom hooks) in PostgreSQL's error reporting system.

## Definition
```c
void EmitErrorReport(void)
```

## Detailed Description
The `EmitErrorReport` function is the central output function in PostgreSQL's error reporting system, responsible for actually emitting error messages to their intended destinations. It processes the current top-of-stack error data and sends it to various outputs including the server log, client connections, and custom logging hooks.

The function operates in several key phases: First, it resets formatted timestamp fields to ensure fresh timestamps for each output. Then it optionally calls a custom emit_log_hook if one is registered, allowing for custom log filtering and transmission mechanisms. The hook can turn off server output but cannot turn it on (since uninteresting messages never reach this point). Finally, it sends the message to the server log and/or client as appropriate based on the error data configuration.

The function manages memory context switching and recursion depth tracking to ensure proper resource management during the emission process. It's typically called by errfinish() for most severity levels, or from PostgresMain for ERROR level messages.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (struct type)
  - CHECK_STACK_DEPTH (macro)
  - [send_message_to_server_log](../s/send_message_to_server_log.md) (function)
  - [send_message_to_frontend](../s/send_message_to_frontend.md) (function)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (function)
- Called from (representative examples):
  - [AutoVacWorkerMain](../A/AutoVacWorkerMain.md)
  - [BackgroundWorkerMain](../B/BackgroundWorkerMain.md)
  - [CheckpointerMain](../C/CheckpointerMain.md)
  - [PostgresMain](../P/PostgresMain.md)
  - [errfinish](../e/errfinish.md)
  - Various worker processes (bgwriter, walwriter, etc.)

## Notes and Other Information
- Called from PostgresMain for ERROR level messages or from errfinish for other levels
- Supports custom logging hooks via emit_log_hook for filtering and custom transmission
- Resets timestamp formatting before each emission to ensure current timestamps
- Hook functions can only disable server output, not enable it
- Manages recursion depth and memory context for safe execution
- Central point for all error message output in PostgreSQL
- Used across many background processes and main server loop
- The hook has access to both translated and original English error text for message identification