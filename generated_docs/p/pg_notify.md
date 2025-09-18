# pg_notify

## Location
src/backend/commands/async.c: 557 - 590

## Overview
A SQL-callable function that implements the NOTIFY command functionality, allowing users to send asynchronous notification events with an optional payload.

## Definition
```c
Datum pg_notify(PG_FUNCTION_ARGS)
```

## Detailed Description
The `pg_notify` function serves as the SQL interface for PostgreSQL's NOTIFY command, enabling applications to send asynchronous notifications to listening processes. It accepts two parameters: a channel name and an optional payload string. The function handles NULL arguments by converting them to empty strings, ensuring robust operation even with incomplete input. It includes a recovery check to prevent NOTIFY operations during database recovery processes, maintaining consistency and preventing potential issues. The actual notification work is delegated to the lower-level `Async_Notify` function.

## Parameters / Member Variables
- Function arguments accessed via PostgreSQL's function call convention:
  - Argument 0: Channel name (text, nullable) - The notification channel identifier
  - Argument 1: Payload (text, nullable) - Optional message content to send with the notification

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring (converts PostgreSQL text type to C string)
  - [PreventCommandDuringRecovery](../P/PreventCommandDuringRecovery.md) (prevents operation during recovery)
  - [Async_Notify](../A/Async_Notify.md) (performs the actual notification work)
  - PG_RETURN_VOID (returns void result to SQL caller)
  - PG_FUNCTION_ARGS, PG_ARGISNULL, PG_GETARG_TEXT_PP (PostgreSQL function interface macros)
- Called from:
  - No direct references found (likely called via SQL function dispatch mechanism)

## Notes and Other Information
- This is the primary entry point for SQL NOTIFY commands executed as functions
- Handles NULL arguments gracefully by substituting empty strings
- Prevents execution during database recovery to maintain system integrity
- Uses PostgreSQL's standard function calling convention (PG_FUNCTION_ARGS)
- Returns void (no result value) as appropriate for notification commands
- Part of the broader asynchronous messaging system alongside LISTEN commands
- The actual notification logic is implemented in the Async_Notify function