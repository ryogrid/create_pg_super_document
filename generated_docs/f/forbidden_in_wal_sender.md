# forbidden_in_wal_sender

## Location
[src/backend/tcop/postgres.c:5026-5047](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L5026-L5047)

## Overview
A security validation function that enforces protocol restrictions in WAL sender processes by throwing errors when unsupported message types are received.

## Definition

```c
struct rlimit rlim;
```
## Detailed Description
This function serves as a security and protocol enforcement mechanism for PostgreSQL's WAL (Write-Ahead Logging) sender processes. WAL senders are specialized backend processes used for streaming replication, and they have strict limitations on what types of operations they can perform. Unlike regular database backends that support the full PostgreSQL protocol, WAL senders are restricted to simple query protocol messages and replication-specific commands.

The function checks if the current process is a WAL sender (using the global am_walsender flag) and if so, generates appropriate error messages based on the type of forbidden message received. It distinguishes between two categories of forbidden operations:
1. Function calls (fastpath protocol) - explicitly forbidden as they're not supported in replication connections
2. Extended query protocol messages (Parse, Bind, Execute, etc.) - forbidden because WAL senders only support simple query protocol

This restriction ensures that replication connections maintain their intended purpose and don't accidentally execute arbitrary database operations.

## Parameters / Member Variables
- : The first character/message type identifier that was received, used to determine the specific type of forbidden operation and generate an appropriate error message

## Dependencies
- Functions called/Symbols referenced:
  - PqMsg_FunctionCall (message type constant for function call detection)
  - ereport (error reporting mechanism)
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md) (multiple locations in src/backend/tcop/postgres.c: 4782, 4806, 4825, 4841, 4880, 4926)

## Notes and Other Information
- Static function scope - only used within postgres.c
- Critical for maintaining security boundaries in replication connections
- Part of PostgreSQL's defense-in-depth approach to preventing misuse of replication connections
- Called before processing Parse, Bind, Execute, FunctionCall, Close, and Describe messages in PostgresMain
- Uses different error messages for function calls vs. extended query protocol to provide clear feedback to clients
- The am_walsender global flag is set during WAL sender initialization and determines if these restrictions apply
- Essential for preventing accidental data modification or unauthorized operations through replication connections