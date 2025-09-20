# initAccount

## Location
[src/bin/pgbench/pgbench.c:4946-4954](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L4946-L4954)

## Overview
The  function generates account initialization data for pgbench's test database by formatting account records into a SQL buffer.

## Definition

```c
static void
initAccount(PQExpBufferData *sql, int64 curr)
```
## Detailed Description
This function is part of pgbench's database initialization process. It formats account data for the pgbench_accounts table by creating tab-separated values representing an account record. The function generates account data with an account ID (curr + 1), a branch ID (calculated as curr / naccounts + 1), an initial balance of 0, and leaves the filler column as blank-padded empty string (handled by default).

## Parameters / Member Variables
- : Pointer to a PQExpBufferData structure where the formatted account data will be written
- : Current account index (0-based) used to calculate the account ID and branch ID

## Dependencies
- Functions called/Symbols referenced:
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md): PostgreSQL utility function for formatting data into a buffer
  - INT64_FORMAT: Macro for platform-specific 64-bit integer formatting
  - naccounts: Global variable representing the number of accounts per branch
- Called from (representative examples):
  - [initGenerateDataClientSide](initGenerateDataClientSide.md): Uses this function during client-side data generation

## Notes and Other Information
- The function generates tab-separated values suitable for COPY operations
- Account IDs are 1-based (curr + 1) while the input curr parameter is 0-based
- Branch assignment uses integer division to distribute accounts evenly across branches
- The filler column is intentionally left empty and will be padded by PostgreSQL's default behavior
- This is a static function only used within the pgbench.c module