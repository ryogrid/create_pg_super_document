# initGenerateDataServerSide

## Location
[src/bin/pgbench/pgbench.c:5117-5161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L5117-L5161)

## Overview
The  function implements server-side data generation for pgbench tables using PostgreSQL's generate_series function and SQL INSERT statements instead of client-side data generation.

## Definition

```c
static void
initGenerateDataServerSide(PGconn *con)
```
## Detailed Description
This function provides an alternative approach to initializing pgbench test data by leveraging PostgreSQL's built-in generate_series function to create data directly on the server. Instead of generating data on the client and transferring it via COPY, this method constructs SQL INSERT statements that generate the required test data entirely within the database server. The approach can be more efficient for large datasets as it eliminates network transfer overhead and allows the server to optimize data generation internally. It maintains the same logical structure and relationships as client-side generation while using server-native SQL constructs.

## Parameters / Member Variables  
- : Active PostgreSQL database connection handle used for executing SQL statements

## Dependencies
- Functions called/Symbols referenced:
  - [executeStatement](../e/executeStatement.md): Executes SQL statements including BEGIN, INSERT, and COMMIT
  - [initTruncateTables](initTruncateTables.md): Truncates all pgbench tables to remove existing data
  - [initPQExpBuffer](initPQExpBuffer.md)/termPQExpBuffer: PostgreSQL buffer management for SQL statement construction
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md): Formats SQL INSERT statements with parameters
  - nbranches, ntellers, naccounts: Global variables defining record counts per table type
  - scale: Global scaling factor for total record generation
  - INT64_FORMAT: Macro for platform-specific 64-bit integer formatting
- Called from (representative examples):
  - [runInitSteps](../r/runInitSteps.md): Main initialization workflow that selects this function for server-side data generation

## Notes and Other Information
- Uses PostgreSQL's generate_series function to create sequences of IDs for data generation
- All operations are wrapped in a single transaction for consistency and optimization
- Branch IDs are calculated using integer division: (tid - 1) / ntellers + 1 and (aid - 1) / naccounts + 1
- The filler column is set to empty string ('') for accounts, maintaining consistency with client-side approach
- Generates the same logical data structure as client-side generation but with different implementation
- May be more efficient than client-side generation for very large datasets due to reduced network overhead
- Requires PostgreSQL server support for generate_series function (available in all supported versions)
- Provides user feedback by printing progress messages to stderr
- Part of pgbench's flexible initialization system allowing choice between client and server-side strategies