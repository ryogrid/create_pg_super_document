# initBranch

## Location
[src/bin/pgbench/pgbench.c:4928-4936](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L4928-L4936)

## Overview
Generates a single row of data for the pgbench_branches table in tab-separated format for COPY operations.

## Definition
```c
static void initBranch(PQExpBufferData *sql, int64 curr)
```

## Detailed Description
The initBranch function generates one row of data for the pgbench_branches table in a format suitable for PostgreSQL's COPY command. It creates a tab-separated line containing the branch ID (curr + 1), an initial balance of 0, and a NULL value for the filler column. The data follows the schema of the pgbench_branches table which includes bid (branch ID), bbalance (branch balance), and filler columns. The function is used during client-side data generation to populate the branches table with initial benchmark data.

## Parameters / Member Variables
- `sql`: PQExpBufferData pointer to the buffer where the formatted row data will be written
- `curr`: int64 current index (0-based) that will be incremented by 1 to create the branch ID

## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (buffer structure for building strings)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (formats and writes data to the buffer)
  - INT64_FORMAT (format macro for 64-bit integers)
- Called from (representative examples):
  - [initGenerateDataClientSide](initGenerateDataClientSide.md) (during client-side data generation for branches)

## Notes and Other Information
- Generates tab-separated values suitable for PostgreSQL COPY command
- Branch ID is curr + 1 (1-based indexing)
- Initial branch balance is set to 0
- Filler column is set to NULL using \\N (escaped for COPY format)
- Static function scope limits its usage to within pgbench.c
- Part of the TPC-B benchmark schema initialization process
- Each branch represents a logical grouping in the benchmark hierarchy