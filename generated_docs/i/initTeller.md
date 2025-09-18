# initTeller

## Location
src/bin/pgbench/pgbench.c: 4937 - 4945

## Overview
Generates a single row of data for the pgbench_tellers table in tab-separated format for COPY operations, establishing the relationship between tellers and branches.

## Definition
```c
static void initTeller(PQExpBufferData *sql, int64 curr)
```

## Detailed Description
The initTeller function generates one row of data for the pgbench_tellers table in a format suitable for PostgreSQL's COPY command. It creates a tab-separated line containing the teller ID (curr + 1), the associated branch ID (calculated as curr / ntellers + 1), an initial balance of 0, and a NULL value for the filler column. This function establishes the hierarchical relationship in the TPC-B benchmark where multiple tellers belong to each branch. The branch assignment is calculated by dividing the current teller index by the number of tellers per branch.

## Parameters / Member Variables
- `sql`: PQExpBufferData pointer to the buffer where the formatted row data will be written
- `curr`: int64 current index (0-based) that will be incremented by 1 to create the teller ID

## Dependencies
- Functions called/Symbols referenced:
  - PQExpBufferData (buffer structure for building strings)
  - printfPQExpBuffer (formats and writes data to the buffer)
  - INT64_FORMAT (format macro for 64-bit integers)
  - ntellers (global variable defining number of tellers per branch)
- Called from (representative examples):
  - initGenerateDataClientSide (during client-side data generation for tellers)

## Notes and Other Information
- Generates tab-separated values suitable for PostgreSQL COPY command
- Teller ID is curr + 1 (1-based indexing)
- Branch ID is calculated as curr / ntellers + 1, distributing tellers evenly across branches
- Initial teller balance is set to 0
- Filler column is set to NULL using \\N (escaped for COPY format)
- Static function scope limits its usage to within pgbench.c
- Part of the TPC-B benchmark schema initialization process
- Establishes the many-to-one relationship between tellers and branches
- The ntellers variable controls how many tellers are assigned to each branch