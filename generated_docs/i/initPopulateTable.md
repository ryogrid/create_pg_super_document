# initPopulateTable

## Location
[src/bin/pgbench/pgbench.c:4955-5084](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L4955-L5084)

## Overview
The  function populates a pgbench table with test data using PostgreSQL's COPY protocol, providing progress reporting and optimized data loading with freeze option on supported server versions.

## Definition

```c
static void
initPopulateTable(PGconn *con, const char *table, int64 base,
				  initRowMethod init_row)
```
## Detailed Description
This function is responsible for efficiently populating pgbench tables with large amounts of test data. It uses PostgreSQL's COPY protocol for high-performance bulk data loading and provides detailed progress reporting. The function automatically uses the COPY FREEZE optimization on PostgreSQL 14+ for all tables except partitioned pgbench_accounts tables. It includes sophisticated progress reporting with time estimates and proper terminal handling for clean display updates.

## Parameters / Member Variables
- `*con`: Active PostgreSQL database connection handle
- `*table`: Name of the table to populate (e.g., "pgbench_accounts", "pgbench_branches")
- `base`: Base number of records per scale unit
- `init_row`: Function pointer to the row initialization function (e.g., initAccount, initBranch)
## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](initPQExpBuffer.md)/termPQExpBuffer: PostgreSQL buffer management functions
  - [PQserverVersion](../P/PQserverVersion.md): Gets PostgreSQL server version for feature detection
  - [PQexec](../P/PQexec.md): Executes the COPY statement
  - [PQputline](../P/PQputline.md)/PQendcopy: COPY protocol functions for data streaming
  - [pg_time_now](../p/pg_time_now.md)/PG_TIME_GET_DOUBLE: Time measurement utilities for progress reporting
  - [pg_snprintf](../p/pg_snprintf.md): Safe string formatting
  - PGRES_COPY_IN: Result status constant
  - LOG_STEP_SECONDS: Constant for progress reporting intervals
- Called from (representative examples):
  - [initGenerateDataClientSide](initGenerateDataClientSide.md): Uses this function to populate branches, tellers, and accounts tables

## Notes and Other Information
- Automatically detects PostgreSQL 14+ and uses COPY FREEZE for better performance
- COPY FREEZE is disabled for partitioned pgbench_accounts tables due to limitations
- Provides two progress reporting modes: verbose (every 100k rows) and quiet (time-based intervals)
- Handles terminal output properly with carriage returns for live updates
- Supports cancellation via CancelRequested flag
- Uses efficient COPY protocol instead of individual INSERT statements
- Progress reporting includes elapsed time and estimated remaining time
- Properly cleans up terminal output formatting when complete

## Simplified Source

```c
static void initPopulateTable(PGconn *con, const char *table, int64 base,
                             initRowMethod init_row) {
    PQExpBufferData sql;
    char copy_statement[256];
    const char *copy_statement_fmt = "copy %s from stdin";
    int64 total = base * scale;
    pg_time_usec_t start;

    initPQExpBuffer(&sql);

    // Use COPY FREEZE on PostgreSQL 14+ for better performance
    if (PQserverVersion(con) >= 140000) {
        if (strcmp(table, "pgbench_accounts") != 0 || partitions == 0)
            copy_statement_fmt = "copy %s from stdin with (freeze on)";
    }

    // Build and execute COPY statement
    pg_snprintf(copy_statement, sizeof(copy_statement), copy_statement_fmt, table);
    PGresult *res = PQexec(con, copy_statement);

    if (PQresultStatus(res) != PGRES_COPY_IN)
        pg_fatal("unexpected copy in result: %s", PQerrorMessage(con));
    PQclear(res);

    start = pg_time_now();

    // Generate and send data rows
    for (int64 k = 0; k < total; k++) {
        // Generate row data using provided function
        init_row(&sql, k);

        if (PQputline(con, sql.data))
            pg_fatal("PQputline failed");

        if (CancelRequested)
            break;

        // Progress reporting every 100k rows or time intervals
        if (!use_quiet && ((k + 1) % 100000 == 0)) {
            double elapsed = PG_TIME_GET_DOUBLE(pg_time_now() - start);
            double remaining = ((double) total - (k + 1)) * elapsed / (k + 1);

            fprintf(stderr, INT64_FORMAT " of " INT64_FORMAT " tuples (%d%%) of %s done "
                           "(elapsed %.2f s, remaining %.2f s)\n",
                    k + 1, total, (int) (((k + 1) * 100) / total),
                    table, elapsed, remaining);
        }
    }

    // Finalize COPY operation
    if (PQputline(con, "\\.\n"))
        pg_fatal("very last PQputline failed");
    if (PQendcopy(con))
        pg_fatal("PQendcopy failed");

    termPQExpBuffer(&sql);
}
```