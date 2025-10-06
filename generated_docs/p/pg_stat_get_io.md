# pg_stat_get_io

## Location
[src/backend/utils/adt/pgstatfuncs.c:1357-1468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1357-L1468)

## Overview
A PostgreSQL function that returns detailed I/O statistics for all backend types, I/O objects, and I/O contexts in a tabular format for the pg_stat_io system view.

## Definition

```c
Datum
pg_stat_get_io(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the backend logic for PostgreSQL's pg_stat_io system view, which provides comprehensive I/O statistics across different backend types (like autovacuum, background writer, checkpointer), I/O objects (relations, temp relations), and I/O contexts (normal, vacuum, bulkread, etc.). The function fetches current I/O statistics from the statistics collector and formats them into a table with columns for backend type, context, object, operation counts, timing information, and reset timestamps.

The function uses a set-returning function (SRF) pattern to generate multiple rows of data, iterating through all valid combinations of backend types, I/O objects, and I/O contexts. For each valid combination, it reports statistics for different I/O operations (reads, writes, extends, etc.) including both operation counts and timing data converted from microseconds to milliseconds.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure for set-returning functions
## Dependencies
- Functions called/Symbols referenced:
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md) (initialize set-returning function)
  - [pgstat_fetch_stat_io](pgstat_fetch_stat_io.md) (fetch I/O statistics from collector)
  - [GetBackendTypeDesc](../G/GetBackendTypeDesc.md) (get backend type description)
  - [pgstat_bktype_io_stats_valid](pgstat_bktype_io_stats_valid.md) (validate backend I/O stats)
  - [pgstat_tracks_io_bktype](pgstat_tracks_io_bktype.md) (check if backend type has I/O tracking)
  - [pgstat_get_io_object_name](pgstat_get_io_object_name.md) (get I/O object name)
  - [pgstat_get_io_context_name](pgstat_get_io_context_name.md) (get I/O context name)
  - [pgstat_tracks_io_object](pgstat_tracks_io_object.md) (check if object/context combination is tracked)
  - [pgstat_tracks_io_op](pgstat_tracks_io_op.md) (check if specific I/O operation is tracked)
  - [pgstat_get_io_op_index](pgstat_get_io_op_index.md) (get column index for operation)
  - [pgstat_get_io_time_index](pgstat_get_io_time_index.md) (get column index for timing)
  - [pg_stat_us_to_ms](pg_stat_us_to_ms.md) (convert microseconds to milliseconds)
  - [tuplestore_putvalues](../t/tuplestore_putvalues.md) (add row to result set)
- Called from (representative examples):
  - SQL queries on pg_stat_io system view

## Notes and Other Information
- This function is the backend implementation for the pg_stat_io system view
- Uses nested loops to iterate through all valid combinations of backend types, I/O objects, and I/O contexts
- Skips invalid combinations to avoid cluttering the view with NULL-only rows
- Timing data is converted from microseconds to milliseconds for better readability
- The conversion factor is hard-coded to BLCKSZ (typically 8192 bytes) for block-oriented operations
- Returns Datum 0 as is standard for set-returning functions that populate their results via tuplestore
- Includes assertion checks in debug builds to validate statistics consistency

## Simplified Source

```c
Datum
pg_stat_get_io(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo;
    PgStat_IO *backends_io_stats;

    // Initialize set-returning function
    InitMaterializedSRF(fcinfo, 0);
    rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

    // Fetch I/O statistics from collector
    backends_io_stats = pgstat_fetch_stat_io();
    Datum reset_time = TimestampTzGetDatum(backends_io_stats->stat_reset_timestamp);

    // Iterate through all backend types
    for (int bktype = 0; bktype < BACKEND_NUM_TYPES; bktype++) {
        if (!pgstat_tracks_io_bktype(bktype))
            continue;

        PgStat_BktypeIO *bktype_stats = &backends_io_stats->stats[bktype];

        // Iterate through all I/O objects and contexts
        for (int io_obj = 0; io_obj < IOOBJECT_NUM_TYPES; io_obj++) {
            for (int io_context = 0; io_context < IOCONTEXT_NUM_TYPES; io_context++) {
                if (!pgstat_tracks_io_object(bktype, io_obj, io_context))
                    continue;

                // Prepare row data with basic columns
                Datum values[IO_NUM_COLUMNS] = {0};
                bool nulls[IO_NUM_COLUMNS] = {0};

                values[IO_COL_BACKEND_TYPE] = CStringGetTextDatum(GetBackendTypeDesc(bktype));
                values[IO_COL_CONTEXT] = CStringGetTextDatum(pgstat_get_io_context_name(io_context));
                values[IO_COL_OBJECT] = CStringGetTextDatum(pgstat_get_io_object_name(io_obj));
                values[IO_COL_RESET_TIME] = reset_time;
                values[IO_COL_CONVERSION] = Int64GetDatum(BLCKSZ);

                // Fill in I/O operation statistics
                for (int io_op = 0; io_op < IOOP_NUM_TYPES; io_op++) {
                    if (pgstat_tracks_io_op(bktype, io_obj, io_context, io_op)) {
                        int op_idx = pgstat_get_io_op_index(io_op);
                        PgStat_Counter count = bktype_stats->counts[io_obj][io_context][io_op];
                        values[op_idx] = Int64GetDatum(count);

                        // Add timing data if available
                        int time_idx = pgstat_get_io_time_index(io_op);
                        if (time_idx != IO_COL_INVALID) {
                            PgStat_Counter time = bktype_stats->times[io_obj][io_context][io_op];
                            values[time_idx] = Float8GetDatum(pg_stat_us_to_ms(time));
                        }
                    }
                }

                // Add row to result set
                tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
            }
        }
    }

    return (Datum) 0;
}
```