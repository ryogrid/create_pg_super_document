# ExecQueryAndProcessResults

## Location
[src/bin/psql/common.c:1446-1832](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L1446-L1832)

## Overview
ExecQueryAndProcessResults is a comprehensive utility function that sends queries to PostgreSQL and handles all result processing, including COPY operations, chunked results, and various output modes.

## Definition
static int ExecQueryAndProcessResults(const char *query,
                                    double *elapsed_msec, bool *svpt_gone_p,
                                    bool is_watch, int min_rows,
                                    const printQueryOpt *opt, FILE *printQueryFout)

## Detailed Description
ExecQueryAndProcessResults serves as the core query execution engine for both SendQuery() and PSQLexecWatch(). It provides sophisticated handling of various PostgreSQL result types and psql output modes:

**Key Responsibilities:**
1. **Query Transmission**: Uses PQsendQuery() or PQsendQueryParams() for asynchronous query execution
2. **Chunked Results**: Implements FETCH_COUNT functionality using PQsetChunkedRowsMode() for large result sets
3. **COPY Operations**: Handles COPY IN/OUT operations with appropriate stream routing
4. **Result Processing**: Manages different result types (tuples, commands, copy, chunked data)
5. **Error Handling**: Comprehensive error detection and connection state management
6. **Output Routing**: Directs output to appropriate streams (pager, files, stdout) based on context
7. **Savepoint Tracking**: Monitors commands that would invalidate temporary savepoints
8. **Timing Measurement**: Records elapsed time for performance analysis

**Special Handling:**
- **Chunked Mode**: When FETCH_COUNT > 0, results are fetched and displayed incrementally
- **Watch Mode**: Special behavior for \watch command with min_rows support
- **COPY Streams**: Intelligent routing of COPY output to appropriate destinations
- **Variable Setting**: Updates psql variables (ERROR, SQLSTATE, ROW_COUNT) based on results

## Parameters / Member Variables
- `query`: The SQL query string to execute
- `elapsed_msec`: Output parameter for execution timing
- `svpt_gone_p`: Tracks whether temporary savepoints have been invalidated
- `is_watch`: Indicates execution from \watch command
- `min_rows`: Minimum rows required for \watch (0 if not applicable)
- `opt`: Print options for result formatting (can be NULL)
- `printQueryFout`: File stream for status output

## Dependencies
- Functions called/Symbols referenced:
  - [PQsendQuery](../P/PQsendQuery.md), PQsendQueryParams
  - [PQgetResult](../P/PQgetResult.md), PQresultStatus
  - [PQsetChunkedRowsMode](../P/PQsetChunkedRowsMode.md)
  - [AcceptResult](../A/AcceptResult.md), HandleCopyResult
  - [PrintQueryResult](../P/PrintQueryResult.md), printQuery
  - [SetupGOutput](../S/SetupGOutput.md), CloseGOutput
  - [PageOutput](../P/PageOutput.md), ClosePager
  - [SetResultVariables](../S/SetResultVariables.md), ClearOrSaveResult
  - [CheckConnection](../C/CheckConnection.md), ClearOrSaveAllResults
- Called from (representative examples):
  - [SendQuery](../S/SendQuery.md) (for regular query execution)
  - [PSQLexecWatch](../P/PSQLexecWatch.md) (for \watch command)

## Notes and Other Information
- Returns 1 for complete success, 0 for interrupt, -1 for errors
- Function is static, only accessible within common.c
- Handles complex result processing including multiple result sets from compound queries
- Implements sophisticated cancellation handling with proper cleanup
- Supports all major psql output modes (\g, \gexec, \gset, \crosstab, etc.)
- Uses asynchronous libpq interface for better responsiveness
- Chunked mode is disabled for certain operations that need complete result sets
- Properly manages pager usage for large result sets going to stdout

## Simplified Source

```c
static int ExecQueryAndProcessResults(const char *query,
                                    double *elapsed_msec, bool *svpt_gone_p,
                                    bool is_watch, int min_rows,
                                    const printQueryOpt *opt, FILE *printQueryFout)
{
    bool timing = pset.timing;
    bool success;
    bool return_early = false;
    instr_time before, after;
    PGresult *result;
    FILE *gfile_fout = NULL;
    bool gfile_is_pipe = false;

    // Start timing if enabled
    if (timing)
        INSTR_TIME_SET_CURRENT(before);

    // Send query (with or without parameters)
    if (pset.bind_flag)
        success = PQsendQueryParams(pset.db, query, pset.bind_nparams, NULL,
                                  (const char *const *) pset.bind_params, NULL, NULL, 0);
    else
        success = PQsendQuery(pset.db, query);

    if (!success) {
        pg_log_info("%s", PQerrorMessage(pset.db));
        CheckConnection();
        return -1;
    }

    // Enable chunked mode if conditions are met
    if (pset.fetch_count > 0 && pset.show_all_results &&
        !pset.crosstab_flag && !pset.gexec_flag &&
        !pset.gset_prefix && !is_watch) {
        if (!PQsetChunkedRowsMode(pset.db, pset.fetch_count))
            pg_log_warning("fetching results in chunked mode failed");
    }

    // Check for cancellation in watch mode
    if (is_watch && cancel_pressed) {
        ClearOrSaveAllResults();
        return 0;
    }

    // Get first result and check min_rows for watch mode
    result = PQgetResult(pset.db);
    if (min_rows > 0 && PQntuples(result) < min_rows)
        return_early = true;

    // Process all results in a loop
    while (result != NULL) {
        ExecStatusType result_status;
        bool is_chunked_result = false;
        PGresult *next_result;
        bool last;

        // Handle errors
        if (!AcceptResult(result, false)) {
            pg_log_info("%s", PQresultErrorMessage(result));
            CheckConnection();
            if (!is_watch)
                SetResultVariables(result, false);

            result_status = PQresultStatus(result);
            ClearOrSaveResult(result);
            success = false;

            // Get next result (special handling for COPY)
            if (result_status == PGRES_COPY_BOTH ||
                result_status == PGRES_COPY_OUT ||
                result_status == PGRES_COPY_IN)
                result = NULL;
            else
                result = PQgetResult(pset.db);

            if (timing) {
                INSTR_TIME_SET_CURRENT(after);
                INSTR_TIME_SUBTRACT(after, before);
                *elapsed_msec = INSTR_TIME_GET_MILLISEC(after);
            }
            continue;
        }

        // Check for savepoint-destroying commands
        if (svpt_gone_p && !*svpt_gone_p) {
            const char *cmd = PQcmdStatus(result);
            *svpt_gone_p = (strcmp(cmd, "COMMIT") == 0 ||
                           strcmp(cmd, "SAVEPOINT") == 0 ||
                           strcmp(cmd, "RELEASE") == 0 ||
                           strcmp(cmd, "ROLLBACK") == 0);
        }

        result_status = PQresultStatus(result);

        // Handle COPY operations
        if (result_status == PGRES_COPY_IN || result_status == PGRES_COPY_OUT) {
            FILE *copy_stream = NULL;

            if (result_status == PGRES_COPY_OUT) {
                // Determine output stream for COPY OUT
                if (is_watch)
                    copy_stream = printQueryFout ? printQueryFout : pset.queryFout;
                else if (pset.copyStream)
                    copy_stream = pset.copyStream;
                else if (pset.gfname) {
                    success &= SetupGOutput(&gfile_fout, &gfile_is_pipe);
                    if (gfile_fout)
                        copy_stream = gfile_fout;
                } else
                    copy_stream = pset.queryFout;
            }

            success &= HandleCopyResult(&result, copy_stream);
        }

        // Handle chunked results
        if (result_status == PGRES_TUPLES_CHUNK) {
            FILE *tuples_fout = printQueryFout ? printQueryFout : pset.queryFout;
            printQueryOpt my_popt = opt ? *opt : pset.popt;
            int64 total_tuples = 0;
            bool is_pager = false;

            // Setup output streams and paging
            my_popt.topt.start_table = true;
            my_popt.topt.stop_table = false;
            my_popt.topt.prior_records = 0;

            success &= SetupGOutput(&gfile_fout, &gfile_is_pipe);
            if (gfile_fout)
                tuples_fout = gfile_fout;

            if (success && tuples_fout == stdout) {
                tuples_fout = PageOutput(INT_MAX, &(my_popt.topt));
                is_pager = true;
            }

            // Process all chunks
            do {
                if (success && !cancel_pressed) {
                    printQuery(result, &my_popt, tuples_fout, is_pager, pset.logfile);
                    fflush(tuples_fout);
                }

                my_popt.topt.start_table = false;
                my_popt.topt.prior_records += PQntuples(result);
                total_tuples += PQntuples(result);

                ClearOrSaveResult(result);
                result = PQgetResult(pset.db);
            } while (PQresultStatus(result) == PGRES_TUPLES_CHUNK);

            // Handle final empty result
            if (PQresultStatus(result) == PGRES_TUPLES_OK) {
                char buf[32];

                if (success && !cancel_pressed) {
                    my_popt.topt.stop_table = true;
                    printQuery(result, &my_popt, tuples_fout, is_pager, pset.logfile);
                    fflush(tuples_fout);
                }

                if (is_pager)
                    ClosePager(tuples_fout);

                PrintQueryStatus(result, printQueryFout);

                // Set result variables manually for chunked results
                SetVariable(pset.vars, "ERROR", "false");
                SetVariable(pset.vars, "SQLSTATE", "00000");
                snprintf(buf, sizeof(buf), INT64_FORMAT, total_tuples);
                SetVariable(pset.vars, "ROW_COUNT", buf);
                is_chunked_result = true;

                ClearOrSaveResult(result);
                result = NULL;
            } else {
                if (is_pager)
                    ClosePager(tuples_fout);
                success &= AcceptResult(result, true);
            }
        }

        // Check for more results
        next_result = PQgetResult(pset.db);
        last = (next_result == NULL);

        // Update timing
        if (timing) {
            INSTR_TIME_SET_CURRENT(after);
            INSTR_TIME_SUBTRACT(after, before);
            *elapsed_msec = INSTR_TIME_GET_MILLISEC(after);
        }

        // Print regular results
        if (result != NULL) {
            FILE *tuples_fout = printQueryFout;

            if (PQresultStatus(result) == PGRES_TUPLES_OK)
                success &= SetupGOutput(&gfile_fout, &gfile_is_pipe);
            if (gfile_fout)
                tuples_fout = gfile_fout;
            if (success)
                success &= PrintQueryResult(result, last, opt, tuples_fout, printQueryFout);
        }

        // Set result variables for the last result
        if (last && !is_watch && !is_chunked_result)
            SetResultVariables(result, success);

        ClearOrSaveResult(result);
        result = next_result;

        // Handle cancellation
        if (cancel_pressed) {
            ClearOrSaveResult(result);
            ClearOrSaveAllResults();
            break;
        }
    }

    // Cleanup
    CloseGOutput(gfile_fout, gfile_is_pipe);

    if (!CheckConnection())
        return -1;

    if (cancel_pressed || return_early)
        return 0;

    return success ? 1 : -1;
}
```