# ts_stat2

## Location
[src/backend/utils/adt/tsvector_op.c:2689-2726](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L2689-L2726)

## Overview
A PostgreSQL SQL function that provides statistical information about text search vectors (tsvector) by executing a user-provided SQL query and filtering results based on specified weight classes.

## Definition

```c
Datum
ts_stat2(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a set-returning function (SRF) that analyzes tsvector data from a SQL query result and returns statistics about the lexemes found. It takes two arguments: a SQL query that returns tsvector columns and a weight specification string. The function executes the SQL query using SPI (Server Programming Interface), processes each tsvector result, and accumulates statistics about lexeme frequencies and weights.

This is an enhanced version of the basic ts_stat function that allows filtering by specific weight classes (A, B, C, D) through the weight specification parameter. The function uses PostgreSQL's SRF framework to return results incrementally rather than building the entire result set in memory.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0:  - SQL query string that must return exactly one tsvector column
  - Argument 1:  - Weight specification string containing characters 'A', 'B', 'C', 'D' (case-insensitive) to filter lexemes by weight classes

## Dependencies
- Functions called/Symbols referenced:
  -  - Check if this is the first call in SRF execution
  -  - [Initialize](../I/Initialize.md) SRF context on first call
  -  - Connect to SPI for SQL execution
  -  - Execute SQL query and accumulate tsvector statistics
  -  - Free copied varlena arguments if necessary
  -  - Setup SRF state for result iteration
  -  - Clean up SPI connection
  -  - Setup context for each SRF call
  -  - Process next result in SRF iteration
  -  - Return next result value
  -  - Signal completion of SRF
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This function is designed to be called from SQL as a table function, typically used in FROM clauses
- The SQL query parameter must return exactly one column of tsvector type, otherwise an error is raised
- Weight filtering allows users to focus analysis on lexemes with specific importance levels in text search
- Memory management is handled through PostgreSQL's memory context system to prevent leaks during SRF execution
- The function uses SPI cursors for efficient processing of large result sets
- Part of PostgreSQL's full-text search functionality for analyzing tsvector statistics

## Simplified Source

```c
Datum ts_stat2(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    Datum result;

    if (SRF_IS_FIRSTCALL()) {
        // First call: initialize and execute query with weight filter
        TSVectorStat *stat;
        text *query_txt = PG_GETARG_TEXT_PP(0);
        text *weights_txt = PG_GETARG_TEXT_PP(1);

        funcctx = SRF_FIRSTCALL_INIT();
        SPI_connect();

        // Execute SQL query and build statistics with weight filtering
        stat = ts_stat_sql(funcctx->multi_call_memory_ctx, query_txt, weights_txt);

        PG_FREE_IF_COPY(query_txt, 0);
        PG_FREE_IF_COPY(weights_txt, 1);
        ts_setup_firstcall(fcinfo, funcctx, stat);
        SPI_finish();
    }

    // Subsequent calls: return next result row
    funcctx = SRF_PERCALL_SETUP();
    if ((result = ts_process_call(funcctx)) != (Datum) 0)
        SRF_RETURN_NEXT(funcctx, result);

    SRF_RETURN_DONE(funcctx);
}
```