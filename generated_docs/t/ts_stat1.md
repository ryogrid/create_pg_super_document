# ts_stat1

## Location
[src/backend/utils/adt/tsvector_op.c:2664-2688](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L2664-L2688)

## Overview
PostgreSQL SQL function that returns a set of rows containing text search statistics (lexeme, document count, occurrence count) for all terms found via a SQL query.

## Definition

```c
Datum
ts_stat1(PG_FUNCTION_ARGS)
```
## Detailed Description
This is a PostgreSQL set-returning function (SRF) that implements the ts_stat(query) SQL function. It takes a SQL query as input parameter, executes it to collect tsvector data, builds comprehensive statistics about lexemes, and returns the results as a set of rows. Each returned row contains three columns: the lexeme (word/term), the number of documents it appears in (ndoc), and the total number of occurrences (nentry).

The function follows PostgreSQL's SRF pattern with initialization on the first call and iteration on subsequent calls. It connects to SPI to execute the provided query, processes all tsvector results through statistical accumulation, and then iterates through the resulting tree structure to return one row per lexeme.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments macro, containing the SQL query as a text parameter
## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL
  - SRF_FIRSTCALL_INIT
  - [SPI_connect](../S/SPI_connect.md)
  - [ts_stat_sql](ts_stat_sql.md)
  - PG_GETARG_TEXT_PP
  - PG_FREE_IF_COPY
  - [ts_setup_firstcall](ts_setup_firstcall.md)
  - [SPI_finish](../S/SPI_finish.md)
  - SRF_PERCALL_SETUP
  - [ts_process_call](ts_process_call.md)
  - SRF_RETURN_NEXT
  - SRF_RETURN_DONE
- Called from (representative examples):
  - No direct references found (called from SQL)

## Notes and Other Information
- Exposed as a PostgreSQL SQL function ts_stat(text) for corpus-wide text search analysis
- Implements the standard SRF pattern with FIRSTCALL/PERCALL phases
- Uses SPI connection to execute the provided SQL query that must return tsvector data
- No weight filtering applied (uses NULL for weight parameter in ts_stat_sql)
- Memory management handled through SRF framework's multi-call memory context
- Returns composite row type with schema: (word text, ndoc int4, nentry int4)
- Part of PostgreSQL's text search functionality for analyzing document collections
- Function continues returning rows until all lexemes in the statistics tree are processed
- Proper cleanup of SPI resources after query execution completes

## Simplified Source

```c
Datum ts_stat1(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    Datum result;

    if (SRF_IS_FIRSTCALL()) {
        // First call: initialize and execute query
        TSVectorStat *stat;
        text *query_txt = PG_GETARG_TEXT_PP(0);

        funcctx = SRF_FIRSTCALL_INIT();
        SPI_connect();

        // Execute SQL query and build statistics (no weight filter)
        stat = ts_stat_sql(funcctx->multi_call_memory_ctx, query_txt, NULL);

        PG_FREE_IF_COPY(query_txt, 0);
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