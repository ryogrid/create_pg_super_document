# ts_stat_sql

## Location
[src/backend/utils/adt/tsvector_op.c:2575-2663](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L2575-L2663)

## Overview
Executes a SQL query that returns tsvector data and accumulates text search statistics from the results, optionally filtering by specified weights.

## Definition


## Detailed Description
This function executes a SQL query provided as a text parameter and processes the returned tsvector data to build comprehensive text search statistics. It uses PostgreSQL's Server Programming Interface (SPI) to prepare and execute the query via a cursor, fetching results in batches of 100 rows. The function validates that the query returns exactly one column of tsvector type, then processes each result through ts_accum to build a statistical tree of lexemes with their document and occurrence counts.

The function supports weight filtering through the ws parameter, allowing analysis of only specific weight classes (A, B, C, D) of tsvector entries. It manages memory allocation in the provided persistent context to ensure the statistics survive beyond the current function call.

## Parameters / Member Variables
- `persistentContext`: Memory context where the TSVectorStat structure should be allocated for persistence
- `txt`: SQL query text that must return a single tsvector column
- `ws`: Optional weight specification string containing combinations of 'A', 'B', 'C', 'D' characters (case insensitive)

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring
  - [SPI_prepare](../S/SPI_prepare.md)
  - [SPI_cursor_open](../S/SPI_cursor_open.md)
  - [SPI_cursor_fetch](../S/SPI_cursor_fetch.md)
  - SPI_gettypeid
  - [IsBinaryCoercible](../I/IsBinaryCoercible.md)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - [pg_mblen](../p/pg_mblen.md)
  - SPI_getbinval
  - ts_accum
  - [SPI_freetuptable](../S/SPI_freetuptable.md)
  - [SPI_cursor_close](../S/SPI_cursor_close.md)
  - [SPI_freeplan](../S/SPI_freeplan.md)
  - elog/ereport
- Called from (representative examples):
  - [ts_stat1](ts_stat1.md)
  - [ts_stat2](ts_stat2.md)

## Notes and Other Information
- Uses SPI cursor interface for memory-efficient processing of large result sets
- Fetches results in batches of 100 rows to avoid excessive memory usage
- Validates query results to ensure exactly one tsvector column is returned
- Supports weight filtering using bitmask representation (A=8, B=4, C=2, D=1)
- Handles multi-byte characters properly when parsing weight specification
- Accumulates statistics across all non-null tsvector values in the result set
- Properly manages SPI resources including plans, cursors, and tuple tables
- Memory allocation occurs in persistent context to ensure data survives function return
- Part of PostgreSQL's text search functionality for analyzing corpus-wide tsvector statistics
- Error handling includes specific messages for SPI operation failures and invalid parameters