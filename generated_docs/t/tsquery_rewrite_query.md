# tsquery_rewrite_query

## Location
src/backend/utils/adt/tsquery_rewrite.c: 280 - 409

## Overview
The `tsquery_rewrite_query` function is a PostgreSQL SQL function that applies multiple rewrite rules to a TSQuery by executing a SQL query that returns replacement patterns.

## Definition
```c
Datum tsquery_rewrite_query(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the `ts_rewrite(tsquery, text)` SQL function, which allows users to rewrite TSQuery expressions by providing a SQL query that returns rewrite rules. The function executes the provided SQL query, which must return exactly two tsquery columns representing pattern-replacement pairs, and applies each rule sequentially to transform the input query.

The function works by:
1. Converting the input TSQuery to an internal tree representation
2. Executing the provided SQL query using the Server Programming Interface (SPI)
3. Processing each row from the query result as a rewrite rule (pattern → replacement)
4. Applying each rule using the `findsubquery` mechanism
5. Preparing the tree for subsequent rules by clearing processing flags and re-sorting
6. Converting the final tree back to TSQuery format

The function handles edge cases such as empty queries, invalid SQL queries, and queries that return incorrect column types. It uses proper memory management and SPI resource cleanup to ensure robust operation.

## Parameters / Member Variables
- Function follows PostgreSQL SQL function convention with `PG_FUNCTION_ARGS`
- `query`: Input TSQuery to be rewritten (argument 0)
- `in`: SQL query text that returns rewrite rules (argument 1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSQUERY_COPY (get TSQuery argument)
  - QT2QTN (convert TSQuery to tree representation)
  - QTNTernary, QTNSort (tree preprocessing)
  - SPI_connect, SPI_prepare, SPI_cursor_open (SQL execution)
  - SPI_cursor_fetch, SPI_getbinval (result processing)
  - findsubquery (pattern replacement)
  - QTNClearFlags, QTNBinary (tree postprocessing)
  - QTN2QT (convert tree back to TSQuery)
- Called from (representative examples):
  - SQL queries using ts_rewrite(tsquery, text) function

## Notes and Other Information
- Implements the two-argument version of ts_rewrite() SQL function
- Requires the SQL query to return exactly two tsquery columns (pattern and replacement)
- Processes rewrite rules in batches of 100 rows for memory efficiency
- Applies rules iteratively, allowing complex multi-step transformations
- Handles memory management carefully with proper SPI resource cleanup
- Returns an empty TSQuery if all patterns are eliminated during rewriting
- The function validates query result structure and provides appropriate error messages
- Uses cursor-based result processing to handle large result sets efficiently