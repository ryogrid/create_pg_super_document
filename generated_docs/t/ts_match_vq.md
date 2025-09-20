# ts_match_vq

## Location
[src/backend/utils/adt/tsvector_op.c:2214-2243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L2214-L2243)

## Overview
The core PostgreSQL function that implements text search matching between a tsvector document and a tsquery, performing the actual @@ operator logic.

## Definition

```c
Datum
ts_match_vq(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the fundamental text search matching operation in PostgreSQL, testing whether a tsvector document matches a tsquery search expression. It extracts the tsvector and tsquery from the function arguments, sets up the necessary data structures for comparison, and delegates to TS_execute for the actual matching logic.

The function handles empty queries as non-matching and properly manages memory cleanup for potentially large input objects. It serves as the primary implementation for the tsvector @@ tsquery operator and is also used by other text search matching variants.

## Parameters / Member Variables
- Uses PostgreSQL's PG_FUNCTION_ARGS macro which provides access to:
  - Argument 0: TSVector (the document vector to search)
  - Argument 1: TSQuery (the search query expression)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSVECTOR
  - PG_GETARG_TSQUERY
  - ARRPTR
  - STRPTR
  - GETOPERAND
  - GETQUERY
  - TS_execute
  - [checkcondition_str](../c/checkcondition_str.md)
  - PG_FREE_IF_COPY
  - PG_RETURN_BOOL
- Called from (representative examples):
  - [ts_match_qv](ts_match_qv.md)
  - [ts_match_tt](ts_match_tt.md)
  - ts_match_tq

## Notes and Other Information
- Returns false immediately for empty queries (optimization)
- Sets up CHKVAL structure to provide TS_execute with access to tsvector data
- Uses TS_EXEC_EMPTY execution mode for standard matching
- Properly handles memory management with PG_FREE_IF_COPY for large objects
- Core implementation used by multiple text search operator variants
- The checkcondition_str callback handles string-based operand matching