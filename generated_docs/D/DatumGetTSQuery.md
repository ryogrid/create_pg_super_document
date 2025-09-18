# DatumGetTSQuery

## Location
src/include/tsearch/ts_type.h: 251 - 256

## Overview
Converts a PostgreSQL Datum value to a TSQuery pointer for text search query operations.

## Definition
```c
static inline TSQuery DatumGetTSQuery(Datum X)
```

## Detailed Description
DatumGetTSQuery is an inline utility function that converts a PostgreSQL Datum to a TSQuery pointer. Unlike the TSVector conversion functions, this function uses DatumGetPointer instead of detoasting macros because TSQuery types are marked as plain storage and cannot be toasted (compressed or stored out-of-line).

The function provides a simple type-safe conversion from the generic Datum type to the specific TSQuery type. TSQuery represents parsed text search queries that can be used to match against TSVector documents in PostgreSQL's full-text search functionality.

## Parameters / Member Variables
- `X`: Input Datum value that represents a TSQuery to be converted

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](DatumGetPointer.md) (function for extracting pointer from Datum)
  - TSQuery (type casting)
- Called from (representative examples):
  - [tsquerysel](../t/tsquerysel.md)
  - [gtsquery_compress](../g/gtsquery_compress.md)
  - [tsquery_rewrite_query](../t/tsquery_rewrite_query.md)
  - [ts_match_tt](../t/ts_match_tt.md)
  - PG_GETARG_TSQUERY

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- TSQuery types are marked as plain storage and cannot be toasted, unlike TSVector
- Uses DatumGetPointer instead of detoasting functions due to storage characteristics
- Essential for text search query processing and matching operations
- Part of the fmgr (function manager) interface functions for text search operations
- Used in query rewriting, GiST indexing, and query execution contexts