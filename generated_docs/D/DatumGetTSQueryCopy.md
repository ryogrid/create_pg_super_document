# DatumGetTSQueryCopy

## Location
[src/include/tsearch/ts_type.h:257-262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/ts_type.h#L257-L262)

## Overview
Converts a PostgreSQL Datum value to a TSQuery pointer, creating a modifiable copy for text search query operations.

## Definition
```c
static inline TSQuery DatumGetTSQueryCopy(Datum X)
```

## Detailed Description
DatumGetTSQueryCopy is an inline utility function that converts a PostgreSQL Datum to a TSQuery pointer while ensuring a writable copy is created. Although TSQuery types are marked as plain storage and cannot be toasted, this function uses PG_DETOAST_DATUM_COPY for API consistency with other similar conversion functions, as noted in the code comments.

This function is used when the calling function needs to modify the TSQuery data structure. Even though TSQuery data is not actually compressed or stored externally, using the copy variant ensures that a modifiable copy is always returned, preventing potential memory corruption issues.

## Parameters / Member Variables
- `X`: Input Datum value that represents a TSQuery to be converted to a modifiable copy

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM_COPY (macro used for API consistency, though TSQuery cannot be toasted)
  - TSQuery (type casting)
- Called from (representative examples):
  - PG_GETARG_TSQUERY_COPY

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Uses PG_DETOAST_DATUM_COPY for API consistency, even though TSQuery cannot be toasted
- Creates a modifiable copy unlike DatumGetTSQuery which may return a read-only reference
- Essential when functions need to modify TSQuery data in-place
- Part of the fmgr (function manager) interface functions for text search operations
- TSQuery types are stored as plain storage and cannot be compressed or stored out-of-line
- Used primarily through the PG_GETARG_TSQUERY_COPY macro interface