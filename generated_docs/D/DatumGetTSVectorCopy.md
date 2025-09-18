# DatumGetTSVectorCopy

## Location
[src/include/tsearch/ts_type.h:124-129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/ts_type.h#L124-L129)

## Overview
Converts a PostgreSQL Datum value to a TSVector pointer, creating a modifiable copy through detoasting if the data is compressed or stored externally.

## Definition
```c
static inline TSVector DatumGetTSVectorCopy(Datum X)
```

## Detailed Description
DatumGetTSVectorCopy is an inline utility function that converts a PostgreSQL Datum to a TSVector pointer while ensuring a writable copy is created. Unlike DatumGetTSVector, this function uses PG_DETOAST_DATUM_COPY, which guarantees that the returned TSVector is a modifiable copy, not just a reference to the original data.

This is crucial when the calling function needs to modify the TSVector data, as the original datum may be stored in read-only memory, compressed, or stored out-of-line via PostgreSQL's TOAST mechanism. The copy operation ensures data integrity and prevents memory corruption.

## Parameters / Member Variables
- `X`: Input Datum value that represents a TSVector to be converted to a modifiable copy

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM_COPY (macro for creating modifiable copies of detoasted data)
  - TSVector (type casting)
- Called from (representative examples):
  - PG_GETARG_TSVECTOR_COPY

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Creates a modifiable copy unlike DatumGetTSVector which may return a read-only reference
- Essential when functions need to modify TSVector data in-place
- More expensive than DatumGetTSVector due to the copy operation, so should only be used when modification is necessary
- Part of the fmgr (function manager) interface functions for text search operations