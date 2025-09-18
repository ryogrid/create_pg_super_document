# DatumGetUUIDP

## Location
src/include/utils/uuid.h: 35 - 39

## Overview
Converts a PostgreSQL Datum back to a pg_uuid_t pointer for UUID data type manipulation.

## Definition
```c
static inline pg_uuid_t *DatumGetUUIDP(Datum X)
```

## Detailed Description
DatumGetUUIDP is an inline function that provides the reverse conversion of UUIDPGetDatum, extracting a pg_uuid_t pointer from a PostgreSQL Datum. This function is essential for UUID data type processing within PostgreSQL's function manager system, allowing functions to access the actual UUID data structure from the generic Datum wrapper.

The function wraps the generic DatumGetPointer() function and casts the result to the appropriate pg_uuid_t pointer type, providing type safety and semantic clarity for UUID-specific operations.

## Parameters / Member Variables
- `X`: A Datum containing a pointer to UUID data that needs to be extracted as a pg_uuid_t pointer

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](DatumGetPointer.md)
  - [pg_uuid_t](../p/pg_uuid_t.md)
- Called from (representative examples):
  - [brin_minmax_multi_distance_uuid](../b/brin_minmax_multi_distance_uuid.md)
  - [uuid_fast_cmp](../u/uuid_fast_cmp.md)
  - [uuid_abbrev_convert](../u/uuid_abbrev_convert.md)
  - PG_GETARG_UUID_P

## Notes and Other Information
- This function is the counterpart to UUIDPGetDatum, completing the bidirectional conversion between Datum and pg_uuid_t pointer
- Extensively used in UUID comparison functions and BRIN index operations
- The function assumes the Datum contains a valid pointer to UUID data; no validation is performed
- Critical for UUID processing in sorting, indexing, and comparison operations