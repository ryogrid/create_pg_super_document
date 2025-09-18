# uuid_fast_cmp

## Location
src/backend/utils/adt/uuid.c: 277 - 291

## Overview
Provides an optimized comparison function specifically designed for PostgreSQL's SortSupport infrastructure, enabling high-performance UUID sorting operations.

## Definition
```c
static int uuid_fast_cmp(Datum x, Datum y, SortSupport ssup)
```

## Detailed Description
The `uuid_fast_cmp` function is a static comparison function optimized for use within PostgreSQL's SortSupport framework. Unlike the standard `uuid_cmp` function which follows the PG_FUNCTION_ARGS convention, this function directly accepts Datum values and efficiently extracts UUID pointers using `DatumGetUUIDP`. It serves as a performance-optimized alternative for sorting operations, bypassing the overhead of the standard function call interface while maintaining the same comparison logic through `uuid_internal_cmp`. The function is specifically designed to be called repeatedly during sort operations where performance is critical, such as large ORDER BY clauses, index builds, and sort-merge joins.

## Parameters / Member Variables
- `x` (Datum): First UUID value as a PostgreSQL Datum
- `y` (Datum): Second UUID value as a PostgreSQL Datum  
- `ssup` (SortSupport): Sort support context (unused in this function but required by interface)

## Dependencies
- Functions called/Symbols referenced:
  - `DatumGetUUIDP`: Macro to extract UUID pointer from Datum
  - `uuid_internal_cmp`: Internal function performing the actual UUID comparison
  - `pg_uuid_t`: UUID data type structure
  - `SortSupport`: Sort support framework interface
- Called from (representative examples):
  - `uuid_sortsupport`: Set as the primary comparator function
  - `uuid_sortsupport`: Set as the full comparator when abbreviation is enabled
  - Sort operations requiring high-performance UUID comparison

## Notes and Other Information
- Static function, not directly accessible outside uuid.c
- Optimized for repeated calls during sort operations
- Bypasses PostgreSQL's standard function call overhead
- Returns the same tri-state integer result as `uuid_internal_cmp`
- Essential component of PostgreSQL's high-performance sorting infrastructure for UUIDs
- The `ssup` parameter is part of the interface but not used in the implementation
- Provides significant performance benefits for large-scale UUID sorting operations