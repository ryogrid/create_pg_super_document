# sqlda_native_total_size

## Location
[src/interfaces/ecpg/ecpglib/sqlda.c:186-204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/sqlda.c#L186-L204)

## Overview
Calculates the total memory size required for a native SQLDA structure including space for data values from a specific row in a PostgreSQL result set.

## Definition

```c
static long
sqlda_native_total_size(const PGresult *res, int row, enum COMPAT_MODE compat)
```
## Detailed Description
This function computes the complete memory footprint needed to allocate a native SQLDA (SQL Descriptor Area) structure that can hold both the metadata and actual data values from a specific row of a PostgreSQL query result. It first calculates the base size for the empty SQLDA structure and then adds the space required for the actual field values. If a negative row number is provided, it returns only the empty structure size without data space.

The function is part of PostgreSQL's ECPG (Embedded SQL in C) interface, specifically handling the native SQLDA format which provides a standardized way to access query results with dynamic column information.

## Parameters / Member Variables
- `*res`: Pointer to a PGresult structure containing the query results
- `row`: The row number for which to calculate data size (negative values return empty structure size only)
- `compat`: Compatibility mode enumeration that affects how data types are interpreted
## Dependencies
- Functions called/Symbols referenced:
  - [sqlda_native_empty_size](sqlda_native_empty_size.md)
  - [sqlda_common_total_size](sqlda_common_total_size.md)
  - COMPAT_MODE
- Called from (representative examples):
  - [ecpg_build_native_sqlda](../e/ecpg_build_native_sqlda.md)

## Notes and Other Information
- This is a static function internal to the SQLDA implementation
- Returns a long value representing the total bytes needed for memory allocation
- The function handles the case where only structure size is needed (when row < 0)
- Part of the ECPG library for embedded SQL functionality in PostgreSQL client applications