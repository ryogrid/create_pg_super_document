# PQfmod

## Location
src/interfaces/libpq/fe-exec.c: 3741 - 3751

## Overview
PQfmod retrieves the type modifier value for a specified field in a PostgreSQL query result, providing information about column-specific formatting or constraints.

## Definition


## Detailed Description
PQfmod extracts the type modifier (atttypmod) for a given field from a PostgreSQL result set. Type modifiers provide additional information about how a column's data type should be interpreted, such as precision for numeric types, length limits for character types, or other type-specific constraints. The function performs bounds checking on the field number and returns 0 if the field is invalid or if no attribute descriptors are available.

## Parameters / Member Variables
- : Pointer to a PGresult structure containing query results
- : Zero-based index of the field/column for which to retrieve the type modifier

## Dependencies
- Functions called/Symbols referenced:
  - check_field_number
- Called from (representative examples):
  - DescribeQuery (src/bin/psql/common.c:1387)
  - ECPGget_desc (src/interfaces/ecpg/ecpglib/descriptor.c:350, 356, 360, 366, 380, 386)

## Notes and Other Information
- Returns 0 for invalid field numbers or when attribute descriptors are not available
- Type modifier values are type-specific and their interpretation depends on the underlying PostgreSQL data type
- Part of the libpq client interface for PostgreSQL database connectivity
- The function relies on the res->attDescs array being properly initialized with attribute descriptor information