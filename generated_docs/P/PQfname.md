# PQfname

## Location
src/interfaces/libpq/fe-exec.c: 3567 - 3588

## Overview
A public libpq function that retrieves the name of a specified field (column) from a query result set.

## Definition
```c
char *PQfname(const PGresult *res, int field_num)
```

## Detailed Description
PQfname returns the column name associated with the given field number in a query result. This function is part of libpq's public API for inspecting result set metadata. It first validates the field number using check_field_number, then accesses the attribute descriptor array to retrieve the column name. The function is commonly used by applications that need to programmatically discover column names in query results, such as generic database tools, ORM libraries, and dynamic query processors.

## Parameters / Member Variables
- `res`: Pointer to a PGresult structure containing the query result data
- `field_num`: The field (column) number for which to retrieve the name, expected to be 0-based

## Dependencies
- Functions called/Symbols referenced:
  - check_field_number (for parameter validation)
- Called from (representative examples):
  - libpqrcv_processTuples
  - dumpTableData_insert
  - readCommandResponse
  - StoreQueryTuple
  - DescribeQuery
  - printCrosstab
  - printQuery
  - ECPGget_desc
  - ecpg_build_compat_sqlda
  - PQprintTuples

## Notes and Other Information
- Returns a pointer to the column name string, or NULL if the field number is invalid
- Returns NULL if res is NULL or if res->attDescs is NULL
- The returned string is owned by the PGresult structure and should not be freed by the caller
- Uses 0-based indexing for field numbers
- Part of the public libpq API (declared in libpq-fe.h)
- Widely used across PostgreSQL tools and applications for result set introspection
- Column names are stored in the attDescs array within the PGresult structure