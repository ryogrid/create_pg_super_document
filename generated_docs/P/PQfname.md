# PQfname

## Location
[src/interfaces/libpq/fe-exec.c:3567-3588](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3567-L3588)

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
  - [check_field_number](../c/check_field_number.md) (for parameter validation)
- Called from (representative examples):
  - [libpqrcv_processTuples](../l/libpqrcv_processTuples.md)
  - [dumpTableData_insert](../d/dumpTableData_insert.md)
  - [readCommandResponse](../r/readCommandResponse.md)
  - [StoreQueryTuple](../S/StoreQueryTuple.md)
  - [DescribeQuery](../D/DescribeQuery.md)
  - [printCrosstab](../p/printCrosstab.md)
  - [printQuery](../p/printQuery.md)
  - [ECPGget_desc](../E/ECPGget_desc.md)
  - [ecpg_build_compat_sqlda](../e/ecpg_build_compat_sqlda.md)
  - [PQprintTuples](PQprintTuples.md)

## Notes and Other Information
- Returns a pointer to the column name string, or NULL if the field number is invalid
- Returns NULL if res is NULL or if res->attDescs is NULL
- The returned string is owned by the PGresult structure and should not be freed by the caller
- Uses 0-based indexing for field numbers
- Part of the public libpq API (declared in libpq-fe.h)
- Widely used across PostgreSQL tools and applications for result set introspection
- Column names are stored in the attDescs array within the PGresult structure

## Simplified Source

```c
char *PQfname(const PGresult *res, int field_num) {
    // Validate field number is in range
    if (!check_field_number(res, field_num))
        return NULL;

    // Return column name if attribute descriptors exist
    if (res->attDescs)
        return res->attDescs[field_num].name;
    else
        return NULL;
}
```