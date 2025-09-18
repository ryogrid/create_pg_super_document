# GetTempToastNamespace

## Location
[src/backend/catalog/namespace.c:3791-3804](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L3791-L3804)

## Overview
This function returns the OID of the current backend's temporary toast table namespace, which must already be assigned before calling this function.

## Definition
```c
Oid GetTempToastNamespace(void)
```

## Detailed Description
The function provides access to the current backend's temporary toast table namespace. It is specifically designed to be used when creating toast tables for temporary tables, which requires that the temporary table namespace infrastructure has already been initialized through `InitTempTableNamespace`.

The function performs an assertion to ensure that `myTempToastNamespace` is a valid OID before returning it. This guarantees that the temporary toast namespace has been properly set up, which is a prerequisite for creating toast tables for temporary relations.

Toast tables are used by PostgreSQL to store large attribute values that exceed the page size limit. When a temporary table requires a toast table, it must be created in the temporary toast namespace to maintain the temporary nature and proper isolation from other backends.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - OidIsValid (macro for OID validation)
  - myTempToastNamespace (global variable)

- Called from (representative examples):
  - [create_toast_table](../c/create_toast_table.md)
  - RangeVarGetRelid

## Notes and Other Information
- The function asserts that `myTempToastNamespace` is valid, meaning it will abort if called before proper initialization
- Must be called only after `InitTempTableNamespace` has been executed
- Specifically used during the creation of toast tables for temporary tables
- The temporary toast namespace follows the naming pattern "pg_toast_temp_[procnumber]"
- Ensures that toast tables for temporary relations are properly isolated within the backend's temporary namespace
- Returns the OID directly without any additional validation beyond the assertion