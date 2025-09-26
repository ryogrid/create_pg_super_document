# GetForeignColumnOptions

## Location
[src/backend/foreign/foreign.c:292-324](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/foreign/foreign.c#L292-L324)

## Overview
Retrieves foreign data wrapper options for a specific column of a foreign table as a list of DefElem structures.

## Definition
```c
List *GetForeignColumnOptions(Oid relid, AttrNumber attnum)
```

## Detailed Description
GetForeignColumnOptions is a specialized function that extracts column-level foreign data wrapper options from PostgreSQL's system catalogs. It queries the pg_attribute system catalog to retrieve the attfdwoptions field for a specific column of a foreign table. These options allow fine-grained control over how individual columns are handled by foreign data wrappers, such as specifying remote column names, data type mappings, or other column-specific parameters. The function returns the options as a list of DefElem structures after untransforming them from their stored format.

## Parameters / Member Variables
- `relid`: Object ID of the relation (foreign table) containing the column
- `attnum`: Attribute number (column number) within the relation

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - [Int16GetDatum](../I/Int16GetDatum.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [untransformRelOptions](../u/untransformRelOptions.md)
  - elog
  - HeapTupleIsValid
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - Foreign data wrapper column analysis routines
  - [Query](../Q/Query.md) planning functions for foreign tables
  - Column metadata inspection utilities

## Notes and Other Information
The function uses the ATTNUM system cache to efficiently look up attribute information. Column-level options provide flexibility for foreign data wrappers to handle heterogeneous remote schemas or apply column-specific transformations. If no options are defined for the column (isnull is true), the function returns NIL (empty list). The function is located in src/backend/foreign/foreign.c:292-324 and is essential for fine-grained foreign table column management.