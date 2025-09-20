# _tableDataInfo

## Location
[src/bin/pg_dump/pg_dump.h:397-401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L397-L401)

## Overview
The  structure represents the actual data content of a table that needs to be dumped by pg_dump, serving as a separate dumpable object for table data.

## Definition

```c
typedef struct _tableDataInfo
{
	DumpableObject dobj;
	TableInfo  *tdtable;		/* link to table to dump */
	char	   *filtercond;		/* WHERE condition to limit rows dumped */
} TableDataInfo;
```
## Detailed Description
The  structure is a specialized container used by pg_dump to represent table data as a distinct dumpable object separate from the table's schema definition. This separation allows pg_dump to handle table schema and table data independently, enabling flexible dump ordering and selective data dumping. The structure maintains a link to the corresponding table's metadata while treating the data itself as a separate entity in the dump process.

## Parameters / Member Variables
- `dobj`: Base dumpable object information containing metadata such as object ID, name, namespace, and dump ordering information for the table data
- `tdtable`: Pointer to the TableInfo structure representing the table whose data this object represents, establishing the connection between the data object and its corresponding table schema
- `filtercond`: WHERE condition to limit rows dumped (NULL if no filtering applied)

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - [TableInfo](../T/TableInfo.md)
- Called from (representative examples):
  - [_tableInfo](_tableInfo.md) (referenced in the dataObj field)

## Notes and Other Information
This structure is crucial for pg_dump's modular approach to database dumping, where table schemas and table data are treated as separate dumpable entities. This design allows for sophisticated dump strategies such as schema-only dumps, data-only dumps, or carefully ordered dumps where data is inserted after all schema objects are created. The  pointer ensures that the data dumping process has access to all necessary table metadata, including column information, data types, and constraints that may affect how the data should be formatted and inserted during restoration.