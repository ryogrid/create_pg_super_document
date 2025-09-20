# TableDataInfo

## Location
[src/bin/pg_dump/pg_dump.h:395-400](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L395-L400)

## Overview
TableDataInfo is a structure used by pg_dump to represent table data that needs to be dumped, providing metadata about the table and optional filtering conditions.

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
TableDataInfo is a fundamental data structure in PostgreSQL's pg_dump utility that represents table data objects during the database dumping process. It extends the base DumpableObject structure to include specific information about table data dumps. This structure serves as a container that links to the actual table metadata (TableInfo) and optionally stores filtering conditions to limit which rows should be included in the dump.

The structure is designed to separate the concept of table schema information from table data dumping, allowing pg_dump to handle data and schema dumps independently. This separation is crucial for scenarios where users want to dump only the schema or only the data, or when applying selective data filters during the dump process.

## Parameters / Member Variables
- `dobj`: Base DumpableObject containing common metadata like catalog ID, dump ID, name, namespace, dependencies, and dump components
- `tdtable`: Pointer to the TableInfo structure that contains the complete metadata about the table whose data is being dumped
- `filtercond`: Optional WHERE clause string that specifies conditions to filter which table rows should be included in the dump

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (inherited base structure)
  - [TableInfo](TableInfo.md) (referenced via tdtable pointer)
- Called from (representative examples):
  - [makeTableDataInfo](../m/makeTableDataInfo.md) (creates TableDataInfo instances)
  - [dumpTableData](../d/dumpTableData.md) (processes table data dumps)
  - [dumpTableData_copy](../d/dumpTableData_copy.md) (COPY-based data dumping)
  - [dumpTableData_insert](../d/dumpTableData_insert.md) (INSERT-based data dumping)
  - [dumpDumpableObject](../d/dumpDumpableObject.md) (generic dump processing)

## Notes and Other Information
- [TableDataInfo](TableDataInfo.md) objects are created by makeTableDataInfo() when pg_dump determines that table data should be included in the dump
- The filtercond member enables selective data dumping based on user-specified WHERE conditions
- This structure is part of pg_dump's internal object model and is not exposed to end users directly
- The separation of TableDataInfo from TableInfo allows for flexible dump strategies where schema and data can be handled independently