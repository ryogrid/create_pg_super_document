# processExtensionTables

## Location
[src/bin/pg_dump/pg_dump.c:18364-18544](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L18364-L18544)

## Overview
Handles extension configuration tables by identifying them for data dumping and managing foreign key dependencies between configuration tables to ensure proper restoration order.

## Definition
```c
void processExtensionTables(Archive *fout, ExtensionInfo extinfo[], int numExtensions)
```

## Detailed Description
This function performs two critical tasks for extension configuration tables:

1. **Identifies and creates dump records for extension configuration tables**: Extension configuration tables are user-modifiable tables whose structure is managed by CREATE EXTENSION but whose data needs to be preserved during dumps. The function creates TableDataInfo objects for these tables to ensure their data is dumped even when the table structure itself is not.

2. **Records foreign key dependencies between configuration tables**: Since foreign keys are created at CREATE EXTENSION time (before data loading), the function determines the optimal restoration order to avoid FK violations. It queries pg_constraint to find FK relationships and registers dependencies between TableDataInfo objects.

The function handles extension include/exclude lists, table-specific include/exclude lists, and schema-level exclusions. It also processes extension condition arrays that can filter which rows are dumped from configuration tables.

## Parameters / Member Variables
- `fout`: Archive context for the dump operation containing dump options
- `extinfo[]`: Array of ExtensionInfo structures containing extension metadata including configuration tables
- `numExtensions`: Number of extensions in the extinfo array

## Dependencies
- Functions called/Symbols referenced:
  - [simple_oid_list_member](../s/simple_oid_list_member.md)
  - [parsePGArray](parsePGArray.md)
  - atooid
  - [findTableByOid](../f/findTableByOid.md)
  - [makeTableDataInfo](../m/makeTableDataInfo.md)
  - [pg_strdup](pg_strdup.md)
  - createPQExpBuffer
  - [printfPQExpBuffer](printfPQExpBuffer.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQfnumber](../P/PQfnumber.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [addObjectDependency](../a/addObjectDependency.md)
  - [PQclear](../P/PQclear.md)
  - destroyPQExpBuffer
  - [pg_fatal](pg_fatal.md)
- Called from:
  - [getSchemaData](../g/getSchemaData.md) (in src/bin/pg_dump/common.c:223)

## Notes and Other Information
- Configuration table data is treated as schema data, so TableDataInfo objects are created even in schema-only mode
- The function cannot handle circular FK dependencies and will produce invalid dumps in such cases (documented limitation)
- Extension configuration and condition arrays must have matching lengths
- FK dependency management ensures data can be restored without constraint violations
- Supports complex filtering via extension include/exclude lists and table/schema-specific exclusions
- Extension condition strings can be used to filter specific rows from configuration tables during dump