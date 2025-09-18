# makeTableDataInfo

## Location
src/bin/pg_dump/pg_dump.c: 2825 - 2898

## Overview
Creates a dumpable object for the data of a specific table, determining whether table data should be included in the pg_dump output.

## Definition


## Detailed Description
This function creates a TableDataInfo object that represents the data content of a table for pg_dump operations. It performs various checks to determine if the table's data should be dumped, including filtering out views, foreign tables, partitioned tables, and unlogged tables based on dump options. The function assigns appropriate object types based on the relation kind (materialized view, sequence, or regular table) and establishes dependencies between the data object and its parent table.

## Parameters / Member Variables
- : Pointer to DumpOptions structure containing dump configuration settings
- : Pointer to TableInfo structure representing the table whose data is being processed

## Dependencies
- Functions called/Symbols referenced:
  - simple_oid_list_member
  - pg_malloc
  - AssignDumpId
  - addObjectDependency
- Types referenced:
  - DumpOptions
  - TableInfo
  - TableDataInfo
  - RELKIND_VIEW
  - RELKIND_FOREIGN_TABLE
  - RELKIND_PARTITIONED_TABLE
  - RELKIND_MATVIEW
  - RELKIND_SEQUENCE
  - DO_REFRESH_MATVIEW
  - DO_SEQUENCE_SET
  - DO_TABLE_DATA
  - DUMP_COMPONENT_DATA
- Called from:
  - getTableData
  - processExtensionTables

## Notes and Other Information
- Only creates TableDataInfo objects when table data will actually be dumped
- Skips views, foreign tables (unless explicitly included), and partitioned tables
- Handles special cases for materialized views and sequences with appropriate object types
- Sets up dependency relationships to ensure proper dump ordering
- Marks the parent table as 'interesting' to ensure column information is collected
- Uses tableoid 0 to prevent confusion with pg_depend entries