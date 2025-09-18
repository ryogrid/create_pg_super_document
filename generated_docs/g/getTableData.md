# getTableData

## Location
[src/bin/pg_dump/pg_dump.c:2806-2824](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L2806-L2824)

## Overview
Sets up dumpable objects representing the contents of tables by iterating through a table array and creating TableDataInfo structures for eligible tables.

## Definition
```c
static void getTableData(DumpOptions *dopt, TableInfo *tblinfo, int numTables, char relkind)
```

## Detailed Description
This function iterates through an array of TableInfo structures and creates corresponding TableDataInfo objects for tables that meet the dumping criteria. It serves as a high-level coordinator that determines which tables should have their data included in the dump based on dump options and relation types. The function filters tables based on whether they have the DUMP_COMPONENT_DATA flag set and optionally matches against a specific relation kind.

This is a key initialization function in the pg_dump process that bridges the gap between table discovery (getting TableInfo structures) and actual data dumping (creating TableDataInfo structures that will later be processed by functions like dumpTableData).

## Parameters / Member Variables
- `dopt`: DumpOptions structure containing user-specified dump configuration and preferences
- `tblinfo`: Array of TableInfo structures representing discovered tables in the database
- `numTables`: Integer count of tables in the tblinfo array
- `relkind`: Character representing the specific relation kind to process, or 0 to process all kinds

## Dependencies
- Functions called/Symbols referenced:
  - DumpOptions (structure type)
  - [TableInfo](../T/TableInfo.md) (structure type)  
  - [makeTableDataInfo](../m/makeTableDataInfo.md)
  - DUMP_COMPONENT_DATA (flag constant)
- Called from (representative examples):
  - fmtQualifiedDumpable (src/bin/pg_dump/pg_dump.c:292)
  - [main](../m/main.md) (src/bin/pg_dump/pg_dump.c:960)
  - [main](../m/main.md) (src/bin/pg_dump/pg_dump.c:967)

## Notes and Other Information
- This is a static function within pg_dump.c
- Acts as a filter and coordinator for table data dumping preparation
- Called multiple times from main() for different relation kinds
- Essential setup function in the pg_dump workflow before actual data dumping begins
- Works in conjunction with makeTableDataInfo to create dumpable data objects
- Simple but critical function for pg_dump's table data handling pipeline
- Located at src/bin/pg_dump/pg_dump.c:2806-2824