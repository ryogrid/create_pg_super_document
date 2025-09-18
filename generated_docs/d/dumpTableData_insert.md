# dumpTableData_insert

## Location
src/bin/pg_dump/pg_dump.c: 2334 - 2602

## Overview
Dumps table data using INSERT statements, providing an alternative to COPY that is more portable and suitable for cross-database exports and smaller data sets.

## Definition


## Detailed Description
This function generates INSERT statements to dump table data, offering more portability than COPY commands. It handles various PostgreSQL data types with appropriate formatting, supports generated columns, and can produce either column-named or positional INSERTs. The function uses a cursor-based approach to fetch data in chunks and supports multi-row INSERT statements for efficiency.

Key features include special handling for generated columns (either excluded or replaced with DEFAULT), proper formatting for different data types (numeric, boolean, bit strings, etc.), support for partition tables with optional root table loading, and conflict resolution with ON CONFLICT DO NOTHING option.

## Parameters / Member Variables
- : Pointer to the Archive structure containing dump configuration and output context
- : Void pointer that contains TableDataInfo structure cast as context data

## Dependencies
- Functions called/Symbols referenced:
  - [TableDataInfo](../T/TableDataInfo.md) (struct)
  - [TableInfo](../T/TableInfo.md) (struct)
  - DumpOptions (struct)
  - RELKIND_FOREIGN_TABLE
  - [set_restrict_relation_kind](../s/set_restrict_relation_kind.md)
  - pg_malloc
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - PGRES_TUPLES_OK
  - [PQnfields](../P/PQnfields.md)
  - [forcePartitionRootLoad](../f/forcePartitionRootLoad.md)
  - [getRootTableInfo](../g/getRootTableInfo.md)
  - appendPQExpBufferChar
  - [PQfname](../P/PQfname.md)
  - [archputs](../a/archputs.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - PQftype
  - [archprintf](../a/archprintf.md)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - appendStringLiteralAH
- Called from (representative examples):
  - [dumpTableData](dumpTableData.md)

## Notes and Other Information
- Generates INSERT statements that are compatible with pg_backup_db.c's ExecuteSimpleCommands()
- Avoids comments, E'' strings, and dollar-quoted strings for compatibility
- Handles generated columns by excluding them from column lists or using DEFAULT values
- Uses cursor-based fetching (FETCH 100) to process large tables efficiently
- Supports --rows-per-insert option for multi-row INSERT statements
- Provides special formatting for numeric types, bit strings, and boolean values
- Can target partition root tables when load-via-partition-root is enabled
- Includes ON CONFLICT DO NOTHING option for conflict resolution
- Handles zero-column tables with DEFAULT VALUES syntax
- Returns 1 on success, with comprehensive error handling throughout