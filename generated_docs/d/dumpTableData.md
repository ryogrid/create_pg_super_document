# dumpTableData

## Location
src/bin/pg_dump/pg_dump.c: 2656 - 2770

## Overview
Creates an archive entry for dumping the contents of a single table, handling both regular and partitioned tables with appropriate copy or insert methods.

## Definition
```c
static void dumpTableData(Archive *fout, const TableDataInfo *tdinfo)
```

## Detailed Description
This function creates an ArchiveEntry for table contents rather than directly dumping data. It handles the complexity of partitioned tables by determining whether data should be loaded via the partition root or directly into the table. The function supports two dump formats: COPY (default) for efficient bulk loading, and INSERT statements for greater compatibility. 

For partitioned tables, it intelligently decides whether to force loading through the root table based on safety considerations (unsafe partitioning schemes) or user preferences (load_via_partition_root option). The function also handles parallel dumping by calculating data length based on table and TOAST pages for job ordering.

## Parameters / Member Variables
- `fout`: Archive structure containing dump options and state information
- `tdinfo`: TableDataInfo structure containing metadata about the table data to be dumped

## Dependencies
- Functions called/Symbols referenced:
  - forcePartitionRootLoad
  - getRootTableInfo  
  - fmtQualifiedDumpable
  - sanitize_line
  - printfPQExpBuffer
  - fmtCopyColumnList
  - dumpTableData_copy
  - dumpTableData_insert
  - ArchiveEntry
  - createPQExpBuffer/destroyPQExpBuffer
- Called from (representative examples):
  - fmtQualifiedDumpable (src/bin/pg_dump/pg_dump.c:206)
  - dumpDumpableObject (src/bin/pg_dump/pg_dump.c:10622)

## Notes and Other Information
- This is a static function within pg_dump.c
- Does not directly dump data but creates archive entries for later processing
- Handles both COPY and INSERT dump formats based on dump_inserts option
- Implements intelligent partition handling with safety checks
- Calculates dataLength for parallel dump job ordering based on table/TOAST pages
- Includes overflow protection for 32-bit systems when calculating data length
- Essential component of PostgreSQL's backup and restore infrastructure
- Located at src/bin/pg_dump/pg_dump.c:2656-2770