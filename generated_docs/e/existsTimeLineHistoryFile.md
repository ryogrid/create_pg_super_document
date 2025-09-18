# existsTimeLineHistoryFile

## Location
src/bin/pg_basebackup/receivelog.c: 258 - 274

## Overview
Checks whether a timeline history file exists for the current timeline in PostgreSQL WAL streaming operations.

## Definition
```c
static bool existsTimeLineHistoryFile(StreamCtl *stream)
```

## Detailed Description
This function determines if a timeline history file exists for the current timeline being processed during WAL streaming. Timeline history files track the lineage of database timelines when they branch (such as during point-in-time recovery or failover scenarios). Timeline 1 is treated as a special case since it never has a history file by definition, and the function returns true for timeline 1 to indicate that no streaming of a history file is needed.

## Parameters / Member Variables
- `stream`: Pointer to StreamCtl structure containing the current timeline ID and walmethod operations for file system access

## Dependencies
- Functions called/Symbols referenced:
  - [StreamCtl](../S/StreamCtl.md) (structure)
  - MAXFNAMELEN (constant)
  - TLHistoryFileName
  - walmethod->ops->existsfile
- Called from (representative examples):
  - [ReceiveXlogStream](../R/ReceiveXlogStream.md)

## Notes and Other Information
- Timeline 1 always returns true since it never has a history file by PostgreSQL convention
- Uses TLHistoryFileName to construct the standardized timeline history filename
- Leverages walmethod operations for file existence checking, making it compatible with different storage methods (files, tar archives)
- Part of the timeline management system in pg_basebackup for handling database timeline branches
- History files are essential for understanding timeline succession during recovery scenarios