# _tableAttachInfo

## Location
src/bin/pg_dump/pg_dump.h: 381 - 385

## Overview
The  structure represents metadata for partition attach operations in pg_dump, specifically used to track the relationship between a partition and its parent partitioned table.

## Definition


## Detailed Description
The  structure is a specialized data structure used by pg_dump to manage partition attachment information during the dump and restore process. It maintains the relationship between a partition table and its parent partitioned table, ensuring that partition hierarchies are properly reconstructed during database restoration.

## Parameters / Member Variables
- : Base dumpable object information containing metadata such as object ID, name, namespace, and dump ordering information
- : Pointer to the TableInfo structure representing the parent partitioned table to which this partition should be attached

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - [TableInfo](../T/TableInfo.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
This structure is part of pg_dump's partition management system and is essential for maintaining table inheritance hierarchies during database dump and restore operations. The structure is relatively simple but critical for ensuring that partitioned tables are properly reconstructed with their correct parent-child relationships. The  pointer establishes the connection that allows pg_dump to generate the appropriate  commands during restoration.