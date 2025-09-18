# xl_heap_truncate

## Location
src/include/access/heapam_xlog.h: 133 - 139

## Overview
The xl_heap_truncate struct represents the WAL record data for TRUNCATE operations on heap tables in PostgreSQL's recovery and replication system.

## Definition


## Detailed Description
This structure records information needed to replay TRUNCATE operations during crash recovery or replication. It supports truncating multiple relations in a single operation and can handle both regular tables and sequences that need to be restarted. The flexible array member allows for variable numbers of relation OIDs to be stored in a single WAL record, making it efficient for bulk truncate operations.

## Parameters / Member Variables
- : The database OID where the truncated relations reside (all relations must be in the same database)
- : The total number of relation OIDs stored in the relids array
- : Control flags that specify additional behavior for the truncate operation
- : A flexible array containing the OIDs of all relations being truncated, followed by sequence OIDs that need restarting

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a data structure)
- Called from (representative examples):
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md) (creates WAL records for TRUNCATE commands)
  - [heap_desc](../h/heap_desc.md) (describes truncate WAL records for debugging)
  - [DecodeTruncate](../D/DecodeTruncate.md) (logical replication decoding of truncate operations)

## Notes and Other Information
- Uses FLEXIBLE_ARRAY_MEMBER to store variable numbers of relation OIDs efficiently
- The SizeOfHeapTruncate macro calculates the actual size including the variable-length relids array
- Supports atomic truncation of multiple tables in a single WAL record
- Handles both regular table truncation and sequence restart operations
- All relations in a single truncate operation must be within the same database
- Critical for maintaining data consistency during crash recovery and replication