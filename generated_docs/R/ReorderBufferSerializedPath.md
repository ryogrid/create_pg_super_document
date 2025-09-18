# ReorderBufferSerializedPath

## Location
src/backend/replication/logical/reorderbuffer.c: 4767 - 4783

## Overview
Generates the file path for a replication slot's serialized (spilled) transaction data file based on the slot name, transaction ID, and segment number.

## Definition


## Detailed Description
This function constructs the filesystem path for spill files used by PostgreSQL's logical replication system. When a transaction in the reorder buffer becomes too large to keep in memory, its changes are serialized ("spilled") to disk. This function generates the standardized path format for these spill files, incorporating the replication slot name, transaction ID, and the WAL segment number converted to LSN format. The resulting path follows the pattern: 

## Parameters / Member Variables
- : Caller-owned buffer of at least MAXPGPATH size to store the generated file path
- : ReplicationSlot pointer (though the function actually uses MyReplicationSlot global)
- : Transaction ID for which the spill file path is being generated
- : WAL segment number that gets converted to LSN format for the filename

## Dependencies
- Functions called/Symbols referenced:
  - XLogSegNoOffsetToRecPtr (converts segment number to LSN)
  - snprintf (formats the path string)
  - NameStr (extracts slot name)
  - LSN_FORMAT_ARGS (formats LSN for filename)
- Called from (representative examples):
  - ReorderBufferSerializeTXN
  - ReorderBufferRestoreChanges
  - ReorderBufferRestoreCleanup

## Notes and Other Information
- The function is static, indicating it's only used within the reorderbuffer.c file
- Despite taking a ReplicationSlot parameter, it actually uses the global MyReplicationSlot variable
- The segment number is converted to LSN at offset 0 within that segment for consistent filename generation
- Spill files are stored in the pg_replslot directory structure under the specific slot's subdirectory
- This path generation is critical for the logical replication system's ability to handle large transactions that exceed memory limits