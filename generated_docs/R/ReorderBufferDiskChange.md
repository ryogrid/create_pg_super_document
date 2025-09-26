# ReorderBufferDiskChange

## Location
src/backend/replication/logical/reorderbuffer.c: 189 - 194

## Overview
ReorderBufferDiskChange is a structure used for disk serialization of reorder buffer changes during logical replication in PostgreSQL.

## Definition


## Detailed Description
This structure serves as a disk serialization wrapper for ReorderBufferChange objects when the reorder buffer needs to spill changes to disk due to memory pressure. When logical replication processes large transactions that exceed available memory, PostgreSQL serializes the changes to temporary files on disk. ReorderBufferDiskChange provides the necessary metadata and structure to properly serialize and deserialize these changes, maintaining the ability to reconstruct the original ReorderBufferChange objects when they are read back from disk.

## Parameters / Member Variables
- : The total size in bytes of the serialized change data, including both the ReorderBufferChange structure and any associated variable-length data
- : The embedded ReorderBufferChange structure containing the actual change information
- : Comment indicating that variable-length data associated with the change follows immediately after this structure in memory/disk

## Dependencies
- Functions called/Symbols referenced:
  - ReorderBufferChange
- Called from (representative examples):
  - ReorderBufferSerializeChange
  - ReorderBufferRestoreChanges
  - ReorderBufferRestoreChange

## Notes and Other Information
- Critical component of PostgreSQL's logical replication memory management system
- Enables processing of arbitrarily large transactions by allowing changes to be temporarily stored on disk
- The serialization format must be platform-independent to ensure compatibility across different systems
- Used when logical_decoding_work_mem is exceeded during transaction processing
- The structure layout is designed for efficient disk I/O operations during serialization and deserialization
- Variable-length data (such as tuple content) follows the fixed-size structure in the serialized format