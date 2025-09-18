# ReorderBufferSerializeTXN

## Location
src/backend/replication/logical/reorderbuffer.c: 3840 - 3934

## Overview
Spills data of a large transaction (and its subtransactions) to disk when memory limits are exceeded, organizing changes into WAL segment-based files for later retrieval.

## Definition


## Detailed Description
This function handles the serialization of large transactions to disk storage when memory usage exceeds configured limits. It recursively processes all subtransactions first, then serializes the main transaction's changes. Changes are organized into separate files based on their WAL segment numbers to maintain locality and efficient access patterns during deserialization.

The function creates transient files in a directory structure that includes the replication slot name, transaction ID, and WAL segment number. Each change is written to the appropriate segment file using ReorderBufferSerializeChange, and the in-memory change list is cleared to free memory.

Key behaviors include:
- Recursive processing of subtransactions before the main transaction
- WAL segment-based file organization for efficient access
- Atomic memory management updates
- Statistics tracking for spill operations
- Proper cleanup of file descriptors and memory structures

## Parameters / Member Variables
- : ReorderBuffer instance containing global state and configuration
- : ReorderBufferTXN to be serialized, including all its changes and subtransactions

## Dependencies
- Functions called/Symbols referenced:
  - ReorderBufferSerializeChange (serializes individual changes)
  - ReorderBufferSerializedPath (generates file paths)
  - ReorderBufferReturnChange (returns change to free pool)
  - ReorderBufferChangeMemoryUpdate (updates memory accounting)
  - UpdateDecodingStats (updates decoding statistics)
  - OpenTransientFile/CloseTransientFile (file I/O operations)
  - XLByteToSeg/XLByteInSeg (WAL segment utilities)
- Called from (representative examples):
  - ReorderBufferCheckMemoryLimit (when memory limits exceeded)
  - ReorderBufferIterTXNInit (during transaction iteration setup)
  - ReorderBufferSerializeTXN (recursive calls for subtransactions)

## Notes and Other Information
- The function is recursive, processing subtransactions before the main transaction
- Files are organized by WAL segment number for efficient access patterns
- Memory statistics are updated atomically to maintain consistency
- The RBTXN_IS_SERIALIZED flag is set to mark the transaction as spilled
- Spill statistics (count, bytes, transactions) are maintained for monitoring
- File descriptors are properly managed to avoid resource leaks