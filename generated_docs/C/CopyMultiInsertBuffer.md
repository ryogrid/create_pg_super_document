# CopyMultiInsertBuffer

## Location
src/backend/commands/copyfrom.c: 75 - 84

## Overview
CopyMultiInsertBuffer is a structure that stores multi-insert data related to a single relation during COPY FROM operations, providing buffering capabilities to optimize bulk insertions by batching multiple tuples before performing the actual insertion.

## Definition


## Detailed Description
CopyMultiInsertBuffer serves as a buffering mechanism for COPY FROM operations in PostgreSQL. It maintains an array of tuple slots that can hold up to MAX_BUFFERED_TUPLES (1000) tuples before requiring a flush operation. This buffering approach significantly improves performance during bulk data loading by reducing the number of individual insert operations and allowing the system to process multiple tuples in batches.

The structure is designed to work with both regular tables (using BulkInsertState for optimization) and foreign tables. It tracks the line numbers from the original copy stream to provide accurate error reporting and maintains a count of currently buffered tuples.

## Parameters / Member Variables
- : Array of TupleTableSlot pointers that store the actual tuple data waiting to be inserted
- : Pointer to ResultRelInfo structure containing metadata about the target relation
- : BulkInsertState for optimizing insertions into regular tables; set to NULL for foreign tables
- : Counter tracking the number of slots currently containing valid tuples
- : Array storing the line numbers from the copy stream corresponding to each buffered tuple for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - MAX_BUFFERED_TUPLES
  - TupleTableSlot
  - [ResultRelInfo](../R/ResultRelInfo.md)
  - [BulkInsertState](../B/BulkInsertState.md)
- Called from (representative examples):
  - [CopyMultiInsertBufferInit](CopyMultiInsertBufferInit.md)
  - [CopyMultiInsertBufferFlush](CopyMultiInsertBufferFlush.md)
  - [CopyMultiInsertBufferCleanup](CopyMultiInsertBufferCleanup.md)
  - [CopyMultiInsertInfoSetupBuffer](CopyMultiInsertInfoSetupBuffer.md)
  - [CopyMultiInsertInfoStore](CopyMultiInsertInfoStore.md)

## Notes and Other Information
- MAX_BUFFERED_TUPLES is deliberately limited to 1000 to prevent quadratic memory growth when copying into partitioned tables with many partitions
- The structure is specifically designed for COPY FROM operations and provides significant performance improvements through batching
- Line number tracking enables accurate error reporting even when tuples are processed in batches
- The bistate member is only used for regular tables and remains NULL for foreign tables, as foreign tables don't benefit from BulkInsertState optimizations