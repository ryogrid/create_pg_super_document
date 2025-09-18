# CheckPointSUBTRANS

## Location
src/backend/access/transam/subtrans.c: 355 - 378

## Overview
Performs a checkpoint operation for the SUBTRANS system by writing all dirty subtransaction status pages to disk during either shutdown or on-the-fly checkpointing.

## Definition


## Detailed Description
CheckPointSUBTRANS is responsible for ensuring that all modified SUBTRANS pages are written to disk during checkpoint operations. While this is not strictly necessary for correctness (as subtransaction status can be reconstructed from WAL during recovery), it improves performance by having the checkpoint process handle the disk writes rather than leaving them for backend processes.

The function uses SimpleLruWriteAll to write all dirty pages in the SUBTRANS buffer pool to disk. The operation is instrumented with PostgreSQL tracing points to allow monitoring of checkpoint performance.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - SimpleLruWriteAll
  - SubTransCtl
  - TRACE_POSTGRESQL_SUBTRANS_CHECKPOINT_START
  - TRACE_POSTGRESQL_SUBTRANS_CHECKPOINT_DONE
- Called from (representative examples):
  - CheckPointGuts (main checkpoint processing)

## Notes and Other Information
- Not required for correctness - subtransaction status can be recovered from WAL
- Improves performance by reducing backend I/O during normal operations
- Part of the overall checkpoint process coordinated by CheckPointGuts
- Uses tracing points for performance monitoring and debugging
- Forces all dirty SUBTRANS pages to disk regardless of their individual flush requirements