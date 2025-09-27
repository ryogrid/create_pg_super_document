# CheckPointSUBTRANS

## Location
[src/backend/access/transam/subtrans.c:355-378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/subtrans.c#L355-L378)

## Overview
Performs a checkpoint operation for the SUBTRANS system by writing all dirty subtransaction status pages to disk during either shutdown or on-the-fly checkpointing.

## Definition

```c
void
CheckPointSUBTRANS(void)
```
## Detailed Description
CheckPointSUBTRANS is responsible for ensuring that all modified SUBTRANS pages are written to disk during checkpoint operations. While this is not strictly necessary for correctness (as subtransaction status can be reconstructed from WAL during recovery), it improves performance by having the checkpoint process handle the disk writes rather than leaving them for backend processes.

The function uses SimpleLruWriteAll to write all dirty pages in the SUBTRANS buffer pool to disk. The operation is instrumented with PostgreSQL tracing points to allow monitoring of checkpoint performance.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [SimpleLruWriteAll](../S/SimpleLruWriteAll.md)
  - SubTransCtl
  - TRACE_POSTGRESQL_SUBTRANS_CHECKPOINT_START
  - TRACE_POSTGRESQL_SUBTRANS_CHECKPOINT_DONE
- Called from (representative examples):
  - [CheckPointGuts](CheckPointGuts.md) (main checkpoint processing)

## Notes and Other Information
- Not required for correctness - subtransaction status can be recovered from WAL
- Improves performance by reducing backend I/O during normal operations
- Part of the overall checkpoint process coordinated by CheckPointGuts
- Uses tracing points for performance monitoring and debugging
- Forces all dirty SUBTRANS pages to disk regardless of their individual flush requirements

## Simplified Source

```c
// Simplified version of CheckPointSUBTRANS
void CheckPointSUBTRANS(void) {
    // Start performance tracing
    TRACE_POSTGRESQL_SUBTRANS_CHECKPOINT_START(true);

    // Write all dirty SUBTRANS pages to disk
    // Note: This is a performance optimization, not required for correctness
    // Subtransaction status can be recovered from WAL if needed
    SimpleLruWriteAll(SubTransCtl, true);

    // End performance tracing
    TRACE_POSTGRESQL_SUBTRANS_CHECKPOINT_DONE(true);
}
```

Key simplifications made:
- Preserved the essential write operation for SUBTRANS pages
- Kept performance tracing calls for monitoring
- Added comments explaining that this is an optimization, not a correctness requirement
- Maintained the checkpoint flag parameter (true) to SimpleLruWriteAll
- Simplified to show the function's core purpose: proactive page writing for performance