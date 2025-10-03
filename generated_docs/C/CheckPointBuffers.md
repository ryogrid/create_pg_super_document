# CheckPointBuffers

## Location
[src/backend/storage/buffer/bufmgr.c:3699-3712](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L3699-L3712)

## Overview
CheckPointBuffers is a function that flushes all dirty blocks in the buffer pool to disk during checkpoint operations, ensuring data durability at specific recovery points.

## Definition

```c
void
CheckPointBuffers(int flags)
```
## Detailed Description
CheckPointBuffers serves as the main entry point for flushing dirty buffers during PostgreSQL checkpoints. It acts as a wrapper around the BufferSync function, providing a clean interface for the checkpoint process. The function is responsible for ensuring that all modified pages in the shared buffer pool are written to persistent storage, which is crucial for maintaining data consistency and enabling crash recovery. Temporary relations are explicitly excluded from this process since they don't participate in checkpoints and don't need to be flushed to disk for recovery purposes.

## Parameters / Member Variables
- `flags`: Control flags that specify the behavior of the checkpoint buffer operation (passed directly to BufferSync)
## Dependencies
- Functions called/Symbols referenced:
  - [BufferSync](../B/BufferSync.md)
- Called from (representative examples):
  - [CheckPointGuts](CheckPointGuts.md)

## Notes and Other Information
- Temporary relations do not participate in checkpoints and are not flushed
- This function is a critical component of PostgreSQL's checkpoint mechanism
- The actual work is delegated to BufferSync, making this function a clean abstraction layer
- Called during both regular scheduled checkpoints and shutdown checkpoints

## Simplified Source

```c
// Simplified version of CheckPointBuffers
void CheckPointBuffers(int flags) {
    // Flush all dirty blocks in buffer pool to disk
    // Note: temporary relations do not participate in checkpoints
    BufferSync(flags);
}
```

Key simplifications made:
- Focused on the core operation: delegate to BufferSync for actual buffer flushing
- Preserved the essential checkpoint buffer management role
- Emphasized the abstraction layer design
- Maintained the key exclusion of temporary relations from checkpoint process