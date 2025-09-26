# BufferSync

## Location
src/backend/storage/buffer/bufmgr.c: 2901 - 3176

## Overview
BufferSync writes out all dirty buffers in the shared buffer pool to disk, implementing the core checkpoint buffer synchronization with load balancing across tablespaces.

## Definition
```c
static void BufferSync(int flags)
```

## Detailed Description
BufferSync is the main function called during checkpoints to write all dirty shared buffers to disk. It uses a two-phase approach: first, it scans all buffers to identify dirty ones and marks them with BM_CHECKPOINT_NEEDED; then it writes the marked buffers in a carefully balanced manner across tablespaces using a binary heap. The function sorts buffers by tablespace, relation, fork, and block number to minimize random I/O. It implements sophisticated load balancing to prevent overwhelming individual tablespaces by writing proportionally from each tablespace based on their buffer counts. The function supports different checkpoint types through flags, writing additional buffer types during shutdown or recovery.

## Parameters / Member Variables
- `flags`: Checkpoint request flags that control behavior (CHECKPOINT_IMMEDIATE, CHECKPOINT_IS_SHUTDOWN, CHECKPOINT_END_OF_RECOVERY, CHECKPOINT_FLUSH_ALL, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - GetBufferDescriptor
  - LockBufHdr
  - UnlockBufHdr
  - BufTagGetRelNumber
  - BufTagGetForkNum
  - sort_checkpoint_bufferids
  - binaryheap_allocate
  - binaryheap_add_unordered
  - binaryheap_build
  - binaryheap_first
  - binaryheap_remove_first
  - binaryheap_replace_first
  - SyncOneBuffer
  - CheckpointWriteDelay
  - IssuePendingWritebacks
- Called from (representative examples):
  - CheckPointBuffers
  - BufferIsPinned

## Notes and Other Information
- Uses BM_CHECKPOINT_NEEDED flag to track buffers that need writing during checkpoint
- Implements sophisticated tablespace load balancing using binary heap for fair I/O distribution
- Sorts buffers by tablespace, relation, fork, and block number to optimize disk access patterns
- Supports different checkpoint modes through flags (shutdown, recovery, immediate, etc.)
- Only writes permanent buffers during normal checkpoints, but writes all dirty buffers during shutdown/recovery
- Includes progress tracking and statistics collection for monitoring checkpoint performance
- Uses writeback context for efficient I/O batching and flushing