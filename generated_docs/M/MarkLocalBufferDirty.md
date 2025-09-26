# MarkLocalBufferDirty

## Location
src/backend/storage/buffer/localbuf.c: 449 - 488

## Overview
Marks a local buffer as dirty, indicating that its contents have been modified and need to be written to disk during the next checkpoint or buffer replacement.

## Definition
void MarkLocalBufferDirty(Buffer buffer)

## Detailed Description
This function marks a local buffer as dirty by setting the BM_DIRTY flag in the buffer's state. Local buffers are used for temporary tables and indexes that are only visible to the current session. When a buffer is marked dirty, PostgreSQL knows that its contents have been modified and must be written to disk before the buffer can be reused or the transaction commits.

The function performs several safety checks including ensuring the buffer is indeed a local buffer and that it has a positive reference count. It also updates the local blocks dirtied statistics for monitoring purposes. The operation uses atomic operations to safely modify the buffer state.

## Parameters / Member Variables
- `buffer`: The Buffer handle identifying the local buffer to mark as dirty (must be a local buffer with negative buffer ID)

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsLocal
  - GetLocalBufferDescriptor
  - pg_atomic_read_u32
  - pg_atomic_unlocked_write_u32
- Called from (representative examples):
  - MarkBufferDirty
  - MarkBufferDirtyHint
  - ResourceOwnerForgetBufferIO

## Notes and Other Information
- Uses Assert() to verify the buffer is local and has positive reference count
- Increments pgBufferUsage.local_blks_dirtied counter when marking a clean buffer dirty for the first time
- Uses atomic operations to safely modify buffer state without locking
- Debug builds can enable LBDEBUG to log buffer dirty operations
- Only operates on local buffers (those with negative buffer IDs)