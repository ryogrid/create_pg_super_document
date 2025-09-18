# LockBuffer

## Location
src/backend/storage/buffer/bufmgr.c: 5132 - 5157

## Overview
LockBuffer acquires or releases the content lock for a buffer, providing exclusive or shared access to buffer contents.

## Definition


## Detailed Description
This function provides the primary interface for acquiring and releasing content locks on buffers in PostgreSQL's buffer management system. It supports three locking modes: unlock, shared lock, and exclusive lock. The function only operates on shared buffers - local buffers (used for temporary tables) do not require locking and are handled as a no-op. The content lock protects the actual data within the buffer page and must be held while reading or modifying buffer contents. This is distinct from the buffer header lock which protects buffer metadata.

## Parameters / Member Variables
- : The Buffer identifier for the buffer to lock or unlock
- : The locking mode - BUFFER_LOCK_UNLOCK, BUFFER_LOCK_SHARE, or BUFFER_LOCK_EXCLUSIVE

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsPinned (assertion check)
  - BufferIsLocal
  - GetBufferDescriptor
  - BufferDescriptorGetContentLock
  - LWLockRelease, LWLockAcquire
  - Lock mode constants (BUFFER_LOCK_UNLOCK, BUFFER_LOCK_SHARE, BUFFER_LOCK_EXCLUSIVE)
  - LW_SHARED, LW_EXCLUSIVE
- Called from (representative examples):
  - UnlockReleaseBuffer (buffer cleanup)
  - Free space management operations
  - Transaction log operations
  - Database command operations
  - Sequence operations

## Notes and Other Information
- Requires the buffer to be pinned before locking (enforced by assertion)
- Local buffers are exempt from locking requirements
- Content locks are implemented using lightweight locks (LWLock)
- Shared locks allow multiple concurrent readers
- Exclusive locks provide sole access for writers
- Critical component of PostgreSQL's concurrency control for buffer access
- Must be paired with appropriate buffer pinning operations
- Used extensively throughout the storage layer for safe buffer access