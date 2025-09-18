# ExtendBufferedRelTo

## Location
src/backend/storage/buffer/bufmgr.c: 909 - 1017

## Overview
Extends a relation to ensure it contains at least a specified number of blocks, returning a buffer for the target block, commonly used for visibility maps, free space maps, and crash recovery scenarios.

## Definition
```c
Buffer ExtendBufferedRelTo(BufferManagerRelation bmr,
                           ForkNumber fork,
                           BufferAccessStrategy strategy,
                           uint32 flags,
                           BlockNumber extend_to,
                           ReadBufferMode mode)
```

## Detailed Description
ExtendBufferedRelTo ensures that a relation contains at least extend_to blocks, and returns a buffer for block (extend_to - 1). This function is particularly useful when code needs to write to a specific page regardless of the current relation size, such as when updating visibility maps, free space maps, or during crash recovery operations.

The function intelligently handles concurrent extensions by checking if another backend has already extended the relation to the desired size. If so, it simply reads the existing buffer rather than extending further. It uses a batched approach, extending up to 64 blocks at a time using an internal buffer array, optimizing for both small and large extensions.

The function can optionally create the fork if it doesn't exist (with EB_CREATE_FORK_IF_NEEDED flag) and can clear the size cache to ensure accurate size information from the kernel.

## Parameters / Member Variables
- `bmr`: BufferManagerRelation containing either a Relation pointer or SMgrRelation pointer with persistence info
- `fork`: Fork number specifying which fork of the relation to extend
- `strategy`: BufferAccessStrategy for buffer replacement policy, can be NULL for default behavior
- `flags`: Control flags (EB_CREATE_FORK_IF_NEEDED, EB_CLEAR_SIZE_CACHE, EB_PERFORMING_RECOVERY, etc.)
- `extend_to`: Target block number - relation will be extended to at least this many blocks
- `mode`: ReadBufferMode specifying how to handle the target buffer (zero and lock, normal read, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetSmgr
  - smgrexists
  - smgrcreate
  - smgrnblocks
  - LockRelationForExtension
  - UnlockRelationForExtension
  - ExtendBufferedRelCommon
  - ReleaseBuffer
  - ReadBuffer_common
  - lengthof
- Called from (representative examples):
  - vm_extend (visibility map extension)
  - XLogReadBufferExtended (WAL recovery)
  - fsm_extend (free space map extension)

## Notes and Other Information
- Returns a buffer for block (extend_to - 1), not extend_to itself
- Handles concurrent extensions gracefully by falling back to reading existing buffers
- Uses batched extension up to 64 blocks at a time for efficiency
- Can create forks on-demand when EB_CREATE_FORK_IF_NEEDED is specified
- Automatically promotes RBM_ZERO_AND_LOCK and RBM_ZERO_AND_CLEANUP_LOCK modes to include EB_LOCK_TARGET
- Essential for auxiliary relation maintenance (VM, FSM) and recovery operations
- Provides strong consistency guarantees for target block accessibility