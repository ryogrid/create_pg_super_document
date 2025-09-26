# AtProcExit_Buffers

## Location
[src/backend/storage/buffer/bufmgr.c:3590-3607](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L3590-L3607)

## Overview
AtProcExit_Buffers is a cleanup function called during backend exit to ensure all shared-buffer locks are released and verify that no buffer pins remain.

## Definition
static void AtProcExit_Buffers(int code, Datum arg)

## Detailed Description
This static function serves as a backend process exit handler that performs critical buffer-related cleanup operations. It is registered as a shutdown callback by InitBufferPoolAccess and is automatically called when a backend process terminates. The function ensures proper cleanup by releasing any remaining shared-buffer locks, checking for buffer leaks, and performing local buffer cleanup. This prevents resource leaks and maintains the integrity of the buffer management system across process boundaries.

## Parameters / Member Variables
- `code`: Exit code passed by the process exit mechanism (standard callback parameter)
- `arg`: Additional argument data passed by the exit callback system (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [UnlockBuffers](../U/UnlockBuffers.md)
  - [CheckForBufferLeaks](../C/CheckForBufferLeaks.md)
  - [AtProcExit_LocalBuffers](AtProcExit_LocalBuffers.md)
- Called from (representative examples):
  - [InitBufferPoolAccess](../I/InitBufferPoolAccess.md) (registers this function as exit callback)
  - Process exit mechanism (automatic callback)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the bufmgr.c file
- Registered as a shutdown callback by InitBufferPoolAccess using on_shmem_exit()
- Critical for preventing buffer locks and pins from being leaked when a backend process exits unexpectedly
- Works in conjunction with the local buffer cleanup system (localbuf.c)
- Part of PostgreSQL's robust cleanup mechanism that ensures system stability even during abnormal process termination
- The function follows the standard callback signature expected by the process exit handling system