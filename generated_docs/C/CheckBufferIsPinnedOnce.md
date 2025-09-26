# CheckBufferIsPinnedOnce

## Location
[src/backend/storage/buffer/bufmgr.c:5179-5211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L5179-L5211)

## Overview
CheckBufferIsPinnedOnce is a debugging/assertion function that verifies the current backend has pinned a buffer exactly once.

## Definition

```c
void
CheckBufferIsPinnedOnce(Buffer buffer)
```
## Detailed Description
This function provides a verification mechanism to ensure that the current backend holds exactly one pin on the specified buffer. It's primarily used for debugging and assertion purposes to detect incorrect buffer pin management. The function distinguishes between local buffers (owned by the current backend) and shared buffers, using different mechanisms to check the pin count for each type.

For local buffers, it checks the LocalRefCount array which tracks pin counts for local buffers. For shared buffers, it uses GetPrivateRefCount to check how many times the current backend has pinned the buffer. If the pin count is not exactly 1, it raises an ERROR.

## Parameters / Member Variables
- `buffer`: The buffer identifier to check for exactly one pin by the current backend

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsLocal
  - GetPrivateRefCount
- Called from (representative examples):
  - GetVictimBuffer
  - LockBufferForCleanup
  - BUFFER_LOCK_EXCLUSIVE

## Notes and Other Information
- This is a debugging/assertion function that will terminate the transaction with an ERROR if the condition is not met
- Only checks pins held by the current backend, ignoring pins held by other backends
- Different checking mechanisms for local vs shared buffers
- Used primarily in buffer management code to ensure correct pin/unpin discipline
- The function name suggests it should be used only when the caller expects exactly one pin