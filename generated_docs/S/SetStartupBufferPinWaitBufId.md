# SetStartupBufferPinWaitBufId

## Location
[src/backend/storage/lmgr/proc.c:664-675](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/proc.c#L664-L675)

## Overview
Sets the buffer ID that the Startup process is waiting on for buffer pin operations, enabling recovery conflict processing for buffer pins.

## Definition
void SetStartupBufferPinWaitBufId(int bufid)

## Detailed Description
This function is used by the buffer manager to communicate which buffer the Startup process is currently waiting to pin. This information is crucial for recovery conflict resolution, as other processes need to know when the Startup process is blocked on a specific buffer.

The function stores the buffer ID in the shared ProcGlobal structure, making it accessible to all processes. When the Startup process is not waiting on any buffer, the value is set to -1 to indicate "not waiting" status.

The operation is atomic and doesn't require locking because:
- The set operation is atomic (simple integer assignment)
- The value is set before other backends examine it
- Uses a volatile pointer to prevent compiler optimizations that could reorder the assignment

## Parameters / Member Variables
- : The buffer ID that the Startup process is waiting on, or -1 to indicate not waiting

## Dependencies
- Functions called/Symbols referenced:
  - [PROC_HDR](../P/PROC_HDR.md) (structure type)

- Called from (representative examples):
  - [LockBufferForCleanup](../L/LockBufferForCleanup.md)

## Notes and Other Information
- Used specifically for recovery conflict processing during Hot Standby
- The volatile pointer prevents code rearrangement by the compiler
- No locking is required due to the atomic nature of integer assignment
- The shared value allows backends to determine if they need to handle recovery conflicts
- Setting bufid to -1 resets the startup process to "not waiting" state
- This mechanism is part of the buffer pin conflict resolution system in PostgreSQL's Hot Standby feature