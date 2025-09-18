# ModifyWaitEvent

## Location
src/backend/storage/ipc/latch.c: 1049 - 1123

## Overview
Modifies an existing wait event in a WaitEventSet by changing the event mask and optionally the associated latch, with optimizations to avoid unnecessary system calls.

## Definition
```c
void ModifyWaitEvent(WaitEventSet *set, int pos, uint32 events, Latch *latch)
```

## Detailed Description
This function modifies a previously added wait event within a WaitEventSet structure. It allows changing the event mask (which events to wait for) and, in the case of latch events, the associated latch pointer. The function includes several important optimizations and constraints:

- **Early return optimization**: If neither the event mask nor the latch changes, the function returns immediately without making system calls
- **Validation**: Ensures latch events cannot be modified to non-latch events and postmaster death events cannot be modified at all
- **Platform-specific handling**: On Unix systems, latch modifications don't require kernel updates since all latches use the same underlying pipe, while Windows requires handle array updates
- **System call delegation**: Calls platform-specific adjustment functions to update the underlying wait mechanism (epoll, kqueue, poll, or Win32)

The function is designed for high-performance scenarios where socket events frequently switch between read and write monitoring.

## Parameters / Member Variables
- `set`: Pointer to the WaitEventSet containing the event to modify
- `pos`: Position index of the event in the set (returned by AddWaitEventToSet)
- `events`: New event mask specifying which events to wait for
- `latch`: New latch pointer (for WL_LATCH_SET events, can be NULL to temporarily disable)

## Dependencies
- Functions called/Symbols referenced:
  - [WaitEventAdjustEpoll](../W/WaitEventAdjustEpoll.md) (Linux)
  - [WaitEventAdjustKqueue](../W/WaitEventAdjustKqueue.md) (BSD)
  - [WaitEventAdjustPoll](../W/WaitEventAdjustPoll.md) (poll-based systems)
  - [WaitEventAdjustWin32](../W/WaitEventAdjustWin32.md) (Windows)
  - elog (error reporting)
- Called from (representative examples):
  - [secure_read](../s/secure_read.md)
  - [secure_write](../s/secure_write.md)
  - [pq_check_connection](../p/pq_check_connection.md)
  - [WalSndWait](../W/WalSndWait.md)
  - [WaitLatch](../W/WaitLatch.md)
  - [SwitchToSharedLatch](../S/SwitchToSharedLatch.md)
  - [SwitchBackToLocalLatch](../S/SwitchBackToLocalLatch.md)

## Notes and Other Information
- Cannot modify latch events to non-latch events or postmaster death events
- Includes optimization for frequent read/write switching on sockets
- On Unix systems, latch modifications don't require kernel updates due to shared pipe mechanism  
- On Windows, old latch handles are left in place when disabled, tolerating spurious wakeups
- Validates that new latches are owned by the current process
- Position parameter must be valid (less than set->nevents)