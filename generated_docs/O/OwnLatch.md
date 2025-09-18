# OwnLatch

## Location
src/backend/storage/ipc/latch.c: 463 - 488

## Overview
Associates a shared latch with the current process, giving the process ownership and the ability to wait on the latch.

## Definition
```c
void OwnLatch(Latch *latch)
```

## Detailed Description
OwnLatch takes ownership of a shared latch by setting the latch's owner_pid to the current process ID. This function is used after a shared latch has been initialized with InitSharedLatch to associate it with a specific process that needs to wait on it. The function includes sanity checks to ensure the latch is marked as shared and that no other process currently owns it. On Unix-like systems, it also verifies that the necessary latch support mechanisms have been properly initialized in the current process.

## Parameters / Member Variables
- `latch`: Pointer to the shared Latch structure to take ownership of

## Dependencies
- Functions called/Symbols referenced:
  - [Latch](../L/Latch.md) (structure type)
  - WAIT_USE_SELF_PIPE (conditional compilation flag)
  - WAIT_USE_SIGNALFD (conditional compilation flag)
  - PANIC (error level)
- Called from (representative examples):
  - [InitWalRecovery](../I/InitWalRecovery.md)
  - InitProcess
  - [InitAuxiliaryProcess](../I/InitAuxiliaryProcess.md)

## Notes and Other Information
- Only works with shared latches (latch->is_shared must be true)
- Panics if the latch is already owned by another process
- No locking is performed, so callers must provide interlocks if concurrent ownership attempts are possible
- Requires InitializeLatchSupport to have been called in the current process on Unix systems
- Once owned, the process can use WaitLatch and related functions on this latch