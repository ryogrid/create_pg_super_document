# DisownLatch

## Location
[src/backend/storage/ipc/latch.c:489-516](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L489-L516)

## Overview
Releases ownership of a shared latch from the current process, making it available for other processes to own.

## Definition
```c
void DisownLatch(Latch *latch)
```

## Detailed Description
DisownLatch removes the current process's ownership of a shared latch by setting the owner_pid field back to 0. This is the counterpart to OwnLatch and is typically called during process cleanup or shutdown. The function includes assertions to ensure that the latch is indeed shared and that the current process is the actual owner before releasing ownership. Once disowned, the latch becomes available for other processes to claim with OwnLatch.

## Parameters / Member Variables
- `latch`: Pointer to the shared Latch structure to disown

## Dependencies
- Functions called/Symbols referenced:
  - [Latch](../L/Latch.md) (structure type)
- Called from (representative examples):
  - [ShutdownWalRecovery](../S/ShutdownWalRecovery.md)
  - [ProcKill](../P/ProcKill.md)
  - [AuxiliaryProcKill](../A/AuxiliaryProcKill.md)

## Notes and Other Information
- Only works with shared latches (latch->is_shared must be true)
- Asserts that the current process actually owns the latch before disowning
- Sets owner_pid to 0, indicating no current owner
- Typically called during process cleanup or shutdown procedures
- After disowning, another process can take ownership with OwnLatch
- Essential for proper cleanup to prevent resource leaks in shared memory