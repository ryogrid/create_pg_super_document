# CreateSharedInvalidationState

## Location
src/backend/storage/ipc/sinvaladt.c: 234 - 271

## Overview
CreateSharedInvalidationState creates and initializes the shared invalidation message buffer in shared memory, setting up the data structures needed for inter-process cache invalidation.

## Definition
void CreateSharedInvalidationState(void)

## Detailed Description
This function allocates and initializes the shared invalidation segment (SISeg) in shared memory. It performs the following initialization steps:

1. Allocates shared memory using ShmemInitStruct with the size calculated by SInvalShmemSize()
2. If the structure already exists (found=true), returns early to avoid re-initialization
3. Initializes message counters (minMsgNum, maxMsgNum) to 0
4. Sets the initial cleanup threshold to CLEANUP_MIN
5. Initializes the spinlock for protecting message number operations
6. Initializes all process state slots to inactive with default values
7. Sets up the pgprocnos array pointer

The function ensures that the shared invalidation subsystem starts in a clean, well-defined state with all backend processes marked as inactive.

## Parameters / Member Variables
(No parameters - function takes void)

## Dependencies
- Functions called/Symbols referenced:
  - [ShmemInitStruct](../S/ShmemInitStruct.md)
  - [SInvalShmemSize](../S/SInvalShmemSize.md)
  - SpinLockInit
- Data types referenced:
  - [SISeg](../S/SISeg.md)
- Constants referenced:
  - CLEANUP_MIN
  - InvalidLocalTransactionId
- [Variables](../V/Variables.md) referenced:
  - NumProcStateSlots
  - shmInvalBuffer (global variable set by this function)
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](CreateOrAttachShmemStructs.md)

## Notes and Other Information
- This function should only be called once during postmaster startup as part of shared memory initialization
- The function is idempotent - if called multiple times, subsequent calls will return early without reinitializing
- The buffer[] array in the SISeg structure is not explicitly initialized since it starts unused
- All process slots are initialized to inactive state (procPid = 0) and will be activated when backends connect
- The pgprocnos array is positioned immediately after the procState array in memory
- This is a critical component of PostgreSQL's cache invalidation system that ensures data consistency across multiple backend processes