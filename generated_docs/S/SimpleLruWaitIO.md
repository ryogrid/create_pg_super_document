# SimpleLruWaitIO

## Location
[src/backend/access/transam/slru.c:445-501](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L445-L501)

## Overview
Waits for any active I/O operations on a specific SLRU page slot to complete and handles recovery from failed I/O operations.

## Definition

```c
static void
SimpleLruWaitIO(SlruCtl ctl, int slotno)
```
## Detailed Description
SimpleLruWaitIO is a static synchronization function that ensures any ongoing I/O operations on a specified SLRU buffer slot are completed before proceeding. The function uses a sophisticated locking mechanism to wait for I/O completion and includes error recovery logic to handle cases where I/O operations may have failed abnormally.

The function operates by:
1. Releasing the bank lock temporarily 
2. Acquiring and immediately releasing the buffer lock in shared mode (this blocks until any exclusive lock holder finishes)
3. Re-acquiring the bank lock in exclusive mode
4. Checking for and recovering from failed I/O operations

The recovery mechanism detects failed I/O by attempting to conditionally acquire the buffer lock. If successful, it means the previous I/O holder has abnormally terminated, and the function resets the page status appropriately based on whether it was a read or write operation.

## Parameters / Member Variables
- `ctl`: SlruCtl structure containing the SLRU control information and shared state
- `slotno`: The buffer slot number to wait for I/O completion on
## Dependencies
- Functions called/Symbols referenced:
  - SlotGetBankNumber (get bank number for slot)
  - [LWLockRelease](../L/LWLockRelease.md) (release lightweight locks)
  - [LWLockAcquire](../L/LWLockAcquire.md) (acquire lightweight locks) 
  - [LWLockConditionalAcquire](../L/LWLockConditionalAcquire.md) (conditionally acquire locks for error detection)
- Called from (representative examples):
  - [SimpleLruReadPage](SimpleLruReadPage.md)
  - [SlruInternalWritePage](SlruInternalWritePage.md)
  - [SlruSelectLRUPage](SlruSelectLRUPage.md)
  - [SimpleLruTruncate](SimpleLruTruncate.md)
  - [SlruDeleteSegment](SlruDeleteSegment.md)

## Notes and Other Information
- This is a static function, only accessible within the slru.c file
- The bank lock must be held in exclusive mode before calling this function
- The function includes sophisticated error recovery for failed I/O operations
- Uses conditional lock acquisition to detect abnormally terminated I/O processes
- Read failures result in SLRU_PAGE_EMPTY status, write failures restore SLRU_PAGE_VALID with dirty flag
- The lock release/acquire pattern ensures proper synchronization without deadlock
- Does not guarantee that new I/O won't start after the function returns
- The slot may contain a different page after the function completes due to concurrent operations

## Simplified Source

```c
// Simplified version of SimpleLruWaitIO
static void
SimpleLruWaitIO(SlruCtl ctl, int slotno)
{
    SlruShared shared = ctl->shared;
    int bankno = SlotGetBankNumber(slotno);

    // Core synchronization pattern: wait for buffer I/O to complete
    // Step 1: Release bank lock to avoid deadlock
    LWLockRelease(&shared->bank_locks[bankno].lock);

    // Step 2: Acquire buffer lock in shared mode (blocks until I/O done)
    LWLockAcquire(&shared->buffer_locks[slotno].lock, LW_SHARED);
    LWLockRelease(&shared->buffer_locks[slotno].lock);

    // Step 3: Re-acquire bank lock for exclusive access
    LWLockAcquire(&shared->bank_locks[bankno].lock, LW_EXCLUSIVE);

    // Recovery logic: detect and fix failed I/O operations
    if (shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS ||
        shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS)
    {
        // Try to acquire buffer lock - if we can, the I/O process failed
        if (LWLockConditionalAcquire(&shared->buffer_locks[slotno].lock, LW_SHARED))
        {
            // Reset page status based on operation type
            if (shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS) {
                shared->page_status[slotno] = SLRU_PAGE_EMPTY;
            } else {
                // Write operation failed - mark as valid but dirty
                shared->page_status[slotno] = SLRU_PAGE_VALID;
                shared->page_dirty[slotno] = true;
            }
            LWLockRelease(&shared->buffer_locks[slotno].lock);
        }
    }
}
```

Key simplifications made:
- Consolidated detailed comments into clear step-by-step flow
- Simplified complex conditional logic with clear explanations
- Removed assert statements for clarity while noting preconditions
- Added high-level comments explaining the synchronization pattern
- Preserved essential error recovery logic with clearer variable handling
- Maintained the critical lock ordering and release patterns