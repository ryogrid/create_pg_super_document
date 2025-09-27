# AtEOXact_Buffers

## Location
[src/backend/storage/buffer/bufmgr.c:3548-3564](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L3548-L3564)

## Overview
AtEOXact_Buffers is a cleanup function called at the end of a transaction to ensure that no buffer pins remain and to perform necessary buffer-related cleanup operations.

## Definition
void AtEOXact_Buffers(bool isCommit)

## Detailed Description
This function serves as a transaction cleanup routine for buffer management in PostgreSQL. As of PostgreSQL 8.0, buffer pins should be automatically released by the ResourceOwner mechanism, making this function primarily a debugging cross-check to verify that no buffer pins remain after transaction completion. The function performs cleanup for both shared buffers (through CheckForBufferLeaks) and local buffers (through AtEOXact_LocalBuffers), and includes an assertion to verify that private reference count overflow tracking is properly reset.

## Parameters / Member Variables
- `isCommit`: Boolean flag indicating whether the transaction is committing (true) or aborting (false)

## Dependencies
- Functions called/Symbols referenced:
  - [CheckForBufferLeaks](../C/CheckForBufferLeaks.md)
  - [AtEOXact_LocalBuffers](AtEOXact_LocalBuffers.md)
  - PrivateRefCountOverflowed (global variable)
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md)
  - [PrepareTransaction](../P/PrepareTransaction.md)  
  - [AbortTransaction](AbortTransaction.md)
  - [BackgroundWriterMain](../B/BackgroundWriterMain.md)
  - [CheckpointerMain](../C/CheckpointerMain.md)
  - [WalWriterMain](../W/WalWriterMain.md)

## Notes and Other Information
- This function is primarily used for debugging purposes since PostgreSQL 8.0 introduced automatic buffer pin cleanup via ResourceOwner
- The function is called by various transaction and background processes to ensure proper buffer cleanup
- Contains an assertion that verifies PrivateRefCountOverflowed is reset to 0, helping detect reference counting issues
- Part of the buffer management subsystem located in src/backend/storage/buffer/bufmgr.c

## Simplified Source

```c
// Simplified version of AtEOXact_Buffers
void AtEOXact_Buffers(bool isCommit) {
    // Step 1: Debug check - verify no shared buffer pins remain
    // This catches any buffer leaks that ResourceOwner missed
    CheckForBufferLeaks();

    // Step 2: Clean up local buffers for this transaction
    // Handles temporary table buffers and other local resources
    AtEOXact_LocalBuffers(isCommit);

    // Step 3: Verify reference count overflow tracking is clean
    // Assert that private ref count overflow counter is reset
    Assert(PrivateRefCountOverflowed == 0);
}
```

Key simplifications made:
- Added descriptive comments explaining each step's purpose
- Clarified the debugging nature of CheckForBufferLeaks
- Explained the distinction between shared and local buffer cleanup
- Made the assertion's purpose explicit