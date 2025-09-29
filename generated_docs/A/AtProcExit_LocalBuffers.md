# AtProcExit_LocalBuffers

## Location
[src/backend/storage/buffer/localbuf.c:830-838](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/localbuf.c#L830-L838)

## Overview
AtProcExit_LocalBuffers ensures that no local buffer pins are held when a backend process is exiting, serving as a critical consistency check during process termination to prevent buffer leaks and potential system issues.

## Definition
```c
void AtProcExit_LocalBuffers(void)
```

## Detailed Description
This function is called during backend process termination to verify that all local buffer pins have been properly released. It serves as the local buffer equivalent of AtProcExit_Buffers and is part of PostgreSQL's comprehensive buffer management cleanup system.

The function performs a final check for local buffer reference count leaks by calling CheckForLocalBufferLeaks(). This is particularly important because any remaining buffer pins at process exit could indicate:
1. Programming errors where buffers weren't properly unpinned
2. Incomplete cleanup of temporary relations
3. Potential resource leaks that could affect system stability

The comment in the source code indicates that if buffer pins remain and assertions are disabled, the backend would likely fail later when trying to drop temporary relations in DropRelationBuffers, making this an important early detection mechanism.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [CheckForLocalBufferLeaks](../C/CheckForLocalBufferLeaks.md) (internal function for detecting buffer reference leaks)
- Called from (representative examples):
  - [AtProcExit_Buffers](AtProcExit_Buffers.md) (main buffer cleanup function during process exit)
  - RelationGetNumberOfBlocks (buffer management context)

## Notes and Other Information
- This function is called as part of the process exit sequence, after all transactions have been completed
- Like AtEOXact_LocalBuffers, the actual leak detection only occurs in debug builds with USE_ASSERT_CHECKING enabled
- Local buffers are used for temporary tables and other backend-private storage, so they should be naturally cleaned up when temporary relations are dropped
- The function serves as a safety net to catch potential programming errors in buffer management
- If buffer pins are detected at this stage, it indicates a serious bug in the temporary relation cleanup logic
- This function complements the shared buffer exit cleanup performed by AtProcExit_Buffers
- Process exit buffer cleanup is more comprehensive than transaction end cleanup, as it's the final opportunity to detect and report resource leaks

## Simplified Source

```c
// Simplified version of AtProcExit_LocalBuffers
void AtProcExit_LocalBuffers(void) {
    // Verify no local buffer pins remain during backend exit
    // This prevents buffer leaks and catches programming errors
    CheckForLocalBufferLeaks();
}
```

Key simplifications made:
- Added explanatory comments to clarify the function's purpose
- Removed the detailed multi-line comment for brevity while preserving the core meaning
- Emphasized that this is a safety check for buffer management consistency
- Maintained the essential logic: calling CheckForLocalBufferLeaks() to detect any remaining buffer pins