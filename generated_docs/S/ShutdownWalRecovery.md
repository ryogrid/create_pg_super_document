# ShutdownWalRecovery

## Location
[src/backend/access/transam/xlogrecovery.c:1608-1651](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L1608-L1651)

## Overview
Performs final cleanup of WAL recovery infrastructure by freeing resources, closing files, removing temporary recovery files, and disowning latches.

## Definition

```c
void
ShutdownWalRecovery(void)
```
## Detailed Description
This function handles the complete teardown of WAL recovery infrastructure after recovery has finished. It performs comprehensive cleanup to ensure no recovery-related resources or temporary files remain:

1. **Statistics Finalization**: Updates pg_stat_recovery_prefetch with final statistics
2. **Resource Cleanup**: Closes open WAL files and frees xlogreader and xlogprefetcher structures  
3. **Temporary File Removal**: Deletes temporary recovery files (RECOVERYXLOG and RECOVERYHISTORY)
4. **Latch Management**: Disowns the recovery wakeup latch for tidiness

The function specifically handles cleanup for archive recovery scenarios, removing partial WAL segments and timeline history files that were created during the recovery process. These temporary files are no longer needed once recovery completes and normal operation begins.

## Parameters / Member Variables
This function takes no parameters and returns void.

## Dependencies
- Functions called/Symbols referenced:
  - [XLogPrefetcherComputeStats](../X/XLogPrefetcherComputeStats.md) (finalizes prefetch statistics)
  - close (closes open WAL file descriptor)
  - [XLogReaderFree](../X/XLogReaderFree.md) (deallocates xlogreader structure)
  - [XLogPrefetcherFree](../X/XLogPrefetcherFree.md) (deallocates xlogprefetcher structure)
  - unlink (removes temporary recovery files)
  - [DisownLatch](../D/DisownLatch.md) (releases latch ownership)
  - XLOGDIR (WAL directory path constant)
- Called from:
  - [StartupXLOG](StartupXLOG.md) (during database startup after recovery completion)

## Notes and Other Information
- Called after FinishWalRecovery() completes the recovery process
- Ignores errors when unlinking temporary files (they may not exist)
- Only disowns recovery latch if ArchiveRecoveryRequested was true
- RECOVERYXLOG file contains partial WAL segment from recovery
- RECOVERYHISTORY file contains recovered timeline history information  
- Essential for preventing resource leaks and ensuring clean recovery completion
- Maintains global readFile descriptor state by setting it to -1 after closing
- Part of the final cleanup sequence that transitions from recovery to normal operation

## Simplified Source

```c
// Simplified version of ShutdownWalRecovery
void ShutdownWalRecovery(void) {
    char recoveryPath[MAXPGPATH];

    // Step 1: Finalize recovery statistics
    XLogPrefetcherComputeStats(xlogprefetcher);

    // Step 2: Clean up WAL reader resources
    if (readFile >= 0) {
        close(readFile);
        readFile = -1;
    }
    XLogReaderFree(xlogreader);
    XLogPrefetcherFree(xlogprefetcher);

    // Step 3: Remove temporary recovery files (if archive recovery was used)
    if (ArchiveRecoveryRequested) {
        // Remove partial WAL segment file
        snprintf(recoveryPath, MAXPGPATH, XLOGDIR "/RECOVERYXLOG");
        unlink(recoveryPath);  // ignore errors

        // Remove timeline history file
        snprintf(recoveryPath, MAXPGPATH, XLOGDIR "/RECOVERYHISTORY");
        unlink(recoveryPath);  // ignore errors
    }

    // Step 4: Release recovery wakeup latch
    if (ArchiveRecoveryRequested) {
        DisownLatch(&XLogRecoveryCtl->recoveryWakeupLatch);
    }
}
```

Key simplifications made:
- Added step-by-step comments to clarify the cleanup sequence
- Grouped operations by logical purpose (statistics, resources, files, latch)
- Preserved all essential logic and error handling approach
- Maintained the conditional cleanup for archive recovery scenarios
- Kept the defensive programming approach of ignoring unlink errors