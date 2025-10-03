# CommitTsShmemInit

## Location
[src/backend/access/transam/commit_ts.c:530-583](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L530-L583)

## Overview
Initializes the CommitTS (commit timestamp) shared memory structures during PostgreSQL system startup, including SLRU control structure setup and configuration of buffer auto-tuning.

## Definition
```c
void CommitTsShmemInit(void)
```

## Detailed Description
This function performs comprehensive initialization of the commit timestamp subsystem's shared memory components during PostgreSQL startup. It handles several critical tasks:

1. **Buffer Auto-tuning**: If `commit_timestamp_buffers` is set to 0 (auto-tune mode), it calculates the optimal buffer count using `CommitTsShmemBuffers()` and updates the configuration parameter. It handles both dynamic default setting and override cases for proper configuration management.

2. **SLRU Initialization**: Sets up the Simple LRU (Least Recently Used) cache control structure (`CommitTsCtl`) with appropriate parameters including page precedence function, buffer count, directory name, and synchronization handlers.

3. **Shared State Setup**: Initializes or attaches to the `CommitTimestampShared` structure that maintains global state. For new instances (postmaster), it initializes default values; for child processes, it verifies the structure exists.

4. **Testing Support**: Includes unit test setup for page precedence logic to ensure correctness.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [CommitTsShmemBuffers](CommitTsShmemBuffers.md)
  - [SetConfigOption](../S/SetConfigOption.md)
  - [CommitTsPagePrecedes](CommitTsPagePrecedes.md)
  - [SimpleLruInit](../S/SimpleLruInit.md)
  - [SlruPagePrecedesUnitTests](../S/SlruPagePrecedesUnitTests.md)
  - [ShmemInitStruct](../S/ShmemInitStruct.md)
  - TIMESTAMP_NOBEGIN
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](CreateOrAttachShmemStructs.md)

## Notes and Other Information
- This function is called once during PostgreSQL startup by the postmaster or standalone backend
- The auto-tuning logic ensures optimal buffer allocation without manual configuration
- The function distinguishes between postmaster (new initialization) and child process (attachment) contexts
- The commit timestamp feature tracks when transactions commit and can be queried for forensic analysis
- Error handling ensures configuration overrides work even when explicitly set to 0 in postgresql.conf
- The SLRU directory "pg_commit_ts" stores the actual timestamp data files on disk

## Simplified Source

```c
// Simplified version of CommitTsShmemInit
void CommitTsShmemInit(void) {
    bool found;

    // Auto-tune buffer count if needed
    if (commit_timestamp_buffers == 0) {
        char buf[32];
        snprintf(buf, sizeof(buf), "%d", CommitTsShmemBuffers());

        // Try to set config option with dynamic default
        SetConfigOption("commit_timestamp_buffers", buf, PGC_POSTMASTER, PGC_S_DYNAMIC_DEFAULT);

        // Force override if dynamic default failed
        if (commit_timestamp_buffers == 0) {
            SetConfigOption("commit_timestamp_buffers", buf, PGC_POSTMASTER, PGC_S_OVERRIDE);
        }
    }

    // Initialize SLRU control structure
    CommitTsCtl->PagePrecedes = CommitTsPagePrecedes;
    SimpleLruInit(CommitTsCtl, "commit_timestamp", CommitTsShmemBuffers(), 0,
                  "pg_commit_ts", LWTRANCHE_COMMITTS_BUFFER,
                  LWTRANCHE_COMMITTS_SLRU, SYNC_HANDLER_COMMIT_TS, false);

    // Initialize shared memory structure
    commitTsShared = ShmemInitStruct("CommitTs shared",
                                     sizeof(CommitTimestampShared), &found);

    // Initialize state for new postmaster process
    if (!IsUnderPostmaster) {
        commitTsShared->xidLastCommit = InvalidTransactionId;
        TIMESTAMP_NOBEGIN(commitTsShared->dataLastCommit.time);
        commitTsShared->dataLastCommit.nodeid = InvalidRepOriginId;
        commitTsShared->commitTsActive = false;
    }
}
```

Key simplifications made:
- Removed detailed comments for brevity while keeping essential logic comments
- Consolidated the buffer auto-tuning logic into a clearer flow
- Removed assertion checks and unit test calls for clarity
- Focused on the main initialization steps: buffer tuning, SLRU setup, and shared memory initialization
- Preserved the essential conditional logic for postmaster vs child process handling