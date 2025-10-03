# PgArchWakeup

## Location
[src/backend/postmaster/pgarch.c:280-296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/pgarch.c#L280-L296)

## Overview
PgArchWakeup wakes up the PostgreSQL archiver process by setting its process latch, signaling it to check for new WAL files to archive.

## Definition
```c
void PgArchWakeup(void)
```

## Detailed Description
This function provides a mechanism to wake up the sleeping archiver process when new WAL files are available for archiving. It retrieves the archiver's process number from shared memory and sets the corresponding process latch. The function is designed to be safe even without acquiring ProcArrayLock, as setting the wrong process latch or no latch at all will not cause system failure - the archiver will be relaunched and resume normal operation. This approach prioritizes performance over strict synchronization since archiver wake-up is not critical for system correctness.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [SetLatch](../S/SetLatch.md): Sets the process latch to wake up the target process
  - INVALID_PROC_NUMBER: Constant indicating invalid process number
  - PgArch: Global shared memory structure containing archiver state
  - ProcGlobal: Global process array structure
- Called from (representative examples):
  - [XLogArchiveNotify](../X/XLogArchiveNotify.md): Notifies archiver when new WAL files are ready

## Notes and Other Information
- Does not acquire ProcArrayLock for performance reasons
- Safe even if wrong process latch is set due to race conditions
- Uses the pgprocno field from shared memory to identify target process
- Only attempts to set latch if process number is valid
- Part of PostgreSQL's WAL archiving notification system
- The archiver will be automatically relaunched if communication fails
- Provides non-blocking wake-up mechanism for archiver process

## Simplified Source

```c
// Simplified version of PgArchWakeup
void PgArchWakeup(void) {
    // Get the archiver's process number from shared memory
    int arch_pgprocno = PgArch->pgprocno;

    // Wake up the archiver if it has a valid process number
    // Note: No lock needed - if we set wrong latch, archiver will be relaunched
    if (arch_pgprocno != INVALID_PROC_NUMBER) {
        SetLatch(&ProcGlobal->allProcs[arch_pgprocno].procLatch);
    }
}
```

Key simplifications made:
- Condensed the detailed comment about ProcArrayLock into a brief note
- Added descriptive comments for each logical step
- Maintained the essential algorithm and safety guarantees
- Preserved the core logic while improving readability