# CheckPointReplicationOrigin

## Location
[src/backend/replication/logical/origin.c:573-698](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L573-L698)

## Overview
Performs a checkpoint of all replication origins' progress, persisting the current replication state to disk in a crash-safe manner as part of PostgreSQL's checkpoint process.

## Definition
```c
void CheckPointReplicationOrigin(void)
```

## Detailed Description
This function creates a persistent checkpoint of all active replication origin states, ensuring that replication progress can be recovered after a crash or restart. The function implements a robust, crash-safe checkpointing mechanism with the following key features:

**File Format**: The checkpoint file follows a structured format:
- Magic number (REPLICATION_STATE_MAGIC)
- Variable number of ReplicationStateOnDisk structures
- CRC32C checksum for integrity verification

**Crash Safety**: Uses a write-to-temporary-file-then-rename pattern to ensure atomicity. The process:
1. Writes to a temporary file (replorigin_checkpoint.tmp)
2. Ensures all referenced WAL data is flushed to disk
3. Atomically renames to the permanent file (replorigin_checkpoint)

**Consistency Guarantees**: Before writing each origin's state, calls XLogFlush() to ensure that the local_lsn referenced in the checkpoint is actually persistent on disk, preventing inconsistencies where the checkpoint refers to uncommitted transactions.

**Concurrency Control**: Uses shared locks to prevent concurrent creation/deletion of origins during checkpointing while allowing multiple readers.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - `unlink`: Removes any existing temporary checkpoint file
  - `[OpenTransientFile](../O/OpenTransientFile.md)`: Opens the temporary checkpoint file for writing
  - `write`: Writes data to the checkpoint file
  - [XLogFlush](../X/XLogFlush.md): Ensures WAL data persistence before checkpointing local_lsn
  - `[LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease`: Provides concurrency control
  - `[CloseTransientFile](CloseTransientFile.md)`: Closes the temporary file
  - [durable_rename](../d/durable_rename.md): Atomically renames temporary file to permanent location
  - `INIT_CRC32C/COMP_CRC32C/FIN_CRC32C`: CRC calculation for integrity checking
- Called from (representative examples):
  - [CheckPointGuts](CheckPointGuts.md): As part of the main PostgreSQL checkpoint process

## Notes and Other Information
- Only performs checkpointing if max_replication_slots > 0
- Uses PANIC level errors for I/O failures since checkpoint integrity is critical for database consistency
- Skips inactive replication origins (those with InvalidRepOriginId)
- The checkpoint file is stored in the pg_logical directory
- CRC32C checksum ensures data integrity and corruption detection
- Temporary file creation uses O_EXCL to prevent race conditions between concurrent checkpoint attempts
- All padding bytes in disk structures are zeroed to ensure deterministic output
- Part of PostgreSQL's critical recovery infrastructure - checkpoint failures can prevent database startup

## Simplified Source

```c
// Simplified version of CheckPointReplicationOrigin
void CheckPointReplicationOrigin(void) {
    const char *tmppath = "pg_logical/replorigin_checkpoint.tmp";
    const char *path = "pg_logical/replorigin_checkpoint";
    int tmpfd;
    int i;
    uint32 magic = REPLICATION_STATE_MAGIC;
    pg_crc32c crc;

    // Early exit if no replication slots configured
    if (max_replication_slots == 0)
        return;

    INIT_CRC32C(crc);

    // Step 1: Prepare temporary file for atomic write
    cleanup_temp_file(tmppath);
    tmpfd = create_checkpoint_file(tmppath);

    // Step 2: Write file header with magic number
    write_with_crc(tmpfd, &magic, sizeof(magic), &crc);

    // Step 3: Write replication origin states
    LWLockAcquire(ReplicationOriginLock, LW_SHARED);

    for (i = 0; i < max_replication_slots; i++) {
        ReplicationStateOnDisk disk_state;
        ReplicationState *curstate = &replication_states[i];
        XLogRecPtr local_lsn;

        // Skip inactive origins
        if (curstate->roident == InvalidRepOriginId)
            continue;

        // Step 3a: Copy current state under lock
        memset(&disk_state, 0, sizeof(disk_state));
        LWLockAcquire(&curstate->lock, LW_SHARED);

        disk_state.roident = curstate->roident;
        disk_state.remote_lsn = curstate->remote_lsn;
        local_lsn = curstate->local_lsn;

        LWLockRelease(&curstate->lock);

        // Step 3b: Ensure WAL consistency before writing
        XLogFlush(local_lsn);  // Guarantee local_lsn is on disk

        // Step 3c: Write state to checkpoint file
        write_with_crc(tmpfd, &disk_state, sizeof(disk_state), &crc);
    }

    LWLockRelease(ReplicationOriginLock);

    // Step 4: Write CRC and finalize file
    FIN_CRC32C(crc);
    write_with_crc(tmpfd, &crc, sizeof(crc), NULL);

    close_checkpoint_file(tmpfd);

    // Step 5: Atomically replace old checkpoint with new one
    durable_rename(tmppath, path, PANIC);
}
```

Key simplifications made:
- Abstracted repetitive error handling into conceptual helper functions
- Organized the checkpoint process into clear sequential steps
- Preserved the critical WAL flushing logic that ensures consistency
- Maintained the atomic write pattern (temp file + rename)
- Simplified while keeping the essential CRC integrity checking
- Focused on the core algorithm: header, state iteration, finalization
- Retained the lock acquisition patterns for proper concurrency control