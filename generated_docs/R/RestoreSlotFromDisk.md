# RestoreSlotFromDisk

## Location
[src/backend/replication/slot.c:2169-2404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L2169-L2404)

## Overview
Loads a single replication slot from disk into memory during startup, performing comprehensive validation, checksum verification, and configuration requirement checks.

## Definition

```c
static void
RestoreSlotFromDisk(const char *name)
```
## Detailed Description
This function handles the complex process of restoring a replication slot from persistent storage into shared memory during PostgreSQL startup. The restoration process includes multiple validation stages:

1. **File Cleanup**: Removes any temporary state files from interrupted operations
2. **File Reading**: Opens and synchronizes the state file, then reads it in two phases (header first, then full content)
3. **Validation**: Verifies magic number, version, length, and checksum integrity
4. **Ephemeral Handling**: Removes non-persistent slots that shouldn't survive restarts
5. **Configuration Checks**: Validates that current server settings (wal_level, hot_standby) support the slot type
6. **Memory Restoration**: Finds an available slot in shared memory and initializes it with the persistent data

The function uses PANIC-level errors for corruption or configuration issues, as these represent fundamental problems that prevent safe database operation.

## Parameters
- : Name of the replication slot to restore from the pg_replslot directory

## Dependencies
- Functions called/Symbols referenced:
  - , , 
  - , 
  - , 
  -  (for REPLICATION_SLOT_RESTORE_SYNC and REPLICATION_SLOT_READ events)
  -  (for directory synchronization)
  - 
  - , , ,  (checksum operations)
  -  (for removing ephemeral slot directories)
  -  (for copying persistent data)
  -  (for setting inactive_since)
- Called from:
  -  (src/backend/replication/slot.c:1932)

## Notes and Other Information
- This is a static function used internally within the slot.c file
- Uses PANIC errors for data corruption or configuration issues that prevent safe startup
- Performs two-phase reading: constant-size header first, then variable-size content
- Validates magic number (SLOT_MAGIC), version (SLOT_VERSION), and length consistency
- Ephemeral slots (persistency != RS_PERSISTENT) are deleted rather than restored
- Enforces WAL level requirements: logical slots need WAL_LEVEL_LOGICAL, physical slots need WAL_LEVEL_REPLICA
- Special handling for standby mode: logical slots require hot_standby to be enabled
- Initializes in-memory state including effective_xmin, candidate values, and timing information
- Uses wait events for monitoring I/O operations during slot restoration
- Critical sections protect directory fsync operations from interruption

## Simplified Source

```c
// Simplified version of RestoreSlotFromDisk
static void
RestoreSlotFromDisk(const char *name)
{
    ReplicationSlotOnDisk slot_data;
    char slot_directory[MAXPGPATH + 12];
    char state_file_path[MAXPGPATH + 22];
    int file_descriptor;
    bool slot_restored = false;
    int bytes_read;
    pg_crc32c calculated_checksum;

    // Build paths for slot directory and state file
    sprintf(slot_directory, "pg_replslot/%s", name);
    sprintf(state_file_path, "%s/state", slot_directory);

    // Clean up any temporary files from previous interrupted operations
    sprintf(temp_path, "%s/state.tmp", slot_directory);
    unlink(temp_path);  // Remove temp file if it exists

    // Open and sync the state file for reading
    file_descriptor = OpenTransientFile(state_file_path, O_RDWR | PG_BINARY);
    if (file_descriptor < 0)
        ereport(PANIC, "could not open replication slot state file");

    // Ensure file is synced to disk before reading
    pg_fsync(file_descriptor);
    fsync_fname(slot_directory, true);  // Sync parent directory too

    // Read the constant-size header portion first
    bytes_read = read(file_descriptor, &slot_data, ReplicationSlotOnDiskConstantSize);
    if (bytes_read != ReplicationSlotOnDiskConstantSize)
        ereport(PANIC, "could not read slot header");

    // Validate header: magic number, version, and length
    if (slot_data.magic != SLOT_MAGIC)
        ereport(PANIC, "invalid magic number in slot file");
    if (slot_data.version != SLOT_VERSION)
        ereport(PANIC, "unsupported slot file version");
    if (slot_data.length != ReplicationSlotOnDiskV2Size)
        ereport(PANIC, "corrupted slot file length");

    // Read the remaining variable-size content
    bytes_read = read(file_descriptor,
                     (char *) &slot_data + ReplicationSlotOnDiskConstantSize,
                     slot_data.length);
    if (bytes_read != slot_data.length)
        ereport(PANIC, "could not read complete slot data");

    CloseTransientFile(file_descriptor);

    // Verify data integrity using checksum
    INIT_CRC32C(calculated_checksum);
    COMP_CRC32C(calculated_checksum,
                checksummed_data, ReplicationSlotOnDiskChecksummedSize);
    FIN_CRC32C(calculated_checksum);

    if (!EQ_CRC32C(calculated_checksum, slot_data.checksum))
        ereport(PANIC, "slot file checksum mismatch");

    // Handle ephemeral slots - delete them instead of restoring
    if (slot_data.slotdata.persistency != RS_PERSISTENT) {
        rmtree(slot_directory, true);  // Remove entire slot directory
        fsync_fname("pg_replslot", true);
        return;
    }

    // Validate configuration requirements for slot type
    if (slot_data.slotdata.database != InvalidOid) {
        // Logical replication slot requirements
        if (wal_level < WAL_LEVEL_LOGICAL)
            ereport(FATAL, "logical slot requires wal_level >= logical");
        if (StandbyMode && !EnableHotStandby)
            ereport(FATAL, "logical slot on standby requires hot_standby = on");
    } else {
        // Physical replication slot requirements
        if (wal_level < WAL_LEVEL_REPLICA)
            ereport(FATAL, "physical slot requires wal_level >= replica");
    }

    // Find an available slot in shared memory and restore data
    for (int i = 0; i < max_replication_slots; i++) {
        ReplicationSlot *memory_slot = &ReplicationSlotCtl->replication_slots[i];

        if (memory_slot->in_use)
            continue;  // Skip slots already in use

        // Copy persistent data from disk to memory
        memcpy(&memory_slot->data, &slot_data.slotdata,
               sizeof(ReplicationSlotPersistentData));

        // Initialize in-memory state variables
        memory_slot->effective_xmin = slot_data.slotdata.xmin;
        memory_slot->effective_catalog_xmin = slot_data.slotdata.catalog_xmin;
        memory_slot->last_saved_confirmed_flush = slot_data.slotdata.confirmed_flush;

        // Reset candidate values (will be set during normal operation)
        memory_slot->candidate_catalog_xmin = InvalidTransactionId;
        memory_slot->candidate_xmin_lsn = InvalidXLogRecPtr;
        memory_slot->candidate_restart_lsn = InvalidXLogRecPtr;
        memory_slot->candidate_restart_valid = InvalidXLogRecPtr;

        // Mark slot as available but not active
        memory_slot->in_use = true;
        memory_slot->active_pid = 0;
        memory_slot->inactive_since = GetCurrentTimestamp();

        slot_restored = true;
        break;
    }

    // Ensure we found an available slot
    if (!slot_restored)
        ereport(FATAL, "no available replication slots for restoration");
}
```

Key simplifications made:
- Removed detailed error handling for each specific error condition
- Consolidated file I/O operations with simplified error checking
- Abstracted complex checksum calculation details
- Used more descriptive variable names (file_descriptor, slot_data, etc.)
- Simplified the slot finding loop logic
- Removed platform-specific considerations and wait event reporting
- Added high-level comments explaining each major step
- Consolidated similar validation checks into clearer logical groups