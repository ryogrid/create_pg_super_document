# StartupReplicationOrigin

## Location
[src/backend/replication/logical/origin.c:699-826](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L699-L826)

## Overview
Recovers replication replay status from checkpoint data saved earlier during startup, restoring the replication origin state from the persistent checkpoint file.

## Definition

```c
void
StartupReplicationOrigin(void)
```
## Detailed Description
StartupReplicationOrigin is responsible for recovering the replication state during PostgreSQL startup by reading from the "pg_logical/replorigin_checkpoint" file. This function is called only at startup and not during every checkpoint read during recovery (e.g., in Hot Standby or Point-in-Time Recovery from a base backup). The function validates the file magic number, reads individual replication states, verifies the checksum, and loads the data into shared memory (replication_states array). It handles various error conditions including file corruption, missing files, and configuration limits being exceeded.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [OpenTransientFile](../O/OpenTransientFile.md)
  - read
  - [CloseTransientFile](../C/CloseTransientFile.md)
  - INIT_CRC32C
  - COMP_CRC32C
  - FIN_CRC32C
  - ereport (PANIC/LOG levels)
  - [ReplicationStateOnDisk](../R/ReplicationStateOnDisk.md) (struct)
  - REPLICATION_STATE_MAGIC
  - ERRCODE_DATA_CORRUPTED
- Called from (representative examples):
  - [StartupXLOG](StartupXLOG.md) (src/backend/access/transam/xlog.c:5588)

## Notes and Other Information
- The function includes assert checking to ensure it's only called once during startup
- Returns early if max_replication_slots is 0, indicating no replication slots are configured
- Handles the case where the checkpoint file doesn't exist (e.g., first startup or standby promotion)
- Uses CRC32C checksum verification to detect file corruption
- Reports recovery progress at LOG level for each recovered replication state
- Critical for maintaining replication consistency across PostgreSQL restarts

## Simplified Source

```c
// Simplified version of StartupReplicationOrigin
void StartupReplicationOrigin(void) {
    const char *path = "pg_logical/replorigin_checkpoint";
    int fd;
    int readBytes;
    uint32 magic = REPLICATION_STATE_MAGIC;
    int last_state = 0;
    pg_crc32c file_crc;
    pg_crc32c crc;

    // Return early if no replication slots configured
    if (max_replication_slots == 0)
        return;

    // Initialize CRC calculation
    INIT_CRC32C(crc);

    // Open the checkpoint file
    fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);

    // Handle file not found (normal for first startup)
    if (fd < 0 && errno == ENOENT)
        return;
    else if (fd < 0)
        ereport(PANIC, (errcode_for_file_access(),
                errmsg("could not open file \"%s\": %m", path)));

    // Verify file magic number
    readBytes = read(fd, &magic, sizeof(magic));
    if (readBytes != sizeof(magic) || magic != REPLICATION_STATE_MAGIC)
        ereport(PANIC, (errmsg("invalid replication checkpoint file")));

    COMP_CRC32C(crc, &magic, sizeof(magic));

    // Read and recover each replication state
    while (true) {
        ReplicationStateOnDisk disk_state;
        readBytes = read(fd, &disk_state, sizeof(disk_state));

        // Check if we've reached the CRC at end of file
        if (readBytes == sizeof(crc)) {
            file_crc = *(pg_crc32c *) &disk_state;
            break;
        }

        // Validate read operation
        if (readBytes != sizeof(disk_state))
            ereport(PANIC, (errmsg("corrupted replication checkpoint file")));

        // Update CRC and check limits
        COMP_CRC32C(crc, &disk_state, sizeof(disk_state));
        if (last_state == max_replication_slots)
            ereport(PANIC, (errmsg("too many replication states, increase max_replication_slots")));

        // Copy state to shared memory
        replication_states[last_state].roident = disk_state.roident;
        replication_states[last_state].remote_lsn = disk_state.remote_lsn;
        last_state++;

        // Log recovery progress
        ereport(LOG, (errmsg("recovered replication state of node %d to %X/%X",
                             disk_state.roident, LSN_FORMAT_ARGS(disk_state.remote_lsn))));
    }

    // Verify file checksum
    FIN_CRC32C(crc);
    if (file_crc != crc)
        ereport(PANIC, (errmsg("replication checkpoint has wrong checksum")));

    // Close the file
    CloseTransientFile(fd);
}
```

Key simplifications made:
- Consolidated error handling with clearer messages
- Removed detailed error message variations for readability
- Simplified conditional logic while preserving functionality
- Added clear comments explaining each major operation
- Preserved essential CRC verification and state recovery logic
- Maintained proper error reporting for critical failures