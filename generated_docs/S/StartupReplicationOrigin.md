# StartupReplicationOrigin

## Location
[src/backend/replication/logical/origin.c:699-826](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L699-L826)

## Overview
Recovers replication replay status from checkpoint data saved earlier during startup, restoring the replication origin state from the persistent checkpoint file.

## Definition


## Detailed Description
StartupReplicationOrigin is responsible for recovering the replication state during PostgreSQL startup by reading from the "pg_logical/replorigin_checkpoint" file. This function is called only at startup and not during every checkpoint read during recovery (e.g., in Hot Standby or Point-in-Time Recovery from a base backup). The function validates the file magic number, reads individual replication states, verifies the checksum, and loads the data into shared memory (replication_states array). It handles various error conditions including file corruption, missing files, and configuration limits being exceeded.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - OpenTransientFile
  - read
  - CloseTransientFile
  - INIT_CRC32C
  - COMP_CRC32C
  - FIN_CRC32C
  - ereport (PANIC/LOG levels)
  - ReplicationStateOnDisk (struct)
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