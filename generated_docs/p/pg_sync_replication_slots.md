# pg_sync_replication_slots

## Location
src/backend/replication/slotfuncs.c: 892 - 933

## Overview
A SQL-callable function that synchronizes failover-enabled replication slots from a primary server to a standby server.

## Definition


## Detailed Description
This function is a PostgreSQL system function that provides manual synchronization of replication slots from a primary server to a standby server. It is specifically designed for failover scenarios where standby servers need to maintain synchronized copies of replication slots to ensure seamless failover operations.

The function establishes a connection to the primary server using the configured primary connection information, then synchronizes all failover-enabled replication slots. This is particularly important in high-availability setups where logical replication subscribers need to continue operating after a failover.

## Parameters / Member Variables
- `fcinfo`: Function call information structure (no arguments required for this function)

## Dependencies
- Functions called/Symbols referenced:
  - CheckSlotPermissions: Validates user permissions for slot operations
  - RecoveryInProgress: Checks if the server is in recovery mode (standby)
  - ValidateSlotSyncParams: Validates slot synchronization parameters
  - load_file: Loads the libpqwalreceiver module
  - CheckAndGetDbnameFromConninfo: Validates primary connection information
  - walrcv_connect: Establishes connection to the primary server
  - SyncReplicationSlots: Performs the actual slot synchronization
  - walrcv_disconnect: Closes the connection to the primary server
- Called from (representative examples):
  - SQL interface as pg_sync_replication_slots function
  - Database administrators managing high-availability setups
  - Automated failover management systems

## Notes and Other Information
- This function can only be executed on a standby server (requires RecoveryInProgress() to be true)
- Requires appropriate permissions (checked by CheckSlotPermissions)
- Uses the primary_conninfo configuration to connect to the primary server
- Creates an application name for the connection based on cluster_name (either "<cluster_name>_slotsync" or "slotsync")
- Only synchronizes failover-enabled replication slots, not all replication slots
- Returns void - success is indicated by successful completion without error
- Part of PostgreSQL's high-availability and disaster recovery infrastructure
- Requires libpqwalreceiver to be available for primary server connection