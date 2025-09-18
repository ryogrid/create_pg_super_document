# create_logical_replication_slots

## Location
src/bin/pg_upgrade/pg_upgrade.c: 929 - 979

## Overview
Restores logical replication slots in the new cluster during pg_upgrade by recreating them with their original configuration and properties.

## Definition
```c
static void create_logical_replication_slots(void)
```

## Detailed Description
The create_logical_replication_slots function is responsible for recreating logical replication slots during a PostgreSQL database upgrade. This function operates as part of the pg_upgrade process to ensure that logical replication configurations are preserved across cluster upgrades.

The function iterates through all databases in the old cluster and examines their logical replication slot configurations. For each database that contains logical replication slots, it connects to the corresponding database in the new cluster and recreates the slots using the pg_create_logical_replication_slot system function.

Each slot is recreated with its original properties including:
- Slot name
- Plugin name  
- Two-phase commit support flag
- Failover capability flag

The function uses a PQExpBuffer to construct dynamic SQL queries for each slot creation, ensuring proper escaping of slot names and plugin names through appendStringLiteralConn. It provides progress feedback during the restoration process and ensures proper cleanup of database connections and query buffers.

## Parameters / Member Variables
This function takes no parameters as it operates on global cluster information stored in the old_cluster structure.

## Dependencies
- Functions called/Symbols referenced:
  - prep_status_progress (progress reporting initiation)
  - connectToServer (database connection establishment)
  - pg_log (logging with PG_STATUS level)
  - createPQExpBuffer (query buffer creation)
  - appendPQExpBuffer (query string construction)
  - appendStringLiteralConn (safe string literal appending)
  - executeQueryOrDie (SQL execution)
  - resetPQExpBuffer (query buffer cleanup between iterations)
  - PQfinish (connection cleanup)
  - destroyPQExpBuffer (query buffer destruction)
  - end_progress_output (progress reporting completion)
  - check_ok (operation verification)
- Data structures used:
  - DbInfo (database information structure)
  - LogicalSlotInfoArr (logical slot information array)
  - LogicalSlotInfo (individual slot information)
- Called from:
  - main (in the pg_upgrade main execution flow)

## Notes and Other Information
- This function is only called when logical replication slots exist in the old cluster
- Skips databases that have no logical replication slots (nslots == 0)
- Creates slots with immediate=false parameter, meaning they start in a consistent state but may need to catch up
- Preserves two-phase commit and failover properties from the original slots
- Uses proper SQL escaping to handle slot names and plugin names that may contain special characters
- Provides database-level progress feedback during the restoration process
- Essential for maintaining logical replication configurations across major version upgrades
- Does not restore the actual slot position/LSN - slots will need to catch up from the current position after upgrade