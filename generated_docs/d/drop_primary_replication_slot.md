# drop_primary_replication_slot

## Location
src/bin/pg_basebackup/pg_createsubscriber.c: 1252 - 1281

## Overview
Drops the physical replication slot on the primary server that was previously used by the standby, as it becomes unnecessary after the standby-to-subscriber transformation.

## Definition
```c
static void drop_primary_replication_slot(struct LogicalRepInfo *dbinfo, const char *slotname)
```

## Detailed Description
This function performs cleanup by removing the physical replication slot on the primary server that was used for streaming replication to the standby. After converting the standby to a logical subscriber, this physical slot is no longer needed and should be dropped to prevent unnecessary retention of WAL files. The function attempts to connect to the primary server and drop the slot, but provides graceful error handling - if the connection fails, it issues warnings rather than failing the entire conversion process, allowing the user to manually clean up the slot later.

## Parameters / Member Variables
- `dbinfo`: Array of LogicalRepInfo structures containing database and connection information (uses the first element for publisher/primary connection info)
- `slotname`: Name of the physical replication slot to be dropped on the primary server

## Dependencies
- Functions called/Symbols referenced:
  - connect_database (connects to primary server, with non-fatal connection flag)
  - [drop_replication_slot](drop_replication_slot.md) (performs the actual slot deletion)
  - [disconnect_database](disconnect_database.md) (closes the connection after slot deletion)
  - pg_log_warning (logs warning messages for connection failures)
  - pg_log_warning_hint (provides helpful hints for manual cleanup)
- Called from:
  - [main](../m/main.md) (primary entry point of pg_createsubscriber utility)

## Notes and Other Information
- This is a static function, only accessible within pg_createsubscriber.c
- Uses global variable primary_slot_name to check if a slot exists
- Designed to be non-fatal - connection failures result in warnings rather than errors
- Critical for preventing WAL file accumulation on the primary server
- Part of the cleanup phase in the standby-to-subscriber conversion workflow
- Provides user guidance through warning hints if automatic cleanup fails
- Uses non-fatal connection mode to gracefully handle primary server unavailability