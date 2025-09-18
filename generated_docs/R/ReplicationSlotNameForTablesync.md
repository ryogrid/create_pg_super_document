# ReplicationSlotNameForTablesync

## Location
src/backend/replication/logical/tablesync.c: 1293 - 1308

## Overview
ReplicationSlotNameForTablesync generates a standardized replication slot name for table synchronization during logical replication, ensuring uniqueness across different subscriptions and clusters.

## Definition
```c
void ReplicationSlotNameForTablesync(Oid suboid, Oid relid, char *syncslotname, Size szslot)
```

## Detailed Description
This function constructs a unique replication slot name specifically for table synchronization operations in logical replication. The naming scheme follows the pattern "pg_{suboid}_sync_{relid}_{system_identifier}" to ensure global uniqueness and avoid collisions between different subscriptions or clusters.

The function is designed with careful consideration of PostgreSQL's NAMEDATALEN constraint (typically 64 characters) and remote node limitations on slot name length. The current naming scheme produces slot names with a maximum length of 50 characters, providing a safe margin below the limit.

The system identifier is appended to prevent slot name collisions between subscriptions in different PostgreSQL clusters, which is crucial for environments where multiple clusters might have overlapping subscription and relation OIDs.

## Parameters / Member Variables
- `suboid`: The OID of the subscription that owns this table synchronization slot
- `relid`: The OID of the relation (table) being synchronized
- `syncslotname`: Output buffer where the generated slot name will be stored
- `szslot`: Size of the syncslotname buffer to prevent buffer overflow

## Dependencies
- Functions called/Symbols referenced:
  - [GetSystemIdentifier](../G/GetSystemIdentifier.md)
  - UINT64_FORMAT (macro for formatting 64-bit integers)
- Called from (representative examples):
  - [DropSubscription](../D/DropSubscription.md)
  - [ReportSlotConnectionError](ReportSlotConnectionError.md)
  - [process_syncing_tables_for_sync](../p/process_syncing_tables_for_sync.md)
  - [LogicalRepSyncTableStart](../L/LogicalRepSyncTableStart.md)

## Notes and Other Information
- The function deliberately avoids using the subscription slot name as part of the tablesync slot name to ensure cleanup operations remain possible even if the subscription slot name changes
- The naming scheme is optimized to stay well under the NAMEDATALEN limit while maintaining uniqueness
- This is a utility function that does not perform any database operations itself, only string formatting
- The generated slot names are used for temporary replication slots created during initial table synchronization