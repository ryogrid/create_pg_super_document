# GetSystemIdentifier

## Location
[src/backend/access/transam/xlog.c:4523-4532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L4523-L4532)

## Overview
Returns the unique system identifier from the PostgreSQL control file, which uniquely identifies this database cluster.

## Definition
```c
uint64 GetSystemIdentifier(void)
```

## Detailed Description
GetSystemIdentifier is a simple accessor function that retrieves the system identifier from the PostgreSQL control file. The system identifier is a 64-bit unique value that is generated when a database cluster is initialized with initdb. This identifier remains constant for the lifetime of the database cluster and is used to ensure that various PostgreSQL components (like WAL files, backup manifests, and replication slots) belong to the correct database cluster.

The function simply returns the system_identifier field from the global ControlFile structure, which must be loaded and available when this function is called.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - ControlFile (global variable access)
  - Assert (assertion check)
- Called from (representative examples):
  - [InitializeBackupManifest](../I/InitializeBackupManifest.md)
  - [manifest_process_system_identifier](../m/manifest_process_system_identifier.md)
  - [ReplicationSlotNameForTablesync](../R/ReplicationSlotNameForTablesync.md)
  - [WalReceiverMain](../W/WalReceiverMain.md)
  - [IdentifySystem](../I/IdentifySystem.md)

## Notes and Other Information
- The function includes an assertion to ensure ControlFile is not NULL before accessing it
- This function is commonly used in backup, replication, and WAL-related operations to verify cluster identity
- The system identifier is critical for preventing data corruption by ensuring components from different clusters are not mixed
- Located in src/backend/access/transam/xlog.c:4523-4532