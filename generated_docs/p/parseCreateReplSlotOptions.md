# parseCreateReplSlotOptions

## Location
src/backend/replication/walsender.c: 1127 - 1203

## Overview
Parses and validates the optional parameters provided to the CREATE_REPLICATION_SLOT command, setting appropriate flags for snapshot handling, WAL reservation, two-phase commit support, and failover capabilities.

## Definition


## Detailed Description
parseCreateReplSlotOptions processes the optional parameters list from a CREATE_REPLICATION_SLOT command and validates them according to PostgreSQL's replication slot creation rules. The function:

1. **Option Validation**: Ensures options are appropriate for the specified replication kind (logical vs physical)
2. **Duplicate Detection**: Prevents conflicting or redundant options from being specified
3. **Parameter Parsing**: Converts string and boolean option values to appropriate internal representations
4. **Type-Specific Restrictions**: Enforces that certain options are only valid for specific replication slot types

The function supports several key options:
- **snapshot**: Controls snapshot export behavior for logical replication (export, nothing, use)
- **reserve_wal**: Determines whether to reserve WAL for physical replication slots
- **two_phase**: Enables two-phase commit support for logical replication
- **failover**: Enables failover support for logical replication slots

## Parameters / Member Variables
- : Pointer to CreateReplicationSlotCmd containing the slot creation command and options list
- : Output parameter set to true if WAL should be reserved (physical slots only)
- : Output parameter indicating the desired snapshot action (logical slots only)
- : Output parameter enabling two-phase commit support (logical slots only)
- : Output parameter enabling failover capability (logical slots only)

## Dependencies
- Functions called/Symbols referenced:
  - [defGetString](../d/defGetString.md)
  - [defGetBoolean](../d/defGetBoolean.md)
  - ereport
  - elog
- Data types/Constants referenced:
  - [CreateReplicationSlotCmd](../C/CreateReplicationSlotCmd.md)
  - [CRSSnapshotAction](../C/CRSSnapshotAction.md)
  - [DefElem](../D/DefElem.md)
  - REPLICATION_KIND_LOGICAL
  - REPLICATION_KIND_PHYSICAL
  - CRS_EXPORT_SNAPSHOT
  - CRS_NOEXPORT_SNAPSHOT
  - CRS_USE_SNAPSHOT
- Called from:
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md)

## Notes and Other Information
- The function is static and only used within the walsender module
- Enforces strict separation between physical and logical replication slot options
- [Snapshot](../S/Snapshot.md)-related options (snapshot, two_phase, failover) are only valid for logical replication slots
- The reserve_wal option is only valid for physical replication slots
- Provides detailed error messages for invalid option combinations or unrecognized parameter values
- Uses PostgreSQL's standard DefElem parsing utilities (defGetString, defGetBoolean)
- Part of the replication slot management infrastructure that supports both streaming and logical replication