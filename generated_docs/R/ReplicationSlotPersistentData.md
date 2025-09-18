# ReplicationSlotPersistentData

## Location
src/include/replication/slot.h: 63 - 130

## Overview
ReplicationSlotPersistentData is a structure that represents the on-disk data of a replication slot that persists across PostgreSQL server restarts, containing all the essential information needed to maintain replication state continuity.

## Definition
```c
typedef struct ReplicationSlotPersistentData
{
    /* The slot's identifier */
    NameData    name;

    /* database the slot is active on */
    Oid         database;

    /*
     * The slot's behaviour when being dropped (or restored after a crash).
     */
    ReplicationSlotPersistency persistency;

    /*
     * xmin horizon for data
     *
     * NB: This may represent a value that hasn't been written to disk yet;
     * see notes for effective_xmin, below.
     */
    TransactionId xmin;

    /*
     * xmin horizon for catalog tuples
     *
     * NB: This may represent a value that hasn't been written to disk yet;
     * see notes for effective_xmin, below.
     */
    TransactionId catalog_xmin;

    /* oldest LSN that might be required by this replication slot */
    XLogRecPtr  restart_lsn;

    /* RS_INVAL_NONE if valid, or the reason for having been invalidated */
    ReplicationSlotInvalidationCause invalidated;

    /*
     * Oldest LSN that the client has acked receipt for.  This is used as the
     * start_lsn point in case the client doesn't specify one, and also as a
     * safety measure to jump forwards in case the client specifies a
     * start_lsn that's further in the past than this value.
     */
    XLogRecPtr  confirmed_flush;

    /*
     * LSN at which we enabled two_phase commit for this slot or LSN at which
     * we found a consistent point at the time of slot creation.
     */
    XLogRecPtr  two_phase_at;

    /*
     * Allow decoding of prepared transactions?
     */
    bool        two_phase;

    /* plugin name */
    NameData    plugin;

    /*
     * Was this slot synchronized from the primary server?
     */
    char        synced;

    /*
     * Is this a failover slot (sync candidate for standbys)? Only relevant
     * for logical slots on the primary server.
     */
    bool        failover;
} ReplicationSlotPersistentData;
```

## Detailed Description
This structure contains all the critical state information for a replication slot that must survive PostgreSQL server restarts. It serves as the persistent storage format for replication slots, ensuring that replication can resume correctly after crashes or planned shutdowns. The structure includes transaction horizons (xmin values), LSN positions for restart and confirmation points, slot metadata, and various flags controlling slot behavior.

## Parameters / Member Variables
- `name`: The unique identifier name of the replication slot
- `database`: The OID of the database this slot is associated with
- `persistency`: Defines the slot's behavior during drop/restore operations after crashes
- `xmin`: Transaction ID horizon for preventing VACUUM from removing required data tuples
- `catalog_xmin`: Transaction ID horizon specifically for catalog tuples (system tables)
- `restart_lsn`: The oldest WAL position that might be needed by this slot for replay
- `invalidated`: Status indicating if the slot is valid or the reason for invalidation
- `confirmed_flush`: Latest LSN confirmed as received by the client, used as fallback start position
- `two_phase_at`: LSN where two-phase commit was enabled or initial consistent point was found
- `two_phase`: Boolean flag indicating if prepared transactions should be decoded
- `plugin`: Name of the output plugin used for logical decoding
- `synced`: Character flag indicating if this slot was synchronized from a primary server
- `failover`: Boolean flag marking this as a failover slot for standby synchronization

## Dependencies
- Functions called/Symbols referenced:
  - [NameData](../N/NameData.md) (for name and plugin fields)
  - ReplicationSlotPersistency (for persistency behavior)
  - [ReplicationSlotInvalidationCause](ReplicationSlotInvalidationCause.md) (for invalidation status)
- Called from (representative examples):
  - [ReplicationSlotOnDisk](ReplicationSlotOnDisk.md)
  - [ReplicationSlotCreate](ReplicationSlotCreate.md)
  - [SaveSlotToPath](../S/SaveSlotToPath.md)
  - [RestoreSlotFromDisk](RestoreSlotFromDisk.md)
  - [ReplicationSlot](ReplicationSlot.md)

## Notes and Other Information
This structure is designed for disk serialization and must maintain binary compatibility across PostgreSQL versions for upgrade scenarios. The xmin and catalog_xmin values may represent uncommitted state that hasn't been written to disk yet, requiring careful handling during persistence operations. The structure supports both physical and logical replication scenarios, with logical-specific fields like plugin name and two-phase commit settings.