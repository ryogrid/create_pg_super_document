# RemoteSlot

## Location
src/backend/replication/logical/slotsync.c: 135 - 148

## Overview
RemoteSlot is a structure that holds information about a logical replication slot fetched from the primary server, used during slot synchronization operations in PostgreSQL logical replication.

## Definition
```c
typedef struct RemoteSlot
{
    char       *name;
    char       *plugin;
    char       *database;
    bool        two_phase;
    bool        failover;
    XLogRecPtr  restart_lsn;
    XLogRecPtr  confirmed_lsn;
    TransactionId catalog_xmin;
    
    /* RS_INVAL_NONE if valid, or the reason of invalidation */
    ReplicationSlotInvalidationCause invalidated;
} RemoteSlot;
```

## Detailed Description
RemoteSlot represents the state and configuration of a logical replication slot as it exists on the primary server. This structure is used by the slot synchronization mechanism to transfer slot information from the primary to standby servers, ensuring that logical replication slots remain consistent across server promotions and failover scenarios.

The structure captures all essential attributes of a replication slot including its identity, configuration parameters, current positions in the WAL stream, and validity status. It serves as an intermediate representation during the synchronization process before the information is applied to local slot structures.

## Parameters / Member Variables
- `name`: Name identifier of the replication slot as defined on the primary server
- `plugin`: Name of the logical decoding output plugin used by this slot (e.g., 'pgoutput', 'test_decoding')
- `database`: Name of the database to which this logical replication slot belongs
- `two_phase`: Boolean flag indicating whether the slot supports two-phase commit transactions
- `failover`: Boolean flag indicating whether the slot should be synchronized during failover scenarios
- `restart_lsn`: WAL Log Sequence Number (LSN) from which WAL processing should restart for this slot
- `confirmed_lsn`: WAL LSN up to which all changes have been confirmed as processed by the subscriber
- `catalog_xmin`: Transaction ID representing the oldest transaction that this slot still needs for catalog lookups
- `invalidated`: Invalidation status of the slot (RS_INVAL_NONE if valid, or specific invalidation reason)

## Dependencies
- Functions called/Symbols referenced:
  - ReplicationSlotInvalidationCause (enumeration for slot invalidation reasons)
- Called from (representative examples):
  - update_local_synced_slot (updates local slots with remote slot information)
  - local_sync_slot_required (determines if slot synchronization is needed)
  - update_and_persist_local_synced_slot (persists synchronized slot data)
  - synchronize_one_slot (performs synchronization of a single slot)

## Notes and Other Information
- This structure is used exclusively during slot synchronization operations and represents transient data
- The invalidated field uses RS_INVAL_NONE to indicate a valid slot, with other values representing specific invalidation causes
- String fields (name, plugin, database) are dynamically allocated and must be properly managed for memory leaks
- The restart_lsn and confirmed_lsn fields are critical for maintaining WAL position consistency during synchronization
- The two_phase and failover flags control advanced replication features and determine sync behavior
- This structure is populated from query results when fetching slot information from the primary server