# create_logical_replication_slot

## Location
src/bin/pg_basebackup/pg_createsubscriber.c: 1324 - 1372

## Overview
A static helper function that creates a new logical replication slot with specified parameters, handling the initialization of the logical decoding context and optionally finding the decoding start point.

## Definition


## Detailed Description
This function is a core helper for creating logical replication slots in PostgreSQL. It handles the complete initialization process including:

1. **Slot Creation**: Creates the replication slot using , initially as ephemeral (or temporary if specified) to handle errors gracefully during initialization.

2. **Decoding Context Initialization**: Creates a logical decoding context using  which validates the output plugin and sets up the necessary infrastructure for logical decoding.

3. **Start Point Discovery**: If  is true, it determines the appropriate decoding start point by calling , which can be a time-consuming operation.

4. **Cleanup**: Frees the decoding context after initialization is complete.

The function is designed with error safety in mind - slots are created as ephemeral initially so they will be automatically cleaned up if the transaction fails. The slot is made persistent only after successful initialization.

## Parameters / Member Variables
- : Name of the replication slot to create
- : Name of the logical decoding output plugin to use
- : If true, creates a temporary slot that will be dropped when the session ends
- : If true, enables two-phase commit support for the slot
- : If true, enables failover support for the slot
- : WAL location from which to start decoding (used when find_startpoint is false)
- : If true, automatically determines the decoding start point; if false, uses restart_lsn

## Dependencies
- Functions called/Symbols referenced:
  - ReplicationSlotCreate
  - CreateInitDecodingContext
  - DecodingContextFindStartpoint
  - FreeDecodingContext
  - LogicalDecodingContext (type)
  - RS_TEMPORARY, RS_EPHEMERAL (slot persistence types)
  - XL_ROUTINE, read_local_xlog_page, wal_segment_open, wal_segment_close (WAL reading functions)

- Called from (representative examples):
  - pg_create_logical_replication_slot
  - copy_replication_slot
  - setup_publisher (in pg_createsubscriber)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the slotfuncs.c file
- The function assumes that MyReplicationSlot is NULL (asserts this condition)
- When find_startpoint is false, the caller is responsible for setting the slot's confirmed_flush to a sensible value
- The function doesn't release the created slot - this is the caller's responsibility
- Plugin validation occurs during the CreateInitDecodingContext call, even when find_startpoint is false
- Error handling is built-in through the ephemeral slot creation strategy