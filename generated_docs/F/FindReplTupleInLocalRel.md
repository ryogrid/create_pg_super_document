# FindReplTupleInLocalRel

## Location
src/backend/replication/logical/worker.c: 2861 - 2907

## Overview
Locates a tuple in the local relation that corresponds to a tuple received from the publication side, using replica identity indexes, primary keys, or sequential scans as appropriate.

## Definition


## Detailed Description
This function is responsible for finding the local tuple that corresponds to a remote tuple during logical replication operations (UPDATE and DELETE). The function employs different search strategies based on the available replica identity configuration:

1. **Privilege Checking**: First verifies SELECT privileges on the local relation since this operation performs a read
2. **Slot Creation**: Creates a new tuple table slot for the local relation to hold the found tuple
3. **Index Validation**: Asserts that either a valid local index OID is provided or the remote relation uses REPLICA_IDENTITY_FULL
4. **Search Strategy Selection**:
   - **Index-based Search**: If a valid index OID is provided, uses RelationFindReplTupleByIndex to perform an efficient index lookup with exclusive tuple locking
   - **Sequential Search**: If no index is available (REPLICA_IDENTITY_FULL case), falls back to RelationFindReplTupleSeq for a full table scan
5. **Debug Assertions**: In debug builds, validates that the provided index is either the replica identity index, primary key, or suitable for REPLICA_IDENTITY_FULL operations

The function returns a boolean indicating whether the tuple was found and populates the localslot parameter with the located tuple data.

## Parameters / Member Variables
- : ApplyExecutionData structure containing execution state and relation mapping information
- : Relation descriptor for the local table where the tuple should be found
- : LogicalRepRelation structure describing the remote relation's metadata
- : OID of the local index to use for tuple lookup (may be InvalidOid for sequential scans)
- : TupleTableSlot containing the search tuple data from the remote publisher
- : Output parameter - pointer to TupleTableSlot pointer that will be set to the found local tuple

## Dependencies
- Functions called/Symbols referenced:
  - TargetPrivilegesCheck
  - table_slot_create
  - index_open (debug builds only)
  - GetRelationIdentityOrPK (debug builds only)
  - IsIndexUsableForReplicaIdentityFull (debug builds only)
  - BuildIndexInfo (debug builds only)
  - index_close (debug builds only)
  - RelationFindReplTupleByIndex
  - RelationFindReplTupleSeq
- Called from (representative examples):
  - apply_handle_update_internal
  - apply_handle_delete_internal
  - apply_handle_tuple_routing

## Notes and Other Information
- The function always checks for SELECT privileges since it performs read operations, regardless of the higher-level operation (UPDATE or DELETE)
- The choice between index-based and sequential search depends on the availability of a valid index OID and the replica identity configuration
- When using index-based search, the function acquires LockTupleExclusive to prevent concurrent modifications
- Debug assertions ensure that the provided index is appropriate for the replica identity strategy being used
- For REPLICA_IDENTITY_FULL tables without a suitable index, the function falls back to sequential scanning, which can be expensive for large tables
- The function is a critical component in ensuring that logical replication can reliably locate target tuples across different replica identity configurations
- The localslot parameter is allocated by this function and must be managed by the caller