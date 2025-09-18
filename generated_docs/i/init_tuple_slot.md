# init_tuple_slot

## Location
[src/backend/replication/pgoutput/pgoutput.c:1156-1247](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L1156-L1247)

## Overview
Initializes tuple table slots and attribute mapping for storing old and new tuple versions during logical replication in the pgoutput plugin.

## Definition


## Detailed Description
This function sets up the tuple storage infrastructure needed for logical replication by creating tuple table slots for both old and new tuple versions. It creates copies of the relation's tuple descriptor that will persist for the lifetime of the cache. When the relation is published under a different identity (ancestor relation), it also builds an attribute mapping between the actual relation and the published relation format. This mapping is essential for converting tuples from the actual relation format to the ancestor's format during replication. The tuple slots use heap tuple operations and are allocated in the plugin's cache memory context for longevity.

## Parameters / Member Variables
- : Pointer to PGOutputData structure containing the plugin's cache memory context
- : The actual relation being replicated
- : Pointer to RelationSyncEntry where the tuple slots and attribute mapping will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [CreateTupleDescCopyConstr](../C/CreateTupleDescCopyConstr.md)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [RelationIdGetRelation](../R/RelationIdGetRelation.md)
  - RelationGetDescr
  - RelationGetRelid
  - [build_attrmap_by_name_if_req](../b/build_attrmap_by_name_if_req.md)
  - [RelationClose](../R/RelationClose.md)
  - TTSOpsHeapTuple (tuple slot operations)
- Called from (representative examples):
  - [get_rel_sync_entry](../g/get_rel_sync_entry.md)

## Notes and Other Information
- Creates persistent tuple descriptors by copying with constraints to ensure they live as long as the cache
- Uses TTSOpsHeapTuple operations for both old and new tuple slots
- Attribute mapping is only created when publish_as_relid differs from the actual relation ID
- The attribute mapping handles cases where a partition is published as its parent table
- All memory allocations use the cache context to ensure proper lifetime management
- Static function only accessible within pgoutput.c
- Part of the lazy initialization infrastructure for relation synchronization entries
- Critical for maintaining tuple format consistency in inheritance-based replication scenarios