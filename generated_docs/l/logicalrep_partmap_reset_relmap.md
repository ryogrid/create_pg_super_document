# logicalrep_partmap_reset_relmap

## Location
src/backend/replication/logical/relation.c: 540 - 566

## Overview
Resets partition map entries that reference a specific remote relation when the publisher sends new relation mapping information.

## Definition
```c
void logicalrep_partmap_reset_relmap(LogicalRepRelation *remoterel)
```

## Detailed Description
This function iterates through the logical replication partition map and resets all entries that reference the specified remote relation. It is called when the publisher sends updated relation mapping to ensure the subscriber's view of the partition structure aligns with the publisher's current state. The function clears existing partition map entries but defers updating the remoterel information until `logicalrep_partition_open` is called, optimizing performance by avoiding unnecessary work during the reset phase.

The function uses hash table iteration to find matching entries based on the remote relation ID, frees the associated resources for each matching entry, and zeros out the entry structure to prepare it for reuse.

## Parameters / Member Variables
- `remoterel`: Pointer to the LogicalRepRelation structure representing the remote relation whose partition map entries should be reset

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)  
  - logicalrep_relmap_free_entry
  - memset
- Types referenced:
  - [LogicalRepRelation](../L/LogicalRepRelation.md)
  - HASH_SEQ_STATUS
  - [LogicalRepPartMapEntry](../L/LogicalRepPartMapEntry.md)
  - [LogicalRepRelMapEntry](../L/LogicalRepRelMapEntry.md)
- Called from (representative examples):
  - [apply_handle_relation](../a/apply_handle_relation.md)

## Notes and Other Information
- The function safely handles the case where LogicalRepPartMap is NULL by returning early
- Memory cleanup is properly handled through logicalrep_relmap_free_entry before zeroing the entry
- This is part of the logical replication subsystem that maintains partition mapping between publisher and subscriber
- The reset operation is designed to be efficient by deferring relation information updates to the actual partition open operation