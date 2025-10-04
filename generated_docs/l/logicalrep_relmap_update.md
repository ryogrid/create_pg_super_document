# logicalrep_relmap_update

## Location
[src/backend/replication/logical/relation.c:164-208](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/relation.c#L164-L208)

## Overview
Updates or creates a new entry in the logical replication relation map cache with the latest relation metadata received from the publisher.

## Definition
void logicalrep_relmap_update(LogicalRepRelation *remoterel)

## Detailed Description
This function maintains the logical replication relation map cache by adding new entries or updating existing ones with fresh relation metadata from the publisher. It ensures that the subscriber has an up-to-date view of the publisher's relation structure for proper data transformation during replication.

The function operates by first checking if the cache system is initialized, initializing it if necessary. It then uses hash table operations to either retrieve an existing entry or create a new one for the given remote relation ID. If an existing entry is found, it is properly cleaned up before being repopulated.

All relation metadata is deep-copied into the cache's memory context, including relation and namespace names, attribute information, and replica identity settings. This ensures cache persistence and proper memory management isolation.

The function handles the complete relation schema including:
- Remote relation identifier
- Namespace and relation names  
- Attribute count, names, and types
- Replica identity configuration
- Primary key/replica identity attribute bitmap

## Parameters / Member Variables
- : Pointer to LogicalRepRelation structure containing the relation metadata to cache

## Dependencies
- Functions called/Symbols referenced:
  - [logicalrep_relmap_init](logicalrep_relmap_init.md)
  - [hash_search](../h/hash_search.md)
  - [logicalrep_relmap_free_entry](logicalrep_relmap_free_entry.md)
  - [pstrdup](../p/pstrdup.md) (PostgreSQL string duplication)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - [bms_copy](../b/bms_copy.md) (bitmap set copy)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [LogicalRepRelation](../L/LogicalRepRelation.md) (relation metadata structure)
  - [LogicalRepRelMapEntry](../L/LogicalRepRelMapEntry.md) (cache entry structure)
  - HASH_ENTER (hash table operation flag)
- Called from (representative examples):
  - [copy_table](../c/copy_table.md)
  - [apply_handle_relation](../a/apply_handle_relation.md)

## Notes and Other Information
- This is a public function, accessible from other modules
- Performs lazy initialization of the cache system if not already done
- Uses proper memory context management to ensure cache data persistence
- Handles both new entries and updates to existing entries seamlessly
- Performs deep copying of all string and array data to prevent dangling pointers
- Critical for maintaining synchronization between publisher and subscriber schemas
- Part of PostgreSQL's logical replication infrastructure for handling schema changes

## Simplified Source

```c
void logicalrep_relmap_update(LogicalRepRelation *remoterel) {
    MemoryContext oldctx;
    LogicalRepRelMapEntry *entry;
    bool found;

    // Initialize cache if not already done
    if (LogicalRepRelMap == NULL)
        logicalrep_relmap_init();

    // Find existing entry or create new one
    entry = hash_search(LogicalRepRelMap, &remoterel->remoteid,
                        HASH_ENTER, &found);

    // Clean up existing entry if found
    if (found)
        logicalrep_relmap_free_entry(entry);

    // Clear entry structure
    memset(entry, 0, sizeof(LogicalRepRelMapEntry));

    // Switch to cache memory context for persistent storage
    oldctx = MemoryContextSwitchTo(LogicalRepRelMapContext);

    // Copy basic relation information
    entry->remoterel.remoteid = remoterel->remoteid;
    entry->remoterel.nspname = pstrdup(remoterel->nspname);
    entry->remoterel.relname = pstrdup(remoterel->relname);

    // Copy attribute information
    entry->remoterel.natts = remoterel->natts;
    entry->remoterel.attnames = palloc(remoterel->natts * sizeof(char *));
    entry->remoterel.atttyps = palloc(remoterel->natts * sizeof(Oid));

    for (int i = 0; i < remoterel->natts; i++) {
        entry->remoterel.attnames[i] = pstrdup(remoterel->attnames[i]);
        entry->remoterel.atttyps[i] = remoterel->atttyps[i];
    }

    // Copy replica identity information
    entry->remoterel.replident = remoterel->replident;
    entry->remoterel.attkeys = bms_copy(remoterel->attkeys);

    // Restore previous memory context
    MemoryContextSwitchTo(oldctx);
}
```