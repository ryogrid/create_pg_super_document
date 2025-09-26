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
  - logicalrep_relmap_init
  - hash_search
  - logicalrep_relmap_free_entry
  - pstrdup (PostgreSQL string duplication)
  - palloc (PostgreSQL memory allocation)
  - bms_copy (bitmap set copy)
  - MemoryContextSwitchTo
  - LogicalRepRelation (relation metadata structure)
  - LogicalRepRelMapEntry (cache entry structure)
  - HASH_ENTER (hash table operation flag)
- Called from (representative examples):
  - copy_table
  - apply_handle_relation

## Notes and Other Information
- This is a public function, accessible from other modules
- Performs lazy initialization of the cache system if not already done
- Uses proper memory context management to ensure cache data persistence
- Handles both new entries and updates to existing entries seamlessly
- Performs deep copying of all string and array data to prevent dangling pointers
- Critical for maintaining synchronization between publisher and subscriber schemas
- Part of PostgreSQL's logical replication infrastructure for handling schema changes