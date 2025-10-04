# logicalrep_partition_open

## Location
[src/backend/replication/logical/relation.c:602-744](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/relation.c#L602-L744)

## Overview
Opens and manages cached logical replication relation map entries for table partitions, handling attribute mapping differences between partitions and their root tables.

## Definition
```c
LogicalRepRelMapEntry *logicalrep_partition_open(LogicalRepRelMapEntry *root,
                                                 Relation partrel, AttrMap *map)
```

## Detailed Description
This function creates or retrieves a cached relation map entry for a table partition in logical replication. It handles the complex task of mapping partition attributes to remote relation attributes, which can differ from the root table's mapping due to partition-specific column arrangements. The function performs deep copying of relation metadata to ensure independence from the root table's entry, which may be freed or rebuilt.

The function first checks for an existing cached entry and updates the local relation pointer if found. For new entries, it copies the remote relation metadata from the root entry and creates a new attribute map specific to the partition. The attribute mapping logic handles the conversion from tuple routing's 1-based attribute numbers to logical replication's 0-based numbering system.

## Parameters / Member Variables
- `root`: Pointer to the root table's LogicalRepRelMapEntry containing base relation information
- `partrel`: Relation pointer for the specific partition being opened
- `map`: AttrMap containing attribute number mappings from partition to root table (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [logicalrep_partmap_init](logicalrep_partmap_init.md)
  - [hash_search](../h/hash_search.md)
  - RelationGetRelid
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - memset
  - [free_attrmap](../f/free_attrmap.md)
  - [pstrdup](../p/pstrdup.md)
  - [palloc](../p/palloc.md)
  - [bms_copy](../b/bms_copy.md)
  - [make_attrmap](../m/make_attrmap.md)
  - memcpy
  - [logicalrep_rel_mark_updatable](logicalrep_rel_mark_updatable.md)
  - [FindLogicalRepLocalIndex](../F/FindLogicalRepLocalIndex.md)
- Types referenced:
  - [LogicalRepRelMapEntry](../L/LogicalRepRelMapEntry.md)
  - [LogicalRepPartMapEntry](../L/LogicalRepPartMapEntry.md)
  - [LogicalRepRelation](../L/LogicalRepRelation.md)
  - [AttrMap](../A/AttrMap.md)
  - [MemoryContext](../M/MemoryContext.md)
  - AttrNumber
- [Hash](../H/Hash.md) operation flags:
  - HASH_ENTER
- Called from (representative examples):
  - [apply_handle_tuple_routing](../a/apply_handle_tuple_routing.md)

## Notes and Other Information
- No corresponding close function exists as the caller handles closing the component relation
- Always updates entry->localrel with the latest partition Relation pointer to handle potential relation cache clearing
- Performs deep copying of remote relation metadata to ensure entry independence
- Handles attribute number conversion between tuple routing (1-based) and logical replication (0-based) systems
- Memory operations are performed in LogicalRepPartMapContext for proper memory management
- Uses FindLogicalRepLocalIndex in the original memory context to avoid memory leaks
- Sets localrelvalid to true to indicate the entry is current and valid
- The function is designed to handle both new partition entries and updates to existing cached entries

## Simplified Source

```c
LogicalRepRelMapEntry *
logicalrep_partition_open(LogicalRepRelMapEntry *root, Relation partrel, AttrMap *map)
{
    LogicalRepRelMapEntry *entry;
    LogicalRepPartMapEntry *part_entry;
    LogicalRepRelation *remoterel = &root->remoterel;
    Oid partOid = RelationGetRelid(partrel);
    AttrMap *attrmap = root->attrmap;
    bool found;
    MemoryContext oldctx;

    // Initialize partition map if needed
    if (LogicalRepPartMap == NULL)
        logicalrep_partmap_init();

    // Find or create partition entry
    part_entry = (LogicalRepPartMapEntry *) hash_search(LogicalRepPartMap, &partOid,
                                                       HASH_ENTER, &found);
    entry = &part_entry->relmapentry;

    // For existing valid entries, just update relation pointer
    if (found && entry->localrelvalid) {
        entry->localrel = partrel;
        return entry;
    }

    // Switch to persistent memory context
    oldctx = MemoryContextSwitchTo(LogicalRepPartMapContext);

    // Initialize new entry
    if (!found) {
        memset(part_entry, 0, sizeof(LogicalRepPartMapEntry));
        part_entry->partoid = partOid;
    }

    // Clean up old attribute map
    if (entry->attrmap) {
        free_attrmap(entry->attrmap);
        entry->attrmap = NULL;
    }

    // Copy remote relation info if not already done
    if (!entry->remoterel.remoteid) {
        entry->remoterel.remoteid = remoterel->remoteid;
        entry->remoterel.nspname = pstrdup(remoterel->nspname);
        entry->remoterel.relname = pstrdup(remoterel->relname);
        entry->remoterel.natts = remoterel->natts;

        // Copy attribute names and types
        entry->remoterel.attnames = palloc(remoterel->natts * sizeof(char *));
        entry->remoterel.atttyps = palloc(remoterel->natts * sizeof(Oid));
        for (int i = 0; i < remoterel->natts; i++) {
            entry->remoterel.attnames[i] = pstrdup(remoterel->attnames[i]);
            entry->remoterel.atttyps[i] = remoterel->atttyps[i];
        }

        entry->remoterel.replident = remoterel->replident;
        entry->remoterel.attkeys = bms_copy(remoterel->attkeys);
    }

    entry->localrel = partrel;
    entry->localreloid = partOid;

    // Create attribute mapping for partition
    if (map) {
        // Use provided partition-to-root mapping
        entry->attrmap = make_attrmap(map->maplen);
        for (AttrNumber attno = 0; attno < entry->attrmap->maplen; attno++) {
            AttrNumber root_attno = map->attnums[attno];
            if (root_attno == 0)  // dropped attribute
                entry->attrmap->attnums[attno] = -1;
            else
                entry->attrmap->attnums[attno] = attrmap->attnums[root_attno - 1];
        }
    } else {
        // Copy root table's attribute mapping directly
        entry->attrmap = make_attrmap(attrmap->maplen);
        memcpy(entry->attrmap->attnums, attrmap->attnums,
               attrmap->maplen * sizeof(AttrNumber));
    }

    // Check if partition supports UPDATE/DELETE operations
    logicalrep_rel_mark_updatable(entry);

    MemoryContextSwitchTo(oldctx);

    // Find appropriate index for replication operations
    entry->localindexoid = FindLogicalRepLocalIndex(partrel, remoterel, entry->attrmap);
    entry->localrelvalid = true;

    return entry;
}
```