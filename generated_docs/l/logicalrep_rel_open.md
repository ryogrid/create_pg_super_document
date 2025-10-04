# logicalrep_rel_open

## Location
[src/backend/replication/logical/relation.c:327-472](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/relation.c#L327-L472)

## Overview
Opens the local relation associated with a remote relation in logical replication, establishing and maintaining the mapping between local and remote relation attributes while handling cache invalidation and relation validation.

## Definition
```c
LogicalRepRelMapEntry *logicalrep_rel_open(LogicalRepRelId remoteid, LOCKMODE lockmode)
```

## Detailed Description
This function is the primary entry point for opening local relations that correspond to remote relations in logical replication. It manages a complex process of relation lookup, validation, attribute mapping, and cache maintenance. The function handles both initial opens and re-opens after cache invalidation due to DDL operations.

The function first searches for an existing entry in the logical replication relation map. If the entry is marked as valid, it attempts to open the relation by OID to check if invalidation has occurred. If the entry is invalid or becomes invalidated, it rebuilds all derived data by re-opening the relation by name, reconstructing attribute mappings, validating replica identity compatibility, and finding appropriate indexes for replication operations.

Key responsibilities include ensuring no relation reference leaks, handling relation renames/drops gracefully, building attribute mappings between local and remote relations, validating that all replicated columns are present locally, checking replica identity compatibility for UPDATE/DELETE operations, and maintaining subscription relation state information.

## Parameters / Member Variables
- `remoteid`: LogicalRepRelId identifying the remote relation to be opened
- `lockmode`: LOCKMODE specifying the type of lock to acquire on the local relation

## Dependencies
- Functions called/Symbols referenced:
  - [logicalrep_relmap_init](logicalrep_relmap_init.md): Initializes the logical replication relation map if not already done
  - [hash_search](../h/hash_search.md): Searches for existing entries in the relation map hash table
  - [try_table_open](../t/try_table_open.md): Attempts to open a table by OID, returning NULL if it fails
  - [table_open](../t/table_open.md): Opens a table by OID with the assumption it exists
  - [table_close](../t/table_close.md): Closes a previously opened table relation
  - [free_attrmap](../f/free_attrmap.md): Deallocates attribute mapping structures
  - [make_attrmap](../m/make_attrmap.md): Creates new attribute mapping structures
  - RangeVarGetRelid: Resolves relation name to OID
  - [makeRangeVar](../m/makeRangeVar.md): Creates a RangeVar structure from namespace and relation names
  - [CheckSubscriptionRelkind](../C/CheckSubscriptionRelkind.md): Validates that the relation kind is supported for subscriptions
  - [logicalrep_rel_att_by_name](logicalrep_rel_att_by_name.md): Finds remote attribute number by name
  - [logicalrep_report_missing_attrs](logicalrep_report_missing_attrs.md): Reports errors for missing local attributes
  - [logicalrep_rel_mark_updatable](logicalrep_rel_mark_updatable.md): Determines if relation supports UPDATE/DELETE operations
  - [FindLogicalRepLocalIndex](../F/FindLogicalRepLocalIndex.md): Finds appropriate local index for replication operations
  - [GetSubscriptionRelState](../G/GetSubscriptionRelState.md): Retrieves current subscription relation state
  - [bms_add_range](../b/bms_add_range.md): Adds range of members to bitmap set
  - [bms_del_member](../b/bms_del_member.md): Removes member from bitmap set
  - [bms_free](../b/bms_free.md): Deallocates bitmap set
- Called from (representative examples):
  - [copy_table](../c/copy_table.md): Initial table synchronization operations
  - [apply_handle_insert](../a/apply_handle_insert.md): Processing INSERT operations from logical replication stream
  - [apply_handle_update](../a/apply_handle_update.md): Processing UPDATE operations from logical replication stream
  - [apply_handle_delete](../a/apply_handle_delete.md): Processing DELETE operations from logical replication stream
  - [apply_handle_truncate](../a/apply_handle_truncate.md): Processing TRUNCATE operations from logical replication stream

## Notes and Other Information
- This function is critical for logical replication performance as it manages relation cache entries
- Handles graceful recovery from DDL operations that invalidate relation cache entries
- Implements a two-phase approach: first try by OID (fast path), then by name (rebuild path)
- The function prevents relation reference leaks by ensuring only one open relation per entry
- Memory management is handled carefully with proper context switching for persistent allocations
- Supports both regular tables and partitioned tables through the same interface
- The attribute mapping built by this function is essential for translating between local and remote tuple formats
- [Relation](../R/Relation.md) state management integrates with the subscription system to track synchronization progress

## Simplified Source

```c
LogicalRepRelMapEntry *
logicalrep_rel_open(LogicalRepRelId remoteid, LOCKMODE lockmode)
{
    LogicalRepRelMapEntry *entry;
    bool found;
    LogicalRepRelation *remoterel;

    // Initialize relation map if needed
    if (LogicalRepRelMap == NULL)
        logicalrep_relmap_init();

    // Find existing entry or error if not found
    entry = hash_search(LogicalRepRelMap, &remoteid, HASH_FIND, &found);
    if (!found)
        elog(ERROR, "no relation map entry for remote relation ID %u", remoteid);

    remoterel = &entry->remoterel;

    // Prevent relation reference leaks
    if (entry->localrel)
        elog(ERROR, "remote relation ID %u is already open", remoteid);

    // Try to open by OID if entry is valid (fast path)
    if (entry->localrelvalid) {
        entry->localrel = try_table_open(entry->localreloid, lockmode);
        if (!entry->localrel) {
            entry->localrelvalid = false;  // Table renamed/dropped
        } else if (!entry->localrelvalid) {
            // Invalidated during open, close and retry
            table_close(entry->localrel, lockmode);
            entry->localrel = NULL;
        }
    }

    // Rebuild relation mapping if invalid (slow path)
    if (!entry->localrelvalid) {
        Oid relid;
        TupleDesc desc;
        int i;
        Bitmapset *missingatts;

        // Clean up old attribute map
        if (entry->attrmap) {
            free_attrmap(entry->attrmap);
            entry->attrmap = NULL;
        }

        // Open relation by name
        relid = RangeVarGetRelid(makeRangeVar(remoterel->nspname,
                                            remoterel->relname, -1),
                               lockmode, true);
        if (!OidIsValid(relid))
            ereport(ERROR, "logical replication target relation does not exist");

        entry->localrel = table_open(relid, NoLock);
        entry->localreloid = relid;

        // Validate relation kind
        CheckSubscriptionRelkind(entry->localrel->rd_rel->relkind,
                               remoterel->nspname, remoterel->relname);

        // Build attribute mapping between local and remote
        desc = RelationGetDescr(entry->localrel);
        entry->attrmap = make_attrmap(desc->natts);

        missingatts = bms_add_range(NULL, 0, remoterel->natts - 1);
        for (i = 0; i < desc->natts; i++) {
            Form_pg_attribute attr = TupleDescAttr(desc, i);
            int attnum;

            if (attr->attisdropped || attr->attgenerated) {
                entry->attrmap->attnums[i] = -1;
                continue;
            }

            // Map local attribute to remote attribute by name
            attnum = logicalrep_rel_att_by_name(remoterel, NameStr(attr->attname));
            entry->attrmap->attnums[i] = attnum;
            if (attnum >= 0)
                missingatts = bms_del_member(missingatts, attnum);
        }

        // Report any missing attributes
        logicalrep_report_missing_attrs(remoterel, missingatts);
        bms_free(missingatts);

        // Check if relation supports UPDATE/DELETE operations
        logicalrep_rel_mark_updatable(entry);

        // Find appropriate index for replication
        entry->localindexoid = FindLogicalRepLocalIndex(entry->localrel,
                                                      remoterel, entry->attrmap);
        entry->localrelvalid = true;
    }

    // Update subscription relation state if needed
    if (entry->state != SUBREL_STATE_READY)
        entry->state = GetSubscriptionRelState(MySubscription->oid,
                                             entry->localreloid, &entry->statelsn);

    return entry;
}
```