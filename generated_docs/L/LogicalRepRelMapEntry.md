# LogicalRepRelMapEntry

## Location
src/include/replication/logicalrelation.h: 19 - 40

## Overview
LogicalRepRelMapEntry is a structure that represents an entry in the logical replication relation map, maintaining the mapping between remote and local relations along with their metadata and synchronization state.

## Definition
```c
typedef struct LogicalRepRelMapEntry
{
    LogicalRepRelation remoterel;   /* key is remoterel.remoteid */

    /*
     * Validity flag -- when false, revalidate all derived info at next
     * logicalrep_rel_open.  (While the localrel is open, we assume our lock
     * on that rel ensures the info remains good.)
     */
    bool        localrelvalid;

    /* Mapping to local relation. */
    Oid         localreloid;    /* local relation id */
    Relation    localrel;       /* relcache entry (NULL when closed) */
    AttrMap    *attrmap;        /* map of local attributes to remote ones */
    bool        updatable;      /* Can apply updates/deletes? */
    Oid         localindexoid;  /* which index to use, or InvalidOid if none */

    /* Sync state. */
    char        state;
    XLogRecPtr  statelsn;
} LogicalRepRelMapEntry;
```

## Detailed Description
LogicalRepRelMapEntry serves as a cache entry in the logical replication system that maps remote relations (from the publication side) to local relations (on the subscription side). This structure is essential for maintaining the correspondence between publisher and subscriber tables during logical replication operations.

The structure stores both the remote relation metadata and the local relation information, including attribute mappings that handle differences in column ordering or dropped columns between the remote and local relations. It also tracks the synchronization state and whether the relation supports updates and deletes.

The entry includes validation mechanisms to ensure that cached information remains accurate, with a validity flag that triggers revalidation when the local relation might have changed. This is crucial for maintaining consistency during long-running replication sessions.

## Parameters / Member Variables
- `remoterel`: LogicalRepRelation structure containing metadata about the remote relation (schema name, relation name, column information, replica identity, etc.)
- `localrelvalid`: Boolean flag indicating whether the cached local relation information is still valid; when false, triggers revalidation at the next logicalrep_rel_open call
- `localreloid`: OID of the corresponding local relation in the subscriber database
- `localrel`: Relation cache entry for the local relation; set to NULL when the relation is closed to free resources
- `attrmap`: Pointer to AttrMap structure that maps local relation attributes to remote relation attributes, handling differences in column ordering
- `updatable`: Boolean flag indicating whether this relation can accept UPDATE and DELETE operations (depends on replica identity and key availability)
- `localindexoid`: OID of the local index to use for finding rows during UPDATE/DELETE operations, or InvalidOid if no suitable index exists
- `state`: Character representing the synchronization state of this relation in the logical replication process
- `statelsn`: XLogRecPtr indicating the LSN position associated with the current synchronization state

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalRepRelation](LogicalRepRelation.md)
  - [AttrMap](../A/AttrMap.md)
- Called from (representative examples):
  - logicalrep_relmap_init
  - logicalrep_relmap_update
  - logicalrep_rel_open
  - logicalrep_rel_close
  - logicalrep_rel_mark_updatable
  - [apply_handle_insert](../a/apply_handle_insert.md)
  - [apply_handle_update](../a/apply_handle_update.md)
  - [apply_handle_delete](../a/apply_handle_delete.md)
  - [apply_handle_truncate](../a/apply_handle_truncate.md)

## Notes and Other Information
- This structure is primarily used in src/backend/replication/logical/relation.c for managing relation mappings in logical replication
- The validity mechanism helps optimize performance by avoiding unnecessary relation lookups while ensuring data consistency
- The attrmap field is crucial for handling schema differences between publisher and subscriber, such as different column orders or dropped columns
- The updatable flag and localindexoid work together to determine if and how UPDATE/DELETE operations can be applied to the local relation
- The synchronization state fields (state and statelsn) are used during initial table synchronization in logical replication to track progress
- Memory management for this structure is handled by the logical replication subsystem, with proper cleanup when relations are closed or invalidated