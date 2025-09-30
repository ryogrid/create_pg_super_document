# reindex_relation

## Location
[src/backend/catalog/index.c:3887-4057](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L3887-L4057)

## Overview
The  function recreates all indexes of a relation and optionally its associated toast table indexes, providing comprehensive index rebuilding with various control flags for different use cases.

## Definition

```c
bool
reindex_relation(const ReindexStmt *stmt, Oid relid, int flags,
				 const ReindexParams *params)
```
## Detailed Description
This function orchestrates the complete reindexing of all indexes belonging to a specified relation. It supports various operational modes controlled by flags, including processing toast table indexes, suppressing index usage during rebuilds, constraint checking control, and persistence management. The function is designed to handle complex scenarios like system catalog reindexing and post-operation cleanup after VACUUM FULL or CLUSTER operations.

Key operational aspects:
1. Opens and locks the target relation with ShareLock
2. Validates that the relation can be reindexed (not partitioned)
3. Retrieves the complete list of indexes for the relation
4. Optionally suppresses index usage during rebuild for consistency
5. Processes toast table indexes first to prevent corruption-related errors
6. Iterates through all indexes, calling reindex_index for each
7. Handles invalid toast indexes by skipping them with warnings
8. Updates progress reporting and maintains transaction consistency

## Parameters / Member Variables
- : Optional REINDEX statement for event trigger collection; can be NULL
- : Object identifier of the relation whose indexes should be reindexed
- : Bitmask controlling operation behavior with these possible values:
  - : Process associated toast table indexes
  - : Mark indexes as pending rebuild for consistency
  - : Enable uniqueness and exclusion constraint validation
  - : Set rebuilt indexes to unlogged persistence
  - : Set rebuilt indexes to permanent persistence
- : Reindex parameters including options and tablespace settings

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)/try_table_open: Opens the target relation
  - [RelationGetIndexList](../R/RelationGetIndexList.md): Retrieves list of all indexes for the relation
  - [SetReindexPending](../S/SetReindexPending.md): Marks indexes as pending rebuild
  - [RemoveReindexPending](../R/RemoveReindexPending.md): Removes indexes from pending rebuild list
  - [reindex_index](reindex_index.md): Performs individual index reconstruction
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md): Ensures transaction visibility
  - [IsToastNamespace](../I/IsToastNamespace.md): Checks if index belongs to toast table
  - [get_index_isvalid](../g/get_index_isvalid.md): Validates index state
  - [ReindexIsProcessingIndex](../R/ReindexIsProcessingIndex.md): Checks if index is currently being processed
- Called from (representative examples):
  - [ReindexTable](../R/ReindexTable.md): Direct command interface for REINDEX TABLE
  - [finish_heap_swap](../f/finish_heap_swap.md): Post-CLUSTER cleanup operations
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md): Index rebuilding after TRUNCATE
  - [ReindexMultipleInternal](../R/ReindexMultipleInternal.md): Batch reindexing operations

## Notes and Other Information
- Returns true if any indexes were successfully rebuilt
- Automatically handles toast table index processing with separate parameters
- Provides comprehensive error handling for invalid toast indexes
- Supports recursive calls for toast relation processing
- Maintains proper transaction boundaries with CommandCounterIncrement after each index
- Prevents reindexing of partitioned tables (which have no physical storage)
- Uses ShareLock on the parent relation to prevent schema/data changes
- Critical for system catalog maintenance and post-operation cleanup scenarios
- Progress reporting integration for long-running operations
- Handles persistence override scenarios for special operational requirements

## Simplified Source

```c
bool reindex_relation(const ReindexStmt *stmt, Oid relid, int flags,
                      const ReindexParams *params)
{
    // Open and lock the target relation
    Relation rel;
    if ((params->options & REINDEXOPT_MISSING_OK) != 0)
        rel = try_table_open(relid, ShareLock);
    else
        rel = table_open(relid, ShareLock);

    if (!rel)
        return false;

    // Validate relation type - partitioned tables are not supported
    if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
        elog(ERROR, "cannot reindex partitioned table \"%s.%s\"",
             get_namespace_name(RelationGetNamespace(rel)),
             RelationGetRelationName(rel));

    // Get all indexes for this relation
    List *indexIds = RelationGetIndexList(rel);
    Oid toast_relid = rel->rd_rel->reltoastrelid;

    // If suppressing index use during rebuild, mark indexes as pending
    if (flags & REINDEX_REL_SUPPRESS_INDEX_USE)
    {
        SetReindexPending(indexIds);
        CommandCounterIncrement();
    }

    bool result = false;

    // Reindex toast table first to prevent corruption errors
    if ((flags & REINDEX_REL_PROCESS_TOAST) && OidIsValid(toast_relid))
    {
        ReindexParams newparams = *params;
        newparams.options &= ~(REINDEXOPT_MISSING_OK);
        newparams.tablespaceOid = InvalidOid;
        result |= reindex_relation(stmt, toast_relid, flags, &newparams);
    }

    // Determine persistence for rebuilt indexes
    char persistence;
    if (flags & REINDEX_REL_FORCE_INDEXES_UNLOGGED)
        persistence = RELPERSISTENCE_UNLOGGED;
    else if (flags & REINDEX_REL_FORCE_INDEXES_PERMANENT)
        persistence = RELPERSISTENCE_PERMANENT;
    else
        persistence = rel->rd_rel->relpersistence;

    // Reindex each individual index
    int i = 1;
    ListCell *indexId;
    foreach(indexId, indexIds)
    {
        Oid indexOid = lfirst_oid(indexId);

        // Skip invalid toast indexes with warning
        if (IsToastNamespace(get_rel_namespace(indexOid)) &&
            !get_index_isvalid(indexOid))
        {
            ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("cannot reindex invalid index \"%s.%s\" on TOAST table, skipping",
                                    get_namespace_name(get_rel_namespace(indexOid)),
                                    get_rel_name(indexOid))));
            if (flags & REINDEX_REL_SUPPRESS_INDEX_USE)
                RemoveReindexPending(indexOid);
            continue;
        }

        // Rebuild the index
        reindex_index(stmt, indexOid, !(flags & REINDEX_REL_CHECK_CONSTRAINTS),
                      persistence, params);
        CommandCounterIncrement();

        // Update progress reporting
        pgstat_progress_update_param(PROGRESS_CLUSTER_INDEX_REBUILD_COUNT, i);
        i++;
    }

    table_close(rel, NoLock);
    return result | (indexIds != NIL);
}
```