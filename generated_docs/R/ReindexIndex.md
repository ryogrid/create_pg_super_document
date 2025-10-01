# ReindexIndex

## Location
[src/backend/commands/indexcmds.c:2788-2841](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L2788-L2841)

## Overview
ReindexIndex recreates a specific index by handling different reindex scenarios including partitioned indexes, concurrent reindexing, and regular reindexing based on the index characteristics and requested options.

## Definition

```c
struct ReindexIndexCallbackState state;
```
## Detailed Description
ReindexIndex implements single-index reindexing by first acquiring appropriate locks and determining the index's characteristics (persistence and kind). It then chooses one of three reindex paths:

1. **Partitioned indexes**: Delegates to ReindexPartitions for indexes on partitioned tables
2. **Concurrent reindexing**: Uses ReindexRelationConcurrently for non-temporary indexes when CONCURRENTLY option is specified
3. **Regular reindexing**: Uses reindex_index for standard reindexing operations

The function handles lock acquisition carefully to avoid deadlocks by using a callback mechanism that locks the underlying table first. For concurrent operations, it uses ShareUpdateExclusiveLock; otherwise, it uses AccessExclusiveLock. Temporary indexes are always reindexed non-concurrently regardless of the CONCURRENTLY option.

## Parameters / Member Variables
- : ReindexStmt containing the target index relation and command details
- : ReindexParams structure with reindexing options and settings
- : Boolean indicating if this is a top-level command

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVarGetRelidExtended](RangeVarGetRelidExtended.md) (resolves relation name to OID with locking)
  - [RangeVarCallbackForReindexIndex](RangeVarCallbackForReindexIndex.md) (callback for lock acquisition)
  - [get_rel_persistence](../g/get_rel_persistence.md) (determines if index is temporary/persistent)
  - [get_rel_relkind](../g/get_rel_relkind.md) (determines relation kind)
  - [ReindexPartitions](ReindexPartitions.md) (handles partitioned index reindexing)
  - ReindexRelationConcurrently (handles concurrent reindexing)
  - [reindex_index](../r/reindex_index.md) (performs the actual index rebuild)
  - Various lock types and constants (ShareUpdateExclusiveLock, AccessExclusiveLock, etc.)
- Called from:
  - [ExecReindex](../E/ExecReindex.md) (src/backend/commands/indexcmds.c:2755)

## Notes and Other Information
- This is a static function internal to indexcmds.c
- Uses different locking strategies based on whether concurrent reindexing is requested
- Temporary indexes cannot be reindexed concurrently and will use regular reindexing even if CONCURRENTLY is specified
- For regular reindexing, automatically adds REINDEXOPT_REPORT_PROGRESS to show progress information
- The callback mechanism prevents deadlocks by ensuring the table lock is acquired before the index lock
- Handles three distinct index types with appropriate specialized functions
- The lock level used must match what reindex_index() expects to avoid lock conflicts

## Simplified Source

```c
static void
ReindexIndex(const ReindexStmt *stmt, const ReindexParams *params, bool isTopLevel)
{
    const RangeVar *indexRelation = stmt->relation;
    struct ReindexIndexCallbackState state;
    Oid indOid;
    char persistence;
    char relkind;

    // Acquire appropriate lock on index (ShareUpdateExclusive for concurrent, AccessExclusive otherwise)
    // Use callback to lock table first to avoid deadlocks
    state.params = *params;
    state.locked_table_oid = InvalidOid;
    indOid = RangeVarGetRelidExtended(indexRelation,
                                     (params->options & REINDEXOPT_CONCURRENTLY) != 0 ?
                                     ShareUpdateExclusiveLock : AccessExclusiveLock,
                                     0,
                                     RangeVarCallbackForReindexIndex,
                                     &state);

    // Get index characteristics
    persistence = get_rel_persistence(indOid);
    relkind = get_rel_relkind(indOid);

    // Choose reindex strategy based on index type and options
    if (relkind == RELKIND_PARTITIONED_INDEX)
    {
        // Handle partitioned indexes
        ReindexPartitions(stmt, indOid, params, isTopLevel);
    }
    else if ((params->options & REINDEXOPT_CONCURRENTLY) != 0 &&
             persistence != RELPERSISTENCE_TEMP)
    {
        // Concurrent reindex for non-temporary indexes
        ReindexRelationConcurrently(stmt, indOid, params);
    }
    else
    {
        // Regular reindex (including temporary indexes)
        ReindexParams newparams = *params;
        newparams.options |= REINDEXOPT_REPORT_PROGRESS;
        reindex_index(stmt, indOid, false, persistence, &newparams);
    }
}
```