# ReindexPartitions

## Location
[src/backend/commands/indexcmds.c:3217-3310](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L3217-L3310)

## Overview
ReindexPartitions reindexes a set of partitions belonging to a partitioned table or partitioned index by processing each physical partition in separate transactions.

## Definition
```c
static void ReindexPartitions(const ReindexStmt *stmt, Oid relid, const ReindexParams *params, bool isTopLevel)
```

## Detailed Description
This function handles reindexing operations on partitioned relations by:

1. **Validation**: Verifies the target relation is partitioned using RELKIND_HAS_PARTITIONS macro
2. **Error Context Setup**: Establishes error callback context for enhanced error reporting during partition processing
3. **Transaction Block Prevention**: Ensures the operation cannot run within a user transaction block since it commits internally
4. **Partition Discovery**: Uses find_all_inheritors() to locate all partitions in the inheritance hierarchy
5. **Physical Partition Filtering**: Filters out partitioned tables/indexes and foreign tables, keeping only relations with physical storage (RELKIND_INDEX and RELKIND_RELATION)
6. **Memory Management**: Creates a separate memory context for cross-transaction storage of partition OIDs
7. **Delegated Processing**: Passes the filtered list to ReindexMultipleInternal() for actual reindexing

## Parameters / Member Variables
- `stmt`: ReindexStmt containing the reindex statement details
- `relid`: OID of the partitioned table or partitioned index to reindex
- `params`: ReindexParams specifying reindex options and parameters
- `isTopLevel`: Boolean indicating if this is a top-level operation (affects transaction block prevention)

## Dependencies
- Functions called/Symbols referenced:
  - [get_rel_relkind](../g/get_rel_relkind.md)
  - [get_rel_name](../g/get_rel_name.md)  
  - [get_rel_namespace](../g/get_rel_namespace.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - [reindex_error_callback](../r/reindex_error_callback.md)
  - [PreventInTransactionBlock](../P/PreventInTransactionBlock.md)
  - AllocSetContextCreate
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [ReindexMultipleInternal](ReindexMultipleInternal.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from:
  - [ReindexIndex](ReindexIndex.md)
  - [ReindexTable](ReindexTable.md)

## Notes and Other Information
- The function uses ShareLock to prevent schema modifications during partition discovery
- Only processes partitions with physical storage (excludes partitioned tables/indexes and foreign tables)
- Each partition is processed in a separate transaction to reduce deadlock risk and enable immediate lock release
- Error context includes qualified relation name for precise error identification
- Memory context management ensures cleanup even in error scenarios since it"s a child of PortalContext
- The function specifically handles both partitioned tables (REINDEX TABLE) and partitioned indexes (REINDEX INDEX) scenarios in transaction block prevention

## Simplified Source

```c
static void
ReindexPartitions(const ReindexStmt *stmt, Oid relid, const ReindexParams *params, bool isTopLevel)
{
    List *partitions = NIL;
    char relkind = get_rel_relkind(relid);
    char *relname = get_rel_name(relid);
    char *relnamespace = get_namespace_name(get_rel_namespace(relid));
    MemoryContext reindex_context;
    List *inhoids;
    ErrorContextCallback errcallback;
    ReindexErrorInfo errinfo;

    Assert(RELKIND_HAS_PARTITIONS(relkind));

    // Set up error context for better error reporting
    errinfo.relname = pstrdup(relname);
    errinfo.relnamespace = pstrdup(relnamespace);
    errinfo.relkind = relkind;
    errcallback.callback = reindex_error_callback;
    errcallback.arg = (void *) &errinfo;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;

    // Prevent running in transaction block since we commit internally
    PreventInTransactionBlock(isTopLevel,
                              relkind == RELKIND_PARTITIONED_TABLE ?
                              "REINDEX TABLE" : "REINDEX INDEX");

    // Pop error context
    error_context_stack = errcallback.previous;

    // Create memory context for cross-transaction storage
    reindex_context = AllocSetContextCreate(PortalContext, "Reindex",
                                            ALLOCSET_DEFAULT_SIZES);

    // Find all partitions with ShareLock to prevent schema changes
    inhoids = find_all_inheritors(relid, ShareLock, NULL);

    // Filter to only physical partitions (exclude partitioned and foreign tables)
    foreach(lc, inhoids)
    {
        Oid partoid = lfirst_oid(lc);
        char partkind = get_rel_relkind(partoid);
        MemoryContext old_context;

        // Keep only relations with physical storage
        if (!RELKIND_HAS_STORAGE(partkind))
            continue;

        Assert(partkind == RELKIND_INDEX || partkind == RELKIND_RELATION);

        // Save partition OID in cross-transaction context
        old_context = MemoryContextSwitchTo(reindex_context);
        partitions = lappend_oid(partitions, partoid);
        MemoryContextSwitchTo(old_context);
    }

    // Process partitions in separate transactions
    ReindexMultipleInternal(stmt, partitions, params);

    // Clean up memory context
    MemoryContextDelete(reindex_context);
}
```