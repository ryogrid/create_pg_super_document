# register_on_commit_action

## Location
[src/backend/commands/tablecmds.c:17522-17557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L17522-L17557)

## Overview
Registers an ON COMMIT action for a newly-created temporary table, storing the action in a backend-local data structure for execution at transaction commit.

## Definition
```c
void register_on_commit_action(Oid relid, OnCommitAction action)
```

## Detailed Description
This function implements the registration mechanism for ON COMMIT actions on temporary tables, supporting CREATE TEMP TABLE statements with ON COMMIT DROP, DELETE ROWS, or PRESERVE ROWS clauses. It creates an OnCommitItem structure in cache memory context to track the relation and its associated action, along with subtransaction information for proper cleanup. The function optimizes by only registering relations that require actual commit-time processing.

## Parameters / Member Variables
- `relid`: Object identifier of the temporary table requiring ON COMMIT processing
- `action`: The specific ON COMMIT action to perform (DROP, DELETE ROWS, PRESERVE ROWS, or NOOP)

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentSubTransactionId](../G/GetCurrentSubTransactionId.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc](../p/palloc.md)
  - [lcons](../l/lcons.md)
- Called from (representative examples):
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md)

## Notes and Other Information
- Only processes actions that require commit-time handling (excludes NOOP and PRESERVE ROWS)
- Uses CacheMemoryContext to ensure registrations survive across transaction boundaries
- Processes actions in reverse registration order using lcons for list prepending
- Tracks subtransaction IDs for proper cleanup in case of subtransaction rollback
- [Backend](../B/Backend.md)-local storage is sufficient since temp tables are session-specific
- Critical component of PostgreSQL's temporary table lifecycle management

## Simplified Source

```c
void register_on_commit_action(Oid relid, OnCommitAction action) {
    OnCommitItem *oc;
    MemoryContext oldcxt;

    // Skip registration for actions that don't need commit-time processing
    if (action == ONCOMMIT_NOOP || action == ONCOMMIT_PRESERVE_ROWS)
        return;

    // Switch to cache context to survive transaction boundaries
    oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

    // Create and initialize the commit item
    oc = palloc(sizeof(OnCommitItem));
    oc->relid = relid;
    oc->oncommit = action;
    oc->creating_subid = GetCurrentSubTransactionId();
    oc->deleting_subid = InvalidSubTransactionId;

    // Add to front of list (reverse processing order)
    on_commits = lcons(oc, on_commits);

    MemoryContextSwitchTo(oldcxt);
}
```