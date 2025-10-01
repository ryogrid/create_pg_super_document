# remove_on_commit_action

## Location
[src/backend/commands/tablecmds.c:17558-17580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L17558-L17580)

## Overview
Unregisters an ON COMMIT action for a relation being deleted by marking the corresponding OnCommitItem entry for deletion after commit.

## Definition
```c
void remove_on_commit_action(Oid relid)
```

## Detailed Description
This function handles the cleanup of ON COMMIT action registrations when a temporary table is dropped. Rather than immediately removing the OnCommitItem from the list, it marks the entry for deletion by setting the deleting_subid field to the current subtransaction ID. This deferred deletion approach ensures proper handling in case the current subtransaction rolls back, allowing the ON COMMIT action to remain registered if the drop operation is aborted.

## Parameters / Member Variables
- `relid`: Object identifier of the relation whose ON COMMIT action should be removed

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentSubTransactionId](../G/GetCurrentSubTransactionId.md)
  - lfirst (list iteration macro)
- Called from (representative examples):
  - [heap_drop_with_catalog](../h/heap_drop_with_catalog.md)

## Notes and Other Information
- Uses lazy deletion by marking entries rather than immediate removal
- Integrates with subtransaction system for proper rollback handling
- Searches the on_commits list linearly to find the matching relation
- Breaks after finding the first match, assuming one entry per relation
- Essential for cleanup when temporary tables with ON COMMIT actions are dropped
- Prevents orphaned ON COMMIT registrations that could cause errors at commit time

## Simplified Source

```c
void remove_on_commit_action(Oid relid)
{
    ListCell *l;

    // Search through registered ON COMMIT actions
    foreach(l, on_commits)
    {
        OnCommitItem *oc = (OnCommitItem *) lfirst(l);

        // Mark matching entry for deletion after commit
        if (oc->relid == relid)
        {
            oc->deleting_subid = GetCurrentSubTransactionId();
            break;
        }
    }
}
```