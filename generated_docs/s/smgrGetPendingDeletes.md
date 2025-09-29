# smgrGetPendingDeletes

## Location
[src/backend/catalog/storage.c:877-917](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/storage.c#L877-L917)

## Overview
smgrGetPendingDeletes returns a list of non-temporary relations scheduled for deletion at the current transaction nesting level.

## Definition
```c
int smgrGetPendingDeletes(bool forCommit, RelFileLocator **ptr)
```

## Detailed Description
This function extracts information about relations that are scheduled for deletion, filtering them based on the commit/abort context and transaction nesting level. It specifically excludes temporary relations (identified by INVALID_PROC_NUMBER) since they are handled separately and don't need to be included in two-phase commit state files or WAL records.

The function performs two passes: first counting the qualifying relations, then allocating memory and copying the RelFileLocator structures. It only includes relations at the current or deeper transaction nesting levels, ensuring that upper-level transaction deletions are not processed prematurely.

## Parameters / Member Variables
- `forCommit`: Boolean indicating whether to return relations scheduled for commit (true) or abort (false)
- `ptr`: Output parameter that will point to a newly allocated array of RelFileLocator structures, or NULL if no relations qualify

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md)
  - [palloc](../p/palloc.md)
- Called from (representative examples):
  - [StartPrepare](../S/StartPrepare.md)
  - [RecordTransactionCommit](../R/RecordTransactionCommit.md)
  - [RecordTransactionAbort](../R/RecordTransactionAbort.md)

## Notes and Other Information
- Returns the count of relations as the function return value
- Only non-temporary relations are included (procNumber must be INVALID_PROC_NUMBER)
- Used in contexts where temporary relations don't matter: two-phase commit preparation and WAL logging
- Memory for the returned array is allocated with palloc() and should be freed by the caller
- Filters based on transaction nesting level to avoid processing upper-level transaction entries
- The filtering by atCommit flag allows different handling for commit vs abort scenarios

## Simplified Source

```c
int smgrGetPendingDeletes(bool forCommit, RelFileLocator **ptr)
{
    int nestLevel = GetCurrentTransactionNestLevel();
    int nrels;
    RelFileLocator *rptr;
    PendingRelDelete *pending;

    // First pass: count qualifying relations
    nrels = 0;
    for (pending = pendingDeletes; pending != NULL; pending = pending->next)
    {
        // Include if: at current nest level, matches commit/abort flag, and is non-temp
        if (pending->nestLevel >= nestLevel &&
            pending->atCommit == forCommit &&
            pending->procNumber == INVALID_PROC_NUMBER)
            nrels++;
    }

    // Return early if no relations found
    if (nrels == 0)
    {
        *ptr = NULL;
        return 0;
    }

    // Allocate memory for result array
    rptr = (RelFileLocator *) palloc(nrels * sizeof(RelFileLocator));
    *ptr = rptr;

    // Second pass: copy qualifying relations to result array
    for (pending = pendingDeletes; pending != NULL; pending = pending->next)
    {
        if (pending->nestLevel >= nestLevel &&
            pending->atCommit == forCommit &&
            pending->procNumber == INVALID_PROC_NUMBER)
        {
            *rptr = pending->rlocator;
            rptr++;
        }
    }

    return nrels;
}
```