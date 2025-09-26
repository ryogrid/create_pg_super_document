# spgBulkDeleteState

## Location
[src/backend/access/spgist/spgvacuum.c:40-53](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgvacuum.c#L40-L53)

## Overview
A structure that maintains local state and parameters for SPGiST vacuum operations, including bulk delete and cleanup processes.

## Definition
```c
typedef struct spgBulkDeleteState
{
    /* Parameters passed in to spgvacuumscan */
    IndexVacuumInfo *info;
    IndexBulkDeleteResult *stats;
    IndexBulkDeleteCallback callback;
    void       *callback_state;

    /* Additional working state */
    SpGistState spgstate;        /* for SPGiST operations that need one */
    spgVacPendingItem *pendingList; /* TIDs we need to (re)visit */
    TransactionId myXmin;        /* for detecting newly-added redirects */
    BlockNumber lastFilledBlock; /* last non-deletable block */
} spgBulkDeleteState;
```

## Detailed Description
The `spgBulkDeleteState` structure serves as the central coordination point for SPGiST vacuum operations. It combines input parameters from the vacuum system with working state needed during the scan process. This structure is passed between various vacuum-related functions to maintain consistency and share state throughout the vacuum operation.

The structure is initialized during bulk delete operations and contains both the parameters needed to perform the vacuum (such as callback functions and statistics) and the working state that accumulates during the scan (such as pending items to revisit and transaction information).

## Parameters / Member Variables
- `info`: Pointer to IndexVacuumInfo containing index-specific vacuum parameters and configuration
- `stats`: Pointer to IndexBulkDeleteResult for collecting and reporting vacuum operation statistics  
- `callback`: Function pointer to IndexBulkDeleteCallback used to determine if specific tuples should be deleted
- `callback_state`: Opaque pointer to state information passed to the callback function
- `spgstate`: SpGistState structure containing SPGiST-specific operational state and cached information
- `pendingList`: Head pointer to a linked list of spgVacPendingItem structures representing TIDs that need to be revisited
- `myXmin`: TransactionId used to detect newly-added redirect tuples during the vacuum process
- `lastFilledBlock`: BlockNumber tracking the last block that contains non-deletable content, used for potential truncation decisions

## Dependencies
- Functions called/Symbols referenced:
  - [IndexVacuumInfo](../I/IndexVacuumInfo.md) (vacuum parameter structure)
  - [IndexBulkDeleteResult](../I/IndexBulkDeleteResult.md) (statistics collection structure)
  - IndexBulkDeleteCallback (callback function type)
  - [SpGistState](../S/SpGistState.md) (SPGiST operational state)
  - [spgVacPendingItem](spgVacPendingItem.md) (pending item list structure)
  - TransactionId (transaction identifier type)
  - BlockNumber (block number type)
- Called from (representative examples):
  - [spgvacuumscan](spgvacuumscan.md) (main vacuum scanning function)
  - [spgbulkdelete](spgbulkdelete.md) (bulk delete entry point)
  - [spgvacuumcleanup](spgvacuumcleanup.md) (vacuum cleanup operations)
  - [vacuumLeafPage](../v/vacuumLeafPage.md) (leaf page vacuum processing)
  - [spgvacuumpage](spgvacuumpage.md) (page-level vacuum processing)
  - [spgprocesspending](spgprocesspending.md) (pending item processing)

## Notes and Other Information
- This structure is allocated and initialized at the start of vacuum operations and passed throughout the vacuum process
- The pendingList is managed dynamically during the scan, with items added via spgAddPendingTID and processed via spgprocesspending
- The myXmin field helps distinguish between old redirects that may be safely removed and new ones that must be preserved
- The structure supports both bulk delete operations (removing specific tuples) and cleanup operations (general maintenance)
- Memory management for the structure and its components follows PostgreSQL's memory context conventions
- The lastFilledBlock field was intended for truncation optimizations but truncation is currently disabled in SPGiST due to concurrency concerns