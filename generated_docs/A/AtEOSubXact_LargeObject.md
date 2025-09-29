# AtEOSubXact_LargeObject

## Location
[src/backend/libpq/be-fsstubs.c:648-674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-fsstubs.c#L648-L674)

## Overview
Handles large object resource management at subtransaction commit or abort by reassigning ownership to parent subtransactions on commit or closing file descriptors on abort.

## Definition
```c
void AtEOSubXact_LargeObject(bool isCommit, SubTransactionId mySubid, SubTransactionId parentSubid)
```

## Detailed Description
AtEOSubXact_LargeObject manages large object file descriptors during subtransaction completion. It provides subtransaction-aware resource management by:

1. Checking if any large object operations occurred (`fscxt` is not NULL)
2. Iterating through all open large object file descriptors in the cookies array
3. For each descriptor belonging to the completing subtransaction (`mySubid`):
   - On commit: Reassigns ownership to the parent subtransaction (`parentSubid`)
   - On abort: Closes the file descriptor immediately via `closeLOfd`

This mechanism ensures that large objects opened in subtransactions are properly handled according to subtransaction semantics - they survive commits by being transferred to the parent context, but are cleaned up on aborts.

## Parameters / Member Variables
- `isCommit`: Boolean flag indicating whether the subtransaction is committing (true) or aborting (false)
- `mySubid`: SubTransactionId of the subtransaction being completed
- `parentSubid`: SubTransactionId of the parent subtransaction (used on commit)

## Dependencies
- Functions called/Symbols referenced:
  - [closeLOfd](../c/closeLOfd.md)
  - [LargeObjectDesc](../L/LargeObjectDesc.md) (struct type)
  - SubTransactionId (type)
- Called from (representative examples):
  - [CommitSubTransaction](../C/CommitSubTransaction.md) (src/backend/access/transam/xact.c:5095)
  - [AbortSubTransaction](AbortSubTransaction.md) (src/backend/access/transam/xact.c:5262)

## Notes and Other Information
- Only operates if `fscxt` is not NULL, indicating large object operations occurred in the transaction
- Each `LargeObjectDesc` tracks its subtransaction ID (`subid`) for proper ownership management
- On subtransaction commit, large objects are inherited by the parent subtransaction rather than being closed
- On subtransaction abort, large objects are immediately closed to free resources
- Part of PostgreSQL's nested transaction support, ensuring large object resources follow subtransaction visibility rules
- Complements the main transaction cleanup handled by `AtEOXact_LargeObject`

## Simplified Source

```c
// Simplified version of AtEOSubXact_LargeObject
void AtEOSubXact_LargeObject(bool isCommit, SubTransactionId mySubid,
                            SubTransactionId parentSubid) {
    // Skip if no large object operations occurred
    if (fscxt == NULL)
        return;

    // Process all large object descriptors
    for (int i = 0; i < cookies_size; i++) {
        LargeObjectDesc *lo = cookies[i];

        // Handle descriptors belonging to this subtransaction
        if (lo != NULL && lo->subid == mySubid) {
            if (isCommit) {
                // On commit: transfer ownership to parent
                lo->subid = parentSubid;
            } else {
                // On abort: close the descriptor
                closeLOfd(i);
            }
        }
    }
}
```

Key simplifications made:
- Preserved the essential control flow and logic
- Added descriptive comments explaining each major step
- Maintained the core algorithm for subtransaction resource management
- Simplified variable declarations for clarity
- Kept all critical functionality intact while making the code more readable