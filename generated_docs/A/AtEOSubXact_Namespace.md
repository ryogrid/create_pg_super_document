# AtEOSubXact_Namespace

## Location
[src/backend/catalog/namespace.c:4558-4597](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L4558-L4597)

## Overview
AtEOSubXact_Namespace manages temporary namespace state during subtransaction end events, either propagating namespace creation flags to parent transactions on commit or cleaning up namespace state on abort.

## Definition

```c
void
AtEOSubXact_Namespace(bool isCommit, SubTransactionId mySubid,
					  SubTransactionId parentSubid)
```
## Detailed Description
This function is called at the end of a subtransaction to handle temporary namespace management. It operates based on whether the subtransaction is committing or aborting:

- **On subtransaction commit**: If the current subtransaction created a temporary namespace (myTempNamespaceSubID == mySubid), the namespace creation responsibility is transferred to the parent subtransaction by updating myTempNamespaceSubID to parentSubid.

- **On subtransaction abort**: If the current subtransaction was responsible for creating a temporary namespace, all related state is reset:
  - myTempNamespaceSubID is invalidated
  - Temporary namespace OIDs are cleared
  - Search path cache is invalidated
  - MyProc->tempNamespaceId is reset

The function ensures proper cleanup and state management for temporary namespaces across PostgreSQL's nested transaction hierarchy.

## Parameters / Member Variables
- `isCommit`: Boolean indicating whether the subtransaction is committing (true) or aborting (false)
- `mySubid`: The SubTransactionId of the current subtransaction being ended
- `parentSubid`: The SubTransactionId of the parent subtransaction
## Dependencies
- Functions called/Symbols referenced:
  - SubTransactionId
  - InvalidSubTransactionId
- Called from (representative examples):
  - [CommitSubTransaction](../C/CommitSubTransaction.md)
  - [AbortSubTransaction](AbortSubTransaction.md)

## Notes and Other Information
- This function is part of PostgreSQL's transaction management system that ensures temporary namespace state is properly maintained across subtransaction boundaries
- The operation of resetting MyProc->tempNamespaceId is assumed to be atomic
- Search path caches are invalidated on abort to ensure they are rebuilt with correct state
- The function handles the critical task of preventing namespace state leakage between transaction levels

## Simplified Source

```c
// Simplified version of AtEOSubXact_Namespace
void AtEOSubXact_Namespace(bool isCommit, SubTransactionId mySubid,
                          SubTransactionId parentSubid) {
    // Check if this subtransaction created the temp namespace
    if (myTempNamespaceSubID == mySubid) {
        if (isCommit) {
            // On commit: transfer namespace ownership to parent
            myTempNamespaceSubID = parentSubid;
        } else {
            // On abort: clean up all temp namespace state
            myTempNamespaceSubID = InvalidSubTransactionId;
            myTempNamespace = InvalidOid;
            myTempToastNamespace = InvalidOid;

            // Invalidate search path caches for rebuild
            baseSearchPathValid = false;
            searchPathCacheValid = false;

            // Reset process-level temp namespace flag
            MyProc->tempNamespaceId = InvalidOid;
        }
    }
}
```

Key simplifications made:
- Removed detailed comments about concurrency and visibility for clarity
- Consolidated namespace cleanup into logical groups
- Added brief descriptive comments for main logic branches
- Preserved all essential state management operations
- Maintained the core commit vs abort logic flow