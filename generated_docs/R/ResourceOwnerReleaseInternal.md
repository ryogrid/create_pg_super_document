# ResourceOwnerReleaseInternal

## Location
[src/backend/utils/resowner/resowner.c:668-800](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/resowner/resowner.c#L668-L800)

## Overview
The core internal function that recursively releases resources owned by a resource owner and its descendants in a controlled manner across different phases of transaction processing.

## Definition

```c
static void
ResourceOwnerReleaseInternal(ResourceOwner owner,
							 ResourceReleasePhase phase,
							 bool isCommit,
							 bool isTopLevel)
```
## Detailed Description
ResourceOwnerReleaseInternal is the workhorse function that handles the complex process of releasing resources in PostgreSQL transactions. It operates in a three-phase approach to ensure proper ordering of resource cleanup:

1. **RESOURCE_RELEASE_BEFORE_LOCKS**: Releases resources that must be freed before locks (buffers, files, etc.)
2. **RESOURCE_RELEASE_LOCKS**: Handles lock management, either releasing or transferring locks to parent resource owners
3. **RESOURCE_RELEASE_AFTER_LOCKS**: Releases remaining resources that depend on locks being released first

The function recursively processes all child resource owners first, then sorts resources by phase and priority to ensure correct release order. It temporarily sets CurrentResourceOwner to provide context to callback functions during resource release.

## Parameters / Member Variables
- `owner`: The ResourceOwner whose resources are to be released
- `phase`: The release phase (BEFORE_LOCKS, LOCKS, or AFTER_LOCKS) determining which resources to process
- `isCommit`: Boolean indicating whether this is a commit (true) or abort (false) operation
- `isTopLevel`: Boolean indicating whether this is a top-level transaction (affects lock handling strategy)
## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerReleaseInternal](ResourceOwnerReleaseInternal.md) (recursive call for child resource owners)
  - [ResourceOwnerSort](ResourceOwnerSort.md) (sorts resources by phase and priority)
  - [ResourceOwnerReleaseAll](ResourceOwnerReleaseAll.md) (releases resources for specific phases)
  - [ProcReleaseLocks](../P/ProcReleaseLocks.md) (releases all locks for top-level transactions)
  - [ReleasePredicateLocks](ReleasePredicateLocks.md) (releases predicate locks)
  - [LockReassignCurrentOwner](../L/LockReassignCurrentOwner.md) (transfers locks to parent on commit)
  - [LockReleaseCurrentOwner](../L/LockReleaseCurrentOwner.md) (releases locks on abort)
- Called from (representative examples):
  - [ResourceOwnerRelease](ResourceOwnerRelease.md) (public interface function)
  - [ResourceOwnerReleaseInternal](ResourceOwnerReleaseInternal.md) (recursive calls)

## Notes and Other Information
- This is a static function, not exposed in the public API
- Uses a three-phase release strategy to handle dependencies between different resource types
- Implements recursive processing to handle nested resource owner hierarchies
- Maintains transaction safety by temporarily switching CurrentResourceOwner context
- Handles both commit and abort scenarios with different lock management strategies
- Includes support for add-on modules through ResourceRelease_callbacks
- Critical for transaction cleanup and proper resource management in PostgreSQL

## Simplified Source

```c
// Simplified version of ResourceOwnerReleaseInternal
static void ResourceOwnerReleaseInternal(ResourceOwner owner,
                                       ResourceReleasePhase phase,
                                       bool isCommit,
                                       bool isTopLevel) {
    ResourceOwner child;
    ResourceOwner saved_owner;

    // Recursively release resources for all child owners first
    for (child = owner->firstchild; child != NULL; child = child->nextchild) {
        ResourceOwnerReleaseInternal(child, phase, isCommit, isTopLevel);
    }

    // Initialize release state and sort resources if needed
    if (!owner->releasing) {
        owner->releasing = true;
    }
    if (!owner->sorted) {
        ResourceOwnerSort(owner);
        owner->sorted = true;
    }

    // Set current owner context for callbacks
    saved_owner = CurrentResourceOwner;
    CurrentResourceOwner = owner;

    // Handle different release phases
    if (phase == RESOURCE_RELEASE_BEFORE_LOCKS) {
        // Release resources that must be freed before locks
        ResourceOwnerReleaseAll(owner, phase, isCommit);
    }
    else if (phase == RESOURCE_RELEASE_LOCKS) {
        if (isTopLevel && owner == TopTransactionResourceOwner) {
            // Release all locks at once for top-level transactions
            ProcReleaseLocks(isCommit);
            ReleasePredicateLocks(isCommit, false);
        }
        else if (!isTopLevel) {
            // Handle locks for subtransactions
            LOCALLOCK **locks = (owner->nlocks > MAX_RESOWNER_LOCKS) ?
                               NULL : owner->locks;
            int nlocks = (owner->nlocks > MAX_RESOWNER_LOCKS) ?
                        0 : owner->nlocks;

            if (isCommit) {
                LockReassignCurrentOwner(locks, nlocks);
            } else {
                LockReleaseCurrentOwner(locks, nlocks);
            }
        }
    }
    else if (phase == RESOURCE_RELEASE_AFTER_LOCKS) {
        // Release resources that depend on locks being released
        ResourceOwnerReleaseAll(owner, phase, isCommit);
    }

    // Execute registered cleanup callbacks
    ResourceReleaseCallbackItem *item;
    for (item = ResourceRelease_callbacks; item; item = item->next) {
        item->callback(phase, isCommit, isTopLevel, item->arg);
    }

    // Restore previous owner context
    CurrentResourceOwner = saved_owner;
}
```

Key simplifications made:
- Removed detailed comments about implementation constraints
- Simplified conditional logic for lock overflow handling
- Consolidated callback execution into cleaner loop
- Removed phase validation assertions for clarity
- Focused on the three-phase resource release pattern
- Maintained all essential logic for proper resource cleanup ordering