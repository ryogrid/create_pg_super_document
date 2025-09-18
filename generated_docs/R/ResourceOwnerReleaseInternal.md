# ResourceOwnerReleaseInternal

## Location
src/backend/utils/resowner/resowner.c: 668 - 800

## Overview
The core internal function that recursively releases resources owned by a resource owner and its descendants in a controlled manner across different phases of transaction processing.

## Definition


## Detailed Description
ResourceOwnerReleaseInternal is the workhorse function that handles the complex process of releasing resources in PostgreSQL transactions. It operates in a three-phase approach to ensure proper ordering of resource cleanup:

1. **RESOURCE_RELEASE_BEFORE_LOCKS**: Releases resources that must be freed before locks (buffers, files, etc.)
2. **RESOURCE_RELEASE_LOCKS**: Handles lock management, either releasing or transferring locks to parent resource owners
3. **RESOURCE_RELEASE_AFTER_LOCKS**: Releases remaining resources that depend on locks being released first

The function recursively processes all child resource owners first, then sorts resources by phase and priority to ensure correct release order. It temporarily sets CurrentResourceOwner to provide context to callback functions during resource release.

## Parameters / Member Variables
- : The ResourceOwner whose resources are to be released
- : The release phase (BEFORE_LOCKS, LOCKS, or AFTER_LOCKS) determining which resources to process
- : Boolean indicating whether this is a commit (true) or abort (false) operation
- : Boolean indicating whether this is a top-level transaction (affects lock handling strategy)

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerReleaseInternal (recursive call for child resource owners)
  - ResourceOwnerSort (sorts resources by phase and priority)
  - ResourceOwnerReleaseAll (releases resources for specific phases)
  - ProcReleaseLocks (releases all locks for top-level transactions)
  - ReleasePredicateLocks (releases predicate locks)
  - LockReassignCurrentOwner (transfers locks to parent on commit)
  - LockReleaseCurrentOwner (releases locks on abort)
- Called from (representative examples):
  - ResourceOwnerRelease (public interface function)
  - ResourceOwnerReleaseInternal (recursive calls)

## Notes and Other Information
- This is a static function, not exposed in the public API
- Uses a three-phase release strategy to handle dependencies between different resource types
- Implements recursive processing to handle nested resource owner hierarchies
- Maintains transaction safety by temporarily switching CurrentResourceOwner context
- Handles both commit and abort scenarios with different lock management strategies
- Includes support for add-on modules through ResourceRelease_callbacks
- Critical for transaction cleanup and proper resource management in PostgreSQL