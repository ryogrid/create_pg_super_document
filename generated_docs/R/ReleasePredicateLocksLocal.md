# ReleasePredicateLocksLocal

## Location
[src/backend/storage/lmgr/predicate.c:3669-3686](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L3669-L3686)

## Overview
Cleans up backend-local predicate lock data structures and resets transaction state variables for the current backend process.

## Definition
```c
static void ReleasePredicateLocksLocal(void)
```

## Detailed Description
ReleasePredicateLocksLocal is a simple but critical cleanup function that handles the backend-local aspects of predicate lock cleanup in PostgreSQL's serializable snapshot isolation system. This function is responsible for clearing the current backend's local state related to serializable transactions and predicate locks.

The function performs three essential cleanup tasks:
1. **Reset Transaction Reference**: Clears the MySerializableXact pointer, indicating this backend no longer has an active serializable transaction
2. **Reset Write Flag**: Clears MyXactDidWrite, resetting the flag that tracks whether the current transaction performed any writes
3. **Destroy Local Lock Table**: If the LocalPredicateLockHash exists, it destroys the hash table that tracks this backend's local view of predicate locks

This function is typically called at the end of transaction cleanup or when a backend needs to dissociate itself from serializable transaction tracking. It ensures that no stale references or data structures remain that could interfere with subsequent transactions or cause memory leaks.

## Parameters / Member Variables
This function takes no parameters but operates on global backend-local variables:
- Resets `MySerializableXact` to `InvalidSerializableXact`
- Resets `MyXactDidWrite` to false
- Destroys `LocalPredicateLockHash` if it exists

## Dependencies
- Functions called/Symbols referenced:
  - [hash_destroy](../h/hash_destroy.md) (PostgreSQL hash table cleanup function)
  - InvalidSerializableXact (null sentinel value)
- Called from:
  - [SerialControl](../S/SerialControl.md) (during serializable transaction setup/cleanup)
  - [ReleasePredicateLocks](ReleasePredicateLocks.md) (multiple call sites for different cleanup scenarios)

## Notes and Other Information
- This is a static (internal) function used only within the predicate locking subsystem
- Essential for preventing memory leaks in backends that handle serializable transactions
- Must be called to properly clean up backend state regardless of whether the transaction committed or aborted
- The function is safe to call multiple times or when no serializable transaction is active
- Particularly important in parallel query scenarios where workers need to clean up their local state independently
- The LocalPredicateLockHash destruction is crucial for reclaiming memory used for tracking local predicate locks

## Simplified Source

```c
// Simplified version of ReleasePredicateLocksLocal
static void ReleasePredicateLocksLocal(void) {
    // Reset serializable transaction reference
    MySerializableXact = InvalidSerializableXact;

    // Clear write tracking flag
    MyXactDidWrite = false;

    // Clean up local predicate lock hash table
    if (LocalPredicateLockHash != NULL) {
        hash_destroy(LocalPredicateLockHash);
        LocalPredicateLockHash = NULL;
    }
}
```

Key simplifications made:
- Added descriptive comments for each cleanup step
- No simplification of logic needed - function is already straightforward
- Preserved all essential functionality as the function is minimal and clear