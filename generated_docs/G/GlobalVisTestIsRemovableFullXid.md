# GlobalVisTestIsRemovableFullXid

## Location
[src/backend/storage/ipc/procarray.c:4221-4262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L4221-L4262)

## Overview
Determines whether a full transaction ID is no longer considered running by any active snapshot and can be safely removed.

## Definition

```c
bool
GlobalVisTestIsRemovableFullXid(GlobalVisState *state,
								FullTransactionId fxid)
```
## Detailed Description
This function performs a comprehensive test to determine if a full transaction ID (fxid) is visible to all currently active snapshots and can therefore be safely removed. It uses a three-tier approach:

1. **Fast path**: If fxid is older than the maybe_needed boundary, it's definitely visible to everyone
2. **Definite rejection**: If fxid is >= definitely_needed boundary, it's very likely still considered running
3. **Uncertain case**: If fxid falls between maybe_needed and definitely_needed boundaries, the function may trigger a boundary update if beneficial and recheck

The state parameter must be initialized for the relation that fxid belongs to, or NULL for general use, to ensure correct results.

## Parameters / Member Variables
- `*state`: Pointer to GlobalVisState containing visibility boundaries for the relevant relation type
- `fxid`: The full transaction ID to test for removability
## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionIdPrecedes
  - FullTransactionIdFollowsOrEquals
  - [GlobalVisTestShouldUpdate](GlobalVisTestShouldUpdate.md)
  - [GlobalVisUpdate](GlobalVisUpdate.md)
- Called from:
  - [GlobalVisTestIsRemovableXid](GlobalVisTestIsRemovableXid.md)
  - [GlobalVisCheckRemovableFullXid](GlobalVisCheckRemovableFullXid.md)

## Notes and Other Information
- Returns true if the transaction ID can be safely removed, false otherwise
- Uses optimized logic to avoid expensive horizon updates when not beneficial
- The uncertain case (between boundaries) may trigger a horizon update for more accurate results
- Critical for vacuum operations, tuple cleanup, and maintaining MVCC correctness
- Part of PostgreSQL's global visibility infrastructure for multi-version concurrency control
- Handles the complexity of determining transaction visibility across potentially long-running transactions

## Simplified Source

```c
bool
GlobalVisTestIsRemovableFullXid(GlobalVisState *state, FullTransactionId fxid)
{
    // Fast path: If transaction is older than maybe_needed, it's definitely visible
    if (FullTransactionIdPrecedes(fxid, state->maybe_needed))
        return true;

    // If transaction is too recent (>= definitely_needed), likely still running
    if (FullTransactionIdFollowsOrEquals(fxid, state->definitely_needed))
        return false;

    // Transaction is in uncertain range - may update boundaries for accuracy
    if (GlobalVisTestShouldUpdate(state)) {
        // Refresh global visibility state
        GlobalVisUpdate();

        // Recheck with updated boundaries
        return FullTransactionIdPrecedes(fxid, state->maybe_needed);
    }

    // Don't update boundaries - assume not removable to be safe
    return false;
}
```