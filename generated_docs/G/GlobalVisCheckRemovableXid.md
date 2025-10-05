# GlobalVisCheckRemovableXid

## Location
[src/backend/storage/ipc/procarray.c:4299-4319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L4299-L4319)

## Overview
A convenience wrapper function that checks whether a 32-bit transaction ID can be safely removed by combining relation-specific global visibility state setup with removability testing for regular transaction IDs.

## Definition
```c
bool GlobalVisCheckRemovableXid(Relation rel, TransactionId xid)
```

## Detailed Description
This function provides a simplified interface for checking 32-bit transaction ID removability by combining two operations: obtaining the appropriate global visibility state for a relation and then testing whether a transaction ID is removable. It serves as a convenience wrapper that encapsulates the common pattern of calling `GlobalVisTestFor()` followed by `GlobalVisTestIsRemovableXid()`.

Similar to `GlobalVisCheckRemovableFullXid()` but works with 32-bit transaction IDs instead of full transaction IDs. The function is typically used in scenarios where you have a specific relation context and need to determine if a transaction can be safely removed or recycled, particularly in index management operations.

## Parameters / Member Variables
- `rel`: Relation for which to obtain global visibility state context
- `xid`: 32-bit transaction ID to test for removability (must be from a wraparound-protected source)

## Dependencies
- Functions called/Symbols referenced:
  - [GlobalVisTestFor](GlobalVisTestFor.md)
  - [GlobalVisTestIsRemovableXid](GlobalVisTestIsRemovableXid.md)
  - [GlobalVisState](GlobalVisState.md) (type)
  - [FullTransactionId](../F/FullTransactionId.md) (type)
- Called from (representative examples):
  - [GinPageIsRecyclable](GinPageIsRecyclable.md)

## Notes and Other Information
- This is a convenience function that simplifies the two-step process of getting visibility state and testing removability for 32-bit transaction IDs
- The underlying `GlobalVisTestIsRemovableXid()` requires that the xid comes from a wraparound-protected source
- Commonly used in GIN index management code for determining page recyclability
- Part of PostgreSQL's global visibility infrastructure used for safe page and tuple recycling
- Provides the same functionality as `GlobalVisCheckRemovableFullXid()` but for 32-bit transaction IDs

## Simplified Source

```c
bool GlobalVisCheckRemovableXid(Relation rel, TransactionId xid) {
    // Get relation-specific global visibility state
    GlobalVisState *state = GlobalVisTestFor(rel);

    // Test if the 32-bit transaction ID is removable
    return GlobalVisTestIsRemovableXid(state, xid);
}
```