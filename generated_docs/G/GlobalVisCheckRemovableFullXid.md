# GlobalVisCheckRemovableFullXid

## Location
[src/backend/storage/ipc/procarray.c:4285-4298](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L4285-L4298)

## Overview
A convenience wrapper function that checks whether a full transaction ID can be safely removed by combining relation-specific global visibility state setup with removability testing.

## Definition
```c
bool GlobalVisCheckRemovableFullXid(Relation rel, FullTransactionId fxid)
```

## Detailed Description
This function provides a simplified interface for checking transaction removability by combining two operations: obtaining the appropriate global visibility state for a relation and then testing whether a full transaction ID is removable. It serves as a convenience wrapper that encapsulates the common pattern of calling `GlobalVisTestFor()` followed by `GlobalVisTestIsRemovableFullXid()`.

The function is typically used in scenarios where you have a specific relation context and need to determine if a transaction can be safely removed or recycled, such as during index page recycling operations.

## Parameters / Member Variables
- `rel`: Relation for which to obtain global visibility state context
- `fxid`: Full transaction ID to test for removability

## Dependencies
- Functions called/Symbols referenced:
  - [GlobalVisTestFor](GlobalVisTestFor.md)
  - [GlobalVisTestIsRemovableFullXid](GlobalVisTestIsRemovableFullXid.md)
  - [GlobalVisState](GlobalVisState.md) (type)
  - [FullTransactionId](../F/FullTransactionId.md) (type)
- Called from (representative examples):
  - [gistPageRecyclable](../g/gistPageRecyclable.md)
  - [_bt_pendingfsm_finalize](../b/_bt_pendingfsm_finalize.md)
  - [BTPageIsRecyclable](../B/BTPageIsRecyclable.md)

## Notes and Other Information
- This is a convenience function that simplifies the two-step process of getting visibility state and testing removability
- Commonly used in index management code for determining page recyclability
- The function automatically handles the relation-specific visibility state setup, making it easier to use than the lower-level functions
- Part of PostgreSQL's global visibility infrastructure used for safe page and tuple recycling

## Simplified Source

```c
bool GlobalVisCheckRemovableFullXid(Relation rel, FullTransactionId fxid) {
    // Get relation-specific global visibility state
    GlobalVisState *state = GlobalVisTestFor(rel);

    // Test if the full transaction ID is removable
    return GlobalVisTestIsRemovableFullXid(state, fxid);
}
```