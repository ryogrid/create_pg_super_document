# SetConstraintStateAddItem

## Location
[src/backend/commands/trigger.c:5716-5745](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L5716-L5745)

## Overview
SetConstraintStateAddItem adds a new per-trigger constraint state item to a SetConstraintState, expanding the storage capacity if needed.

## Definition

```c
static SetConstraintState
SetConstraintStateAddItem(SetConstraintState state,
						  Oid tgoid, bool tgisdeferred)
```
## Detailed Description
This function adds a new trigger constraint state entry to an existing SetConstraintState structure. If the current allocation is insufficient, it automatically expands the storage using a doubling strategy with a minimum size of 8 entries. The function updates the state with the new trigger OID and its deferred status, then increments the count of active states.

The function may return a different pointer than the input if reallocation occurs, so callers must use the return value rather than assuming the original pointer remains valid.

## Parameters / Member Variables
- : The SetConstraintState structure to add the item to
- : Object ID of the trigger being added to the constraint state
- : Boolean indicating whether this trigger is currently in deferred mode

## Dependencies
- Functions called/Symbols referenced:
  - SetConstraintState (parameter and return type)
  - [repalloc](../r/repalloc.md) (for expanding storage when needed)
  - [SetConstraintStateData](SetConstraintStateData.md) (for size calculations)
  - [SetConstraintTriggerData](SetConstraintTriggerData.md) (array element type and size calculations)
  - Max (macro for minimum allocation size)
- Called from:
  - [AfterTriggersTableData](../A/AfterTriggersTableData.md) (src/backend/commands/trigger.c:3995)
  - [AfterTriggerSetState](../A/AfterTriggerSetState.md) (src/backend/commands/trigger.c:5990)

## Notes and Other Information
- Uses exponential growth (doubling) for efficient memory management
- Ensures minimum allocation of 8 entries to reduce frequent reallocations
- Returns potentially different pointer due to reallocation - callers must use return value
- Maintains the state object in the same memory context as the original allocation
- The function handles both the growth management and the actual data insertion
- Each trigger state tracks both the trigger OID and its current deferred status