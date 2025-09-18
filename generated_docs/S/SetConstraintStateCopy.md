# SetConstraintStateCopy

## Location
[src/backend/commands/trigger.c:5696-5715](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L5696-L5715)

## Overview
SetConstraintStateCopy creates a deep copy of an existing SetConstraintState structure, duplicating all constraint state information and trigger data.

## Definition


## Detailed Description
This function performs a complete deep copy of a SetConstraintState structure. It creates a new SetConstraintState with the same capacity as the original, then copies all the state information including the global constraint flags and the entire array of individual trigger states. The function uses SetConstraintStateCreate for allocation and memcpy for efficient bulk copying of the trigger state array.

This copying mechanism is essential for maintaining separate constraint states across different transaction contexts or when preserving constraint state snapshots.

## Parameters / Member Variables
- : The original SetConstraintState structure to copy from

## Dependencies
- Functions called/Symbols referenced:
  - SetConstraintState (parameter and return type)
  - [SetConstraintStateCreate](SetConstraintStateCreate.md) (for creating the new state structure)
  - SetConstraintTriggerData (for memcpy size calculation)
  - memcpy (for copying the trigstates array)
- Called from:
  - [AfterTriggersTableData](../A/AfterTriggersTableData.md) (src/backend/commands/trigger.c:3994)
  - [AfterTriggerSetState](../A/AfterTriggerSetState.md) (src/backend/commands/trigger.c:5762)

## Notes and Other Information
- Creates a complete independent copy, not just a reference
- Copies both global flags (all_isset, all_isdeferred) and individual trigger states
- Uses the original's numstates count for the new allocation size
- The memcpy operation efficiently copies the entire trigstates array in one operation
- Essential for maintaining constraint state isolation across different execution contexts
- The copied state is allocated in TopTransactionContext like the original