# AfterTriggerBeginSubXact

## Location
src/backend/commands/trigger.c: 5388 - 5435

## Overview
Initializes the after-trigger subsystem state for a new subtransaction by saving the current trigger state to a transaction stack.

## Definition
```c
void AfterTriggerBeginSubXact(void)
```

## Detailed Description
AfterTriggerBeginSubXact prepares the after-trigger subsystem for a new subtransaction by preserving the current transaction state. It dynamically manages a transaction stack that stores trigger-related state information for each subtransaction level. The function ensures proper nesting of subtransactions by saving the current events queue, query depth, and firing counter state. The stack grows automatically as needed, starting with space for 8 subtransaction levels and doubling when more space is required.

The function operates by:
1. Getting the current transaction nesting level
2. Expanding the transaction stack if necessary (initially 8 levels, doubles when needed)
3. Saving current trigger state (events queue, query depth, firing counter) at the appropriate stack level
4. Initializing the state pointer to NULL for the new subtransaction level

## Parameters / Member Variables
None - the function takes no parameters and determines the transaction level internally.

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTransactionNestLevel
  - MemoryContextAlloc
  - repalloc
  - AfterTriggersTransData (struct type)
- Called from (representative examples):
  - StartSubTransaction (src/backend/access/transam/xact.c:5028)

## Notes and Other Information
- The transaction stack wastes the first couple of entries since subtransactions start at nest level 2
- SET CONSTRAINTS state is saved lazily - only when it actually changes
- Per-subtransaction event contexts are created only when needed
- The stack uses exponential growth strategy (doubling) for efficient memory management
- All stack memory is allocated in TopTransactionContext for proper lifetime management