# AfterTriggerBeginSubXact

## Location
[src/backend/commands/trigger.c:5388-5435](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L5388-L5435)

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
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [repalloc](../r/repalloc.md)
  - [AfterTriggersTransData](AfterTriggersTransData.md) (struct type)
- Called from (representative examples):
  - [StartSubTransaction](../S/StartSubTransaction.md) (src/backend/access/transam/xact.c:5028)

## Notes and Other Information
- The transaction stack wastes the first couple of entries since subtransactions start at nest level 2
- SET CONSTRAINTS state is saved lazily - only when it actually changes
- Per-subtransaction event contexts are created only when needed
- The stack uses exponential growth strategy (doubling) for efficient memory management
- All stack memory is allocated in TopTransactionContext for proper lifetime management

## Simplified Source

```c
void
AfterTriggerBeginSubXact(void)
{
    int my_level = GetCurrentTransactionNestLevel();

    // Ensure transaction stack is large enough
    while (my_level >= afterTriggers.maxtransdepth)
    {
        if (afterTriggers.maxtransdepth == 0)
        {
            // Initialize for 8 subtransaction levels
            afterTriggers.trans_stack = (AfterTriggersTransData *)
                MemoryContextAlloc(TopTransactionContext,
                                 8 * sizeof(AfterTriggersTransData));
            afterTriggers.maxtransdepth = 8;
        }
        else
        {
            // Double the stack size
            int new_alloc = afterTriggers.maxtransdepth * 2;

            afterTriggers.trans_stack = (AfterTriggersTransData *)
                repalloc(afterTriggers.trans_stack,
                         new_alloc * sizeof(AfterTriggersTransData));
            afterTriggers.maxtransdepth = new_alloc;
        }
    }

    // Save current trigger state on the stack
    afterTriggers.trans_stack[my_level].state = NULL;
    afterTriggers.trans_stack[my_level].events = afterTriggers.events;
    afterTriggers.trans_stack[my_level].query_depth = afterTriggers.query_depth;
    afterTriggers.trans_stack[my_level].firing_counter = afterTriggers.firing_counter;
}
```