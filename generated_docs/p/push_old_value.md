# push_old_value

## Location
[src/backend/utils/misc/guc.c:2136-2216](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L2136-L2216)

## Overview
The push_old_value function manages the stack of previous GUC (Grand Unified Configuration) variable values during transactional assignments, ensuring proper rollback semantics for configuration changes within transaction nesting levels.

## Definition

```c
static void
push_old_value(struct config_generic *gconf, GucAction action)
```
## Detailed Description
This function handles the complex logic of maintaining a stack of GUC variable states during transactional operations. It ensures that configuration changes can be properly rolled back when transactions are aborted or when exiting nested transaction contexts. The function operates differently based on whether a stack entry already exists at the current nesting level and the type of action being performed.

Key behaviors:
- If not inside a nest level (GUCNestLevel == 0), the function does nothing
- For existing stack entries at the current nest level, it adjusts the state based on the action
- SET actions override prior actions at the same level
- LOCAL actions create masked values when following SET actions
- SAVE actions maintain existing SAVE state
- New stack entries are allocated in TopTransactionContext for proper memory management

## Parameters / Member Variables
- `*gconf`: Pointer to the GUC variable's configuration structure containing current state and stack information
- `action`: The type of GUC action being performed (GUC_ACTION_SET, GUC_ACTION_LOCAL, or GUC_ACTION_SAVE)
## Dependencies
- Functions called/Symbols referenced:
  - [discard_stack_value](../d/discard_stack_value.md)
  - [set_stack_value](../s/set_stack_value.md)  
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - [slist_push_head](../s/slist_push_head.md)
  - [GucStack](../G/GucStack.md) (struct)
  - [GucAction](../G/GucAction.md) (enum)
  - GUC_ACTION_SET, GUC_ACTION_LOCAL, GUC_ACTION_SAVE (enum values)
  - GUC_SET, GUC_SET_LOCAL, GUC_LOCAL, GUC_SAVE (state enum values)
- Called from (representative examples):
  - Configuration assignment functions during transactional GUC operations
  - Functions handling SET and SET LOCAL SQL commands

## Notes and Other Information
- This is a static function within guc.c, not exposed externally
- Critical for maintaining ACID properties of configuration changes
- Uses TopTransactionContext to ensure stack entries survive transaction boundaries
- The function handles complex state transitions between SET and SET LOCAL operations
- Maintains proper cleanup of masked values to prevent memory leaks
- Part of PostgreSQL's sophisticated GUC system that allows configuration changes to be transactional

## Simplified Source

```c
static void
push_old_value(struct config_generic *gconf, GucAction action)
{
    GucStack *stack;

    // Exit early if not in a transaction nest level
    if (GUCNestLevel == 0)
        return;

    // Check if stack entry exists at current nest level
    stack = gconf->stack;
    if (stack && stack->nest_level >= GUCNestLevel) {
        // Adjust existing stack entry based on action type
        switch (action) {
            case GUC_ACTION_SET:
                // SET overrides prior actions, cleanup if needed
                if (stack->state == GUC_SET_LOCAL) {
                    discard_stack_value(gconf, &stack->masked);
                }
                stack->state = GUC_SET;
                break;

            case GUC_ACTION_LOCAL:
                // SET LOCAL after SET: save SET's value as masked
                if (stack->state == GUC_SET) {
                    stack->masked_scontext = gconf->scontext;
                    stack->masked_srole = gconf->srole;
                    set_stack_value(gconf, &stack->masked);
                    stack->state = GUC_SET_LOCAL;
                }
                break;

            case GUC_ACTION_SAVE:
                // SAVE maintains existing SAVE state
                break;
        }
        return;
    }

    // Create new stack entry for this nest level
    stack = (GucStack *) MemoryContextAllocZero(TopTransactionContext,
                                               sizeof(GucStack));

    // Initialize new stack entry
    stack->prev = gconf->stack;
    stack->nest_level = GUCNestLevel;
    stack->source = gconf->source;
    stack->scontext = gconf->scontext;
    stack->srole = gconf->srole;

    // Set state based on action type
    switch (action) {
        case GUC_ACTION_SET:   stack->state = GUC_SET; break;
        case GUC_ACTION_LOCAL: stack->state = GUC_LOCAL; break;
        case GUC_ACTION_SAVE:  stack->state = GUC_SAVE; break;
    }

    // Save current value and link to stack
    set_stack_value(gconf, &stack->prior);

    if (gconf->stack == NULL)
        slist_push_head(&guc_stack_list, &gconf->stack_link);
    gconf->stack = stack;
}
```