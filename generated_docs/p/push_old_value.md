# push_old_value

## Location
src/backend/utils/misc/guc.c: 2136 - 2216

## Overview
The push_old_value function manages the stack of previous GUC (Grand Unified Configuration) variable values during transactional assignments, ensuring proper rollback semantics for configuration changes within transaction nesting levels.

## Definition


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
- : Pointer to the GUC variable's configuration structure containing current state and stack information
- : The type of GUC action being performed (GUC_ACTION_SET, GUC_ACTION_LOCAL, or GUC_ACTION_SAVE)

## Dependencies
- Functions called/Symbols referenced:
  - discard_stack_value
  - set_stack_value  
  - MemoryContextAllocZero
  - slist_push_head
  - GucStack (struct)
  - GucAction (enum)
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