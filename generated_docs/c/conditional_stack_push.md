# conditional_stack_push

## Location
src/fe_utils/conditional.c: 53 - 68

## Overview
Pushes a new conditional state onto the conditional stack, representing entry into a new nested conditional block.

## Definition


## Detailed Description
This function creates a new conditional branch by pushing a new IfStackElem onto the top of the conditional stack. It allocates memory for a new stack element, initializes it with the provided conditional state, and links it to the existing stack structure. The function implements a typical stack push operation using a linked list, where new elements are added at the head. The query_len and paren_depth fields are initialized to -1, indicating they need to be set later using appropriate setter functions when the actual values become available.

## Parameters / Member Variables
- `cstack`: ConditionalStack pointer to the stack where the new state should be pushed
- `new_state`: ifState enum value representing the state of the new conditional block (IFSTATE_TRUE, IFSTATE_FALSE, IFSTATE_IGNORED, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc (memory allocation for new stack element)
  - [IfStackElem](../I/IfStackElem.md) (structure type for stack elements)
  - ifState (enum type for conditional states)
- Called from (representative examples):
  - [advanceConnectionState](../a/advanceConnectionState.md) (in pgbench)
  - [executeMetaCommand](../e/executeMetaCommand.md) (in pgbench)
  - [CheckConditional](../C/CheckConditional.md) (in pgbench)
  - [HandleSlashCmds](../H/HandleSlashCmds.md) (in psql)
  - [exec_command_if](../e/exec_command_if.md) (in psql)

## Notes and Other Information
- Creates a new IfStackElem on the heap, so each push must eventually have a corresponding pop to avoid memory leaks
- The query_len and paren_depth are initialized to -1 and should be set using conditional_stack_set_query_len and conditional_stack_set_paren_depth
- Used when entering \if, \elif, or \else blocks in psql and pgbench scripts
- The new state determines whether the current conditional block should execute or be ignored
- Part of the nested conditional handling system that allows for complex conditional logic in frontend scripts