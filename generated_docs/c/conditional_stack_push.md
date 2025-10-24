# conditional_stack_push

## Location
[src/fe_utils/conditional.c:53-68](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/conditional.c#L53-L68)

## Overview
Pushes a new conditional state onto the conditional stack, representing entry into a new nested conditional block.

## Definition

```c
void
conditional_stack_push(ConditionalStack cstack, ifState new_state)
```
## Detailed Description
This function creates a new conditional branch by pushing a new IfStackElem onto the top of the conditional stack. It allocates memory for a new stack element, initializes it with the provided conditional state, and links it to the existing stack structure. The function implements a typical stack push operation using a linked list, where new elements are added at the head. The query_len and paren_depth fields are initialized to -1, indicating they need to be set later using appropriate setter functions when the actual values become available.

## Parameters / Member Variables
- `cstack`: ConditionalStack pointer to the stack where the new state should be pushed
- `new_state`: ifState enum value representing the state of the new conditional block (IFSTATE_TRUE, IFSTATE_FALSE, IFSTATE_IGNORED, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md) (memory allocation for new stack element)
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

## Simplified Source

```c
void conditional_stack_push(ConditionalStack cstack, ifState new_state) {
    // Allocate new stack element
    IfStackElem *elem = pg_malloc(sizeof(IfStackElem));

    // Initialize the element
    elem->if_state = new_state;
    elem->query_len = -1;        // To be set later
    elem->paren_depth = -1;      // To be set later

    // Add to stack head (push operation)
    elem->next = cstack->head;
    cstack->head = elem;
}
```