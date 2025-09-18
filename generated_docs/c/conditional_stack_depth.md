# conditional_stack_depth

## Location
[src/fe_utils/conditional.c:84-105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/conditional.c#L84-L105)

## Overview
Returns the current depth of a conditional stack, primarily used for debugging purposes to determine how many nested conditional blocks are currently active.

## Definition


## Detailed Description
This function traverses a conditional stack (implemented as a linked list) and counts the number of elements to determine the stack depth. It handles the case where the stack might be NULL by returning -1, and otherwise iterates through all stack elements starting from the head to count the total depth. This is particularly useful for debugging conditional processing in PostgreSQL frontend utilities where nested if/elif/else constructs need to be tracked.

## Parameters / Member Variables
- : A ConditionalStack pointer representing the conditional stack to measure. If NULL, the function returns -1 to indicate an invalid or uninitialized stack.

## Dependencies
- Functions called/Symbols referenced:
  - [ConditionalStack](../C/ConditionalStack.md) (typedef)
  - [IfStackElem](../I/IfStackElem.md) (struct type)
- Called from (representative examples):
  - Used primarily for debugging and diagnostic purposes in conditional processing

## Notes and Other Information
- Returns -1 for NULL stack input as an error indicator
- The function performs a simple linear traversal, so time complexity is O(n) where n is the stack depth
- This is a utility function mainly intended for debugging rather than production logic
- The stack is implemented as a singly-linked list with head pointer access