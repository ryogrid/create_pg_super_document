# add_variable_to_tail

## Location
[src/interfaces/ecpg/preproc/variable.c:389-406](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/variable.c#L389-L406)

## Overview
Appends a new variable and its optional indicator variable at the end of an argument list, implementing a FIFO (First In, First Out) insertion strategy.

## Definition

```c
void
add_variable_to_tail(struct arguments **list, struct variable *var, struct variable *ind)
```
## Detailed Description
The  function adds a new entry to the end of a linked list of arguments. This function is part of PostgreSQL's ECPG preprocessor and provides an alternative insertion method to . It traverses the entire list to find the last node, then appends the new entry there, maintaining the original insertion order.

The function handles both empty lists (where the new node becomes the head) and non-empty lists (where the new node is appended to the tail). This FIFO behavior preserves the order in which variables are encountered during parsing.

## Parameters / Member Variables
- `**list`: Double pointer to the head of the arguments list; allows modification of the list head pointer when the list is initially empty
- `*var`: Pointer to the main variable to be added to the list
- `*ind`: Pointer to the indicator variable associated with the main variable (can be NULL if no indicator is needed)
## Dependencies
- Functions called/Symbols referenced:
  - : Memory allocation function used to create new argument nodes
  - : The node structure for the linked list
  - : Structure representing ECPG variables
- Called from (representative examples):
  - Grammar rules in ecpg.header and ecpg.addons
  - Used to build argument lists while preserving insertion order

## Notes and Other Information
- Unlike , this function preserves the chronological order of variable additions
- The function performs a full list traversal to find the tail, making it O(n) in complexity
- Handles the edge case of empty lists by setting the new node as the list head
- Used in contexts where maintaining the original order of variables is important
- The function uses the same memory allocation strategy as other ECPG functions