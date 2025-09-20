# remove_variable_from_list

## Location
[src/interfaces/ecpg/preproc/variable.c:407-435](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/variable.c#L407-L435)

## Overview
Removes a specific variable from an argument list by searching for the variable pointer and unlinking the corresponding node from the linked list.

## Definition

```c
void
remove_variable_from_list(struct arguments **list, struct variable *var)
```
## Detailed Description
The  function searches through a linked list of arguments to find and remove a node that contains a specific variable. The function performs a linear search comparing variable pointers (not variable names or content) to locate the target entry. Once found, it properly unlinks the node from the list, handling both head and middle/tail removal cases.

The function uses pointer comparison for identification, meaning it removes the first occurrence of a node containing the exact same variable pointer. The removed node is not freed, following ECPG's memory management patterns where cleanup is handled elsewhere.

## Parameters / Member Variables
- : Double pointer to the head of the arguments list; allows modification of the list head pointer when removing the first element
- : Pointer to the variable to be removed from the list; the function searches for this exact pointer value

## Dependencies
- Functions called/Symbols referenced:
  - : The node structure for the linked list
  - : Structure representing ECPG variables
- Called from (representative examples):
  - Various locations in ECPG grammar rules (ecpg.trailer)
  - Used to remove variables from  list during processing

## Notes and Other Information
- The function uses pointer comparison (), not content comparison
- Handles edge cases: removing the head node and removing nodes from the middle/end of the list
- The function does not free the removed node's memory - memory management is handled elsewhere in ECPG
- Only removes the first occurrence of the variable if multiple entries exist
- Uses a  flag to track whether the variable was located, though the current implementation doesn't use this information
- The function gracefully handles the case where the variable is not found in the list