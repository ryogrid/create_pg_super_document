# drop_assignments

## Location
[src/interfaces/ecpg/preproc/descriptor.c:33-45](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/descriptor.c#L33-L45)

## Overview
Deallocates and removes all assignment entries from the global assignments linked list, cleaning up memory used by variable-descriptor assignments.

## Definition

```c
struct assignment *old_head = assignments;
```
## Detailed Description
This function iterates through the global assignments linked list and deallocates all nodes and their associated memory. It's a cleanup function that ensures proper memory management by freeing both the variable name strings and the assignment structures themselves. The function continues until the assignments list is completely empty.

As a static function, it's only accessible within the descriptor.c file and serves as an internal cleanup mechanism for the ECPG preprocessor's assignment tracking system.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - free (standard library deallocation function)
  - [assignment](../a/assignment.md) (struct type)
- Called from (representative examples):
  - [output_get_descr_header](../o/output_get_descr_header.md)
  - [output_get_descr](../o/output_get_descr.md)  
  - [output_set_descr_header](../o/output_set_descr_header.md)
  - [output_set_descr](../o/output_set_descr.md)

## Notes and Other Information
- Static function with file-local scope in descriptor.c
- Implements proper cleanup by freeing both the variable string and the assignment structure
- Called by various output functions to clean up assignments after processing descriptor operations
- Ensures no memory leaks in the assignment tracking system
- Uses standard free() rather than a custom deallocator, contrasting with the mm_alloc used in push_assignment

## Simplified Source

```c
static void drop_assignments(void) {
    // Traverse and free all assignment nodes
    while (assignments) {
        struct assignment *old_head = assignments;

        // Move to next node
        assignments = old_head->next;

        // Free variable string and node
        free(old_head->variable);
        free(old_head);
    }
}
```