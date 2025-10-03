# RS_free

## Location
[src/backend/tsearch/regis.c:166-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/regis.c#L166-L181)

## Overview
Frees the memory allocated for a compiled regular expression by deallocating all RegisNode structures in the linked list.

## Definition

```c
void
RS_free(Regis *r)
```
## Detailed Description
RS_free performs cleanup for a compiled regular expression by traversing the linked list of RegisNode structures and deallocating each node using pfree(). It follows the standard pattern for freeing linked lists: save the next pointer, free the current node, and advance to the next node. After freeing all nodes, it sets the node pointer to NULL to prevent dangling references and ensure the Regis structure is in a clean state.

## Parameters / Member Variables
- `*r`: Pointer to the Regis structure containing the compiled pattern to free
## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
- Types referenced:
  - [Regis](Regis.md) (main regex structure)
  - [RegisNode](RegisNode.md) (pattern node structure)
- Called from:
  - No direct references found in the codebase (likely used in cleanup contexts)

## Notes and Other Information
- Essential for preventing memory leaks in regex pattern processing
- Sets the node pointer to NULL after cleanup for safety
- Uses PostgreSQL's memory management (pfree) rather than standard free()
- Part of the complete lifecycle management for compiled regex patterns
- Should be called when a compiled regex pattern is no longer needed
- Follows PostgreSQL's memory management conventions