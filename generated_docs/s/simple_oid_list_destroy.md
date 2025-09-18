# simple_oid_list_destroy

## Location
src/fe_utils/simple_list.c: 106 - 124

## Overview
Destroys a simple OID linked list by freeing all allocated memory for the list cells, designed for frontend PostgreSQL utilities.

## Definition
```c
void simple_oid_list_destroy(SimpleOidList *list)
```

## Detailed Description
This function deallocates all memory associated with a SimpleOidList by iterating through each cell in the linked list and freeing them one by one. It starts from the head of the list and traverses through each node, storing the next pointer before freeing the current cell to avoid accessing freed memory. The function only frees the individual cells but does not modify the list structure itself (head and tail pointers remain unchanged). This is part of the simple list facilities designed for frontend code, providing basic memory management functionality without the complexity of the backend's List infrastructure.

## Parameters / Member Variables
- `list`: Pointer to the SimpleOidList structure whose cells should be destroyed

## Dependencies
- Functions called/Symbols referenced:
  - [pg_free](../p/pg_free.md) (for memory deallocation)
- Data structures used:
  - [SimpleOidList](../S/SimpleOidList.md) (the list container structure)
  - [SimpleOidListCell](../S/SimpleOidListCell.md) (individual list node structure)
- Called from:
  - No references found in the current codebase (may be called from external code or used for cleanup purposes)

## Notes and Other Information
- Safely handles empty lists (when head is NULL)
- Uses a temporary variable to store the next pointer before freeing each cell to avoid use-after-free bugs
- Only deallocates the list cells, not the list structure itself - the caller is responsible for managing the SimpleOidList container
- After calling this function, the list's head and tail pointers will still point to freed memory and should be reset by the caller if the list is to be reused
- This is specifically designed for frontend utilities and is simpler than backend List facilities
- Provides proper cleanup to prevent memory leaks when an OID list is no longer needed
- The function follows standard linked list destruction patterns used throughout PostgreSQL frontend utilities