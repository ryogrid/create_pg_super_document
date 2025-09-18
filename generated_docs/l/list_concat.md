# list_concat

## Location
[src/backend/nodes/list.c:561-597](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L561-L597)

## Overview
The  function concatenates two lists by appending all elements of the second list to the end of the first list, returning the modified first list.

## Definition


## Detailed Description
This function performs an efficient concatenation of two lists by destructively modifying the first list to include all elements from the second list. The operation is equivalent to calling  for each element of list2 in order, but is more efficient as it performs the concatenation in a single operation using . 

The function handles edge cases where either list might be NIL, and ensures type compatibility between the two lists. If list1 needs to be enlarged to accommodate the additional elements, the function automatically resizes the internal storage. The operation takes time proportional to at least the length of list2, and potentially additional time proportional to list1's length if storage enlargement is required.

Importantly, list1 is destructively modified while list2 remains unchanged. However, for pointer lists, both lists will point to the same underlying structures after concatenation.

## Parameters / Member Variables
- : The destination list that will be modified to contain the concatenated result (can be NIL)
- : The source list whose elements will be appended to list1 (can be NIL, marked const as it's not modified)

## Dependencies
- Functions called/Symbols referenced:
  -  - Creates a copy when list1 is NIL
  -  - Expands list1's storage capacity if needed
  -  - Validates list consistency after concatenation

- Called from (representative examples):
  -  (src/backend/backup/basebackup_incremental.c:556)
  -  (src/backend/catalog/aclchk.c:866-878)
  -  (src/backend/commands/tablecmds.c:997)
  -  (src/backend/optimizer/path/allpaths.c:2095-2117)
  -  (src/backend/optimizer/path/indxpath.c:318-400)
  -  (src/backend/parser/parse_target.c:152-255)

## Notes and Other Information
- This is a destructive operation on list1; callers should use the return value as the new pointer
- The function ensures type compatibility between lists through assertion
- Handles self-concatenation (list1 == list2) safely using memcpy
- Widely used throughout PostgreSQL for combining query elements, path lists, constraint lists, and other collections
- More efficient than iteratively appending elements from list2 to list1
- The function may return a different pointer than the input list1 if memory reallocation occurs