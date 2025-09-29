# list_concat_copy

## Location
[src/backend/nodes/list.c:598-630](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L598-L630)

## Overview
The  function creates a new list by concatenating the elements of two input lists, leaving both original lists unchanged.

## Definition

```c
List *
list_concat_copy(const List *list1, const List *list2)
```
## Detailed Description
This function provides a non-destructive alternative to  by creating a completely new list that contains all elements from both input lists in sequence. Unlike , neither input list is modified during the operation. The function is more efficient than the equivalent operation  because it creates the result list with the correct size from the beginning and uses direct memory copying.

The function handles edge cases where either input list might be NIL by simply returning a copy of the non-NIL list. It ensures type compatibility between the input lists and creates the result list with optimal storage allocation. The implementation uses  for efficient bulk copying of list elements.

## Parameters / Member Variables
- : The first source list whose elements will appear first in the result (marked const as it's not modified)
- : The second source list whose elements will appear after list1's elements (marked const as it's not modified)

## Dependencies
- Functions called/Symbols referenced:
  -  - Creates a copy when one of the input lists is NIL
  -  - Creates a new list with the combined length
  -  - Validates the resulting list's consistency

- Called from (representative examples):
  -  (src/backend/access/hash/hashvalidate.c:394)
  -  (src/backend/commands/indexcmds.c:649)
  -  (src/backend/optimizer/path/indxpath.c:1127)
  -  (src/backend/optimizer/util/clauses.c:3821, 3840)
  -  (src/backend/optimizer/util/relnode.c:2325-2401)

## Notes and Other Information
- This is a non-destructive operation that preserves both input lists
- More efficient than manually copying and concatenating lists
- Particularly useful when the original lists need to be preserved for other operations
- For pointer lists, the result will share the same underlying structures as the inputs
- The function automatically handles memory allocation for the optimal result size
- Commonly used in query optimization where multiple constraint or path lists need to be combined without affecting the originals
- Prior to PostgreSQL v13, some code unnecessarily copied list2 as well, but this is no longer needed

## Simplified Source

```c
List *
list_concat_copy(const List *list1, const List *list2)
{
    List *result;
    int new_len;

    // Handle edge cases where either list is NIL
    if (list1 == NIL)
        return list_copy(list2);
    if (list2 == NIL)
        return list_copy(list1);

    // Ensure both lists are of the same type
    Assert(list1->type == list2->type);

    // Create new list with combined length
    new_len = list1->length + list2->length;
    result = new_list(list1->type, new_len);

    // Copy elements from both lists using efficient memory operations
    memcpy(result->elements, list1->elements,
           list1->length * sizeof(ListCell));
    memcpy(result->elements + list1->length, list2->elements,
           list2->length * sizeof(ListCell));

    check_list_invariants(result);
    return result;
}
```