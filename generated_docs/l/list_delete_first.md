# list_delete_first

## Location
[src/backend/nodes/list.c:943-956](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L943-L956)

## Overview
Removes and deallocates the first element from a PostgreSQL List, returning the modified list structure.

## Definition

```c
List *
list_delete_first(List *list)
```
## Detailed Description
This function provides a safe and efficient way to remove the first element from a PostgreSQL List data structure. It serves as a replacement for the Lisp-style traversal pattern "list = lnext(list);" when the intent is to modify the list rather than just traverse it. 

The function handles the NIL (empty list) case gracefully by returning NIL without error. For non-empty lists, it delegates to list_delete_nth_cell() with index 0 to perform the actual deletion.

An important performance consideration is that this operation takes O(n) time proportional to the list length, since all remaining entries must be shifted forward. The documentation suggests considering list_delete_last() instead if the application can reverse the list order, as that operation is more efficient.

## Parameters / Member Variables
- : The PostgreSQL List from which to remove the first element. Can be NIL (empty list).

## Dependencies
- Functions called/Symbols referenced:
  - [check_list_invariants](../c/check_list_invariants.md): Validates list structure integrity
  - [list_delete_nth_cell](list_delete_nth_cell.md): Performs the actual deletion of the cell at index 0
- Called from (representative examples):
  - [gistFindPath](../g/gistFindPath.md): GiST index path finding
  - [CopyMultiInsertInfoFlush](../C/CopyMultiInsertInfoFlush.md): COPY command processing
  - [ExplainNode](../E/ExplainNode.md): Query plan explanation
  - [simplify_or_arguments](../s/simplify_or_arguments.md): Query optimization
  - [transformPLAssignStmt](../t/transformPLAssignStmt.md): PL/pgSQL statement transformation

## Notes and Other Information
- Returns NIL if the input list is empty (no error is raised)
- The function modifies the original list structure, unlike simple traversal operations
- Performance warning: O(n) complexity due to element shifting - consider using list_delete_last() for better performance if list order can be reversed
- Commonly used in parsing, optimization, and execution contexts where lists need to be consumed element by element
- The operation is atomic and maintains list structure invariants through check_list_invariants()