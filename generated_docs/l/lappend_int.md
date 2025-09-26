# lappend_int

## Location
[src/backend/nodes/list.c:357-374](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L357-L374)

## Overview
Appends an integer value to a PostgreSQL IntList data structure, returning a pointer to the modified list.

## Definition

```c
List *
lappend_int(List *list, int datum)
```
## Detailed Description
The  function is a specialized version of  designed specifically for integer lists (T_IntList). It appends an integer value to the end of an IntList, handling both empty lists (NIL) and existing lists with elements. Like , this function may or may not destructively modify the original list structure, so callers must use the returned value rather than the original list pointer.

When the input list is NIL, the function creates a new IntList with a single integer element. For existing lists, it adds a new tail cell and stores the integer value. The function includes type assertions to ensure the list is specifically an integer list and performs invariant checking.

This function is widely used throughout PostgreSQL for building lists of integer identifiers, column numbers, partition bounds, and other numeric collections.

## Parameters / Member Variables
- : The IntList to append to, or NIL to create a new integer list
- : The integer value to be appended to the list

## Dependencies
- Functions called/Symbols referenced:
  - IsIntegerList (assertion check for integer list type)
  - [new_list](../n/new_list.md) (creates new list when input is NIL, with T_IntList type)
  - [new_tail_cell](../n/new_tail_cell.md) (adds new cell to existing list)
  - llast_int (macro to access last integer element of list)
  - [check_list_invariants](../c/check_list_invariants.md) (debugging/validation function)
- Called from (representative examples):
  - [find_all_inheritors](../f/find_all_inheritors.md) (inheritance hierarchy processing)
  - [CopyGetAttnums](../C/CopyGetAttnums.md) (COPY command attribute handling)
  - [ExecInitExprRec](../E/ExecInitExprRec.md) (expression initialization)
  - [list_union_int](list_union_int.md) (integer list set operations)
  - [transformInsertStmt](../t/transformInsertStmt.md) (INSERT statement processing)

## Notes and Other Information
- Specialized for integer values only, part of PostgreSQL's typed list system
- Extensively used for column numbers, attribute lists, and partition bounds
- Must use return value as function may reallocate the list structure
- Type-safe alternative to using lappend with integer pointers
- One of the most frequently used list manipulation functions in PostgreSQL codebase