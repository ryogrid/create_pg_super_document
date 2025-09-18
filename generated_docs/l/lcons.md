# lcons

## Location
[src/backend/nodes/list.c:495-512](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L495-L512)

## Overview
Prepends a new element to the beginning of a PostgreSQL List, functioning as a "left cons" operation that adds an element at the front while maintaining list structure.

## Definition
```c
List *lcons(void *datum, List *list)
```

## Detailed Description
The `lcons` function implements a prepend operation for PostgreSQL's generic List data structure, adding a new element at the beginning (head) of the list. The name "lcons" reflects the "left cons" operation from functional programming languages, where new elements are constructed at the front of a list structure.

This function has O(n) time complexity proportional to the length of the list, as all existing entries must be shifted to make room for the new head element. The function may or may not destructively modify the original list object, so callers must always use the returned List pointer rather than continuing to use the original pointer.

Starting from PostgreSQL 8.0, the function modifies the original list structure rather than creating a separate copy, which is an important behavioral change from earlier versions.

## Parameters / Member Variables
- `datum`: The void pointer data to be prepended to the front of the list
- `list`: The target List to prepend to (can be NIL for empty list)

## Dependencies
- Functions called/Symbols referenced:
  - `IsPointerList`: Validates that the list contains pointer elements
  - [new_list](../n/new_list.md): Creates a new list structure with specified type and initial capacity
  - [new_head_cell](../n/new_head_cell.md): Internal helper function to create space for a new head element
  - `linitial`: Macro to access/set the first element of the list
  - [check_list_invariants](../c/check_list_invariants.md): Debug function to verify list structural integrity

- Called from (representative examples):
  - [gistFindPath](../g/gistFindPath.md): GiST index path finding operations
  - [find_expr_references_walker](../f/find_expr_references_walker.md): Expression dependency analysis
  - [ExplainNode](../E/ExplainNode.md): Query plan explanation functionality
  - [sort_inner_and_outer](../s/sort_inner_and_outer.md): Join path optimization
  - `transformCaseExpr`: CASE expression parsing
  - [RewriteQuery](../R/RewriteQuery.md): Query rewrite operations
  - Multiple other optimizer and parser functions

## Notes and Other Information
- Only works with pointer-based lists, verified by `IsPointerList` assertion
- Time complexity is O(n) where n is the current list length
- Returns the modified list (may be the same object or a new one)
- Callers MUST use the return value, not the original list pointer
- Behavioral change from pre-8.0: now modifies original list destructively
- Handles NIL (empty) lists by creating a new single-element list
- Maintains list invariants through `check_list_invariants` in debug builds
- Widely used throughout PostgreSQL for building lists incrementally from front to back
- Essential building block for many list construction patterns in the codebase