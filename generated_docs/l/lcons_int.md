# lcons_int

## Location
src/backend/nodes/list.c: 513 - 530

## Overview
Prepends an integer value to the beginning of a PostgreSQL List that specifically contains integer elements, providing type-safe list construction with "left cons" semantics.

## Definition
```c
List *lcons_int(int datum, List *list)
```

## Detailed Description
The `lcons_int` function is a type-safe variant of `lcons` specifically designed for Lists containing integer values. It implements a prepend operation that adds a new integer element at the beginning (head) of the list while enforcing type safety through `IsIntegerList` validation. The function follows the same "left cons" pattern as its generic counterpart but ensures that only integer values can be added to integer lists.

Like `lcons`, this function has O(n) time complexity proportional to the length of the list, as all existing entries must be shifted to accommodate the new head element. The function may modify the original list structure, so callers must always use the returned List pointer rather than continuing to use the original pointer.

## Parameters / Member Variables
- `datum`: The integer value to be prepended to the front of the list
- `list`: The target integer List to prepend to (can be NIL for empty list)

## Dependencies
- Functions called/Symbols referenced:
  - `IsIntegerList`: Validates that the list contains integer elements
  - `new_list`: Creates a new list structure with T_IntList type and initial capacity
  - `new_head_cell`: Internal helper function to create space for a new head element
  - `linitial_int`: Macro to access/set the first integer element of the list
  - `check_list_invariants`: Debug function to verify list structural integrity

- Called from (representative examples):
  - `ExplainOpenGroup`: Query explanation grouping operations
  - `ExplainOpenSetAsideGroup`: Explanation output management
  - `ExplainRestoreGroup`: Explanation group restoration
  - `ExplainBeginOutput`: Query plan output initialization
  - `ExecInitAgg`: Aggregate node initialization in executor
  - `forfive`: Macro for five-way list iteration

## Notes and Other Information
- Type-safe version that only works with integer lists, verified by `IsIntegerList` assertion
- Creates T_IntList type when starting from NIL, ensuring proper list type classification
- Time complexity is O(n) where n is the current list length
- Returns the modified list (may be the same object or a new one)
- Callers MUST use the return value, not the original list pointer
- Uses `linitial_int` macro for type-safe integer access instead of generic `linitial`
- Maintains list invariants through `check_list_invariants` in debug builds
- Part of PostgreSQL's type-safe list API that prevents mixing different data types in lists
- Commonly used in query explanation and execution contexts where integer lists are needed
- Follows the same destructive modification pattern as `lcons` introduced in PostgreSQL 8.0