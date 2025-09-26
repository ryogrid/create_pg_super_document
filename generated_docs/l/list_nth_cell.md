# list_nth_cell

## Location
[src/include/nodes/pg_list.h:277-287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pg_list.h#L277-L287)

## Overview
Returns the nth cell (zero-indexed) from a PostgreSQL list structure, with assertion checking to ensure valid access.

## Definition
static inline ListCell *list_nth_cell(const List *list, int n)

## Detailed Description
The list_nth_cell function provides indexed access to elements within a PostgreSQL List structure. Unlike other list access functions, this function performs strict validation using assertions to ensure the list is not NULL and the index is within valid bounds. This function is fundamental to many other list access macros and functions in PostgreSQL.

The function operates on PostgreSQL's internal List data structure, returning a pointer to the nth element in the elements array. It uses assertions rather than graceful error handling, making it suitable for internal use where the caller is expected to ensure valid parameters.

## Parameters / Member Variables
- : A const pointer to the List structure. Must not be NIL (NULL).
- : The zero-based index of the cell to retrieve. Must be >= 0 and < list->length.

## Dependencies
- Functions called/Symbols referenced: 
  - Assert (macro for bounds checking)
  - NIL (constant representing NULL list)
- Called from (representative examples):
  - [find_all_inheritors](../f/find_all_inheritors.md) (src/backend/catalog/pg_inherits.c:313)
  - [ProcedureCreate](../P/ProcedureCreate.md) (src/backend/catalog/pg_proc.c:527)
  - [build_subplan](../b/build_subplan.md) (src/backend/optimizer/plan/subselect.c:470)
  - [split_pathtarget_at_srfs](../s/split_pathtarget_at_srfs.md) (src/backend/optimizer/util/tlist.c:997, 999)
  - linitial, lsecond, lthird, lfourth (list access macros in src/include/nodes/pg_list.h)
  - [list_nth](list_nth.md), list_nth_int, list_nth_oid (value extraction functions)

## Notes and Other Information
- This function is marked as static inline for performance optimization
- Part of the PostgreSQL list manipulation API defined in src/include/nodes/pg_list.h
- Uses assertions for parameter validation rather than graceful error handling
- Serves as the foundation for many higher-level list access macros (linitial, lsecond, etc.)
- Expected to be used in contexts where bounds have already been verified
- The function will cause an assertion failure in debug builds if invalid parameters are provided
- Critical building block for PostgreSQL's list-based data structures throughout the codebase